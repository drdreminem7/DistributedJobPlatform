package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"distributedjobplatform/internal/observability"
)

type Job struct {
	ID                string          `json:"id"`
	Queue             string          `json:"queue"`
	Type              string          `json:"type"`
	Payload           json.RawMessage `json:"payload"`
	Status            string          `json:"status"`
	Result            json.RawMessage `json:"result,omitempty"`
	AttemptCount      int             `json:"attempt_count"`
	LastError         *string         `json:"last_error,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
	StartedAt         *time.Time      `json:"started_at,omitempty"`
	CompletedAt       *time.Time      `json:"completed_at,omitempty"`
	LeaseOwner        string          `json:"-"`
	LeaseToken        int64           `json:"-"`
	LeaseExpiresAt    *time.Time      `json:"-"`
	Priority          int             `json:"priority"`
	ScheduledAt       time.Time       `json:"scheduled_at"`
	MaxAttempts       int             `json:"max_attempts"`
	IdempotencyKey    string          `json:"-"`
	RequestHash       string          `json:"-"`
	CancelRequestedAt *time.Time      `json:"cancel_requested_at,omitempty"`
	ReplayCount       int             `json:"replay_count"`
	QueueWaitSeconds  float64         `json:"-"`
}

type CreateParams struct {
	Queue          string
	Type           string
	Payload        json.RawMessage
	Priority       int
	ScheduledAt    *time.Time
	MaxAttempts    int
	IdempotencyKey string
	RequestHash    string
}

type Attempt struct {
	AttemptNumber int        `json:"attempt_number"`
	WorkerID      string     `json:"worker_id"`
	LeaseToken    int64      `json:"lease_token"`
	Status        string     `json:"status"`
	StartedAt     time.Time  `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
	ErrorMessage  *string    `json:"error_message,omitempty"`
}

type Store struct {
	DB      *sql.DB
	Metrics *observability.Metrics
}

var ErrNotFound = errors.New("job not found")
var ErrLeaseLost = errors.New("current lease is not owned by this worker")
var ErrCancelRequested = errors.New("job cancellation requested")
var ErrIdempotencyConflict = errors.New("idempotency key was used for a different job")
var ErrNotCancellable = errors.New("job is already terminal")
var ErrNotReplayable = errors.New("job is not dead-lettered")

const columns = `id, queue_name, job_type, payload, status, result, attempt_count,
    last_error, created_at, updated_at, started_at, completed_at,
    lease_owner, lease_token, lease_expires_at, priority, scheduled_at,
    max_attempts, idempotency_key, request_hash, cancel_requested_at, replay_count`

func scanJob(row interface{ Scan(...any) error }) (Job, error) {
	var job Job
	var payload, result []byte
	var lastError sql.NullString
	var started, completed sql.NullTime
	var leaseOwner sql.NullString
	var leaseExpires sql.NullTime
	var idempotencyKey, requestHash sql.NullString
	var cancelRequested sql.NullTime
	err := row.Scan(&job.ID, &job.Queue, &job.Type, &payload, &job.Status,
		&result, &job.AttemptCount, &lastError, &job.CreatedAt, &job.UpdatedAt,
		&started, &completed, &leaseOwner, &job.LeaseToken, &leaseExpires,
		&job.Priority, &job.ScheduledAt, &job.MaxAttempts, &idempotencyKey,
		&requestHash, &cancelRequested, &job.ReplayCount)
	if err != nil {
		return Job{}, err
	}
	job.Payload = payload
	job.Result = result
	if lastError.Valid {
		job.LastError = &lastError.String
	}
	if started.Valid {
		job.StartedAt = &started.Time
	}
	if completed.Valid {
		job.CompletedAt = &completed.Time
	}
	if leaseOwner.Valid {
		job.LeaseOwner = leaseOwner.String
	}
	if leaseExpires.Valid {
		job.LeaseExpiresAt = &leaseExpires.Time
	}
	if idempotencyKey.Valid {
		job.IdempotencyKey = idempotencyKey.String
	}
	if requestHash.Valid {
		job.RequestHash = requestHash.String
	}
	if cancelRequested.Valid {
		job.CancelRequestedAt = &cancelRequested.Time
	}
	return job, nil
}

func (s Store) Create(ctx context.Context, queue, jobType string, payload json.RawMessage) (Job, error) {
	job, _, err := s.CreateWithOptions(ctx, CreateParams{
		Queue: queue, Type: jobType, Payload: payload, MaxAttempts: 3,
	})
	return job, err
}

func (s Store) CreateWithOptions(ctx context.Context, p CreateParams) (Job, bool, error) {
	if _, err := s.DB.ExecContext(ctx, `INSERT INTO queue_policies (queue_name)
	    VALUES ($1) ON CONFLICT DO NOTHING`, p.Queue); err != nil {
		return Job{}, false, err
	}
	var scheduled, key, hash any
	if p.ScheduledAt != nil {
		scheduled = p.ScheduledAt.UTC()
	}
	if p.IdempotencyKey != "" {
		key, hash = p.IdempotencyKey, p.RequestHash
	}
	row := s.DB.QueryRowContext(ctx, `INSERT INTO jobs
	    (queue_name, job_type, payload, priority, scheduled_at, max_attempts,
	     idempotency_key, request_hash, status)
	    VALUES ($1, $2, $3, $4, COALESCE($5::timestamptz, now()), $6,
	            $7, $8, CASE WHEN $5::timestamptz > now() THEN 'scheduled' ELSE 'queued' END)
	    ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
	    RETURNING `+columns, p.Queue, p.Type, string(p.Payload), p.Priority,
		scheduled, p.MaxAttempts, key, hash)
	job, err := scanJob(row)
	if err == nil {
		if s.Metrics != nil {
			s.Metrics.Enqueued()
		}
		return job, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, err
	}
	job, err = scanJob(s.DB.QueryRowContext(ctx, `SELECT `+columns+` FROM jobs WHERE idempotency_key = $1`, p.IdempotencyKey))
	if err != nil {
		return Job{}, false, err
	}
	if job.RequestHash != p.RequestHash {
		return Job{}, false, ErrIdempotencyConflict
	}
	return job, false, nil
}

func (s Store) Get(ctx context.Context, id string) (Job, error) {
	job, err := scanJob(s.DB.QueryRowContext(ctx, `SELECT `+columns+` FROM jobs WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return job, err
}

func (s Store) List(ctx context.Context, limit int) ([]Job, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT `+columns+` FROM jobs
        ORDER BY created_at DESC, id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]Job, 0)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s Store) Claim(ctx context.Context, owner string, duration time.Duration) (Job, error) {
	if owner == "" || duration < time.Millisecond {
		return Job{}, errors.New("lease owner and positive duration are required")
	}
	excluded := make([]string, 0)
	for {
		tx, err := s.DB.BeginTx(ctx, nil)
		if err != nil {
			return Job{}, err
		}
		expired, dead, cancelled, err := finishExpired(ctx, tx)
		if err != nil {
			tx.Rollback()
			return Job{}, err
		}
		var id, queue, previousStatus string
		var readyAt time.Time
		err = tx.QueryRowContext(ctx, `SELECT id, queue_name, status,
		    CASE WHEN status = 'running' THEN lease_expires_at
		         ELSE greatest(scheduled_at, created_at) END FROM jobs
		    WHERE status IN ('queued', 'scheduled', 'running')
		      AND (status = 'queued'
		       OR (status = 'scheduled' AND scheduled_at <= clock_timestamp())
		       OR (status = 'running' AND lease_expires_at <= clock_timestamp()
		           AND attempt_count < max_attempts AND cancel_requested_at IS NULL))
		      AND NOT (queue_name = ANY($1::text[]))
		    ORDER BY priority * 60 - extract(epoch FROM
		        ((CASE WHEN status = 'running' THEN lease_expires_at
		            ELSE greatest(scheduled_at, created_at) END) AT TIME ZONE 'UTC')) DESC,
		        CASE WHEN status = 'running' THEN lease_expires_at
		            ELSE greatest(scheduled_at, created_at) END ASC, id ASC
		    FOR UPDATE SKIP LOCKED LIMIT 1`, excluded).Scan(&id, &queue, &previousStatus, &readyAt)
		if errors.Is(err, sql.ErrNoRows) {
			if err := tx.Commit(); err != nil {
				return Job{}, err
			}
			if s.Metrics != nil {
				s.Metrics.LeaseExpired(expired)
				s.Metrics.DeadLettered(dead)
				s.Metrics.CancelledN(cancelled)
			}
			return Job{}, ErrNotFound
		}
		if err != nil {
			tx.Rollback()
			return Job{}, err
		}
		available, err := queueAvailable(ctx, tx, queue)
		if err != nil {
			tx.Rollback()
			return Job{}, err
		}
		if !available {
			if err := tx.Commit(); err != nil {
				return Job{}, err
			}
			if s.Metrics != nil {
				s.Metrics.LeaseExpired(expired)
				s.Metrics.DeadLettered(dead)
				s.Metrics.CancelledN(cancelled)
			}
			excluded = append(excluded, queue)
			continue
		}
		job, err := scanJob(tx.QueryRowContext(ctx, `UPDATE jobs
		    SET status = 'running', lease_owner = $2,
		        lease_token = lease_token + 1,
		        lease_expires_at = clock_timestamp() + $3::bigint * interval '1 millisecond',
		        attempt_count = attempt_count + 1,
		        started_at = clock_timestamp(), updated_at = clock_timestamp()
		    WHERE id = $1 RETURNING `+columns, id, owner, duration.Milliseconds()))
		if err != nil {
			tx.Rollback()
			return Job{}, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE job_attempts
		    SET status = 'lease_expired', finished_at = now()
		    WHERE job_id = $1 AND status = 'running'`, job.ID); err != nil {
			tx.Rollback()
			return Job{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO job_attempts
		    (job_id, attempt_number, worker_id, lease_token, status, started_at)
		    VALUES ($1, $2, $3, $4, 'running', $5)`, job.ID, job.AttemptCount,
			job.LeaseOwner, job.LeaseToken, job.StartedAt); err != nil {
			tx.Rollback()
			return Job{}, err
		}
		if err := tx.Commit(); err != nil {
			return Job{}, err
		}
		job.QueueWaitSeconds = max(0, job.StartedAt.Sub(readyAt).Seconds())
		if s.Metrics != nil {
			s.Metrics.LeaseExpired(expired)
			s.Metrics.DeadLettered(dead)
			s.Metrics.CancelledN(cancelled)
			s.Metrics.Started(job.QueueWaitSeconds)
			if previousStatus == "running" {
				s.Metrics.LeaseExpired(1)
			}
		}
		return job, nil
	}
}

func finishExpired(ctx context.Context, tx *sql.Tx) (int, int, int, error) {
	rows, err := tx.QueryContext(ctx, `WITH expired AS (
	    SELECT id FROM jobs WHERE status = 'running' AND lease_expires_at <= now()
	      AND (attempt_count >= max_attempts OR cancel_requested_at IS NOT NULL)
	    ORDER BY lease_expires_at ASC FOR UPDATE SKIP LOCKED LIMIT 32
	)
	UPDATE jobs AS j SET
	    status = CASE WHEN j.cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'dead_lettered' END,
	    last_error = CASE WHEN j.cancel_requested_at IS NOT NULL
	        THEN 'lease expired after cancellation request' ELSE 'lease expired after final attempt' END,
	    completed_at = now(), updated_at = now(),
	    lease_owner = NULL, lease_expires_at = NULL
	FROM expired WHERE j.id = expired.id RETURNING j.id, j.status`)
	if err != nil {
		return 0, 0, 0, err
	}
	type outcome struct{ id, status string }
	jobs := make([]outcome, 0)
	dead, cancelled := 0, 0
	for rows.Next() {
		var job outcome
		if err := rows.Scan(&job.id, &job.status); err != nil {
			rows.Close()
			return 0, 0, 0, err
		}
		jobs = append(jobs, job)
		if job.status == "cancelled" {
			cancelled++
		} else {
			dead++
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, 0, 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, 0, 0, err
	}
	for _, job := range jobs {
		message := "lease expired after final attempt"
		if job.status == "cancelled" {
			message = "lease expired after cancellation request"
		}
		if _, err := tx.ExecContext(ctx, `UPDATE job_attempts
		    SET status = 'lease_expired', finished_at = now(),
		        error_message = $2
		    WHERE job_id = $1 AND status = 'running'`, job.id, message); err != nil {
			return 0, 0, 0, err
		}
	}
	return len(jobs), dead, cancelled, nil
}

func (s Store) Renew(ctx context.Context, job Job, duration time.Duration) error {
	if duration < time.Millisecond {
		return errors.New("positive lease duration is required")
	}
	var cancelRequested sql.NullTime
	err := s.DB.QueryRowContext(ctx, `UPDATE jobs
        SET lease_expires_at = now() + $4::bigint * interval '1 millisecond',
            updated_at = now()
        WHERE id = $1 AND status = 'running' AND lease_owner = $2
	      AND lease_token = $3 AND lease_expires_at > now()
	    RETURNING cancel_requested_at`, job.ID, job.LeaseOwner, job.LeaseToken,
		duration.Milliseconds()).Scan(&cancelRequested)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("renew lease: %w", err)
	}
	if cancelRequested.Valid {
		return ErrCancelRequested
	}
	return nil
}

func (s Store) Complete(ctx context.Context, job Job, result json.RawMessage, runErr error) error {
	_, err := s.Finish(ctx, job, result, runErr, false, 0)
	return err
}

func (s Store) Finish(ctx context.Context, job Job, result json.RawMessage, runErr error, retryable bool, delay time.Duration) (Job, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback()
	var cancelRequested sql.NullTime
	var attempts, maxAttempts int
	err = tx.QueryRowContext(ctx, `SELECT cancel_requested_at, attempt_count, max_attempts
	    FROM jobs WHERE id = $1 AND status = 'running' AND lease_owner = $2
	      AND lease_token = $3 AND lease_expires_at > clock_timestamp() FOR UPDATE`,
		job.ID, job.LeaseOwner, job.LeaseToken).Scan(&cancelRequested, &attempts, &maxAttempts)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrLeaseLost
	}
	if err != nil {
		return Job{}, err
	}
	status := "succeeded"
	if cancelRequested.Valid {
		status = "cancelled"
	} else if runErr != nil && retryable && attempts < maxAttempts {
		status = "scheduled"
	} else if runErr != nil && retryable {
		status = "dead_lettered"
	} else if runErr != nil {
		status = "failed"
	}
	var resultValue, lastError any
	if status == "succeeded" && result != nil {
		resultValue = string(result)
	}
	if runErr != nil && status != "cancelled" {
		lastError = runErr.Error()
	}
	finished, err := scanJob(tx.QueryRowContext(ctx, `UPDATE jobs
	    SET status = $2, result = $3::jsonb, last_error = $4,
	        scheduled_at = CASE WHEN $2 = 'scheduled'
	            THEN now() + $5::bigint * interval '1 millisecond' ELSE scheduled_at END,
	        completed_at = CASE WHEN $2 = 'scheduled' THEN NULL ELSE now() END,
	        updated_at = now(), lease_owner = NULL, lease_expires_at = NULL
	    WHERE id = $1 AND status = 'running' AND lease_owner = $6
	      AND lease_token = $7 AND lease_expires_at > clock_timestamp()
	    RETURNING `+columns, job.ID, status, resultValue, lastError,
		delay.Milliseconds(), job.LeaseOwner, job.LeaseToken))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrLeaseLost
	}
	if err != nil {
		return Job{}, err
	}
	attemptStatus := status
	if status == "scheduled" {
		attemptStatus = "retry_scheduled"
	}
	res, err := tx.ExecContext(ctx, `UPDATE job_attempts
	    SET status = $3, finished_at = now(), error_message = $4
	    WHERE job_id = $1 AND attempt_number = $2 AND lease_token = $5
	      AND status = 'running'`, job.ID, attempts, attemptStatus, lastError, job.LeaseToken)
	if err != nil {
		return Job{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return Job{}, err
	}
	if n != 1 {
		return Job{}, errors.New("current attempt was not recorded")
	}
	if err := tx.Commit(); err != nil {
		return Job{}, err
	}
	if s.Metrics != nil {
		s.Metrics.Finished(status)
	}
	return finished, nil
}

func (s Store) Cancel(ctx context.Context, id string) (Job, error) {
	job, err := scanJob(s.DB.QueryRowContext(ctx, `UPDATE jobs
	    SET status = CASE WHEN status = 'running' THEN status ELSE 'cancelled' END,
	        cancel_requested_at = COALESCE(cancel_requested_at, now()),
	        completed_at = CASE WHEN status = 'running' THEN completed_at ELSE now() END,
	        updated_at = now()
	    WHERE id = $1 AND status IN ('queued', 'scheduled', 'running')
	    RETURNING `+columns, id))
	if err == nil {
		if job.Status == "cancelled" && s.Metrics != nil {
			s.Metrics.Cancelled()
		}
		return job, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Job{}, err
	}
	job, err = s.Get(ctx, id)
	if err != nil {
		return Job{}, err
	}
	if job.Status == "cancelled" {
		return job, nil
	}
	return Job{}, ErrNotCancellable
}

func (s Store) Replay(ctx context.Context, id string, additionalAttempts int) (Job, error) {
	if additionalAttempts < 1 || additionalAttempts > 20 {
		return Job{}, errors.New("additional attempts must be between 1 and 20")
	}
	job, err := scanJob(s.DB.QueryRowContext(ctx, `UPDATE jobs
	    SET status = 'queued', max_attempts = attempt_count + $2,
	        replay_count = replay_count + 1, scheduled_at = now(),
	        completed_at = NULL, cancel_requested_at = NULL,
	        result = NULL, updated_at = now()
	    WHERE id = $1 AND status = 'dead_lettered'
	    RETURNING `+columns, id, additionalAttempts))
	if err == nil {
		return job, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Job{}, err
	}
	if _, err := s.Get(ctx, id); err != nil {
		return Job{}, err
	}
	return Job{}, ErrNotReplayable
}

func (s Store) Attempts(ctx context.Context, id string) ([]Attempt, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT attempt_number, worker_id, lease_token,
	    status, started_at, finished_at, error_message FROM job_attempts
	    WHERE job_id = $1 ORDER BY attempt_number DESC`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	attempts := make([]Attempt, 0)
	for rows.Next() {
		var attempt Attempt
		var finished sql.NullTime
		var message sql.NullString
		if err := rows.Scan(&attempt.AttemptNumber, &attempt.WorkerID,
			&attempt.LeaseToken, &attempt.Status, &attempt.StartedAt,
			&finished, &message); err != nil {
			return nil, err
		}
		if finished.Valid {
			attempt.FinishedAt = &finished.Time
		}
		if message.Valid {
			attempt.ErrorMessage = &message.String
		}
		attempts = append(attempts, attempt)
	}
	return attempts, rows.Err()
}
