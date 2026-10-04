package postgres

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"os"
	"time"
)

type QueuePolicy struct {
	Queue            string `json:"queue"`
	ConcurrencyLimit *int   `json:"concurrency_limit,omitempty"`
	RatePerSecond    *int   `json:"rate_per_second,omitempty"`
	Burst            *int   `json:"burst,omitempty"`
}

type QueueInfo struct {
	QueuePolicy
	Queued       int `json:"queued"`
	Scheduled    int `json:"scheduled"`
	Running      int `json:"running"`
	Expired      int `json:"expired"`
	DeadLettered int `json:"dead_lettered"`
}

type WorkerInfo struct {
	ID              string     `json:"id"`
	Hostname        string     `json:"hostname"`
	ProcessID       int        `json:"process_id"`
	StartedAt       time.Time  `json:"started_at"`
	LastHeartbeatAt time.Time  `json:"last_heartbeat_at"`
	StoppedAt       *time.Time `json:"stopped_at,omitempty"`
	Healthy         bool       `json:"healthy"`
	ActiveJobs      int        `json:"active_jobs"`
}

func queueAvailable(ctx context.Context, tx *sql.Tx, queue string) (bool, error) {
	var concurrency, rate, burst sql.NullInt64
	var tokens float64
	var refilledAt, dbNow time.Time
	err := tx.QueryRowContext(ctx, `SELECT concurrency_limit, rate_per_second,
	    burst, tokens, refilled_at, clock_timestamp()
	    FROM queue_policies WHERE queue_name = $1 FOR UPDATE`, queue).
		Scan(&concurrency, &rate, &burst, &tokens, &refilledAt, &dbNow)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if concurrency.Valid {
		var active int
		err := tx.QueryRowContext(ctx, `SELECT count(*) FROM jobs
		    WHERE queue_name = $1 AND status = 'running'
		      AND lease_expires_at > clock_timestamp()`, queue).Scan(&active)
		if err != nil {
			return false, err
		}
		if active >= int(concurrency.Int64) {
			return false, nil
		}
	}
	if !rate.Valid {
		return true, nil
	}
	elapsed := math.Max(0, dbNow.Sub(refilledAt).Seconds())
	tokens = math.Min(float64(burst.Int64), tokens+elapsed*float64(rate.Int64))
	available := tokens >= 1
	if available {
		tokens--
	}
	if _, err := tx.ExecContext(ctx, `UPDATE queue_policies
	    SET tokens = $2, refilled_at = $3 WHERE queue_name = $1`,
		queue, tokens, dbNow); err != nil {
		return false, err
	}
	return available, nil
}

func (s Store) SetQueuePolicy(ctx context.Context, policy QueuePolicy) error {
	if policy.Queue == "" {
		return errors.New("queue name is required")
	}
	if policy.ConcurrencyLimit != nil && *policy.ConcurrencyLimit < 1 {
		return errors.New("concurrency limit must be positive")
	}
	if (policy.RatePerSecond == nil) != (policy.Burst == nil) {
		return errors.New("rate and burst must be set together")
	}
	if policy.RatePerSecond != nil && (*policy.RatePerSecond < 1 || *policy.Burst < 1) {
		return errors.New("rate and burst must be positive")
	}
	_, err := s.DB.ExecContext(ctx, `INSERT INTO queue_policies
	    (queue_name, concurrency_limit, rate_per_second, burst, tokens, refilled_at)
	    VALUES ($1, $2, $3, $4, COALESCE($4, 0), now())
	    ON CONFLICT (queue_name) DO UPDATE SET
	      concurrency_limit = EXCLUDED.concurrency_limit,
	      rate_per_second = EXCLUDED.rate_per_second,
	      burst = EXCLUDED.burst,
	      tokens = EXCLUDED.tokens,
	      refilled_at = now()`, policy.Queue, policy.ConcurrencyLimit,
		policy.RatePerSecond, policy.Burst)
	return err
}

func (s Store) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	rows, err := s.DB.QueryContext(ctx, `WITH names AS (
	    SELECT queue_name FROM jobs UNION SELECT queue_name FROM queue_policies
	)
	SELECT n.queue_name,
	    count(j.id) FILTER (WHERE j.status = 'queued')::int,
	    count(j.id) FILTER (WHERE j.status = 'scheduled')::int,
	    count(j.id) FILTER (WHERE j.status = 'running' AND j.lease_expires_at > now())::int,
	    count(j.id) FILTER (WHERE j.status = 'running' AND j.lease_expires_at <= now())::int,
	    count(j.id) FILTER (WHERE j.status = 'dead_lettered')::int,
	    p.concurrency_limit, p.rate_per_second, p.burst
	FROM names n
	LEFT JOIN jobs j ON j.queue_name = n.queue_name
	LEFT JOIN queue_policies p ON p.queue_name = n.queue_name
	GROUP BY n.queue_name, p.concurrency_limit, p.rate_per_second, p.burst
	ORDER BY n.queue_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	queues := make([]QueueInfo, 0)
	for rows.Next() {
		var q QueueInfo
		var concurrency, rate, burst sql.NullInt64
		if err := rows.Scan(&q.Queue, &q.Queued, &q.Scheduled, &q.Running,
			&q.Expired, &q.DeadLettered, &concurrency, &rate, &burst); err != nil {
			return nil, err
		}
		if concurrency.Valid {
			value := int(concurrency.Int64)
			q.ConcurrencyLimit = &value
		}
		if rate.Valid {
			value := int(rate.Int64)
			q.RatePerSecond = &value
		}
		if burst.Valid {
			value := int(burst.Int64)
			q.Burst = &value
		}
		queues = append(queues, q)
	}
	return queues, rows.Err()
}

func (s Store) RegisterWorker(ctx context.Context, id string) error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, `INSERT INTO workers
	    (worker_id, hostname, process_id) VALUES ($1, $2, $3)
	    ON CONFLICT (worker_id) DO UPDATE SET
	      hostname = EXCLUDED.hostname, process_id = EXCLUDED.process_id,
	      started_at = now(), last_heartbeat_at = now(), stopped_at = NULL`,
		id, hostname, os.Getpid())
	return err
}

func (s Store) Heartbeat(ctx context.Context, id string) error {
	result, err := s.DB.ExecContext(ctx, `UPDATE workers
	    SET last_heartbeat_at = now() WHERE worker_id = $1 AND stopped_at IS NULL`, id)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s Store) StopWorker(ctx context.Context, id string) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE workers
	    SET stopped_at = now(), last_heartbeat_at = now() WHERE worker_id = $1`, id)
	return err
}

func (s Store) ListWorkers(ctx context.Context) ([]WorkerInfo, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT w.worker_id, w.hostname, w.process_id,
	    w.started_at, w.last_heartbeat_at, w.stopped_at,
	    (w.stopped_at IS NULL AND w.last_heartbeat_at > now() - interval '10 seconds'),
	    count(j.id)::int
	FROM workers w LEFT JOIN jobs j ON j.lease_owner = w.worker_id
	    AND j.status = 'running' AND j.lease_expires_at > now()
	GROUP BY w.worker_id
	ORDER BY w.last_heartbeat_at DESC LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	workers := make([]WorkerInfo, 0)
	for rows.Next() {
		var worker WorkerInfo
		var stopped sql.NullTime
		if err := rows.Scan(&worker.ID, &worker.Hostname, &worker.ProcessID,
			&worker.StartedAt, &worker.LastHeartbeatAt, &stopped,
			&worker.Healthy, &worker.ActiveJobs); err != nil {
			return nil, err
		}
		if stopped.Valid {
			worker.StoppedAt = &stopped.Time
		}
		workers = append(workers, worker)
	}
	return workers, rows.Err()
}
