package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"distributedjobplatform/internal/executor"
	"distributedjobplatform/internal/observability"
	"distributedjobplatform/internal/retry"
	"distributedjobplatform/internal/storage/postgres"
)

type Store interface {
	RegisterWorker(context.Context, string) error
	Heartbeat(context.Context, string) error
	StopWorker(context.Context, string) error
	Claim(context.Context, string, time.Duration) (postgres.Job, error)
	Renew(context.Context, postgres.Job, time.Duration) error
	Finish(context.Context, postgres.Job, json.RawMessage, error, bool, time.Duration) (postgres.Job, error)
}

type Worker struct {
	Store         Store
	Log           *slog.Logger
	WorkerID      string
	PollInterval  time.Duration
	ClaimTimeout  time.Duration
	LeaseDuration time.Duration
	Metrics       *observability.Metrics
	Execute       func(context.Context, string, json.RawMessage, int) (json.RawMessage, error)
}

func (w Worker) Run(ctx context.Context) {
	interval := w.PollInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	logger := w.Log
	if logger == nil {
		logger = slog.Default()
	}
	if w.WorkerID == "" {
		logger.Error("worker ID is required")
		return
	}
	registerCtx, registerCancel := context.WithTimeout(ctx, 5*time.Second)
	err := w.Store.RegisterWorker(registerCtx, w.WorkerID)
	registerCancel()
	if err != nil {
		logger.Error("worker registration failed", "worker_id", w.WorkerID, "error", err)
		return
	}
	heartbeatStop := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatStop:
				return
			case <-ticker.C:
				heartbeatCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := w.Store.Heartbeat(heartbeatCtx, w.WorkerID)
				cancel()
				if err != nil {
					logger.Error("worker heartbeat failed", "worker_id", w.WorkerID, "error", err)
				}
			}
		}
	}()
	defer func() {
		close(heartbeatStop)
		<-heartbeatDone
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := w.Store.StopWorker(stopCtx, w.WorkerID); err != nil {
			logger.Error("worker stop update failed", "worker_id", w.WorkerID, "error", err)
		}
	}()
	leaseDuration := w.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	claimTimeout := w.ClaimTimeout
	if claimTimeout <= 0 {
		claimTimeout = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		claimCtx, cancel := context.WithTimeout(ctx, claimTimeout)
		pollStarted := time.Now()
		job, err := w.Store.Claim(claimCtx, w.WorkerID, leaseDuration)
		cancel()
		if w.Metrics != nil {
			w.Metrics.ObservePoll(time.Since(pollStarted).Seconds())
		}
		if err == nil {
			logger.Info("job claimed", "job_id", job.ID, "worker_id", w.WorkerID,
				"queue", job.Queue, "job_type", job.Type, "attempt", job.AttemptCount,
				"lease_token", job.LeaseToken, "queue_wait_seconds", job.QueueWaitSeconds)
			w.runJob(job, leaseDuration, logger)
			continue
		}
		if !errors.Is(err, postgres.ErrNotFound) && !errors.Is(err, context.Canceled) {
			logger.Error("job claim failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w Worker) runJob(job postgres.Job, leaseDuration time.Duration, logger *slog.Logger) {
	started := time.Now()
	execCtx, cancelExec := context.WithTimeout(context.Background(), 15*time.Second)
	stopRenew := make(chan struct{})
	renewDone := make(chan struct{})
	renewErrors := make(chan error, 1)
	go func() {
		defer close(renewDone)
		interval := leaseDuration / 3
		if interval > time.Second {
			interval = time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopRenew:
				return
			case <-ticker.C:
				renewCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := w.Store.Renew(renewCtx, job, leaseDuration)
				cancel()
				if err != nil {
					renewErrors <- err
					cancelExec()
					return
				}
			}
		}
	}()
	executionStarted := time.Now()
	execute := w.Execute
	if execute == nil {
		execute = executor.Execute
	}
	result, runErr := runExecutor(execCtx, execute, job)
	if w.Metrics != nil {
		w.Metrics.ObserveExecution(time.Since(executionStarted).Seconds())
	}
	close(stopRenew)
	<-renewDone
	cancelExec()
	select {
	case err := <-renewErrors:
		if !errors.Is(err, postgres.ErrCancelRequested) {
			logger.Error("lease renewal failed", "job_id", job.ID,
				"worker_id", w.WorkerID, "lease_token", job.LeaseToken, "error", err)
			return
		}
	default:
	}
	retryable := executor.IsRetryable(runErr)
	var delay time.Duration
	if retryable {
		delay = retry.Delay(job.AttemptCount)
	}
	finishCtx, finishCancel := context.WithTimeout(context.Background(), 5*time.Second)
	finished, finishErr := w.Store.Finish(finishCtx, job, result, runErr, retryable, delay)
	finishCancel()
	if finishErr != nil {
		logger.Error("job completion failed", "job_id", job.ID, "worker_id", w.WorkerID,
			"queue", job.Queue, "job_type", job.Type, "attempt", job.AttemptCount,
			"lease_token", job.LeaseToken, "error", finishErr)
	} else {
		logger.Info("job finished", "job_id", job.ID, "worker_id", w.WorkerID,
			"queue", job.Queue, "job_type", job.Type, "attempt", job.AttemptCount,
			"lease_token", job.LeaseToken,
			"status", finished.Status, "duration", time.Since(started))
	}
}

func runExecutor(ctx context.Context, execute func(context.Context, string, json.RawMessage, int) (json.RawMessage, error), job postgres.Job) (result json.RawMessage, err error) {
	defer func() {
		if recover() != nil {
			result = nil
			err = errors.New("executor panic")
		}
	}()
	return execute(ctx, job.Type, job.Payload, job.AttemptCount)
}
