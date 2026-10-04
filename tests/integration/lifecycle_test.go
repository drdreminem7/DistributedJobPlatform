package integration

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"distributedjobplatform/internal/api"
	"distributedjobplatform/internal/storage/postgres"
	"distributedjobplatform/internal/worker"
)

func waitStatus(t *testing.T, store postgres.Store, id, wanted string, timeout time.Duration) postgres.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := store.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status == wanted {
			return job
		}
		time.Sleep(10 * time.Millisecond)
	}
	job, _ := store.Get(context.Background(), id)
	t.Fatalf("job did not reach %s: %+v", wanted, job)
	return postgres.Job{}
}

func startWorker(t *testing.T, store postgres.Store, id string) {
	t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		(worker.Worker{Store: store, WorkerID: id, PollInterval: 10 * time.Millisecond,
			Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Run(ctx)
	}()
	t.Cleanup(func() { stop(); <-done })
}

func TestIdempotentSubmission(t *testing.T) {
	store := leaseStore(t)
	handler := (api.Server{Store: store}).Handler()
	body := `{"idempotency_key":"client-42","type":"checksum","payload":{"data":"abc"}}`
	const submissions = 12
	var wg sync.WaitGroup
	results := make(chan *httptest.ResponseRecorder, submissions)
	for i := 0; i < submissions; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			results <- w
		}()
	}
	wg.Wait()
	close(results)
	ids := make(map[string]bool)
	created := 0
	for response := range results {
		if response.Code == http.StatusCreated {
			created++
		} else if response.Code != http.StatusOK {
			t.Fatalf("unexpected duplicate response: %d %s", response.Code, response.Body.String())
		}
		var job postgres.Job
		if err := json.Unmarshal(response.Body.Bytes(), &job); err != nil {
			t.Fatal(err)
		}
		ids[job.ID] = true
	}
	if created != 1 || len(ids) != 1 {
		t.Fatalf("created %d rows with %d IDs", created, len(ids))
	}
	changed := request(t, handler, http.MethodPost, "/v1/jobs", `{"idempotency_key":"client-42","type":"checksum","payload":{"data":"different"}}`)
	if changed.Code != http.StatusConflict {
		t.Fatalf("changed request accepted: %d", changed.Code)
	}
}

func TestRetryAndDeadLetterReplay(t *testing.T) {
	store := leaseStore(t)
	handler := (api.Server{Store: store}).Handler()
	startWorker(t, store, "retry-worker")
	created := request(t, handler, http.MethodPost, "/v1/jobs", `{"type":"unstable","payload":{"fail_until_attempt":2},"max_attempts":3}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", created.Code, created.Body.String())
	}
	var job postgres.Job
	if err := json.Unmarshal(created.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	final := waitStatus(t, store, job.ID, "succeeded", 5*time.Second)
	if final.AttemptCount != 3 {
		t.Fatalf("expected three attempts: %+v", final)
	}
	attempts, err := store.Attempts(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 3 || attempts[0].Status != "succeeded" || attempts[1].Status != "retry_scheduled" || attempts[2].Status != "retry_scheduled" {
		t.Fatalf("incorrect retry history: %+v", attempts)
	}
	dead := request(t, handler, http.MethodPost, "/v1/jobs", `{"type":"unstable","payload":{"fail_until_attempt":2},"max_attempts":1}`)
	if dead.Code != http.StatusCreated {
		t.Fatalf("create dead-letter candidate: %d", dead.Code)
	}
	if err := json.Unmarshal(dead.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	deadJob := waitStatus(t, store, job.ID, "dead_lettered", 3*time.Second)
	if deadJob.AttemptCount != 1 {
		t.Fatalf("expected one failed attempt: %+v", deadJob)
	}
	replayed := request(t, handler, http.MethodPost, "/v1/jobs/"+job.ID+"/replay", `{"additional_attempts":2}`)
	if replayed.Code != http.StatusOK {
		t.Fatalf("replay: %d %s", replayed.Code, replayed.Body.String())
	}
	final = waitStatus(t, store, job.ID, "succeeded", 5*time.Second)
	if final.AttemptCount != 3 || final.ReplayCount != 1 || final.MaxAttempts != 3 {
		t.Fatalf("incorrect replay result: %+v", final)
	}
	attempts, err = store.Attempts(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 3 || attempts[2].Status != "dead_lettered" || attempts[2].ErrorMessage == nil {
		t.Fatalf("dead-letter history was not preserved: %+v", attempts)
	}
}

func TestScheduledPriorityAndCancellation(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	future := time.Now().Add(time.Hour)
	low, _, err := store.CreateWithOptions(ctx, postgres.CreateParams{
		Queue: "default", Type: "checksum", Payload: json.RawMessage(`{"data":"low"}`),
		Priority: 1, ScheduledAt: &future, MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	high, _, err := store.CreateWithOptions(ctx, postgres.CreateParams{
		Queue: "default", Type: "checksum", Payload: json.RawMessage(`{"data":"high"}`),
		Priority: 90, ScheduledAt: &future, MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if low.Status != "scheduled" || high.Status != "scheduled" {
		t.Fatal("future jobs were not scheduled")
	}
	if _, err := store.Claim(ctx, "worker", time.Minute); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("future job was claimable: %v", err)
	}
	if _, err := store.DB.ExecContext(ctx, `UPDATE jobs SET scheduled_at = now() - interval '1 second' WHERE id IN ($1, $2)`, low.ID, high.ID); err != nil {
		t.Fatal(err)
	}
	first, err := store.Claim(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != high.ID {
		t.Fatalf("priority order ignored: got %s", first.ID)
	}
	if _, err := store.Cancel(ctx, low.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Claim(ctx, "worker", time.Minute); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("cancelled job was claimable: %v", err)
	}
	if _, err := store.Cancel(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.Renew(ctx, first, time.Minute); !errors.Is(err, postgres.ErrCancelRequested) {
		t.Fatalf("running cancellation was not visible: %v", err)
	}
	finished, err := store.Finish(ctx, first, nil, context.Canceled, false, 0)
	if err != nil || finished.Status != "cancelled" {
		t.Fatalf("running cancellation failed: %+v %v", finished, err)
	}
	if _, err := store.Cancel(ctx, first.ID); err != nil {
		t.Fatalf("cancel should be idempotent: %v", err)
	}
}

func TestWaitingJobOvertakesNewHighPriorityWork(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	low, _, err := store.CreateWithOptions(ctx, postgres.CreateParams{
		Queue: "default", Type: "checksum", Payload: json.RawMessage(`{"data":"low"}`),
		Priority: 0, MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.ExecContext(ctx, `UPDATE jobs
	    SET created_at = now() - interval '102 minutes',
	        scheduled_at = now() - interval '102 minutes'
	    WHERE id = $1`, low.ID); err != nil {
		t.Fatal(err)
	}
	high, _, err := store.CreateWithOptions(ctx, postgres.CreateParams{
		Queue: "default", Type: "checksum", Payload: json.RawMessage(`{"data":"high"}`),
		Priority: 100, MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Claim(ctx, "fair-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != low.ID {
		t.Fatalf("old low-priority job lost to newer job %s", first.ID)
	}
	second, err := store.Claim(ctx, "fair-worker", time.Minute)
	if err != nil || second.ID != high.ID {
		t.Fatalf("new high-priority job was not next: %+v %v", second, err)
	}
}

func TestPastScheduleDoesNotGrantArtificialAge(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	past := time.Now().Add(-24 * time.Hour)
	low, _, err := store.CreateWithOptions(ctx, postgres.CreateParams{
		Queue: "default", Type: "checksum", Payload: json.RawMessage(`{"data":"low"}`),
		Priority: 0, ScheduledAt: &past, MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	high, _, err := store.CreateWithOptions(ctx, postgres.CreateParams{
		Queue: "default", Type: "checksum", Payload: json.RawMessage(`{"data":"high"}`),
		Priority: 100, MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Claim(ctx, "fair-worker", time.Minute)
	if err != nil || first.ID != high.ID {
		t.Fatalf("past client timestamp changed priority order: %+v %v", first, err)
	}
	second, err := store.Claim(ctx, "fair-worker", time.Minute)
	if err != nil || second.ID != low.ID {
		t.Fatalf("low-priority job was not next: %+v %v", second, err)
	}
}

func TestWorkerCancelsRunningSleep(t *testing.T) {
	store := leaseStore(t)
	startWorker(t, store, "cancel-worker")
	handler := (api.Server{Store: store}).Handler()
	created := request(t, handler, http.MethodPost, "/v1/jobs", `{"type":"sleep","payload":{"milliseconds":5000}}`)
	var job postgres.Job
	if err := json.Unmarshal(created.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, store, job.ID, "running", 2*time.Second)
	cancelled := request(t, handler, http.MethodPost, "/v1/jobs/"+job.ID+"/cancel", "")
	if cancelled.Code != http.StatusOK {
		t.Fatalf("cancel: %d %s", cancelled.Code, cancelled.Body.String())
	}
	final := waitStatus(t, store, job.ID, "cancelled", 3*time.Second)
	if final.CancelRequestedAt == nil {
		t.Fatal("cancellation time missing")
	}
	attempts, err := store.Attempts(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].Status != "cancelled" {
		t.Fatalf("incorrect cancellation history: %+v", attempts)
	}
}
