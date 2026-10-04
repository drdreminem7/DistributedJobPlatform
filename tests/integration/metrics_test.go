package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"distributedjobplatform/internal/api"
	"distributedjobplatform/internal/observability"
	"distributedjobplatform/internal/storage/postgres"
	"distributedjobplatform/internal/worker"
)

func TestMetricsReflectCommittedWorkAndQueueState(t *testing.T) {
	metrics := observability.New()
	store := leaseStore(t)
	store.Metrics = metrics
	handler := (api.Server{Store: store, Metrics: metrics,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Handler()
	created := request(t, handler, http.MethodPost, "/v1/jobs",
		`{"type":"checksum","payload":{"data":"metrics"}}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", created.Code, created.Body.String())
	}
	var job postgres.Job
	if err := json.Unmarshal(created.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	checkMetric(t, handler, "jobs_enqueued_total 1")
	checkMetric(t, handler, "queue_depth 1")

	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		(worker.Worker{Store: store, WorkerID: "metrics-worker", Metrics: metrics,
			PollInterval: 10 * time.Millisecond,
			Log:          slog.New(slog.NewTextHandler(io.Discard, nil))}).Run(ctx)
	}()
	t.Cleanup(func() { stop(); <-done })
	waitStatus(t, store, job.ID, "succeeded", 3*time.Second)
	waitMetric(t, handler, "jobs_succeeded_total 1", time.Second)
	checkMetric(t, handler, "active_workers 1")
	checkMetric(t, handler, "queue_depth 0")
	checkMetric(t, handler, "active_jobs 0")
	checkMetric(t, handler, "jobs_started_total 1")
	checkMetric(t, handler, "jobs_succeeded_total 1")
	checkMetric(t, handler, "queue_wait_seconds_count 1")
	checkMetric(t, handler, "execution_seconds_count 1")

	retrying := request(t, handler, http.MethodPost, "/v1/jobs",
		`{"type":"unstable","payload":{"fail_until_attempt":1},"max_attempts":2}`)
	if retrying.Code != http.StatusCreated {
		t.Fatalf("create retrying: %d %s", retrying.Code, retrying.Body.String())
	}
	if err := json.Unmarshal(retrying.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, store, job.ID, "succeeded", 5*time.Second)
	waitMetric(t, handler, "jobs_retried_total 1", time.Second)
	waitMetric(t, handler, "jobs_succeeded_total 2", time.Second)

	future := time.Now().Add(time.Hour).Format(time.RFC3339)
	cancelBody := `{"type":"checksum","payload":{"data":"cancel"},"scheduled_at":"` + future + `"}`
	created = request(t, handler, http.MethodPost, "/v1/jobs", cancelBody)
	if created.Code != http.StatusCreated {
		t.Fatalf("create scheduled: %d %s", created.Code, created.Body.String())
	}
	if err := json.Unmarshal(created.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	cancelled := request(t, handler, http.MethodPost, "/v1/jobs/"+job.ID+"/cancel", "")
	if cancelled.Code != http.StatusOK {
		t.Fatalf("cancel: %d %s", cancelled.Code, cancelled.Body.String())
	}
	checkMetric(t, handler, "jobs_cancelled_total 1")
	checkMetric(t, handler, "queue_depth 0")
}

func checkMetric(t *testing.T, handler http.Handler, sample string) {
	t.Helper()
	response := request(t, handler, http.MethodGet, "/metrics", "")
	if response.Code != http.StatusOK {
		t.Fatalf("metrics: %d %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "\n"+sample+"\n") {
		t.Fatalf("missing metric %q", sample)
	}
}

func waitMetric(t *testing.T, handler http.Handler, sample string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		response := request(t, handler, http.MethodGet, "/metrics", "")
		if response.Code == http.StatusOK && strings.Contains(response.Body.String(), "\n"+sample+"\n") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	checkMetric(t, handler, sample)
}

func TestExpiredLeaseMetrics(t *testing.T) {
	metrics := observability.New()
	store := leaseStore(t)
	store.Metrics = metrics
	ctx := context.Background()
	job, _, err := store.CreateWithOptions(ctx, postgres.CreateParams{
		Queue: "default", Type: "checksum", Payload: json.RawMessage(`{"data":"expired"}`),
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Claim(ctx, "metrics-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.ExecContext(ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Claim(ctx, "metrics-worker", time.Minute); err != postgres.ErrNotFound {
		t.Fatalf("final expired attempt was not removed: %v", err)
	}
	handler := (api.Server{Store: store, Metrics: metrics}).Handler()
	checkMetric(t, handler, "lease_expirations_total 1")
	checkMetric(t, handler, "jobs_dead_lettered_total 1")
}
