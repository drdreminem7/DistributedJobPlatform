package integration

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"distributedjobplatform/internal/api"
	"distributedjobplatform/internal/storage/postgres"
	"distributedjobplatform/internal/worker"
)

func intPtr(value int) *int { return &value }

func TestQueueConcurrencyLimitAcrossClaimers(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	if err := store.SetQueuePolicy(ctx, postgres.QueuePolicy{
		Queue: "limited", ConcurrencyLimit: intPtr(3),
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := store.Create(ctx, "limited", "sleep", json.RawMessage(`{"milliseconds":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	const claimers = 30
	var wg sync.WaitGroup
	start := make(chan struct{})
	claimed := make(chan postgres.Job, claimers)
	errorsCh := make(chan error, claimers)
	for i := 0; i < claimers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			job, err := store.Claim(ctx, "claimer", time.Minute)
			if err == nil {
				claimed <- job
			} else if !errors.Is(err, postgres.ErrNotFound) {
				errorsCh <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(claimed)
	close(errorsCh)
	for err := range errorsCh {
		t.Errorf("claim error: %v", err)
	}
	jobs := make([]postgres.Job, 0)
	for job := range claimed {
		jobs = append(jobs, job)
	}
	if len(jobs) != 3 {
		t.Fatalf("expected 3 active leases, got %d", len(jobs))
	}
	queues, err := store.ListQueues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(queues) != 1 || queues[0].Running != 3 || queues[0].Queued != 17 {
		t.Fatalf("wrong queue counts: %+v", queues)
	}
	if err := store.Complete(ctx, jobs[0], json.RawMessage(`{"done":true}`), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Claim(ctx, "replacement", time.Minute); err != nil {
		t.Fatalf("slot was not released: %v", err)
	}
}

func TestRateLimitAndOtherQueueProgress(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	if err := store.SetQueuePolicy(ctx, postgres.QueuePolicy{
		Queue: "hot", RatePerSecond: intPtr(1), Burst: intPtr(1),
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, _, err := store.CreateWithOptions(ctx, postgres.CreateParams{
			Queue: "hot", Type: "checksum", Payload: json.RawMessage(`{"data":"hot"}`),
			Priority: 100, MaxAttempts: 3,
		}); err != nil {
			t.Fatal(err)
		}
	}
	cold, _, err := store.CreateWithOptions(ctx, postgres.CreateParams{
		Queue: "cold", Type: "checksum", Payload: json.RawMessage(`{"data":"cold"}`),
		Priority: 1, MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Claim(ctx, "worker-a", time.Minute)
	if err != nil || first.Queue != "hot" {
		t.Fatalf("first claim: %+v %v", first, err)
	}
	second, err := store.Claim(ctx, "worker-b", time.Minute)
	if err != nil || second.ID != cold.ID {
		t.Fatalf("blocked queue held up other work: %+v %v", second, err)
	}
	if _, err := store.Claim(ctx, "worker-c", time.Minute); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("rate limit allowed another hot claim: %v", err)
	}
	if _, err := store.DB.ExecContext(ctx, `UPDATE queue_policies
	    SET refilled_at = now() - interval '2 seconds' WHERE queue_name = 'hot'`); err != nil {
		t.Fatal(err)
	}
	third, err := store.Claim(ctx, "worker-c", time.Minute)
	if err != nil || third.Queue != "hot" {
		t.Fatalf("token refill did not allow claim: %+v %v", third, err)
	}
}

func TestWorkerRegistryAndReadAPIs(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	if err := store.RegisterWorker(ctx, "visible-worker"); err != nil {
		t.Fatal(err)
	}
	if err := store.Heartbeat(ctx, "visible-worker"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetQueuePolicy(ctx, postgres.QueuePolicy{
		Queue: "visible", ConcurrencyLimit: intPtr(2),
	}); err != nil {
		t.Fatal(err)
	}
	handler := (api.Server{Store: store}).Handler()
	workersResponse := request(t, handler, http.MethodGet, "/v1/workers", "")
	if workersResponse.Code != http.StatusOK {
		t.Fatalf("workers endpoint: %d", workersResponse.Code)
	}
	var workersBody struct {
		Workers []postgres.WorkerInfo `json:"workers"`
	}
	if err := json.Unmarshal(workersResponse.Body.Bytes(), &workersBody); err != nil {
		t.Fatal(err)
	}
	if len(workersBody.Workers) != 1 || !workersBody.Workers[0].Healthy {
		t.Fatalf("worker not visible: %+v", workersBody.Workers)
	}
	queuesResponse := request(t, handler, http.MethodGet, "/v1/queues", "")
	var queuesBody struct {
		Queues []postgres.QueueInfo `json:"queues"`
	}
	if err := json.Unmarshal(queuesResponse.Body.Bytes(), &queuesBody); err != nil {
		t.Fatal(err)
	}
	if len(queuesBody.Queues) != 1 || queuesBody.Queues[0].ConcurrencyLimit == nil || *queuesBody.Queues[0].ConcurrencyLimit != 2 {
		t.Fatalf("queue policy not visible: %+v", queuesBody.Queues)
	}
	if _, err := store.DB.ExecContext(ctx, `UPDATE workers SET last_heartbeat_at = now() - interval '20 seconds' WHERE worker_id = 'visible-worker'`); err != nil {
		t.Fatal(err)
	}
	workers, err := store.ListWorkers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 1 || workers[0].Healthy {
		t.Fatalf("stale worker shown healthy: %+v", workers)
	}
	if err := store.StopWorker(ctx, "visible-worker"); err != nil {
		t.Fatal(err)
	}
	workers, err = store.ListWorkers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if workers[0].StoppedAt == nil {
		t.Fatal("stopped worker has no stop time")
	}
}

func TestWorkerRunRegistersAndStops(t *testing.T) {
	store := leaseStore(t)
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		(worker.Worker{Store: store, WorkerID: "lifecycle-worker",
			Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Run(ctx)
	}()
	defer func() { stop(); <-done }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		workers, err := store.ListWorkers(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(workers) == 1 && workers[0].Healthy {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	workers, err := store.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 1 || !workers[0].Healthy {
		t.Fatalf("worker did not register: %+v", workers)
	}
	stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop")
	}
	workers, err = store.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if workers[0].StoppedAt == nil || workers[0].Healthy {
		t.Fatalf("worker still active: %+v", workers[0])
	}
}
