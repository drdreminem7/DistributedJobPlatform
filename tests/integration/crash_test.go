package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"distributedjobplatform/internal/storage/postgres"
	"distributedjobplatform/internal/worker"
)

func TestWorkerHelperProcess(t *testing.T) {
	if os.Getenv("DJP_TEST_WORKER") != "1" {
		return
	}
	db := openTestDB(t, os.Getenv("TEST_DATABASE_URL"))
	defer db.Close()
	(worker.Worker{Store: postgres.Store{DB: db}, WorkerID: "killed-worker",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), PollInterval: 10 * time.Millisecond}).Run(context.Background())
}

func TestKilledWorkerJobIsReclaimed(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	created, err := store.Create(ctx, "default", "sleep", json.RawMessage(`{"milliseconds":3000}`))
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestWorkerHelperProcess$")
	child.Env = append(os.Environ(), "DJP_TEST_WORKER=1")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	var first postgres.Job
	for time.Now().Before(deadline) {
		first, err = store.Get(ctx, created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if first.Status == "running" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if first.Status != "running" {
		t.Fatal("child worker did not claim the job")
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	if _, err := store.DB.ExecContext(ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, created.ID); err != nil {
		t.Fatal(err)
	}
	secondCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		(worker.Worker{Store: store, WorkerID: "recovery-worker",
			Log: slog.New(slog.NewTextHandler(io.Discard, nil)), PollInterval: 10 * time.Millisecond}).Run(secondCtx)
	}()
	defer func() { stop(); <-done }()
	deadline = time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		final, err := store.Get(ctx, created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if final.Status == "succeeded" {
			if final.AttemptCount != 2 || final.LeaseToken != first.LeaseToken+1 || !strings.Contains(string(final.Result), "3000") {
				t.Fatalf("incorrect recovery outcome: %+v", final)
			}
			return
		}
		if final.Status == "failed" {
			t.Fatalf("recovered job failed: %+v", final)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("recovery worker did not finish the job")
}

func TestScheduledJobSurvivesWorkerDeath(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	due := time.Now().Add(time.Second)
	job, _, err := store.CreateWithOptions(ctx, postgres.CreateParams{
		Queue: "default", Type: "checksum", Payload: json.RawMessage(`{"data":"scheduled"}`),
		ScheduledAt: &due, MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestWorkerHelperProcess$")
	child.Env = append(os.Environ(), "DJP_TEST_WORKER=1")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	deadline := time.Now().Add(500 * time.Millisecond)
	registered := false
	for time.Now().Before(deadline) {
		workers, err := store.ListWorkers(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(workers) > 0 && workers[0].ID == "killed-worker" {
			registered = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !registered {
		t.Fatal("scheduled worker did not start")
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	startWorker(t, store, "scheduled-recovery")
	final := waitStatus(t, store, job.ID, "succeeded", 3*time.Second)
	if final.AttemptCount != 1 {
		t.Fatalf("scheduled job was not claimed once after restart: %+v", final)
	}
}
