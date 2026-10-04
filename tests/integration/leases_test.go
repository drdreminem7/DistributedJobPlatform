package integration

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"distributedjobplatform/internal/storage/postgres"
	"distributedjobplatform/migrations"
)

func leaseStore(t *testing.T) postgres.Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	db := openTestDB(t, dsn)
	t.Cleanup(func() { db.Close() })
	if err := migrations.Apply(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`TRUNCATE job_attempts, jobs, queue_policies, workers`); err != nil {
		t.Fatal(err)
	}
	return postgres.Store{DB: db}
}

func TestConcurrentClaimsHaveUniqueJobs(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	const jobs = 20
	const contenders = 50
	for i := 0; i < jobs; i++ {
		if _, err := store.Create(ctx, "default", "checksum", json.RawMessage(`{"data":"x"}`)); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan postgres.Job, contenders)
	errorsCh := make(chan error, contenders)
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			job, err := store.Claim(ctx, "worker-"+strconv.Itoa(n), time.Minute)
			if err == nil {
				results <- job
			} else if !errors.Is(err, postgres.ErrNotFound) {
				errorsCh <- err
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		t.Errorf("claim failed: %v", err)
	}
	seen := make(map[string]bool)
	for job := range results {
		if seen[job.ID] {
			t.Errorf("job %s was claimed twice", job.ID)
		}
		seen[job.ID] = true
		if job.AttemptCount != 1 || job.LeaseToken != 1 || job.LeaseOwner == "" || job.LeaseExpiresAt == nil {
			t.Errorf("invalid lease: %+v", job)
		}
	}
	if len(seen) != jobs {
		t.Fatalf("claimed %d of %d jobs", len(seen), jobs)
	}
}

func TestExpiredLeaseIsRecoveredAndStaleCompletionIsFenced(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	created, err := store.Create(ctx, "default", "checksum", json.RawMessage(`{"data":"recover"}`))
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Claim(ctx, "same-worker-id", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != created.ID {
		t.Fatal("claimed wrong job")
	}
	if _, err := store.DB.ExecContext(ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, first.ID); err != nil {
		t.Fatal(err)
	}
	second, err := store.Claim(ctx, "same-worker-id", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID || second.LeaseToken != first.LeaseToken+1 || second.AttemptCount != 2 {
		t.Fatalf("recovery did not advance ownership: first=%+v second=%+v", first, second)
	}
	if err := store.Complete(ctx, first, json.RawMessage(`{"stale":true}`), nil); !errors.Is(err, postgres.ErrLeaseLost) {
		t.Fatalf("stale completion accepted: %v", err)
	}
	if err := store.Renew(ctx, first, time.Minute); !errors.Is(err, postgres.ErrLeaseLost) {
		t.Fatalf("stale renewal accepted: %v", err)
	}
	if err := store.Complete(ctx, second, json.RawMessage(`{"winner":true}`), nil); err != nil {
		t.Fatal(err)
	}
	final, err := store.Get(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]bool
	if err := json.Unmarshal(final.Result, &result); err != nil {
		t.Fatal(err)
	}
	if final.Status != "succeeded" || !result["winner"] || final.LeaseOwner != "" || final.LeaseExpiresAt != nil {
		t.Fatalf("incorrect recovered result: %+v", final)
	}
}

func TestLeaseRenewalRequiresCurrentOwnership(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	if _, err := store.Create(ctx, "default", "sleep", json.RawMessage(`{"milliseconds":1}`)); err != nil {
		t.Fatal(err)
	}
	job, err := store.Claim(ctx, "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	oldExpiry := *job.LeaseExpiresAt
	if err := store.Renew(ctx, job, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	renewed, err := store.Get(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.LeaseExpiresAt == nil || !renewed.LeaseExpiresAt.After(oldExpiry) {
		t.Fatal("renewal did not extend the lease")
	}
	if _, err := store.Claim(ctx, "worker-b", time.Second); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("active job was acquired by another worker: %v", err)
	}
}

func TestExpiredFinalAttemptDeadLetters(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	created, _, err := store.CreateWithOptions(ctx, postgres.CreateParams{
		Queue: "default", Type: "sleep", Payload: json.RawMessage(`{"milliseconds":1}`), MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(ctx, "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.ExecContext(ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Claim(ctx, "worker-b", time.Minute); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("exhausted job was reclaimed: %v", err)
	}
	final, err := store.Get(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != "dead_lettered" || final.AttemptCount != 1 || final.CompletedAt == nil {
		t.Fatalf("exhausted job was not dead-lettered: %+v", final)
	}
	if err := store.Complete(ctx, claimed, nil, nil); !errors.Is(err, postgres.ErrLeaseLost) {
		t.Fatalf("expired worker completed job: %v", err)
	}
	attempts, err := store.Attempts(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].Status != "lease_expired" {
		t.Fatalf("wrong attempt history: %+v", attempts)
	}
}
