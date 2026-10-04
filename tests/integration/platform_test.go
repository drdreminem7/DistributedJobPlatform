package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"distributedjobplatform/internal/api"
	"distributedjobplatform/internal/storage/postgres"
	"distributedjobplatform/internal/worker"
	"distributedjobplatform/migrations"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func openTestDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		t.Fatal(err)
	}
	return db
}

func request(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if method == http.MethodPost {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func TestDurableEnqueueAndWorker(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openTestDB(t, dsn)
	if err := migrations.Apply(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Apply(ctx, db); err != nil {
		t.Fatalf("second migration run failed: %v", err)
	}
	if _, err := db.Exec(`TRUNCATE job_attempts, jobs, queue_policies, workers`); err != nil {
		t.Fatal(err)
	}
	store := postgres.Store{DB: db}
	handler := (api.Server{Store: store, Ready: db.PingContext}).Handler()
	created := request(t, handler, http.MethodPost, "/v1/jobs", `{"type":"checksum","payload":{"data":"abc"}}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("enqueue: %d %s", created.Code, created.Body.String())
	}
	var job postgres.Job
	if err := json.Unmarshal(created.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	if job.ID == "" || job.Status != "queued" || created.Header().Get("Location") != "/v1/jobs/"+job.ID {
		t.Fatalf("unexpected created job: %+v", job)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTestDB(t, dsn)
	defer db.Close()
	store = postgres.Store{DB: db}
	handler = (api.Server{Store: store, Ready: db.PingContext}).Handler()
	got := request(t, handler, http.MethodGet, "/v1/jobs/"+job.ID, "")
	if got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"status":"queued"`) {
		t.Fatalf("job lost after reconnect: %d %s", got.Code, got.Body.String())
	}
	bad := request(t, handler, http.MethodPost, "/v1/jobs", `{"type":"sleep","payload":{"milliseconds":10001}}`)
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("invalid job accepted: %d %s", bad.Code, bad.Body.String())
	}
	list := request(t, handler, http.MethodGet, "/v1/jobs?limit=10", "")
	var listed struct {
		Jobs []postgres.Job `json:"jobs"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Jobs) != 1 {
		t.Fatalf("expected one persisted job, got %d", len(listed.Jobs))
	}

	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		(worker.Worker{Store: store, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), WorkerID: "integration-worker", PollInterval: 10 * time.Millisecond}).Run(workerCtx)
	}()
	defer func() { stop(); <-done }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		current, err := store.Get(ctx, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Status == "succeeded" {
			if current.AttemptCount != 1 || current.CompletedAt == nil || !strings.Contains(string(current.Result), "ba7816bf") {
				t.Fatalf("incorrect completion: %+v", current)
			}
			if err := store.Complete(ctx, postgres.Job{ID: job.ID, LeaseOwner: "stale", LeaseToken: 1}, nil, nil); !errors.Is(err, postgres.ErrLeaseLost) {
				t.Fatalf("terminal job was completed twice: %v", err)
			}
			badJob, err := store.Create(ctx, "default", "sleep", json.RawMessage(`{"milliseconds":"wrong"}`))
			if err != nil {
				t.Fatal(err)
			}
			failureDeadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(failureDeadline) {
				failed, err := store.Get(ctx, badJob.ID)
				if err != nil {
					t.Fatal(err)
				}
				if failed.Status == "failed" {
					if failed.LastError == nil || failed.AttemptCount != 1 {
						t.Fatalf("failure not recorded: %+v", failed)
					}
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Fatal("worker did not record executor failure")
		}
		if current.Status == "failed" {
			t.Fatalf("job failed: %+v", current)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(fmt.Sprintf("worker did not finish job %s", job.ID))
}
