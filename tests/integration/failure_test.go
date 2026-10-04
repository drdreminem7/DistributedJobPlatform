package integration

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"distributedjobplatform/internal/api"
	"distributedjobplatform/internal/executor"
	"distributedjobplatform/internal/storage/postgres"
	"distributedjobplatform/internal/worker"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const crashEnqueueBody = `{"idempotency_key":"crash-response","type":"checksum","payload":{"data":"committed"}}`

type delayedWriter struct{ header http.Header }

func (w *delayedWriter) Header() http.Header       { return w.header }
func (w *delayedWriter) Write([]byte) (int, error) { return 0, errors.New("response blocked") }
func (w *delayedWriter) WriteHeader(int) {
	fmt.Fprintln(os.Stdout, "committed")
	time.Sleep(time.Hour)
}

func TestFaultHelperProcess(t *testing.T) {
	switch os.Getenv("DJP_FAULT_CASE") {
	case "api-response":
		db := openTestDB(t, os.Getenv("TEST_DATABASE_URL"))
		defer db.Close()
		handler := (api.Server{Store: postgres.Store{DB: db},
			Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Handler()
		r := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(crashEnqueueBody))
		r.Header.Set("Content-Type", "application/json")
		handler.ServeHTTP(&delayedWriter{header: make(http.Header)}, r)
	case "effect":
		db := openTestDB(t, os.Getenv("TEST_DATABASE_URL"))
		defer db.Close()
		store := postgres.Store{DB: db}
		job, err := store.Claim(context.Background(), "effect-worker", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := appendEffect(os.Getenv("DJP_EFFECT_FILE")); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(os.Stdout, job.ID)
		time.Sleep(time.Hour)
	}
}

func startFaultChild(t *testing.T, faultCase, effectFile string) (*bufio.Reader, func()) {
	t.Helper()
	child := exec.Command(os.Args[0], "-test.run=^TestFaultHelperProcess$")
	child.Env = append(os.Environ(), "DJP_FAULT_CASE="+faultCase, "DJP_EFFECT_FILE="+effectFile)
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		_ = child.Process.Kill()
		_ = child.Wait()
	}
	t.Cleanup(stop)
	return bufio.NewReader(stdout), stop
}

func childLine(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	type result struct {
		line string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		line, err := reader.ReadString('\n')
		done <- result{line, err}
	}()
	select {
	case output := <-done:
		if output.err != nil {
			t.Fatal(output.err)
		}
		return strings.TrimSpace(output.line)
	case <-time.After(5 * time.Second):
		t.Fatal("fault helper did not reach the crash point")
		return ""
	}
}

func appendEffect(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err := file.WriteString("effect\n"); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

func TestAPICrashAfterEnqueueCommit(t *testing.T) {
	store := leaseStore(t)
	reader, stop := startFaultChild(t, "api-response", "")
	if line := childLine(t, reader); line != "committed" {
		t.Fatalf("unexpected child signal: %q", line)
	}
	stop()
	var id string
	if err := store.DB.QueryRow(`SELECT id FROM jobs WHERE idempotency_key = 'crash-response'`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	handler := (api.Server{Store: store, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Handler()
	retry := request(t, handler, http.MethodPost, "/v1/jobs", crashEnqueueBody)
	if retry.Code != http.StatusOK {
		t.Fatalf("retry after lost response: %d %s", retry.Code, retry.Body.String())
	}
	var job postgres.Job
	if err := json.Unmarshal(retry.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	if job.ID != id {
		t.Fatalf("retry created another job: %s != %s", job.ID, id)
	}
	var count int
	if err := store.DB.QueryRow(`SELECT count(*) FROM jobs WHERE idempotency_key = 'crash-response'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("expected one committed row, count=%d error=%v", count, err)
	}
}

func TestCrashAfterEffectBeforeAcknowledgement(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	created, _, err := store.CreateWithOptions(ctx, postgres.CreateParams{
		Queue: "default", Type: "checksum", Payload: json.RawMessage(`{"data":"effect"}`),
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "effects")
	reader, stop := startFaultChild(t, "effect", marker)
	if id := childLine(t, reader); id != created.ID {
		t.Fatalf("child claimed wrong job: %s", id)
	}
	stop()
	if _, err := store.DB.ExecContext(ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, created.ID); err != nil {
		t.Fatal(err)
	}
	second, err := store.Claim(ctx, "replacement", time.Minute)
	if err != nil || second.ID != created.ID || second.AttemptCount != 2 {
		t.Fatalf("job was not reclaimed: %+v %v", second, err)
	}
	if err := appendEffect(marker); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finish(ctx, second, json.RawMessage(`{"ok":true}`), nil, false, 0); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "effect\neffect\n" {
		t.Fatalf("expected duplicate external effects, got %q", data)
	}
	final, err := store.Get(ctx, created.ID)
	if err != nil || final.Status != "succeeded" {
		t.Fatalf("job did not complete after duplicate effect: %+v %v", final, err)
	}
}

func TestTerminatedDatabaseConnectionRecovers(t *testing.T) {
	store := leaseStore(t)
	store.DB.SetMaxOpenConns(1)
	ctx := context.Background()
	connection, err := store.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var backend int
	if err := connection.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&backend); err != nil {
		t.Fatal(err)
	}
	control, err := sql.Open("pgx", os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	var terminated bool
	if err := control.QueryRowContext(ctx, `SELECT pg_terminate_backend($1)`, backend).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("could not terminate database connection: %v %v", terminated, err)
	}
	connection.Close()
	params := postgres.CreateParams{
		Queue: "default", Type: "checksum", Payload: json.RawMessage(`{"data":"reconnected"}`),
		MaxAttempts: 3, IdempotencyKey: "reconnected", RequestHash: "reconnected-request",
	}
	job, _, err := store.CreateWithOptions(ctx, params)
	if err != nil {
		job, _, err = store.CreateWithOptions(ctx, params)
	}
	if err != nil {
		t.Fatalf("idempotent retry did not recover: %v", err)
	}
	startWorker(t, store, "reconnected-worker")
	waitStatus(t, store, job.ID, "succeeded", 3*time.Second)
}

type lostRenewalStore struct {
	postgres.Store
	stop context.CancelFunc
}

func (s lostRenewalStore) Renew(context.Context, postgres.Job, time.Duration) error {
	s.stop()
	return errors.New("injected database connection loss")
}

func TestConnectionLossDuringRenewalLeavesJobRecoverable(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	created, err := store.Create(ctx, "default", "sleep", json.RawMessage(`{"milliseconds":500}`))
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		(worker.Worker{Store: lostRenewalStore{Store: store, stop: stopFirst},
			WorkerID: "lost-renewal", PollInterval: 10 * time.Millisecond,
			LeaseDuration: 300 * time.Millisecond,
			Log:           slog.New(slog.NewTextHandler(io.Discard, nil))}).Run(firstCtx)
	}()
	t.Cleanup(func() { stopFirst(); <-firstDone })
	waitStatus(t, store, created.ID, "running", time.Second)
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop after renewal loss")
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		(worker.Worker{Store: store, WorkerID: "renewal-recovery",
			PollInterval: 10 * time.Millisecond, LeaseDuration: 300 * time.Millisecond,
			Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Run(secondCtx)
	}()
	t.Cleanup(func() { stopSecond(); <-secondDone })
	final := waitStatus(t, store, created.ID, "succeeded", 3*time.Second)
	if final.AttemptCount != 2 {
		t.Fatalf("renewal loss did not consume one attempt: %+v", final)
	}
	attempts, err := store.Attempts(ctx, created.ID)
	if err != nil || len(attempts) != 2 || attempts[1].Status != "lease_expired" {
		t.Fatalf("wrong recovery history: %+v %v", attempts, err)
	}
}

type slowClaimStore struct {
	postgres.Store
	delayed atomic.Bool
}

func (s *slowClaimStore) Claim(ctx context.Context, owner string, duration time.Duration) (postgres.Job, error) {
	if s.delayed.CompareAndSwap(false, true) {
		<-ctx.Done()
		return postgres.Job{}, ctx.Err()
	}
	return s.Store.Claim(ctx, owner, duration)
}

func TestSlowClaimTimesOutAndWorkerContinues(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	job, err := store.Create(ctx, "default", "checksum", json.RawMessage(`{"data":"slow-query"}`))
	if err != nil {
		t.Fatal(err)
	}
	slow := &slowClaimStore{Store: store}
	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		(worker.Worker{Store: slow, WorkerID: "slow-claim", PollInterval: 10 * time.Millisecond,
			ClaimTimeout: 100 * time.Millisecond,
			Log:          slog.New(slog.NewTextHandler(io.Discard, nil))}).Run(workerCtx)
	}()
	t.Cleanup(func() { stop(); <-done })
	waitStatus(t, store, job.ID, "succeeded", 3*time.Second)
	if !slow.delayed.Load() {
		t.Fatal("slow claim was not exercised")
	}
}

func TestShutdownTimeoutLeavesLeaseForRecovery(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	job, err := store.Create(ctx, "default", "sleep", json.RawMessage(`{"milliseconds":1000}`))
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "worker")
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", binary, "./cmd/worker")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v %s", err, output)
	}
	process := exec.Command(binary)
	process.Env = append(os.Environ(),
		"DATABASE_URL="+os.Getenv("TEST_DATABASE_URL"),
		"METRICS_ADDR=127.0.0.1:0", "WORKER_SHUTDOWN_TIMEOUT=100ms")
	var output bytes.Buffer
	process.Stdout = &output
	process.Stderr = &output
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = process.Process.Kill()
			_ = process.Wait()
		}
	})
	waitStatus(t, store, job.ID, "running", 3*time.Second)
	if err := process.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- process.Wait() }()
	select {
	case err := <-exited:
		stopped = true
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 {
			t.Fatalf("worker did not time out: %v %s", err, output.String())
		}
	case <-time.After(2 * time.Second):
		_ = process.Process.Kill()
		<-exited
		stopped = true
		t.Fatalf("worker did not exit on shutdown timeout: %s", output.String())
	}
	current, err := store.Get(ctx, job.ID)
	if err != nil || current.Status != "running" {
		t.Fatalf("timed-out worker lost its lease: %+v %v", current, err)
	}
	if _, err := store.DB.ExecContext(ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	startWorker(t, store, "shutdown-recovery")
	final := waitStatus(t, store, job.ID, "succeeded", 3*time.Second)
	if final.AttemptCount != 2 {
		t.Fatalf("job was not recovered after shutdown timeout: %+v", final)
	}
}

type partitionDialer struct {
	blocked atomic.Bool
	mu      sync.Mutex
	conns   []net.Conn
}

func (p *partitionDialer) dial(original func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		if p.blocked.Load() {
			return nil, errors.New("injected database network outage")
		}
		conn, err := original(ctx, network, address)
		if err != nil {
			return nil, err
		}
		p.mu.Lock()
		p.conns = append(p.conns, conn)
		blocked := p.blocked.Load()
		p.mu.Unlock()
		if blocked {
			conn.Close()
			return nil, errors.New("injected database network outage")
		}
		return conn, nil
	}
}

func (p *partitionDialer) block() {
	p.blocked.Store(true)
	p.mu.Lock()
	for _, conn := range p.conns {
		conn.Close()
	}
	p.conns = nil
	p.mu.Unlock()
}

func TestTemporaryDatabaseNetworkOutageRecovers(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	job, err := store.Create(ctx, "default", "sleep", json.RawMessage(`{"milliseconds":1000}`))
	if err != nil {
		t.Fatal(err)
	}
	config, err := pgx.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	partition := &partitionDialer{}
	config.DialFunc = partition.dial(config.DialFunc)
	faultDB := stdlib.OpenDB(*config)
	faultDB.SetMaxOpenConns(2)
	t.Cleanup(func() { faultDB.Close() })
	if err := faultDB.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		(worker.Worker{Store: postgres.Store{DB: faultDB}, WorkerID: "partition-worker",
			PollInterval: 10 * time.Millisecond, LeaseDuration: 300 * time.Millisecond,
			Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Run(workerCtx)
	}()
	t.Cleanup(func() { stop(); <-done })
	waitStatus(t, store, job.ID, "running", time.Second)
	partition.block()
	time.Sleep(450 * time.Millisecond)
	partition.blocked.Store(false)
	final := waitStatus(t, store, job.ID, "succeeded", 5*time.Second)
	if final.AttemptCount != 2 {
		t.Fatalf("outage recovery did not reclaim the lease: %+v", final)
	}
}

func TestLongExecutorCannotOverwriteNewLease(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	job, _, err := store.CreateWithOptions(ctx, postgres.CreateParams{
		Queue: "default", Type: "checksum", Payload: json.RawMessage(`{"data":"winner"}`),
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	started := make(chan struct{})
	firstCtx, stopFirst := context.WithCancel(ctx)
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		(worker.Worker{Store: store, WorkerID: "late-worker", PollInterval: 10 * time.Millisecond,
			LeaseDuration: 300 * time.Millisecond,
			Log:           slog.New(slog.NewTextHandler(io.Discard, nil)),
			Execute: func(context.Context, string, json.RawMessage, int) (json.RawMessage, error) {
				close(started)
				<-release
				return json.RawMessage(`{"late":true}`), nil
			},
		}).Run(firstCtx)
	}()
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		stopFirst()
		<-firstDone
	})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first executor did not start")
	}
	if _, err := store.DB.ExecContext(ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	startWorker(t, store, "winner-worker")
	final := waitStatus(t, store, job.ID, "succeeded", 3*time.Second)
	if final.AttemptCount != 2 || strings.Contains(string(final.Result), "late") {
		t.Fatalf("late executor overwrote new attempt: %+v", final)
	}
	close(release)
	released = true
	stopFirst()
	<-firstDone
	after, err := store.Get(ctx, job.ID)
	if err != nil || string(after.Result) != string(final.Result) || after.LeaseToken != final.LeaseToken {
		t.Fatalf("late executor changed terminal job: %+v %v", after, err)
	}
}

func TestClaimRollbackPreservesJobAndRateToken(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	one := 1
	if err := store.SetQueuePolicy(ctx, postgres.QueuePolicy{
		Queue: "atomic", RatePerSecond: &one, Burst: &one,
	}); err != nil {
		t.Fatal(err)
	}
	job, err := store.Create(ctx, "atomic", "checksum", json.RawMessage(`{"data":"rollback"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.ExecContext(ctx, `ALTER TABLE job_attempts
	    ADD CONSTRAINT injected_failure CHECK (false) NOT VALID`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.DB.ExecContext(context.Background(), `ALTER TABLE job_attempts DROP CONSTRAINT IF EXISTS injected_failure`)
	})
	if _, err := store.Claim(ctx, "rollback-worker", time.Minute); err == nil {
		t.Fatal("injected attempt failure did not fail claim")
	}
	current, err := store.Get(ctx, job.ID)
	if err != nil || current.Status != "queued" || current.AttemptCount != 0 {
		t.Fatalf("claim changed job before rollback: %+v %v", current, err)
	}
	var tokens float64
	if err := store.DB.QueryRowContext(ctx, `SELECT tokens FROM queue_policies WHERE queue_name = 'atomic'`).Scan(&tokens); err != nil || tokens != 1 {
		t.Fatalf("claim spent rate token before rollback: %f %v", tokens, err)
	}
	if _, err := store.DB.ExecContext(ctx, `ALTER TABLE job_attempts DROP CONSTRAINT injected_failure`); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(ctx, "rollback-worker", time.Minute)
	if err != nil || claimed.ID != job.ID || claimed.AttemptCount != 1 {
		t.Fatalf("job could not be claimed after rollback: %+v %v", claimed, err)
	}
}

func TestCancelledExpiredLeaseCannotRunAgain(t *testing.T) {
	for _, maxAttempts := range []int{1, 3} {
		t.Run(strconv.Itoa(maxAttempts), func(t *testing.T) {
			store := leaseStore(t)
			ctx := context.Background()
			created, _, err := store.CreateWithOptions(ctx, postgres.CreateParams{
				Queue: "default", Type: "sleep", Payload: json.RawMessage(`{"milliseconds":1000}`),
				MaxAttempts: maxAttempts,
			})
			if err != nil {
				t.Fatal(err)
			}
			claimed, err := store.Claim(ctx, "dead-worker", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Cancel(ctx, created.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := store.DB.ExecContext(ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, created.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Claim(ctx, "replacement", time.Minute); !errors.Is(err, postgres.ErrNotFound) {
				t.Fatalf("cancelled job was reclaimed: %v", err)
			}
			final, err := store.Get(ctx, created.ID)
			if err != nil {
				t.Fatal(err)
			}
			if final.Status != "cancelled" || final.AttemptCount != 1 || final.CompletedAt == nil {
				t.Fatalf("incorrect cancellation recovery: %+v", final)
			}
			if _, err := store.Finish(ctx, claimed, nil, nil, false, 0); !errors.Is(err, postgres.ErrLeaseLost) {
				t.Fatalf("old worker finished cancelled job: %v", err)
			}
			attempts, err := store.Attempts(ctx, created.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(attempts) != 1 || attempts[0].Status != "lease_expired" {
				t.Fatalf("wrong attempt history: %+v", attempts)
			}
		})
	}
}

func TestExecutorPanicDoesNotKillWorker(t *testing.T) {
	store := leaseStore(t)
	ctx := context.Background()
	panicJob, err := store.Create(ctx, "default", "checksum", json.RawMessage(`{"data":"panic"}`))
	if err != nil {
		t.Fatal(err)
	}
	nextJob, err := store.Create(ctx, "default", "checksum", json.RawMessage(`{"data":"next"}`))
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		(worker.Worker{
			Store: store, WorkerID: "panic-worker", PollInterval: 10 * time.Millisecond,
			Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
			Execute: func(ctx context.Context, jobType string, payload json.RawMessage, attempt int) (json.RawMessage, error) {
				if strings.Contains(string(payload), "panic") {
					panic("injected executor failure")
				}
				return executor.Execute(ctx, jobType, payload, attempt)
			},
		}).Run(workerCtx)
	}()
	t.Cleanup(func() { stop(); <-done })
	failed := waitStatus(t, store, panicJob.ID, "failed", 3*time.Second)
	if failed.LastError == nil || *failed.LastError != "executor panic" {
		t.Fatalf("panic outcome was not recorded: %+v", failed)
	}
	succeeded := waitStatus(t, store, nextJob.ID, "succeeded", 3*time.Second)
	if succeeded.AttemptCount != 1 {
		t.Fatalf("worker did not continue after panic: %+v", succeeded)
	}
}
