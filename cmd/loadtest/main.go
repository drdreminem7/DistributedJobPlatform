package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"distributedjobplatform/internal/api"
	"distributedjobplatform/internal/executor"
	"distributedjobplatform/internal/storage/postgres"
	"distributedjobplatform/internal/worker"
	"distributedjobplatform/migrations"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type percentiles struct {
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
}

type report struct {
	RecordedAt            time.Time   `json:"recorded_at"`
	Host                  string      `json:"host"`
	GoVersion             string      `json:"go_version"`
	PostgresVersion       string      `json:"postgres_version"`
	OS                    string      `json:"os"`
	Arch                  string      `json:"arch"`
	CPUs                  int         `json:"cpus"`
	Jobs                  int         `json:"jobs"`
	Producers             int         `json:"producers"`
	Workers               int         `json:"workers"`
	PayloadBytes          int         `json:"payload_bytes"`
	EnqueueSeconds        float64     `json:"enqueue_seconds"`
	CompletionSeconds     float64     `json:"completion_seconds"`
	EnqueuePerSecond      float64     `json:"enqueue_per_second"`
	JobsPerSecond         float64     `json:"jobs_per_second"`
	EnqueueLatency        percentiles `json:"enqueue_latency"`
	ClaimLatency          percentiles `json:"claim_latency"`
	QueueWait             percentiles `json:"queue_wait"`
	ExecutorDuration      percentiles `json:"executor_duration"`
	AttemptDuration       percentiles `json:"attempt_duration"`
	AppCPUSeconds         float64     `json:"app_cpu_seconds"`
	AppHeapAllocBytes     uint64      `json:"app_heap_alloc_bytes"`
	AppPeakRSSBytes       int64       `json:"app_peak_rss_bytes"`
	DBConnectionsPeak     int         `json:"db_connections_peak"`
	DBLockWaitSamples     int         `json:"db_lock_wait_samples"`
	DBSamples             int         `json:"db_samples"`
	CompletedSuccessfully int         `json:"completed_successfully"`
}

type measurements struct {
	postgres.Store
	mu        sync.Mutex
	claim     []float64
	wait      []float64
	executor  []float64
	finished  atomic.Int64
	succeeded atomic.Int64
	expected  int64
	done      chan struct{}
}

func (m *measurements) Claim(ctx context.Context, owner string, duration time.Duration) (postgres.Job, error) {
	start := time.Now()
	job, err := m.Store.Claim(ctx, owner, duration)
	if err == nil {
		m.mu.Lock()
		m.claim = append(m.claim, time.Since(start).Seconds()*1000)
		m.wait = append(m.wait, job.QueueWaitSeconds*1000)
		m.mu.Unlock()
	}
	return job, err
}

func (m *measurements) Finish(ctx context.Context, job postgres.Job, result json.RawMessage, runErr error, retryable bool, delay time.Duration) (postgres.Job, error) {
	finished, err := m.Store.Finish(ctx, job, result, runErr, retryable, delay)
	if err == nil && finished.Status != "scheduled" {
		if finished.Status == "succeeded" {
			m.succeeded.Add(1)
		}
		if m.finished.Add(1) == m.expected {
			close(m.done)
		}
	}
	return finished, err
}

func (m *measurements) Execute(ctx context.Context, kind string, payload json.RawMessage, attempt int) (json.RawMessage, error) {
	start := time.Now()
	result, err := executor.Execute(ctx, kind, payload, attempt)
	m.mu.Lock()
	m.executor = append(m.executor, time.Since(start).Seconds()*1000)
	m.mu.Unlock()
	return result, err
}

func summarize(values []float64) percentiles {
	if len(values) == 0 {
		return percentiles{}
	}
	sort.Float64s(values)
	at := func(p float64) float64 {
		index := int(math.Ceil(p*float64(len(values)))) - 1
		return values[max(0, index)]
	}
	return percentiles{P50: at(.5), P95: at(.95), P99: at(.99)}
}

func cpuSeconds(usage syscall.Rusage) float64 {
	return float64(usage.Utime.Sec+usage.Stime.Sec) +
		float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}

func run(ctx context.Context, db *sql.DB, jobs, producers, workers, payloadBytes int) (report, error) {
	if err := migrations.Apply(ctx, db); err != nil {
		return report{}, err
	}
	var existingJobs int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM jobs`).Scan(&existingJobs); err != nil {
		return report{}, err
	}
	if existingJobs != 0 {
		return report{}, fmt.Errorf("benchmark database must have no jobs; found %d", existingJobs)
	}
	var pgVersion string
	if err := db.QueryRowContext(ctx, `SHOW server_version`).Scan(&pgVersion); err != nil {
		return report{}, err
	}
	host, _ := os.Hostname()
	queue := "bench_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	base := postgres.Store{DB: db}
	m := &measurements{Store: base, expected: int64(jobs), done: make(chan struct{})}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := httptest.NewServer((api.Server{Store: base, Log: logger}).Handler())
	defer server.Close()
	workerCtx, stopWorkers := context.WithCancel(ctx)
	var workerGroup sync.WaitGroup
	for i := 0; i < workers; i++ {
		workerGroup.Add(1)
		go func(index int) {
			defer workerGroup.Done()
			(worker.Worker{
				Store: m, WorkerID: queue + "_" + strconv.Itoa(index), Log: logger,
				PollInterval: 10 * time.Millisecond, Execute: m.Execute,
			}).Run(workerCtx)
		}(i)
	}
	defer func() { stopWorkers(); workerGroup.Wait() }()

	var initialUsage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &initialUsage); err != nil {
		return report{}, err
	}
	latencies := make([]float64, jobs)
	body, err := json.Marshal(map[string]any{
		"queue": queue, "type": "checksum", "max_attempts": 1,
		"payload": map[string]string{"data": string(bytes.Repeat([]byte("x"), payloadBytes))},
	})
	if err != nil {
		return report{}, err
	}
	transport := &http.Transport{MaxIdleConns: producers, MaxIdleConnsPerHost: producers}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	indices := make(chan int)
	errorsCh := make(chan error, jobs)
	var producerGroup sync.WaitGroup
	var connectionPeak, lockWaitSamples, dbSamples atomic.Int64
	stopSample := make(chan struct{})
	sampleDone := make(chan struct{})
	var sampleOnce sync.Once
	stopSampling := func() {
		sampleOnce.Do(func() {
			close(stopSample)
			<-sampleDone
		})
	}
	go func() {
		defer close(sampleDone)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSample:
				return
			case <-ticker.C:
				sampleCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
				var connections, locks int64
				err := db.QueryRowContext(sampleCtx, `SELECT count(*),
				    count(*) FILTER (WHERE wait_event_type = 'Lock')
				    FROM pg_stat_activity WHERE datname = current_database()`).Scan(&connections, &locks)
				cancel()
				if err == nil {
					dbSamples.Add(1)
					if locks > 0 {
						lockWaitSamples.Add(1)
					}
					if connections > connectionPeak.Load() {
						connectionPeak.Store(connections)
					}
				}
			}
		}
	}()
	defer stopSampling()
	start := time.Now()
	for i := 0; i < producers; i++ {
		producerGroup.Add(1)
		go func() {
			defer producerGroup.Done()
			for index := range indices {
				request, err := http.NewRequestWithContext(ctx, http.MethodPost,
					server.URL+"/v1/jobs", bytes.NewReader(body))
				if err != nil {
					errorsCh <- err
					continue
				}
				request.Header.Set("Content-Type", "application/json")
				began := time.Now()
				response, err := client.Do(request)
				latencies[index] = time.Since(began).Seconds() * 1000
				if err != nil {
					errorsCh <- err
					continue
				}
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if response.StatusCode != http.StatusCreated {
					errorsCh <- fmt.Errorf("enqueue returned %s", response.Status)
				}
			}
		}()
	}
	for i := 0; i < jobs; i++ {
		indices <- i
	}
	close(indices)
	producerGroup.Wait()
	enqueueEnd := time.Now()
	close(errorsCh)
	if len(errorsCh) > 0 {
		return report{}, <-errorsCh
	}
	select {
	case <-m.done:
	case <-ctx.Done():
		return report{}, fmt.Errorf("completed %d of %d jobs: %w", m.finished.Load(), jobs, ctx.Err())
	}
	completionEnd := time.Now()
	stopSampling()
	stopWorkers()
	workerGroup.Wait()
	var finalUsage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &finalUsage); err != nil {
		return report{}, err
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	rows, err := db.QueryContext(ctx, `SELECT
	    extract(epoch FROM a.finished_at - a.started_at) * 1000
	    FROM job_attempts a JOIN jobs j ON j.id = a.job_id
	    WHERE j.queue_name = $1 AND a.status = 'succeeded'`, queue)
	if err != nil {
		return report{}, err
	}
	attemptTimes := make([]float64, 0, jobs)
	for rows.Next() {
		var value float64
		if err := rows.Scan(&value); err != nil {
			rows.Close()
			return report{}, err
		}
		attemptTimes = append(attemptTimes, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return report{}, err
	}
	rows.Close()
	if len(attemptTimes) != jobs || m.succeeded.Load() != int64(jobs) {
		return report{}, errors.New("not every job finished successfully")
	}
	m.mu.Lock()
	claimTimes := append([]float64(nil), m.claim...)
	queueWaits := append([]float64(nil), m.wait...)
	executorTimes := append([]float64(nil), m.executor...)
	m.mu.Unlock()
	peakRSS := finalUsage.Maxrss
	if runtime.GOOS == "linux" {
		peakRSS *= 1024
	}
	return report{
		RecordedAt: time.Now().UTC(), Host: host, GoVersion: runtime.Version(),
		PostgresVersion: pgVersion, OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(),
		Jobs: jobs, Producers: producers, Workers: workers, PayloadBytes: payloadBytes,
		EnqueueSeconds: enqueueEnd.Sub(start).Seconds(), CompletionSeconds: completionEnd.Sub(start).Seconds(),
		EnqueuePerSecond: float64(jobs) / enqueueEnd.Sub(start).Seconds(),
		JobsPerSecond:    float64(jobs) / completionEnd.Sub(start).Seconds(),
		EnqueueLatency:   summarize(latencies), ClaimLatency: summarize(claimTimes),
		QueueWait: summarize(queueWaits), ExecutorDuration: summarize(executorTimes),
		AttemptDuration:   summarize(attemptTimes),
		AppCPUSeconds:     cpuSeconds(finalUsage) - cpuSeconds(initialUsage),
		AppHeapAllocBytes: memory.Alloc, AppPeakRSSBytes: peakRSS,
		DBConnectionsPeak: int(connectionPeak.Load()), DBLockWaitSamples: int(lockWaitSamples.Load()),
		DBSamples:             int(dbSamples.Load()),
		CompletedSuccessfully: int(m.succeeded.Load()),
	}, nil
}

func main() {
	dsn := flag.String("database-url", os.Getenv("BENCH_DATABASE_URL"), "dedicated PostgreSQL benchmark database")
	jobs := flag.Int("jobs", 1000, "number of checksum jobs")
	producers := flag.Int("producers", 8, "concurrent HTTP producers")
	workers := flag.Int("workers", 4, "concurrent workers")
	payloadBytes := flag.Int("payload-bytes", 256, "checksum input size")
	timeout := flag.Duration("timeout", 3*time.Minute, "maximum benchmark duration")
	flag.Parse()
	if *dsn == "" || *jobs < 1 || *producers < 1 || *workers < 1 || *payloadBytes < 1 || *payloadBytes > 16*1024 || *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "set a benchmark database URL and positive jobs, producers, workers, payload bytes, and timeout")
		os.Exit(2)
	}
	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()
	db.SetMaxOpenConns(max(16, *producers+*workers+4))
	db.SetMaxIdleConns(max(8, *workers+2))
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	result, err := run(ctx, db, *jobs, *producers, *workers, *payloadBytes)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
