package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Snapshot struct {
	ActiveWorkers float64
	ActiveJobs    float64
	QueueDepth    float64
}

type Metrics struct {
	Registry         *prometheus.Registry
	enqueued         prometheus.Counter
	started          prometheus.Counter
	succeeded        prometheus.Counter
	failed           prometheus.Counter
	retried          prometheus.Counter
	deadLettered     prometheus.Counter
	cancelled        prometheus.Counter
	leaseExpirations prometheus.Counter
	activeWorkers    prometheus.Gauge
	activeJobs       prometheus.Gauge
	queueDepth       prometheus.Gauge
	queueWait        prometheus.Histogram
	execution        prometheus.Histogram
	pollDuration     prometheus.Histogram
}

func New() *Metrics {
	registry := prometheus.NewRegistry()
	m := &Metrics{
		Registry: registry,
		enqueued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobs_enqueued_total", Help: "Jobs committed by this API process.",
		}),
		started: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobs_started_total", Help: "Attempts claimed by this worker process.",
		}),
		succeeded: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobs_succeeded_total", Help: "Jobs completed successfully by this worker process.",
		}),
		failed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobs_failed_total", Help: "Jobs completed with a nonretryable failure by this worker process.",
		}),
		retried: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobs_retried_total", Help: "Attempts scheduled for retry by this worker process.",
		}),
		deadLettered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobs_dead_lettered_total", Help: "Jobs dead-lettered by this worker process.",
		}),
		cancelled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobs_cancelled_total", Help: "Jobs cancelled by this process.",
		}),
		leaseExpirations: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lease_expirations_total", Help: "Expired leases recovered or dead-lettered by this worker process.",
		}),
		activeWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "active_workers", Help: "Workers with a current heartbeat in PostgreSQL.",
		}),
		activeJobs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "active_jobs", Help: "Running jobs with an unexpired lease in PostgreSQL.",
		}),
		queueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "queue_depth", Help: "Queued and scheduled jobs in PostgreSQL.",
		}),
		queueWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "queue_wait_seconds", Help: "Time from eligibility to acquisition.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900},
		}),
		execution: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "execution_seconds", Help: "Executor run time, including unsuccessful attempts.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15},
		}),
		pollDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "poll_duration_seconds", Help: "Time spent in one claim call.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2},
		}),
	}
	registry.MustRegister(
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.enqueued, m.started, m.succeeded, m.failed, m.retried, m.deadLettered,
		m.cancelled, m.leaseExpirations, m.activeWorkers, m.activeJobs, m.queueDepth,
		m.queueWait, m.execution, m.pollDuration,
	)
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

func (m *Metrics) Enqueued() { m.enqueued.Inc() }

func (m *Metrics) Started(waitSeconds float64) {
	m.started.Inc()
	m.queueWait.Observe(waitSeconds)
}

func (m *Metrics) Finished(status string) {
	switch status {
	case "succeeded":
		m.succeeded.Inc()
	case "failed":
		m.failed.Inc()
	case "scheduled":
		m.retried.Inc()
	case "dead_lettered":
		m.deadLettered.Inc()
	case "cancelled":
		m.cancelled.Inc()
	}
}

func (m *Metrics) Cancelled() { m.cancelled.Inc() }

func (m *Metrics) CancelledN(count int) { m.cancelled.Add(float64(count)) }

func (m *Metrics) LeaseExpired(count int) {
	m.leaseExpirations.Add(float64(count))
}

func (m *Metrics) DeadLettered(count int) {
	m.deadLettered.Add(float64(count))
}

func (m *Metrics) ObserveExecution(seconds float64) {
	m.execution.Observe(seconds)
}

func (m *Metrics) ObservePoll(seconds float64) {
	m.pollDuration.Observe(seconds)
}

func (m *Metrics) SetSnapshot(snapshot Snapshot) {
	m.activeWorkers.Set(snapshot.ActiveWorkers)
	m.activeJobs.Set(snapshot.ActiveJobs)
	m.queueDepth.Set(snapshot.QueueDepth)
}
