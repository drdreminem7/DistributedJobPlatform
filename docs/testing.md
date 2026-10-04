# Testing strategy

| Layer | Command or location | What it checks |
| --- | --- | --- |
| Pure Go unit tests | `go test ./internal/...` | Executor validation and results; retry delay bounds. |
| PostgreSQL integration | `TEST_DATABASE_URL=... go test -race ./...` | API and storage state changes, concurrent claims, fencing, scheduling, rate and concurrency limits, metrics, cancellation, and worker behavior. |
| Fault injection | `tests/integration/crash_test.go`, `failure_test.go` | Killed API and worker processes, response-loss windows, temporary database connection loss, stale leases, panic containment, rollback, and shutdown timeout. |
| SQL plan experiment | `benchmarks/*.sql` | Large-backlog claim plans and index use at 100,000 and 1,000,000 synthetic jobs. |
| End-to-end load | `go run ./cmd/loadtest` | Throughput and latency distributions for HTTP enqueue and real worker execution on a dedicated database. |
| Container smoke test | `deploy/compose.yml` | Image build, database readiness, API health, worker completion, two-worker registration, queue-policy CLI, Prometheus targets, Grafana dashboard provisioning, and backup restore into a second database. |

The GitHub Actions workflow starts a disposable PostgreSQL service, runs `go vet ./...` and the full race-enabled test suite, validates Compose syntax, and builds the application image. Integration tests truncate job, attempt, queue-policy, and worker tables and must never target a live database. The load driver also needs a dedicated empty database.

The fault tests shorten or force lease expiry to reach recovery paths quickly. They prove the state transition rather than a production recovery-time target. The connection-outage test blocks the worker's database path; it does not stop and restore the PostgreSQL server. A multi-host network partition, backup restore, and failover remain separate deployment exercises. Details and outcomes are in the [failure model](failure-model.md).
