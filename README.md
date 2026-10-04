# Durable Distributed Job Platform

A distributed background job service built with Go and PostgreSQL. An HTTP API stores jobs durably; independent workers claim, execute, and recover them. PostgreSQL is the queue, scheduler, lease ledger, and source of truth.

The platform supports scheduled jobs, priority aging, bounded retries, cancellation, idempotent submission, dead-letter replay, queue-wide concurrency and rate limits, worker heartbeats, structured logs, and Prometheus metrics. Its delivery model is **at least once**: a worker crash can cause a task to execute again, while lease tokens prevent an outdated worker from overwriting the stored result.

## Architecture

```mermaid
flowchart LR
    Client[HTTP client] --> API[Job API]
    API --> DB[(PostgreSQL)]
    DB <--> W1[Worker]
    DB <--> W2[Worker]
    W1 --> E1[Executor]
    W2 --> E2[Executor]
    API --> M[Prometheus metrics]
    W1 --> M
    W2 --> M
```

Workers select eligible jobs with `FOR UPDATE SKIP LOCKED`. A claim records an attempt, owner, lease expiry, and fencing token in one transaction. Workers renew leases while executing; another worker can reclaim a job after expiry. Retryable failures use capped exponential backoff with jitter. Retryable work that exhausts its attempt budget moves to `dead_lettered` for inspection and explicit replay.

Scheduling and recovery need no separate coordinator. Workers query due scheduled jobs and expired leases directly. Claim order gives waiting jobs one effective priority point per minute, using an indexed rank to avoid sorting the backlog on each claim.

## Quick start with Docker

From the repository root, copy the local deployment settings, change the passwords, and start PostgreSQL, the API, and one worker:

```sh
cp deploy/.env.example deploy/.env
docker compose --env-file deploy/.env -f deploy/compose.yml up -d --build
curl -fsS http://127.0.0.1:8080/readyz
```

The API binds to loopback on port 8080 by default. Set `API_HOST_PORT` in `deploy/.env` if that port is occupied. [Deployment](docs/deployment.md) covers scaling workers, health checks, backups, and the container stack's limits.

Submit a checksum job and list its status:

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"checksum","payload":{"data":"abc"},"idempotency_key":"example-1"}'

curl -sS http://127.0.0.1:8080/v1/jobs
```

The create response contains the job ID and a `Location` header. Fetch that URL to see the result and attempt count. The built-in executors are `checksum`, `sleep`, and `unstable`; `unstable` is useful for trying retries:

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"unstable","payload":{"fail_until_attempt":2},"max_attempts":3}'
```

## Run from source

Requires Go 1.27.1 or newer and Docker with Compose. Start the development database, then launch the API and worker in separate terminals:

```sh
docker compose up -d db
DATABASE_URL='postgres://jobs:jobs@localhost:5433/jobs?sslmode=disable' go run ./cmd/api
```

```sh
DATABASE_URL='postgres://jobs:jobs@localhost:5433/jobs?sslmode=disable' go run ./cmd/worker
```

Both processes apply pending schema migrations on startup. The API uses port 8080 by default; set `PORT` to change it. Run more worker processes to increase execution capacity.
## API and operations

| Endpoint | Purpose |
| --- | --- |
| `POST /v1/jobs` | Submit a job, optionally with `queue`, `priority`, `scheduled_at`, `max_attempts`, and `idempotency_key`. |
| `GET /v1/jobs` | List jobs. |
| `GET /v1/jobs/{id}` | Read job state and result. |
| `GET /v1/jobs/{id}/attempts` | Inspect execution history. |
| `POST /v1/jobs/{id}/cancel` | Cancel queued work or request cancellation of a running job. |
| `POST /v1/jobs/{id}/replay` | Replay a dead-lettered job with an added attempt budget. |
| `GET /v1/queues` | View queue counts and limits. |
| `GET /v1/workers` | View worker heartbeats. |
| `GET /healthz`, `GET /readyz` | Process health and database readiness. |
| `GET /metrics` | Prometheus metrics. |

The default queue is `default`; priority ranges from 0 to 100, and `scheduled_at` is an RFC 3339 timestamp. Jobs default to three total attempts. Reusing an idempotency key with the same request returns the original job; changing the request returns `409`. Submission idempotency does not deduplicate a task's external effects.

Queue policies are shared by all workers because they live in PostgreSQL. Set a concurrency cap and token-bucket rate limit with:

```sh
DATABASE_URL='postgres://jobs:jobs@localhost:5433/jobs?sslmode=disable' \
  go run ./cmd/queuectl -queue default -concurrency 3 -rate 5 -burst 10
```

The command replaces the queue's policy. Zero disables a limit. See [queue controls](docs/queue-controls.md) for the exact counting and refill rules.

## Observability and performance

The API serves metrics at `:8080/metrics`. Workers serve metrics at their logged addresses. Start the container monitoring stack with:

```sh
docker compose --env-file deploy/.env -f deploy/compose.yml --profile observability up -d
```

Prometheus is exposed at `localhost:9090` and Grafana at `localhost:3000`; Grafana's local password is set in `deploy/.env`. The [observability guide](docs/observability.md) explains the metrics and dashboard.

![Grafana dashboard showing queue depth, worker activity, and latency](docs/images/dashboard.png)

In one local 5,000-job benchmark with eight producers and four workers, the indexed claim order completed **2,903 jobs/s**, versus **825 jobs/s** with the original backlog sort. Both runs completed every job. These are single-host measurements, not deployment capacity estimates. The [performance report](docs/performance.md) includes p50/p95/p99 latencies, PostgreSQL plans, resource use, and reproduction steps.

![Local benchmark comparison of completion throughput and p95 claim duration](benchmarks/results/performance.png)

## Tests

Run unit tests with `go test ./...`. Integration tests require a disposable PostgreSQL database because they truncate job, attempt, policy, and worker tables:

```sh
docker compose exec db psql -U jobs -d jobs -c 'CREATE DATABASE jobs_test'
TEST_DATABASE_URL='postgres://jobs:jobs@localhost:5433/jobs_test?sslmode=disable' \
  go test -race ./...
```

The integration suite covers concurrent claims, retries, scheduling, cancellation, queue limits, worker crashes, lease expiry, stale completions, and temporary database connection failures. See the [failure model](docs/failure-model.md) for the injected faults and observed recovery.

## Guarantees and scope

Only the current, unexpired lease owner can renew or finish a job. A lease protects PostgreSQL state; it cannot prevent a previous worker from repeating an external effect after a crash. Tasks that call external systems need their own idempotency or downstream fencing. A job can finish as `succeeded`, `failed`, `cancelled`, or `dead_lettered` depending on execution and its attempt budget.

The included executors are deterministic demonstrations. The local Compose stack provides a reproducible single-host setup; it does not provide API authentication, TLS termination, a backup schedule, or multi-host deployment. See [architecture](docs/architecture.md), [delivery semantics](docs/delivery-semantics.md), [state machine](docs/state-machine.md), [database design](docs/database.md), [testing strategy](docs/testing.md), [security](docs/security.md), and [known limitations](docs/limitations.md) for the precise behavior and boundaries.
