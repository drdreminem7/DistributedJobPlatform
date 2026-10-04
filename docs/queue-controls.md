# Queue controls and workers

Each queue may have a concurrency limit and a token bucket. A queue with no configured limits remains unrestricted. The settings live in PostgreSQL, so all worker processes use the same values.

The concurrency limit counts `running` jobs with unexpired leases. A claim locks the chosen job row, then the queue policy row, and checks the current active count. Competing claims for the same queue serialize on the policy row. When a queue is full, the worker releases that candidate and checks other queues. Completion frees a slot when its transaction commits. An expired lease no longer counts as active, even if the old process is still executing; lease fencing protects the stored result but cannot stop an external effect.

The rate limit applies to claims per second. The policy row stores the current token balance and last refill time. A claim refills up to `burst` using database time and consumes one token in the same transaction as the job claim. If the worker dies immediately after claiming, that token remains spent. A failed transaction spends none. This is a queue-wide limit, not a per-process counter.

Set or replace a policy with database credentials:

```sh
DATABASE_URL='postgres://jobs:jobs@localhost:5433/jobs?sslmode=disable' \
  go run ./cmd/queuectl -queue email -concurrency 3 -rate 5 -burst 10
```

Zero disables a limit. `-rate` and `-burst` must be set together. The command replaces the whole policy. `GET /v1/queues` reports the limits plus queued, scheduled, active, expired, and dead-lettered counts.

Workers register when they start, update a heartbeat every two seconds, and mark themselves stopped after graceful shutdown. `GET /v1/workers` considers a worker healthy when its heartbeat is less than ten seconds old and it has not stopped. A missing heartbeat is a visibility signal; job recovery depends on lease expiry, not on this flag.

The worker currently executes one job at a time. Claim order uses an effective priority that gains one point per minute since the job became eligible. The waiting clock starts at the later of creation and scheduled time, so a client-supplied timestamp in the past cannot grant an immediate boost. A low-priority job eventually outranks newer high-priority jobs; jobs already waiting ahead of it still need to drain. A retry or manual replay receives a fresh scheduled time. The continuous rank is indexed, so workers scan in fairness order without sorting the backlog. The index adds storage and write cost; policy rows and active-job counts still add database work to each claim. See the [performance report](performance.md) before raising throughput targets.
