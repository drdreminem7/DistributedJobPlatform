# Requirements and scope

## Functional requirements

- Accept bounded, validated JSON jobs through an HTTP API and return an ID only after the database commit.
- Read jobs, attempts, queue counts, queue policies, and worker heartbeats.
- Execute allow-listed tasks with independent worker processes and recover work after worker death.
- Support due-time scheduling, aging priority order, retry delays, cancellation, dead-letter replay, and bounded attempt budgets.
- Deduplicate identical submissions by idempotency key and reject conflicting reuse of a key.
- Enforce shared per-queue concurrency and rate limits across workers.
- Expose liveness, readiness, structured logs, and Prometheus metrics.

## Correctness requirements

- A claim must atomically choose a job, assign a lease, record an attempt, and apply queue policy changes.
- Only the current unexpired lease owner and token may renew or finish a job.
- A worker must not execute future scheduled jobs or reclaim a job with a pending cancellation request.
- Terminal results and attempt history must remain inspectable after a process restart.
- Execution is at least once while workers and PostgreSQL are available and attempts remain; external effects may repeat.

## Operating constraints

- PostgreSQL is the source of truth. API and workers use parameterized SQL and serialized schema migrations.
- Each worker runs one job at a time. Queue policy limits are shared in PostgreSQL.
- The built-in tasks are deterministic local demonstrations. The local Compose deployment binds the API to loopback.
- A wider deployment needs authentication, TLS termination, backup recovery, and a production monitoring setup. See [security](security.md) and [known limitations](limitations.md).
