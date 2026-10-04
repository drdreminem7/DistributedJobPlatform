# Database design

PostgreSQL owns job state, scheduling, queue policies, and worker visibility. The API and workers serialize embedded migrations with a transaction-scoped advisory lock. `schema_migrations` records applied versions.

```mermaid
erDiagram
    jobs ||--o{ job_attempts : has
    jobs {
        uuid id PK
        text queue_name
        text job_type
        jsonb payload
        text status
        int priority
        timestamptz scheduled_at
        int attempt_count
        int max_attempts
        text idempotency_key
        text request_hash
        text lease_owner
        bigint lease_token
        timestamptz lease_expires_at
        timestamptz cancel_requested_at
    }
    job_attempts {
        bigint id PK
        uuid job_id FK
        int attempt_number
        text worker_id
        bigint lease_token
        text status
        timestamptz started_at
        timestamptz finished_at
    }
    queue_policies {
        text queue_name PK
        int concurrency_limit
        int rate_per_second
        int burst
        float tokens
        timestamptz refilled_at
    }
    workers {
        text worker_id PK
        text hostname
        int process_id
        timestamptz last_heartbeat_at
        timestamptz stopped_at
    }
```

`job_attempts.job_id` has the only declared foreign key in this diagram. Queue names and worker IDs in other tables are logical references: the claim transaction creates a queue-policy row as needed, and a worker record is for observation rather than ownership. Deleting a job cascades to its attempts.

One claim transaction selects an eligible row with `FOR UPDATE SKIP LOCKED`, checks the queue policy, increments `attempt_count` and `lease_token`, records an attempt, and spends a rate token. A failed transaction rolls all of that back. The row lock ends at commit; the owner, token, and expiry guard later renewals and completion. Jobs and attempts are updated together when an attempt finishes.

The unique partial index on nonnull `idempotency_key` prevents duplicate submissions for one key. The claim-rank partial index covers active statuses and orders by continuous priority aging, eligible time, and ID. Other indexes support due scheduled jobs, expired leases, queue-wide active counts, attempt history, and recent-job reads. The rank index uses disk and increases write work on status and lease changes; see [performance](performance.md).
