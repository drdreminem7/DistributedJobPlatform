# ADR 001: PostgreSQL as the queue

## Context

The job API needs a durable record, atomic state changes, history, and a way for independent workers to compete without executing the same active lease. Adding a broker would also require coordinating broker acknowledgements with database writes.

## Decision

Use PostgreSQL for both the job record and work selection. Workers claim with `FOR UPDATE SKIP LOCKED`; job, attempt, and queue-policy changes commit together.

## Alternatives

Redis or a dedicated message broker could offer different throughput and delivery tools, but would add another consistency boundary for this workload. An in-memory queue cannot preserve jobs across process restarts.

## Consequences

The system has one durable source of truth and can explain recovery from database rows. PostgreSQL becomes the throughput and availability dependency. Claim queries, indexes, vacuum pressure, and backup recovery need measurement and operational care. A broker can be reconsidered if a representative workload exceeds these limits.
