# ADR 003: Worker-driven scheduling

## Context

Scheduled jobs, retries, and expired leases all have timestamps that determine when they become eligible. A separate scheduler would need its own coordination and recovery path.

## Decision

Workers query due scheduled jobs and expired leases directly. They order eligible work by priority plus continuous waiting age. A partial index stores the equivalent rank and tie-break order, letting a claim read from the index instead of sorting the backlog.

## Alternatives

A scheduler process could move due jobs into a ready queue, but would add a new process and transition. Strict priority is simpler but can starve low-priority jobs. The former stepwise aging expression required scanning and sorting a large eligible set.

## Consequences

There is one recovery path and no scheduler singleton. Polling still costs database work when no jobs are ready. The rank index costs storage and writes, including lease renewals, and a large future backlog can require scanning past ineligible entries. The [performance report](../performance.md) records the measured tradeoff.
