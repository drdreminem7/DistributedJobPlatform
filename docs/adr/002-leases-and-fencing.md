# ADR 002: Leases and fencing tokens

## Context

A row lock protects a claim transaction, but cannot stay open for the length of a task. A worker may crash or pause after claiming; another worker must eventually recover the job without allowing the old worker to overwrite its result.

## Decision

Store the lease owner, expiry, and a monotonically increasing token on each job. Increment the token on every claim. Renew and finish only when the caller still owns the unexpired token. Reclaim after expiry while attempt budget remains.

## Alternatives

A permanent row lock would hold a database transaction across execution. A heartbeat alone describes worker liveness but does not establish job ownership. A lease without a token cannot distinguish successive owners that reuse an ID.

## Consequences

Worker death is recoverable and stale database completion is rejected. A claim consumes an attempt even if its worker dies immediately. Execution can overlap around lease expiry, and the token does not fence an arbitrary external side effect; downstream operations need idempotency or their own fencing support.
