# Known limitations and next work

- The built-in executors perform local deterministic tasks. A real side-effecting executor needs a downstream idempotency key or fencing protocol because execution can repeat.
- The HTTP API and metrics endpoints have no authentication or TLS. The supplied stack binds exposed ports to loopback and is intended for trusted local use.
- Each worker executes one job at a time. The queue-wide concurrency limit counts unexpired leases, not work an old process may still be doing after expiry.
- The container monitoring configuration scrapes one worker target. A scaled worker fleet needs per-instance discovery for complete process counters.
- PostgreSQL is a single primary in the supplied stack. Automated failover, backup schedules, and restore drills are not included.
- Startup migrations are serialized and have a ten-second deadline. Building the claim-rank index on a large existing table needs a planned maintenance window.
- The benchmark uses deterministic tasks on one host. Its throughput is not a multi-host capacity claim; mixed workloads and large payloads need new measurements.
- Multi-tenant isolation, ingress quotas, distributed tracing, and alert rules are not implemented. The connection-outage tests do not cover a full PostgreSQL server restart or a network partition between hosts.

The [security notes](security.md), [deployment guide](deployment.md), and [failure model](failure-model.md) explain the current operational boundary.
