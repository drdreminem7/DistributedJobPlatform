# Security considerations

The HTTP API, worker metrics endpoint, and Grafana demo stack are for local or trusted-network use. The API has no authentication or authorization. A caller who can reach it can submit jobs, inspect payloads and results, cancel work, and replay dead letters. Bind it to loopback or put an authenticated gateway with TLS in front of it before sharing access. Queue rate limits govern worker claims; they do not protect the submission endpoint from abuse.

The API limits request bodies to 64 KiB, validates job types and payloads, checks queue and idempotency-key formats, and uses parameterized SQL. It does not implement tenants, per-user quotas, payload encryption, or per-queue permissions. The idempotency key namespace is global. Treat payloads, results, errors, and PostgreSQL backups as potentially sensitive data.

The container image runs as a nonroot user. API and worker containers have a read-only filesystem and `no-new-privileges`; PostgreSQL uses an internal network with no host port in the deployment Compose file. The example `.env` contains local demonstration values and is ignored by Git. Replace its credentials before use, avoid committing real secrets, and use a secret manager for a wider deployment. Environment variables and container metadata can reveal credentials to administrators of the host.

Structured logs omit payloads and idempotency keys, but they include job and worker identifiers and error context. Restrict access to logs, Prometheus, and Grafana. The supplied monitoring configuration scrapes one worker; collecting metrics from a scaled worker fleet needs explicit service discovery. A worker lease fences PostgreSQL writes, not calls to external systems. Side-effecting executors must use a downstream idempotency key or another fencing mechanism.

The repository does not establish a production security claim. Review dependency updates, network policy, database privileges, backups, recovery, and monitoring before deployment outside a trusted environment.
