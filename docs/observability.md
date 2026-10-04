# Observability

The API exposes `GET /metrics` on its normal HTTP port. Each worker also exposes `GET /metrics`. A worker picks a free local port by default and logs the address as `worker metrics listening`. Set `METRICS_ADDR=:9091` when a fixed port is needed. Each worker needs its own address.

The endpoints expose Prometheus counters for committed enqueues, claims, successful jobs, terminal failures, scheduled retries, dead letters, cancellations, and expired leases. Histograms record time from job eligibility to claim, executor run time, and the duration of each claim call. Queue wait starts at the later of creation and scheduled time; after a lease expires, it starts at lease expiry. The API and workers write structured JSON logs with job, queue, worker, attempt, and lease context at the relevant transitions. Job payloads and idempotency keys are not logged.

`active_workers`, `active_jobs`, and `queue_depth` are PostgreSQL snapshots refreshed on each metrics request. Queue depth includes future scheduled jobs. The same cluster-wide gauges appear on every process, so use `max` rather than `sum` when querying them across targets. Counters and histograms are local to each process; sum their rates across API and worker targets. They reset on process restart. A crash after a database commit can leave one event absent from a local counter, so the job and attempt rows remain the source of truth for audits.

The container dashboard uses one API and one worker. Set the passwords in `deploy/.env`, then start the stack with:

```sh
cp deploy/.env.example deploy/.env
docker compose --env-file deploy/.env -f deploy/compose.yml --profile observability up -d --build
```

Prometheus is available at `http://localhost:9090`; Grafana is at `http://localhost:3000` with the password from `deploy/.env`. The provisioned **Distributed Job Platform** dashboard shows queue depth, worker and job counts, throughput, latency percentiles, retry and failure rates, and lease expiry. The container configuration scrapes `api:8080` and `worker:9091`. Add worker targets or service discovery when scaling beyond one worker. The development Compose file at the repository root instead scrapes host-run API and worker processes through `host.docker.internal`.

![Grafana dashboard with two workers and a disposable 100-job workload](images/dashboard.png)

The screenshot was captured from the running container stack after submitting 100 checksum jobs under a five-claims-per-second queue policy. Prometheus reported both configured targets as healthy.

The HTTP metrics endpoint is unauthenticated, like the rest of the API. This setup is for local development. Tracing, alert rules, and a production scrape/discovery setup are not included.
