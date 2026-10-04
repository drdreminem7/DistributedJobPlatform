# Container deployment

The deployment Compose file builds one nonroot image for the API, worker, and queue-policy CLI. PostgreSQL stays on the internal Compose network. The API, Prometheus, and Grafana bind to loopback on the host. This stack is intended for a single-host demonstration and operational testing.

## Start

From the repository root:

```sh
cp deploy/.env.example deploy/.env
```

Edit `deploy/.env` before starting. Set the same URL-encoded PostgreSQL password in `POSTGRES_PASSWORD` and `DATABASE_URL`; change `GRAFANA_ADMIN_PASSWORD` too. Choose `API_HOST_PORT` if port 8080 is occupied. Then run:

```sh
docker compose --env-file deploy/.env -f deploy/compose.yml up -d --build
docker compose --env-file deploy/.env -f deploy/compose.yml ps
curl -fsS http://127.0.0.1:8080/readyz
```

Replace 8080 in the `curl` command if `API_HOST_PORT` differs. Docker waits for PostgreSQL's health check before starting the API and worker. Both apply pending migrations under a PostgreSQL advisory lock. The API has a readiness-based container health check. A worker registers itself and updates its heartbeat; inspect `GET /v1/workers` to check it.

Start the optional monitoring services with:

```sh
docker compose --env-file deploy/.env -f deploy/compose.yml --profile observability up -d
```

Prometheus listens on `127.0.0.1:9090`; Grafana listens on `127.0.0.1:3000`. The container scrape configuration targets the API and one worker service. If you scale workers, configure discovery or one target per worker to collect every process-local counter. Queue and worker gauges already reflect PostgreSQL-wide state and should not be summed across targets.

## Operate

Increase execution capacity with independent workers:

```sh
docker compose --env-file deploy/.env -f deploy/compose.yml up -d --scale worker=3
```

Each worker executes one job at a time. Queue policies still apply across all workers. Check API readiness, queue counts, and heartbeats:

```sh
curl -fsS http://127.0.0.1:8080/readyz
curl -fsS http://127.0.0.1:8080/v1/queues
curl -fsS http://127.0.0.1:8080/v1/workers
```

Set a shared queue policy from the application image:

```sh
docker compose --env-file deploy/.env -f deploy/compose.yml run --rm worker \
  /app/queuectl -queue default -concurrency 3 -rate 5 -burst 10
```

That command starts a short-lived worker-service container only to run the CLI. It does not start its worker loop. To stop the stack while retaining the database volume:

```sh
docker compose --env-file deploy/.env -f deploy/compose.yml --profile observability down
```

Do not add `--volumes` unless you intend to delete the database.

## Backup and upgrades

Take a consistent PostgreSQL dump before an upgrade:

```sh
docker compose --env-file deploy/.env -f deploy/compose.yml exec -T db \
  pg_dump -U jobs -d jobs -Fc > jobs.dump
```

Store the dump securely and test restoration into a separate database:

```sh
docker compose --env-file deploy/.env -f deploy/compose.yml exec -T db \
  createdb -U jobs jobs_restore
docker compose --env-file deploy/.env -f deploy/compose.yml exec -T db \
  pg_restore -U jobs -d jobs_restore < jobs.dump
docker compose --env-file deploy/.env -f deploy/compose.yml exec -T db \
  psql -U jobs -d jobs_restore -c 'SELECT count(*) FROM jobs'
```

Use a fresh restore database name for each drill. The application image runs migrations at startup; migration 006 builds a claim-order index inside a transaction. On a large existing jobs table this can delay startup and block writes. Schedule that migration during a maintenance window and test its duration on a copy of production-sized data. There is no automatic down-migration or database rollback. Preserve the prior application image and a tested backup before changing versions.

The API and worker startup database and migration deadline is ten seconds. A migration that exceeds it fails startup and rolls back its transaction; schedule large schema changes separately rather than relying on automatic startup migration. Workers stop taking new work on SIGTERM and have a 20-second shutdown budget by default (`WORKER_SHUTDOWN_TIMEOUT` overrides it). An interrupted job remains recoverable after its lease expires.

## Boundary

The provided Compose stack is loopback-only and uses a local `.env` file. It has no API authentication, TLS termination, secret manager, managed PostgreSQL failover, or backup schedule. Do not expose its API or monitoring ports directly to untrusted networks. See [security](security.md) and [known limitations](limitations.md) before adapting it to another environment.
