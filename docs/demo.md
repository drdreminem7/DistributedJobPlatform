# Five-minute local demo

Start the container stack from the repository root after setting passwords in `deploy/.env`:

```sh
cp deploy/.env.example deploy/.env
docker compose --env-file deploy/.env -f deploy/compose.yml up -d --build
curl -fsS http://127.0.0.1:8080/readyz
```

Submit a job, copy its returned ID, and inspect the completed result and attempt:

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"checksum","payload":{"data":"demo"},"idempotency_key":"demo-checksum"}'

curl -sS http://127.0.0.1:8080/v1/jobs/REPLACE_WITH_ID
curl -sS http://127.0.0.1:8080/v1/jobs/REPLACE_WITH_ID/attempts
```

Repeat the POST with the same key to show that it returns the original job. Then submit an `unstable` job with `fail_until_attempt:2` and `max_attempts:3` to show retry history. Use `GET /v1/queues` and `GET /v1/workers` to inspect queue state and worker heartbeats.

Start the monitoring stack and open Grafana at `http://127.0.0.1:3000` with the password set in `deploy/.env`:

```sh
docker compose --env-file deploy/.env -f deploy/compose.yml --profile observability up -d
```

The **Distributed Job Platform** dashboard shows queue depth, throughput, and latency. To demonstrate independent workers, scale to two and read `GET /v1/workers` again:

```sh
docker compose --env-file deploy/.env -f deploy/compose.yml up -d --scale worker=2
curl -sS http://127.0.0.1:8080/v1/workers
```

Stop without deleting the PostgreSQL volume:

```sh
docker compose --env-file deploy/.env -f deploy/compose.yml --profile observability down
```
