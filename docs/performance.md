# Performance report

## Method

`cmd/loadtest` starts the real HTTP handler and worker loop in one Go process, backed by a dedicated PostgreSQL database. Eight HTTP producers enqueue 5,000 checksum jobs with 256-byte input; four workers execute them. The driver records latency distributions, completion throughput, application CPU and memory, and sampled database connections and lock waits. It checks that every job succeeded. The database is emptied before each run.

The runs below used a 10-core, 16 GiB Apple Silicon Mac, Go 1.27.1, and PostgreSQL 18.1 with 128 MB shared buffers on a local Unix socket. The runner, API, workers, and database shared one host. Percentiles use the nearest-rank method. The saved raw reports are in [benchmarks/results](../benchmarks/results).

| Measure | Original claim order | Indexed claim order |
| --- | ---: | ---: |
| Completed jobs | 5,000 | 5,000 |
| Completion time | 6.06 s | 1.72 s |
| Completion throughput | 825 jobs/s | 2,903 jobs/s |
| Enqueue throughput | 8,725 jobs/s | 8,928 jobs/s |
| Enqueue p50 / p95 / p99 | 0.69 / 1.99 / 4.41 ms | 0.70 / 1.90 / 3.65 ms |
| Successful claim p50 / p95 / p99 | 4.36 / 8.15 / 8.74 ms | 0.73 / 2.71 / 4.42 ms |
| Queue wait p50 / p95 / p99 | 4.00 / 5.44 / 5.48 s | 0.77 / 1.13 / 1.16 s |
| Executor p50 / p95 / p99 | 0.006 / 0.016 / 0.036 ms | 0.005 / 0.013 / 0.048 ms |
| Attempt duration p50 / p95 / p99 | 0.25 / 0.61 / 1.35 ms | 0.21 / 0.85 / 1.62 ms |
| Application CPU | 3.61 s | 3.09 s |
| Peak application RSS | 27.1 MB | 26.3 MB |
| Peak sampled DB connections | 13 | 13 |
| Samples with a DB lock wait | 2 / 24 | 6 / 6 |

The completion throughput was 3.5 times higher in these two runs. This is a local observation, not a capacity guarantee. The shorter optimized run also produced fewer database samples, so the lock-wait counts are not directly comparable. Application CPU excludes PostgreSQL and does not identify database CPU cost.

![Completion throughput and p95 claim duration in the local benchmark](../benchmarks/results/performance.png)

The plot is generated from the two saved JSON reports with `Rscript benchmarks/plot.R`. A PDF version is saved alongside the PNG.

## Bottleneck and change

The original query calculated `priority + floor(wait_seconds / 60)` for every eligible job, then sorted the entire set. This made each claim scan and sort the backlog. The new rank uses continuous waiting time: `priority * 60 - eligible_at_epoch`. A common current-time term cancels when comparing jobs, so the rank can be indexed. It still gives one priority point per minute of waiting, although jobs close to a minute boundary can now swap order relative to the former stepwise calculation. A new partial index covers the active statuses and the complete rank and tie-break order. Scheduled jobs still become claimable only at their due time. Expired running jobs still require an available attempt and no pending cancellation.

An `EXPLAIN (ANALYZE, BUFFERS)` experiment seeded 90% queued and 10% future-scheduled jobs, with no workers. At 100,000 jobs, the original claim took 147 ms; the indexed query took 0.336 ms. At 1,000,000 jobs, the original query took 1,757 ms and spilled 61.7 MB to a disk sort. The indexed query took 0.625 ms using `jobs_claim_rank_idx` with no sort. The one-million-job index occupied 56 MB; the jobs table and all indexes occupied 363 MB after adding it, versus 307 MB before. See the saved [SQL experiments](../benchmarks) and [plans](../benchmarks/results).

The index increases storage and write work for enqueue, status changes, and lease renewal. The index is created by migration 006 inside the migration transaction, so applying it to a large existing table can hold locks and delay startup. The benchmark does not measure that migration time or database CPU. These results were single runs on one host, with tiny deterministic executors and no external services. They do not establish performance under network latency, large payloads, mixed queues, heavy retries, long jobs, or a much larger worker fleet. A large high-priority future backlog may also force the index scan past ineligible rows. Repeat tests on deployment-sized hardware and workload before setting service limits.

## Reproduce

Use a disposable database. `benchmarks/seed.sql` truncates tables. The load driver requires an empty jobs table and leaves its jobs in the database. Create the benchmark database once, then truncate it before each load run.

```sh
docker compose exec db psql -U jobs -d jobs -c 'CREATE DATABASE bench'
export BENCH_DATABASE_URL='postgres://jobs:jobs@localhost:5433/bench?sslmode=disable'
go run ./cmd/loadtest -jobs 5000 -producers 8 -workers 4 -payload-bytes 256

psql "$BENCH_DATABASE_URL" -v rows=1000000 -f benchmarks/seed.sql
psql "$BENCH_DATABASE_URL" -f benchmarks/claim-plan-ranked.sql
```

`claim-plan.sql` and `rank-index.sql` document the original query and the one-time index experiment. Migration 006 now creates the index automatically; do not run `rank-index.sql` against a migrated database.
