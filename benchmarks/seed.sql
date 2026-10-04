TRUNCATE job_attempts, jobs, queue_policies, workers;

INSERT INTO queue_policies (queue_name) VALUES ('bench_sql');

INSERT INTO jobs (queue_name, job_type, payload, status, priority, scheduled_at, max_attempts)
SELECT 'bench_sql', 'checksum', '{"data":"x"}'::jsonb,
       CASE WHEN n % 10 = 0 THEN 'scheduled' ELSE 'queued' END,
       (n % 101)::smallint,
       CASE WHEN n % 10 = 0 THEN now() + interval '1 hour' ELSE now() END,
       1
FROM generate_series(1, :rows) AS n;

ANALYZE jobs;

SELECT count(*) AS jobs,
       pg_size_pretty(pg_relation_size('jobs')) AS table_size,
       pg_size_pretty(pg_indexes_size('jobs')) AS index_size,
       pg_size_pretty(pg_total_relation_size('jobs')) AS total_size
FROM jobs;
