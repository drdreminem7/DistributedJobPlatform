CREATE TABLE queue_policies (
    queue_name TEXT PRIMARY KEY CHECK (queue_name ~ '^[A-Za-z0-9_-]{1,64}$'),
    concurrency_limit INTEGER CHECK (concurrency_limit > 0),
    rate_per_second INTEGER CHECK (rate_per_second > 0),
    burst INTEGER CHECK (burst > 0),
    tokens DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (tokens >= 0),
    refilled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((rate_per_second IS NULL) = (burst IS NULL))
);

CREATE TABLE workers (
    worker_id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL,
    process_id INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    stopped_at TIMESTAMPTZ
);

INSERT INTO queue_policies (queue_name)
SELECT DISTINCT queue_name FROM jobs ON CONFLICT DO NOTHING;

CREATE INDEX jobs_running_queue_idx ON jobs (queue_name, lease_expires_at)
    WHERE status = 'running';
CREATE INDEX workers_heartbeat_idx ON workers (last_heartbeat_at);
