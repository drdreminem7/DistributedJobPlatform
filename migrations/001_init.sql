CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name TEXT NOT NULL CHECK (queue_name ~ '^[A-Za-z0-9_-]{1,64}$'),
    job_type TEXT NOT NULL CHECK (job_type IN ('checksum', 'sleep')),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    result JSONB,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    CHECK ((status IN ('succeeded', 'failed')) = (completed_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS jobs_queued_oldest_idx
    ON jobs (created_at ASC, id ASC) WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS jobs_recent_idx
    ON jobs (created_at DESC, id DESC);
