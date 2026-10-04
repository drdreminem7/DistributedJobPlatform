ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs DROP CONSTRAINT jobs_job_type_check;
ALTER TABLE jobs DROP CONSTRAINT jobs_check;

ALTER TABLE jobs ADD COLUMN priority SMALLINT NOT NULL DEFAULT 0 CHECK (priority BETWEEN 0 AND 100);
ALTER TABLE jobs ADD COLUMN scheduled_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE jobs ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 1 CHECK (max_attempts >= 1);
ALTER TABLE jobs ADD COLUMN idempotency_key TEXT;
ALTER TABLE jobs ADD COLUMN request_hash TEXT;
ALTER TABLE jobs ADD COLUMN cancel_requested_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN replay_count INTEGER NOT NULL DEFAULT 0 CHECK (replay_count >= 0);

ALTER TABLE jobs ADD CONSTRAINT jobs_status_check CHECK (
    status IN ('queued', 'scheduled', 'running', 'succeeded', 'failed', 'cancelled', 'dead_lettered')
);
ALTER TABLE jobs ADD CONSTRAINT jobs_job_type_check CHECK (
    job_type IN ('checksum', 'sleep', 'unstable')
);
ALTER TABLE jobs ADD CONSTRAINT jobs_check CHECK (
    (status IN ('succeeded', 'failed', 'cancelled', 'dead_lettered')) = (completed_at IS NOT NULL)
);
ALTER TABLE jobs ADD CONSTRAINT jobs_idempotency_hash_check CHECK (
    (idempotency_key IS NULL) = (request_hash IS NULL)
);

CREATE UNIQUE INDEX jobs_idempotency_key_idx ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

DROP INDEX jobs_queued_oldest_idx;
CREATE INDEX jobs_queued_priority_idx ON jobs
    (priority DESC, scheduled_at ASC, created_at ASC, id ASC) WHERE status = 'queued';
CREATE INDEX jobs_scheduled_due_idx ON jobs
    (scheduled_at ASC, priority DESC, created_at ASC, id ASC) WHERE status = 'scheduled';

CREATE TABLE job_attempts (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    lease_token BIGINT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('running', 'succeeded', 'failed', 'retry_scheduled', 'dead_lettered', 'cancelled', 'lease_expired')
    ),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    UNIQUE (job_id, attempt_number)
);

CREATE INDEX job_attempts_job_idx ON job_attempts (job_id, attempt_number DESC);
