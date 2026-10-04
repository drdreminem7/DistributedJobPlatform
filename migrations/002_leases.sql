ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_owner TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_token BIGINT NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;

UPDATE jobs
SET lease_owner = 'legacy-v0', lease_token = 1, lease_expires_at = now()
WHERE status = 'running' AND lease_owner IS NULL;

ALTER TABLE jobs ADD CONSTRAINT jobs_running_has_lease CHECK (
    (status = 'running') = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
);

CREATE INDEX jobs_expired_lease_idx
    ON jobs (lease_expires_at ASC, id ASC) WHERE status = 'running';
