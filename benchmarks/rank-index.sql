CREATE INDEX jobs_claim_rank_idx ON jobs (
    (priority * 60 - extract(epoch FROM
        ((CASE WHEN status = 'running' THEN lease_expires_at
               ELSE greatest(scheduled_at, created_at) END) AT TIME ZONE 'UTC'))) DESC,
    (CASE WHEN status = 'running' THEN lease_expires_at
          ELSE greatest(scheduled_at, created_at) END) ASC,
    id ASC
) WHERE status IN ('queued', 'scheduled', 'running');
