EXPLAIN (ANALYZE, BUFFERS, SETTINGS)
SELECT id, queue_name, status,
       CASE WHEN status = 'running' THEN lease_expires_at
            ELSE greatest(scheduled_at, created_at) END
FROM jobs
WHERE status IN ('queued', 'scheduled', 'running')
  AND (status = 'queued'
   OR (status = 'scheduled' AND scheduled_at <= clock_timestamp())
   OR (status = 'running' AND lease_expires_at <= clock_timestamp()
       AND attempt_count < max_attempts AND cancel_requested_at IS NULL))
  AND NOT (queue_name = ANY(ARRAY[]::text[]))
ORDER BY priority * 60 - extract(epoch FROM
    ((CASE WHEN status = 'running' THEN lease_expires_at
           ELSE greatest(scheduled_at, created_at) END) AT TIME ZONE 'UTC')) DESC,
    CASE WHEN status = 'running' THEN lease_expires_at
         ELSE greatest(scheduled_at, created_at) END ASC, id ASC
FOR UPDATE SKIP LOCKED LIMIT 1;
