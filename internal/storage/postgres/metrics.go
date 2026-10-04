package postgres

import (
	"context"

	"distributedjobplatform/internal/observability"
)

func (s Store) MetricSnapshot(ctx context.Context) (observability.Snapshot, error) {
	var snapshot observability.Snapshot
	var activeWorkers, activeJobs, queueDepth int64
	err := s.DB.QueryRowContext(ctx, `SELECT
	    (SELECT count(*) FROM workers WHERE stopped_at IS NULL
	        AND last_heartbeat_at > now() - interval '10 seconds'),
	    (SELECT count(*) FROM jobs WHERE status = 'running'
	        AND lease_expires_at > now()),
	    (SELECT count(*) FROM jobs WHERE status IN ('queued', 'scheduled'))`).
		Scan(&activeWorkers, &activeJobs, &queueDepth)
	snapshot.ActiveWorkers = float64(activeWorkers)
	snapshot.ActiveJobs = float64(activeJobs)
	snapshot.QueueDepth = float64(queueDepth)
	return snapshot, err
}
