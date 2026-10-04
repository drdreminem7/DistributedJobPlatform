# Failure assumptions and crash windows

PostgreSQL is the durable primary. The API, worker processes, their connections, and HTTP clients may fail independently. Database time decides lease and schedule eligibility. A successful database commit survives an application restart; recovery from loss of PostgreSQL itself depends on backups or a separate high-availability design.

| Failure point | Persisted result | Recovery |
| --- | --- | --- |
| Before enqueue commit | No job exists. | The client may retry. |
| After enqueue commit, before response | The job exists, but the client may not know its ID. | Retry with the same idempotency key and request. |
| Before claim commit | Job and rate token remain unchanged. | Another worker can claim normally. |
| After claim commit, before completion | Attempt and lease exist; a rate token is spent. | A worker can reclaim after expiry if attempts remain. |
| After an external effect, before completion commit | The effect may have happened without a stored success. | Later execution may repeat it; downstream idempotency is required. |
| After completion commit | Terminal job and attempt rows persist. | A stale worker cannot replace the result. |

A cancelled running job can finish before the cancellation commits; their transaction order determines the result. If the cancellation commits first and the worker dies, another worker finalizes `cancelled` after lease expiry without starting another attempt. A worker that loses database connectivity during renewal cancels its local task and leaves the row for expiry-based recovery.

A queue concurrency cap counts unexpired running leases. A paused worker can still be performing an external effect after its lease expires, so the cap does not bound every possible side effect. Heartbeat staleness is a visibility signal, not an ownership change. The [fault-injection report](failure-model.md) records observed behavior for these cases.
