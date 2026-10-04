# Job states

| State | Meaning |
| --- | --- |
| `queued` | Ready for a worker. |
| `scheduled` | Available when `scheduled_at` has passed. This also holds retries. |
| `running` | Owned by a worker with a lease and token. |
| `succeeded` | Completed successfully. |
| `failed` | Stopped after a nonretryable error. |
| `cancelled` | Cancelled before or during execution. |
| `dead_lettered` | Retryable work used its attempt budget. |

## Transitions

| From | Event | To |
| --- | --- | --- |
| `queued` or due `scheduled` | Worker claim | `running` |
| Expired `running` with attempts left and no cancel request | New worker claim | `running` with a new token |
| Expired `running` on final attempt and no cancel request | Worker sweep | `dead_lettered` |
| `running` | Success | `succeeded` |
| `running` | Retryable error, attempts left | `scheduled` |
| `running` | Retryable error, no attempts left | `dead_lettered` |
| `running` | Nonretryable error | `failed` |
| `queued` or `scheduled` | Cancel | `cancelled` |
| `running` | Cancel request, then worker acknowledgment | `cancelled` |
| Expired `running` with a cancel request | Worker sweep | `cancelled` |
| `dead_lettered` | Manual replay with added attempts | `queued` |

The worker queries due scheduled jobs directly. There is no separate scheduler process. Claim order gives each eligible job one effective priority point per minute of waiting. A past client-supplied schedule does not add waiting age before creation.

`attempt_count` increases on every claim, including crash recovery. `max_attempts` is the total claim budget. Replay adds a fresh budget without resetting `attempt_count`, and old `job_attempts` rows remain available.

Only the current, unexpired lease owner and token may renew or finish a running job. Terminal jobs have `completed_at`; running jobs have lease ownership. Cancellation of a running job sets `cancel_requested_at` first. The worker checks it during renewal and stops a cooperative executor. If the worker dies, a later poll finalizes cancellation after lease expiry. If completion commits before the cancel request, cancellation returns a conflict.
