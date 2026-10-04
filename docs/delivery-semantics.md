# Delivery semantics

A committed job is attempted while a worker and PostgreSQL are available. Each claim consumes one of its `max_attempts` slots. A retryable executor error schedules another attempt with capped exponential backoff and jitter. An expired lease can also be reclaimed. When the final attempt fails or expires, the job is dead-lettered for inspection and manual replay.

Execution may happen more than once. A worker can finish an external effect and die before it records success. Another worker may later claim the expired job. The lease token prevents the first worker from changing PostgreSQL state after a newer claim; it cannot undo or deduplicate an external effect.

An idempotency key covers job **submission**. Repeating the same request with the same key returns its original job. Reusing the key for different input returns `409`. It does not make execution or external effects idempotent.

A running job with a cancellation request becomes `cancelled` when its worker acknowledges the request. If that worker dies, a later worker finalizes cancellation after the old lease expires. It does not spend another attempt executing the cancelled job.

`POST /v1/jobs/{id}/replay` is an operator action, not proof that the previous attempt had no effect. Check the attempt history and any downstream system before replaying a side-effecting job.

The platform does not promise exactly-once execution or exactly-once external effects. Its attempt budget also means a job can end in `failed`, `cancelled`, or `dead_lettered` without succeeding.
