# Queue Max — Documentation

Task queue with SQLite persistence, sharding, rate limiting, and circuit breaker.

See [Stress Test Results](stress-test.md) for throughput and resilience benchmarks.

## Quick Start

```python
from queue_max import Queue, Worker

queue = Queue()
queue.enqueue({"task": "example"})
worker = Worker("worker-1", lambda p: print(p), queue)
worker.start()
```

## Core Concepts

### Queue
The main entry point. Manages shards, rate limiting, circuit breaker, and event system.

### Worker
Processes jobs from the queue in a background thread. Supports sync, async, and pool with auto-scaling.

### Shard
Each shard is an independent SQLite database file (WAL mode). Enables true concurrent write access.

### Rate Limiter
Token bucket algorithm shared across all workers. Can be per-minute, per-second, or per-hour.

### Circuit Breaker
Three states: CLOSED (normal), OPEN (failing fast), HALF_OPEN (testing recovery). Automatically tracks job failures from `Queue.fail_job()` and resets on `Queue.complete_job()`.

## API Reference

### Queue

- `Queue(shards, rate_limit, max_retries, data_dir, circuit_breaker_threshold, circuit_breaker_timeout, rate_limiter_timeout)` — Create a queue
- `enqueue(payload, pagina_id, priority, max_retries)` — Enqueue a job
- `enqueue_batch(jobs)` — Enqueue multiple jobs grouped by shard
- `enqueue_from_file(filepath, fmt)` — Enqueue from JSONL or CSV
- `pop_job(worker_id)` — Atomically pop next job
- `complete_job(job_id, shard_id)` — Mark job as done (resets circuit breaker)
- `fail_job(job_id, shard_id, error, permanent)` — Mark job as failed (triggers circuit breaker)
- `retry_failed_jobs(shard_id)` — Retry all failed jobs
- `cleanup_old_jobs(days)` — Remove old completed/failed jobs
- `purge_queue(status)` — Purge all or filtered jobs
- `recover_orphans()` — Recover jobs stuck in processing
- `get_failed_jobs(limit)` — Get failed jobs across all shards
- `get_stats()` — Get queue + rate limiter + circuit breaker stats
- `on(event, callback)` — Register event listener
- `batch()` — Context manager that disables events

### Worker

- `Worker(worker_id, process_function, queue, poll_interval, job_timeout, callbacks)` — Create a worker
- `start()` / `stop(timeout)` — Lifecycle
- `get_stats()` — Processed, failed, retried, throughput, uptime

### Circuit Breaker States

- **CLOSED** — Normal operation
- **OPEN** — Requests blocked, failing fast
- **HALF_OPEN** — Testing if service recovered

### Events

- `job_enqueued(job_id, shard_id)`
- `job_completed(job_id, shard_id)`
- `job_failed(job_id, shard_id, error)`
- `job_retried(job_id, shard_id, error)`
- `alert(type, pending, threshold)`
