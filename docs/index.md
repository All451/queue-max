# Queue Max — Reference

Task queue with SQLite persistence, sharding, rate limiting, and circuit breaker.

See [README](../README.md) for full documentation and [Stress Test Results](stress-test.md) for benchmarks.

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
Main entry point. Manages shards, rate limiting, circuit breaker, and event system.

```python
Queue(shards, rate_limit, max_retries, data_dir,
      circuit_breaker_threshold, circuit_breaker_timeout,
      rate_limiter_timeout)
```

### Shard
Each shard is an independent SQLite database file in WAL mode. Default 6 shards.
Jobs are routed by `pagina_id % num_shards` or randomly.

### Worker
Processes jobs from the queue in a background thread. Supports sync and async functions.

```python
Worker(worker_id, process_function, queue, poll_interval, job_timeout)
AsyncWorker("id", async_func, queue)
WorkerPool([workers], auto_scale=True, min_workers=2, max_workers=20)
```

### Rate Limiter
Token bucket algorithm shared across all workers.

```python
RateLimiter(rate_limit=160, unit=RateLimitUnit.PER_MINUTE)
```

### Circuit Breaker
Three states: CLOSED → OPEN → HALF_OPEN → CLOSED. Tracks failures from `fail_job()`,
resets on `complete_job()`.

```python
CircuitBreaker(failure_threshold=5, recovery_timeout=60)
```

## Queue Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `enqueue(payload, pagina_id, priority, max_retries)` | `{id, shard_id}` | Enqueue a job |
| `enqueue_batch(jobs)` | `{total}` | Batch enqueue (1 transaction per shard) |
| `enqueue_from_file(path, fmt)` | `{total}` | Enqueue from JSONL or CSV |
| `pop_job(worker_id)` | `Job` or `None` | Atomically pop next job |
| `complete_job(job_id, shard_id)` | — | Mark as done (resets circuit breaker) |
| `fail_job(job_id, shard_id, error, permanent)` | — | Mark as failed (triggers circuit breaker) |
| `retry_failed_jobs(shard_id)` | `int` | Retry all failed jobs |
| `cleanup_old_jobs(days)` | `int` | Remove old completed/failed jobs |
| `purge_queue(status)` | `int` | Purge all or filtered jobs |
| `recover_orphans()` | `int` | Recover stuck processing jobs |
| `heartbeat(shard_id, worker_id)` | — | Update worker heartbeat |
| `get_stats()` | `dict` | Queue + rate limiter + circuit breaker stats |
| `get_failed_jobs(limit)` | `list[Job]` | Failed jobs across all shards |
| `get_processing_jobs()` | `list[Job]` | Currently processing jobs |
| `get_pending_count()` | `int` | Total pending jobs |
| `is_empty()` | `bool` | No pending jobs |
| `wait_until_empty(timeout)` | `bool` | Block until empty |
| `wait_for_jobs(count, timeout)` | `bool` | Block until N jobs queued |
| `on(event, callback)` | `Queue` | Register event listener |
| `batch()` | context | Disable events temporarily |

## Events (built-in)

| Event | Payload |
|-------|---------|
| `job_enqueued` | `job_id, shard_id` |
| `job_completed` | `job_id, shard_id` |
| `job_failed` | `job_id, shard_id, error` |
| `job_retried` | `job_id, shard_id, error` |
| `alert` | `type, pending, threshold` |

## Events (typed, with bubus)

Requires: `pip install queue-max[events]`

```python
from queue_max.contrib.events import QueueEventBus, JobCompleted

queue = Queue()
events = QueueEventBus(queue)

@events.on(JobCompleted)
def handler(event: JobCompleted):
    print(f"Done: {event.job_id}")

# Wait for event
event = events.expect(JobEnqueued, timeout=30)

# Metrics
events.get_metrics()
events.log_tree()
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `NUM_SHARDS` | 6 | Number of shards |
| `RATE_LIMIT_MAX` | 160 | Requests per minute |
| `QUEUE_MAX_RETRIES` | 3 | Max retries after first failure |
| `CIRCUIT_FAILURE_THRESHOLD` | 5 | Failures before circuit opens |
| `CIRCUIT_TIMEOUT` | 60 | Seconds before recovery |
| `DB_BUSY_TIMEOUT` | 30000 | SQLite busy timeout (ms) |
| `DATA_DIR` | ./data | Shard file directory |
| `HEARTBEAT_INTERVAL` | 5000 | Heartbeat interval (ms) |
| `STUCK_TIMEOUT` | 30000 | Orphan timeout (ms) |
| `CLEANUP_DAYS` | 7 | Job cleanup age |
| `QUEUE_MAX_LOG_LEVEL` | WARNING | Log level |

## CLI

```bash
queue-max stats [--json] [--shard N]
queue-max worker --function MODULE:FUNC [--workers N]
queue-max enqueue --payload '{"task":"test"}' [--priority N] [--json]
queue-max list --status failed|processing|all [--limit N]
queue-max retry [--shard N] [--job-id N]
queue-max purge --days N
```
