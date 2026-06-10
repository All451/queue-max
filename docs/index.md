# Robusta Queue Documentation

Super robust task queue with SQLite sharding, rate limiting, and circuit breaker.

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
The main entry point. Manages shards, rate limiting, and circuit breaker.

### Worker
Processes jobs from the queue in a background thread.

### Shard
Each shard is an independent SQLite database file for true concurrent access.

### Rate Limiter
Token bucket algorithm that limits requests per minute across all workers.

### Circuit Breaker
Prevents cascading failures by failing fast when errors exceed threshold.

## API Reference

### Queue

- `Queue(shards, rate_limit, max_retries, data_dir)` - Create a queue
- `enqueue(payload, pagina_id, priority, max_retries)` - Enqueue a job
- `enqueue_batch(jobs)` - Enqueue multiple jobs
- `pop_job(worker_id)` - Atomically pop next job
- `complete_job(job_id, shard_id)` - Mark job as done
- `fail_job(job_id, shard_id, error, permanent)` - Mark job as failed
- `retry_failed_jobs(shard_id)` - Retry all failed jobs
- `cleanup_old_jobs(days)` - Remove old jobs
- `get_failed_jobs(limit)` - Get failed jobs
- `get_stats()` - Get queue statistics

### Worker

- `Worker(worker_id, process_function, queue)` - Create a worker
- `start()` - Start processing
- `stop(timeout)` - Graceful shutdown
- `get_stats()` - Get worker statistics

### Circuit Breaker States

- **CLOSED** - Normal operation, requests pass through
- **OPEN** - Requests blocked, failing fast
- **HALF_OPEN** - Testing if service recovered
