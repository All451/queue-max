# Queue Max

Task queue library with SQLite persistence, sharding, rate limiting, and circuit breaker.

No Redis or RabbitMQ required. **Zero external dependencies** (except `typing-extensions`).

```python
from queue_max import Queue, Worker

queue = Queue()
queue.enqueue({"task": "send_email", "to": "user@example.com"})

Worker("worker-1", lambda p: print(p), queue).start()
```

## Installation

```bash
pip install queue-max
```

With framework integrations:

```bash
pip install queue-max[django]
pip install queue-max[fastapi]
pip install queue-max[flask]
```

## Quick Start

### Basic enqueue + worker

```python
from queue_max import Queue, Worker
import time

queue = Queue(shards=3, rate_limit=100)

def process(payload: dict) -> str:
    print(f"Processing: {payload}")
    return "done"

# Enqueue jobs
for i in range(5):
    queue.enqueue({"task": f"job-{i}"}, priority=i % 3)

# Process with a worker
worker = Worker("example", process, queue, poll_interval=0.1)
worker.start()
time.sleep(3)
worker.stop()

stats = queue.get_stats()
print(f"Processed: {stats.get('pending')} pending")
```

### With decorator

```python
from queue_max import task

@task(priority=2, max_retries=3)
def send_email(to: str, subject: str):
    return send(to, subject)

# Enqueue for background processing
send_email.delay("user@example.com", "Hello")

# Schedule for later
from datetime import datetime, timezone, timedelta
future = datetime.now(timezone.utc) + timedelta(minutes=5)
send_email.schedule_at(future, "user@example.com", "Scheduled!")

# Parallel processing
send_email.map(["a@b.com", "c@d.com"], "Welcome!")
```

## Features

**SQLite Persistence** — Jobs are stored in SQLite with WAL mode. No external services needed.

**Physical Sharding** — Multiple `.db` files (default 6) allow true concurrent write access. Each shard is an independent database.

**Rate Limiting** — Token bucket algorithm shared across all workers. Configurable per minute/second/hour.

**Circuit Breaker** — Three-state (CLOSED → OPEN → HALF_OPEN → CLOSED). Automatically tracks job failures: after N consecutive failures the circuit opens, rejecting new jobs. Recovers after a timeout. Resets on success.

**Retry with Backoff** — Exponential backoff (base × 2^(n-1)) with ±20% jitter. Transient errors (5xx, timeout, 429) retry automatically. Client errors (4xx except 429) are permanent.

**Heartbeat and Orphan Recovery** — Workers send heartbeats every 5s. Jobs with stale heartbeats (>30s) are auto-recovered as pending.

**Priority Queues** — Three levels: low (0), medium (1), high (2). Ordering is per-shard.

**CLI** — Built-in commands for stats, workers, enqueue, retry, purge, and listing.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `NUM_SHARDS` | 6 | Number of shard databases |
| `RATE_LIMIT_MAX` | 160 | Max requests per minute |
| `QUEUE_MAX_RETRIES` | 3 | Max retry attempts after first failure |
| `CIRCUIT_FAILURE_THRESHOLD` | 5 | Consecutive failures before circuit opens |
| `CIRCUIT_TIMEOUT` | 60 | Seconds before circuit recovery attempt |
| `DB_BUSY_TIMEOUT` | 30000 | SQLite busy timeout (ms) |
| `DATA_DIR` | ./data | Directory for shard databases |
| `HEARTBEAT_INTERVAL` | 5000 | Worker heartbeat interval (ms) |
| `STUCK_TIMEOUT` | 30000 | Orphan job timeout (ms) |
| `CLEANUP_DAYS` | 7 | Age threshold for job cleanup |
| `QUEUE_MAX_LOG_LEVEL` | WARNING | Log level |

Full list in [.env.example](.env.example).

## API Reference

### Queue

```python
from queue_max import Queue

queue = Queue(
    shards=None,           # Number of shards (default: NUM_SHARDS or 6)
    rate_limit=None,       # Requests per minute (default: RATE_LIMIT_MAX or 160)
    max_retries=None,      # Max retries after first failure (default: QUEUE_MAX_RETRIES or 3)
    data_dir=None,         # Directory for shard files (default: DATA_DIR or ./data)
    circuit_breaker_threshold=None,  # Failures before circuit opens (default: 5)
    circuit_breaker_timeout=None,    # Seconds before recovery (default: 60)
    rate_limiter_timeout=5.0,        # Seconds to wait for rate limit token
)

# Enqueue
queue.enqueue(payload, pagina_id=None, priority=0, max_retries=None)
queue.enqueue_batch([{"payload": {...}, "pagina_id": 1, "priority": 2}, ...])
queue.enqueue_from_file("jobs.jsonl", fmt="jsonl")

# Process
job = queue.pop_job(worker_id)       # Returns Job or None
queue.complete_job(job_id, shard_id)
queue.fail_job(job_id, shard_id, error, permanent=False)

# Management
queue.retry_failed_jobs(shard_id=None)
queue.cleanup_old_jobs(days=7)
queue.purge_queue(status=None)
queue.recover_orphans()
queue.heartbeat(shard_id, worker_id)
queue.wait_until_empty(timeout=None)
queue.get_failed_jobs(limit=100)
queue.get_processing_jobs()
stats = queue.get_stats()

# Events
queue.on("job_completed", lambda job_id, shard_id: print(f"Done: {job_id}"))
queue.on("alert", lambda type, pending, threshold: print(f"Alert: {pending} pending"))

# Context manager (auto-close)
with Queue() as queue:
    queue.enqueue({"task": "example"})
```

### Job

```python
from queue_max import Job, JobStatus, JobPriority

# Job properties
job.id              # int — job ID (unique per shard)
job.payload         # dict — job data
job.status          # JobStatus — PENDING, PROCESSING, COMPLETED, FAILED, CANCELLED
job.priority_int    # int — 0, 1, or 2
job.shard_id        # int — which shard holds this job
job.tentativas      # int — attempt count
job.max_tentativas  # int — max attempts allowed
job.last_error      # str or None
job.worker_id       # str or None
job.created_at      # ISO timestamp
job.next_retry_at   # ISO timestamp or None

# Convenience checks
job.is_pending      # bool
job.is_processing   # bool
job.is_completed    # bool
job.is_failed       # bool
job.can_retry       # bool — tentativas < max_tentativas
job.remaining_retries  # int
job.age_seconds     # float or None
```

### Worker

```python
from queue_max import Worker, AsyncWorker, WorkerPool

# Basic worker
worker = Worker(
    worker_id="worker-1",
    process_function=my_func,  # Callable[[dict], Any]
    queue=queue,
    poll_interval=1.0,
    job_timeout=None,  # Max seconds per job execution
    on_job_start=None,
    on_job_complete=None,
    on_job_error=None,
)
worker.start()
worker.stop(timeout=10.0)
stats = worker.get_stats()
# stats: worker_id, state, processed, failed, retried, throughput_jobs_per_hour, ...

# Async worker (for coroutines)
async def async_process(payload):
    await some_api(payload)
    return "ok"

async_worker = AsyncWorker("async-1", async_process, queue)
async_worker.start()

# Worker pool with auto-scaling
pool = WorkerPool(
    workers=[worker1, worker2],
    auto_scale=True,
    min_workers=2,
    max_workers=20,
    scale_up_threshold=100,   # Add worker when pending > 100
    scale_down_threshold=10,  # Remove worker when pending < 10
)
pool.start_all()
pool.stop_all()
pool.wait_for_idle(timeout=30)
```

### Rate Limiter

```python
from queue_max import RateLimiter, RateLimitUnit

limiter = RateLimiter(rate_limit=10, unit=RateLimitUnit.PER_SECOND)
limiter.acquire(timeout=5.0)        # Blocks until token available
limiter.try_acquire()               # Non-blocking
limiter.get_remaining_tokens()      # float
limiter.update_rate_limit(20)       # Change limit dynamically
limiter.reset()
```

### Circuit Breaker

```python
from queue_max import CircuitBreaker

cb = CircuitBreaker(failure_threshold=5, recovery_timeout=60.0)
cb.state         # CircuitState: CLOSED, OPEN, or HALF_OPEN
cb.is_allowed()  # Check if request can pass through
cb.call(func)    # Execute with CB protection (raises CircuitBreakerOpenError)
cb.reset()
cb.get_stats()
```

### Decorators

```python
from queue_max import task, periodic_task, retryable_task

@task(priority=2, max_retries=5, timeout=30)
def process_order(order_id: int):
    """.delay() enqueues, direct call executes synchronously."""
    ...

# Schedule
process_order.delay(42)                       # Background
process_order.schedule_in(300, 42)            # In 5 minutes
process_order.schedule_at(datetime(...), 42)  # At specific time
process_order.map([101, 102, 103], coupon=1)  # Parallel

@periodic_task(interval=3600, priority=1)
def cleanup_old_data():
    """Runs every hour."""
    ...

# cleanup_old_data.start_scheduler()

@retryable_task(max_retries=5, retry_on=[TimeoutError])
def fetch_external_data(url: str):
    """Retries synchronously on timeout."""
    ...
```

## Retry & Failure Flow

When a job fails, the system checks if the error is retryable:

| Error | Retryable? | Behavior |
|-------|-----------|----------|
| 4xx (except 429) | No | Permanent failure → Dead Letter Queue |
| 429, 5xx, timeout | Yes | Retry with exponential backoff + jitter |
| ConnectionError | Yes | Retry with exponential backoff + jitter |

**Backoff formula**: `base × 2^(attempt-1)` ± 20% jitter, capped at 3600s.

**Circuit breaker** tracks ALL job failures. After N consecutive failures (default 5), the circuit opens and `pop_job()` returns `None`. Workers stop polling until the recovery timeout expires, then one job is let through (HALF_OPEN). On success, the circuit closes.

```python
queue = Queue(max_retries=2, circuit_breaker_threshold=3)

# With max_retries=2, a job gets 3 total attempts:
#   Attempt 1 → fail → retry (backoff ~120s)
#   Attempt 2 → fail → retry (backoff ~240s)
#   Attempt 3 → fail → Dead Letter Queue

# With circuit_breaker_threshold=3, after 3 permanent failures:
#   Circuit OPENS → pop_job() returns None for all workers
#   After timeout → HALF_OPEN → one job allowed through
#   If success → CLOSED (normal operation resumes)
```

## CLI

```bash
# Stats
queue-max stats
queue-max stats --json --shard 0

# Start workers
queue-max worker --function mymodule:myfunction --workers 4

# Enqueue
queue-max enqueue --payload '{"task":"test"}' --priority 2 --json

# List & manage
queue-max list --status failed --limit 20
queue-max list --status processing
queue-max retry
queue-max retry --shard 0 --job-id 42
queue-max purge --days 7
```

## Framework Integrations

### Django

```python
# settings.py
INSTALLED_APPS = ["queue_max.contrib.django", ...]
QUEUE_MAX = {"SHARDS": 4, "RATE_LIMIT": 160}

# tasks.py
from queue_max.contrib.django import task

@task
def my_task(user_id):
    ...
```

Management commands: `python manage.py queue_worker`, `queue_stats`, `queue_purge`.

### FastAPI

```python
from fastapi import FastAPI
from queue_max.contrib.fastapi import QueueMiddleware

app = FastAPI()
app.add_middleware(QueueMiddleware, max_workers=4)
```

### Flask

```python
from flask import Flask
from queue_max.contrib.flask import QueueExtension

app = Flask(__name__)
queue = QueueExtension(app)

@queue.task
def my_task():
    ...
```

## Performance

| Scenario | Config | Throughput |
|----------|--------|-----------|
| Burst | 20 workers, 10 shards | **~3.300 jobs/sec** |
| Contention | 10 workers, 1 shard | **~1.660 jobs/sec** |
| 30% failure | 8 workers, max_retries=2 | **Stable** — circuit breaker adequate |

- Max queue size: 1M+ jobs per shard
- [Detailed stress test results](docs/stress-test.md)

## Running Tests

```bash
pip install -e ".[all]"
PYTHONPATH=src pytest tests/ -v
```

## License

MIT
