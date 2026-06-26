# Queue Max

<p align="center">
  <a href="https://pypi.org/project/queue-max/"><img src="https://img.shields.io/pypi/v/queue-max?color=blue" alt="PyPI version"></a>
  <a href="https://pypi.org/project/queue-max/"><img src="https://img.shields.io/pypi/pyversions/queue-max" alt="Python versions"></a>
  <a href="https://github.com/All451/queue-max/actions"><img src="https://img.shields.io/github/actions/workflow/status/All451/queue-max/ci.yml?branch=main" alt="CI status"></a>
  <img src="https://img.shields.io/badge/coverage-97%25-brightgreen" alt="Coverage 97%">
  <a href="https://github.com/All451/queue-max/blob/main/LICENSE"><img src="https://img.shields.io/github/license/All451/queue-max" alt="License MIT"></a>
  <img src="https://img.shields.io/badge/dependencies-0-success" alt="Zero dependencies">
  <img src="https://img.shields.io/badge/stress-500k%20jobs%20%7C%200%20duplicates-brightgreen" alt="500k jobs, 0 duplicates">
</p>

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

Optional extras:

```bash
pip install queue-max[events]    # Typed events with bubus + Pydantic
pip install queue-max[django]    # Django management commands
pip install queue-max[fastapi]   # FastAPI middleware
pip install queue-max[flask]     # Flask extension
```

## Architecture

```
                    ┌──────────────────────────────────────────────────────────┐
                    │                        Queue                            │
                    │  ┌──────────┐  ┌────────────┐  ┌───────────┐  ┌──────┐  │
                    │  │Connection│  │  ShardRepo  │  │RateLimiter│  │Circuit│  │
                    │  │ Manager  │  │ (CRUD+Retry)│  │   Token   │  │Breaker│  │
                    │  └────┬─────┘  └──────┬─────┘  └───────────┘  └──────┘  │
                    └───────┼────────────────┼────────────────────────────────┘
                            │                │
          ┌──────────────────┼────────────────┼──────────────────┐
          ▼                  ▼                ▼                  ▼
    ┌──────────┐      ┌──────────┐      ┌──────────┐      ┌──────────┐
    │ shard_0  │      │ shard_1  │      │ shard_N  │      │constants │
    │ fila.db  │      │ fila.db  │      │ fila.db  │      │ schema   │
    │   WAL    │      │   WAL    │      │   WAL    │      │ SQL      │
    └──────────┘      └──────────┘      └──────────┘      └──────────┘
          │                  │                  │
          └──────────────────┼──────────────────┘
                             ▼
                   ┌───────────────────┐
                   │  Worker (thread)  │
                   │  AsyncWorker      │
                   │  WorkerPool       │
                   └───────────────────┘
```

Each shard is an independent SQLite file in WAL mode. Workers scan shard groups in random order to distribute load.

## Features

**SQLite Persistence** — Jobs are stored in SQLite with WAL mode. No external services needed.

**Physical Sharding** — Multiple `.db` files (default 6) allow true concurrent write access. Each shard is an independent database with its own lock.

**Rate Limiting** — Token bucket algorithm shared across all workers. Configurable per minute/second/hour.

**Circuit Breaker** — Three-state (CLOSED → OPEN → HALF_OPEN → CLOSED). Automatically tracks job failures from `Queue.fail_job()` and resets on `Queue.complete_job()`.

**Retry with Backoff** — Exponential backoff (base × 2^(n-1)) with ±20% jitter. Transient errors (5xx, timeout, 429) retry automatically. Client errors (4xx except 429) are permanent.

**Dead Letter Queue** — Jobs that exhaust all retries are moved to a DLQ table for inspection.

**Heartbeat and Orphan Recovery** — Workers send heartbeats every 5s. Jobs with stale heartbeats (>30s) are recovered as pending.

**Priority Queues** — Three levels: low (0), medium (1), high (2). Ordering is per-shard.

**Event System** — Simple callbacks built-in. Optional typed events with bubus + Pydantic (`queue-max[events]`).

**CLI** — Built-in commands for stats, workers, enqueue, retry, purge, and listing.

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
print(f"Stats: {stats}")
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
| `CACHE_SIZE` | 10000 | SQLite cache size (pages) |
| `MMAP_SIZE` | 268435456 | Memory-mapped I/O size (bytes) |
| `HEARTBEAT_INTERVAL` | 5000 | Worker heartbeat interval (ms) |
| `STUCK_TIMEOUT` | 30000 | Orphan job timeout (ms) |
| `RECOVERY_INTERVAL` | 10000 | Orphan recovery check interval (ms) |
| `CLEANUP_DAYS` | 7 | Age threshold for job cleanup |
| `QUEUE_ALERT_THRESHOLD` | 1000 | Pending jobs before alert event |
| `QUEUE_MAX_LOG_LEVEL` | WARNING | Log level |

Full list in [.env.example](.env.example).

## API Reference

### Queue

```python
from queue_max import Queue

queue = Queue(
    shards=None,                          # Number of shards (default: NUM_SHARDS or 6)
    rate_limit=None,                      # Requests per minute (default: RATE_LIMIT_MAX or 160)
    max_retries=None,                     # Max retries after first failure (default: QUEUE_MAX_RETRIES or 3)
    data_dir=None,                        # Directory for shard files (default: DATA_DIR or ./data)
    circuit_breaker_threshold=None,       # Failures before circuit opens (default: 5)
    circuit_breaker_timeout=None,         # Seconds before recovery (default: 60)
    rate_limiter_timeout=5.0,             # Seconds to wait for rate limit token
)

# ── Enqueue ──
queue.enqueue(payload, pagina_id=None, priority=0, max_retries=None)
queue.enqueue_batch([{"payload": {...}, "pagina_id": 1, "priority": 2}, ...])  # pagina_id for consistent sharding
queue.enqueue_from_file("jobs.jsonl", fmt="jsonl")

# ── Process ──
job = queue.pop_job(worker_id)       # Returns Job or None
queue.complete_job(job_id, shard_id)
queue.fail_job(job_id, shard_id, error, permanent=False)

# ── Management ──
queue.retry_failed_jobs(shard_id=None)
queue.cleanup_old_jobs(days=7)
queue.purge_queue(status=None)       # "pending", "failed", "processing", or None (all)
queue.recover_orphans()
queue.heartbeat(shard_id, worker_id)
queue.wait_until_empty(timeout=None)
queue.get_failed_jobs(limit=100)
queue.get_processing_jobs()
stats = queue.get_stats()

# ── Events ──
queue.on("job_completed", lambda job_id, shard_id: print(f"Done: {job_id}"))
queue.on("alert", lambda type, pending, threshold: print(f"Alert: {pending}"))

# ── Context manager (auto-close) ──
with Queue() as queue:
    queue.enqueue({"task": "example"})
```

### Queue Stats

```python
stats = queue.get_stats()
# {
#     "pending": int,
#     "processing": int,
#     "failed": int,
#     "num_shards": int,
#     "rate_limit": int,
#     "max_retries": int,
#     "circuit_state": "closed" | "open" | "half_open",
#     "circuit_failures": int,
#     "tokens_available": float,
#     "uptime_seconds": float,
#     "is_healthy": bool,
# }
```

### Job

```python
from queue_max import Job, JobStatus, JobPriority

# Job properties
job.id                # int — job ID (unique per shard)
job.payload           # dict — job data
job.partition_key     # int or None — consistent shard routing key
job.status            # JobStatus — PENDING, PROCESSING, COMPLETED, FAILED, CANCELLED
job.priority_int      # int — 0, 1, or 2
job.shard_id          # int — which shard holds this job
job.attempts          # int — attempt count
job.max_attempts      # int — max attempts allowed = max_retries + 1
job.last_error        # str or None
job.error_type        # str or None
job.worker_id         # str or None
job.created_at        # ISO timestamp
job.next_retry_at     # ISO timestamp or None (retry scheduled for the future)
job.started_at        # ISO timestamp or None
job.completed_at      # ISO timestamp or None

# Convenience checks
job.is_pending        # bool
job.is_processing     # bool
job.is_completed      # bool
job.is_failed         # bool
job.is_cancelled      # bool
job.is_terminal       # bool — completed, failed, or cancelled
job.can_retry         # bool — attempts < max_attempts
job.remaining_retries # int
job.age_seconds       # float or None
job.processing_time_seconds  # float or None
```

### Worker

```python
from queue_max import Worker, AsyncWorker, WorkerPool

# Basic worker
worker = Worker(
    worker_id="worker-1",
    process_function=my_func,          # Callable[[dict], Any]
    queue=queue,
    poll_interval=1.0,                 # Seconds between polls when queue is empty
    job_timeout=None,                  # Max seconds per job execution
    on_job_start=None,                 # Callback(worker_id, job_id, payload)
    on_job_complete=None,              # Callback(worker_id, job_id, result)
    on_job_error=None,                 # Callback(worker_id, job_id, error, permanent)
)
worker.start()
worker.stop(timeout=10.0)

stats = worker.get_stats()
# {
#     "worker_id": str,
#     "state": "running" | "stopped" | ...,
#     "is_running": bool,
#     "processed": int,
#     "failed": int,
#     "retried": int,
#     "throughput_jobs_per_hour": float,
#     "uptime_seconds": float,
#     "current_job_id": int or None,
# }

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
    scale_up_threshold=100,    # Add worker when pending > 100
    scale_down_threshold=10,   # Remove worker when pending < 10
    scale_check_interval=60,   # Check every 60s
)
pool.start_all()
pool.stop_all()
pool.wait_for_idle(timeout=30)
```

### Rate Limiter

```python
from queue_max import RateLimiter, RateLimitUnit

limiter = RateLimiter(rate_limit=10, unit=RateLimitUnit.PER_SECOND)
limiter.acquire(timeout=5.0)        # Blocks until token available, raises RateLimitError
limiter.try_acquire()               # Non-blocking, returns bool
limiter.get_remaining_tokens()      # float
limiter.get_retry_after()           # float — seconds until next token
limiter.update_rate_limit(20)       # Change limit dynamically
limiter.reset()
limiter.get_stats()
```

### Circuit Breaker

```python
from queue_max import CircuitBreaker

cb = CircuitBreaker(failure_threshold=5, recovery_timeout=60.0)
cb.is_allowed()       # Check if request can pass through
cb.call(func)         # Execute with CB protection (raises CircuitBreakerOpenError)
cb.record_success()   # Reset failure count (called automatically by Queue.complete_job)
cb.record_failure()   # Increment failure count (called automatically by Queue.fail_job)
cb.state              # CircuitState: CLOSED, OPEN, or HALF_OPEN
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

process_order.delay(42)                       # Background
process_order.schedule_in(300, 42)            # In 5 minutes
process_order.schedule_at(datetime(...), 42)  # At specific time
process_order.map([101, 102, 103], coupon=1)  # Parallel
process_order.get_stats()                     # Task statistics

@periodic_task(interval=3600, priority=1)
def cleanup_old_data():
    """Auto-runs every hour."""
    ...

cleanup_old_data.start_scheduler()  # Starts daemon thread

@retryable_task(max_retries=5, retry_on=[TimeoutError])
def fetch_external_data(url: str):
    """Sync retry wrapper. .delay() still enqueues async."""
    ...
```

## Retry & Failure Flow

### Error Classification

When a job fails, `is_retryable_error()` classifies the error:

| Error | Retryable? | Behavior |
|-------|-----------|----------|
| 4xx (except 429) | No | Permanent → Dead Letter Queue |
| 429 Too Many Requests | Yes | Retry with backoff |
| 5xx Server Error | Yes | Retry with backoff |
| Timeout | Yes | Retry with backoff |
| ConnectionError | Yes | Retry with backoff |

### Retry Mechanics

- **Backoff formula**: `base × 2^(attempt-1)` ± 20% jitter, capped at 3600s
- `max_retries=N` means N retries = N+1 total attempts
- Each attempt increments `tentativas` in the database
- After exhausting retries, the job moves to the Dead Letter Queue

### Circuit Breaker Integration

The circuit breaker is wired into `Queue.complete_job()` and `Queue.fail_job()`:

1. `fail_job()` — calls `circuit_breaker.record_failure()` (increments counter)
2. `complete_job()` — calls `circuit_breaker.record_success()` (resets counter)
3. After N consecutive failures (default 5), circuit opens → `pop_job()` returns `None`
4. After recovery timeout, next `pop_job()` transitions to HALF_OPEN
5. If the next job succeeds → CLOSED. If it fails → OPEN again.

```python
queue = Queue(max_retries=2, circuit_breaker_threshold=3)

# Retry flow (max_retries=2 → 3 total attempts):
#   Attempt 1 → fail → retry (backoff ~120s)
#   Attempt 2 → fail → retry (backoff ~240s)
#   Attempt 3 → fail → Dead Letter Queue

# Circuit breaker flow (threshold=3):
#   After 3 permanent failures → CIRCUIT OPENS
#   pop_job() returns None for all workers
#   After timeout → HALF_OPEN → one job passes
#   If success → CLOSED. If fail → OPEN again.
```

## Dead Letter Queue

Jobs that exhaust all retries or fail with `permanent=True` are moved to the `dead_letter_queue` table:

```python
# Inspect DLQ for a specific shard
dlq = queue.shard_manager.get_dead_letter_queue(shard_id=0)

for entry in dlq:
    print(f"Job {entry['original_job_id']}: {entry['error']}")
```

## Orphan Recovery

A job stuck in `processing` status (worker died without completing or failing) is recovered:

```python
orphans = queue.recover_orphans()
print(f"Recovered {orphans} stuck jobs")
```

The recovery checks for jobs where `heartbeat` is older than `STUCK_TIMEOUT` (default 30s).
Recovered jobs are reset to `pending` status with `next_retry_at` set to now.

## Event System

### Built-in Callbacks (no extra dependencies)

```python
queue.on("job_enqueued",  lambda job_id, shard_id: print(f"Enqueued {job_id}"))
queue.on("job_completed", lambda job_id, shard_id: update_metrics())
queue.on("job_failed",    lambda job_id, shard_id, error: alert(error))
queue.on("job_retried",   lambda job_id, shard_id, error: log_retry(error))
queue.on("alert",         lambda type, pending, threshold: notify(pending))

# Suppress events during batch
with queue.batch():
    for item in items:
        queue.enqueue(item)
```

Available events:

| Event | Payload | Trigger |
|-------|---------|--------|
| `job_enqueued` | `job_id`, `shard_id` | `enqueue()` |
| `job_completed` | `job_id`, `shard_id` | `complete_job()` |
| `job_failed` | `job_id`, `shard_id`, `error` | `fail_job(permanent=True)` |
| `job_retried` | `job_id`, `shard_id`, `error` | `fail_job()` with retry |
| `alert` | `type`, `pending`, `threshold` | Pending > `QUEUE_ALERT_THRESHOLD` |

### Typed Events with bubus (optional)

Requires: `pip install queue-max[events]`

Typed Pydantic events with async dispatch, event tree debugging, `expect()` for waiting,
and pattern matching:

```python
from queue_max.contrib.events import QueueEventBus, JobCompleted, JobFailed

queue = Queue(shards=3)
events = QueueEventBus(queue)

# Typed handler
@events.on(JobCompleted)
def handle(event: JobCompleted) -> None:
    print(f"Job {event.job_id} done in shard {event.shard_id}")

# Wildcard handler
@events.on("job_*")
def log_all(event):
    print(f"{type(event).__name__}: {event.model_dump()}")

# Wait for a specific event (blocks until received)
failed = events.expect(JobFailed, timeout=30)
print(f"Job {failed.job_id} failed: {failed.error}")

# Event tree for debugging causality
print(events.log_tree())

# Metrics
metrics = events.get_metrics()

# Context manager
with QueueEventBus(queue) as events:
    @events.on(JobCompleted)
    def handler(event): ...

# Available typed events:
#   JobEnqueued  — job_id, shard_id
#   JobCompleted — job_id, shard_id
#   JobFailed    — job_id, shard_id, error
#   JobRetried   — job_id, shard_id, error
#   Alert        — alert_type, pending, threshold
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

## Database Schema

Each shard is a separate `.db` file (`data/shard_0.db`, `data/shard_1.db`, ...) with WAL mode.

### `fila` table (the queue)

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment job ID |
| `pagina_id` | INTEGER NULL | Optional ID for consistent shard routing |
| `payload` | TEXT | JSON-serialized job data |
| `status` | TEXT | pending, processing, completed, failed, cancelled, scheduled |
| `priority` | INTEGER | 0=low, 1=medium, 2=high |
| `tentativas` | INTEGER | Attempt counter |
| `max_tentativas` | INTEGER | Max attempts allowed = max_retries + 1 |
| `last_error` | TEXT NULL | Error message |
| `error_type` | TEXT NULL | Exception class name |
| `error_stack` | TEXT NULL | Full traceback |
| `worker_id` | TEXT NULL | Currently processing worker |
| `heartbeat` | TEXT NULL | ISO timestamp of last activity |
| `created_at` | TEXT | Creation timestamp |
| `started_at` | TEXT NULL | Processing start timestamp |
| `completed_at` | TEXT NULL | Completion/failure timestamp |
| `next_retry_at` | TEXT NULL | Scheduled retry timestamp |

### `dead_letter_queue` table

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Row ID |
| `original_job_id` | INTEGER | Reference to original fila.id |
| `payload` | TEXT | Original job payload |
| `error` | TEXT | Error message |
| `error_type` | TEXT | Exception type |
| `failed_at` | TEXT | Failure timestamp |
| `shard_id` | INTEGER | Originating shard |

### `shard_metadata` table

Per-shard statistics: version, created_at, last_vacuum, total_jobs_processed, total_jobs_failed.

## Performance

| Scenario | Config | Result |
|----------|--------|--------|
| Burst | 20 workers, 10 shards | **~3.300 jobs/sec** |
| Contention | 10 workers, 1 shard | **~1.660 jobs/sec** |
| 30% failure | 8 workers, max_retries=2 | **Stable** |
| **100k jobs** | **16 workers, 8 shards** | **1.400 jobs/s — 0 duplicatas** |
| **500k jobs** | **16 workers, 8 shards** | **355 jobs/s — 0 duplicatas** |
| Exactly-once (500k) | 16 workers, 8 shards | **✅ 0 perdas, 0 duplicatas** |

- Max queue size: 1M+ jobs per shard
- [Detailed stress test results](docs/stress-test.md)
- [Run your own: `python tests/stress_test.py`](tests/stress_test.py)

## Running Tests

```bash
pip install -e ".[all]"
PYTHONPATH=src pytest tests/ -v
```

## License

MIT
