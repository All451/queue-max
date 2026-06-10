# Queue Max

Task queue library with SQLite persistence, sharding, rate limiting, and circuit breaker.

No Redis or RabbitMQ required. Zero external dependencies (except typing-extensions).

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

```python
from queue_max import Queue, Worker

# Create queue
queue = Queue()

# Enqueue a job
queue.enqueue({"task": "send_email", "to": "user@example.com"}, priority=2)

# Define processor
def process(payload):
    print(f"Processing: {payload}")

# Start worker
worker = Worker("worker-1", process, queue)
worker.start()
```

## Features

**SQLite Persistence** -- Jobs are stored in SQLite with WAL mode. No external services needed.

**Physical Sharding** -- Multiple .db files allow concurrent read/write across workers.

**Rate Limiting** -- Token bucket algorithm shared across all workers. Configurable per minute.

**Circuit Breaker** -- Stops calling failing services after N consecutive errors. Recovers automatically.

**Retry with Backoff** -- Exponential backoff with jitter for failed jobs. Configurable max retries.

**Heartbeat and Recovery** -- Workers send periodic heartbeats. Orphaned jobs are recovered automatically.

**Priority Queues** -- Three levels: low (0), medium (1), high (2).

**CLI** -- Built-in command line for stats, workers, and queue management.

## Configuration via Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| NUM_SHARDS | 6 | Number of shard databases |
| RATE_LIMIT_MAX | 160 | Max requests per minute |
| QUEUE_MAX_RETRIES | 3 | Default max retry attempts |
| DB_BUSY_TIMEOUT | 30000 | SQLite busy timeout (ms) |
| HEARTBEAT_INTERVAL | 5000 | Worker heartbeat interval (ms) |
| STUCK_TIMEOUT | 30000 | Orphan job timeout (ms) |
| DATA_DIR | ./data | Directory for shard files |

## API Overview

### Queue

```python
from queue_max import Queue

queue = Queue(shards=6, rate_limit=160, max_retries=3)

# Enqueue jobs
queue.enqueue(payload, pagina_id=None, priority=0, max_retries=None)
queue.enqueue_batch([{"payload": {...}}, ...])

# Process jobs
job = queue.pop_job(worker_id)
queue.complete_job(job_id, shard_id)
queue.fail_job(job_id, shard_id, error, permanent=False)

# Management
queue.retry_failed_jobs()
queue.cleanup_old_jobs(days=7)
queue.recover_orphans()
stats = queue.get_stats()
```

### Worker

```python
from queue_max import Worker, WorkerPool

worker = Worker("worker-1", process_function, queue)
worker.start()
worker.stop()

pool = WorkerPool([worker1, worker2])
pool.start_all()
pool.stop_all()
```

### Decorator

```python
from queue_max import task

@task(priority=2, max_retries=3)
def send_email(to: str, subject: str):
    return send(to, subject)

send_email.delay("user@example.com", "Hello")
```

### CLI

```bash
queue-max stats
queue-max worker --function mymodule:myfunction --workers 4
queue-max enqueue --payload '{"task": "test"}' --priority 2
queue-max list --status failed --limit 20
queue-max retry
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
def my_task(user_id): ...
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
def my_task(): ...
```

## Performance

- Enqueue: ~1,860 jobs/second (6 shards, WAL mode)
- Process: ~1,500 jobs/second (6 shards, 6 workers)
- Pop latency: <50ms average
- Max queue size: 1M+ jobs per shard

## Running Tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

## License

MIT
