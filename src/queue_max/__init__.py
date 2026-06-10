"""Robusta Queue - Super robust task queue with SQLite sharding.

A zero-dependency (except typing-extensions) task queue library with:
- SQLite-backed persistence with WAL mode
- Physical sharding for concurrent access
- Token bucket rate limiting
- Circuit breaker pattern
- Exponential backoff with jitter
- Automatic orphan job recovery
- Heartbeat monitoring
- CLI for queue management
- Framework integrations (Django, FastAPI, Flask)

Example:
    >>> from queue_max import Queue, Worker
    >>> queue = Queue()
    >>> queue.enqueue({"task": "send_email"})
    >>> worker = Worker("worker-1", lambda p: print(p), queue)
    >>> worker.start()
"""

from queue_max.core.circuit_breaker import CircuitBreaker, CircuitState
from queue_max.core.queue import Queue
from queue_max.core.rate_limiter import RateLimitUnit, RateLimiter
from queue_max.core.worker import AsyncWorker, Worker, WorkerPool, WorkerState
from queue_max.core.decorator import periodic_task, retryable_task, task
from queue_max.exceptions import (
    CircuitBreakerOpenError,
    ConfigurationError,
    JobFailedError,
    QueueError,
    RateLimitError,
    ShardError,
)
from queue_max.models.job import Job, JobPriority, JobResult, JobStatus

__version__ = "0.1.0"

__all__ = [
    "Queue",
    "Worker",
    "AsyncWorker",
    "WorkerPool",
    "WorkerState",
    "Job",
    "JobStatus",
    "JobPriority",
    "JobResult",
    "task",
    "periodic_task",
    "retryable_task",
    "RateLimiter",
    "RateLimitUnit",
    "CircuitBreaker",
    "CircuitState",
    "QueueError",
    "RateLimitError",
    "CircuitBreakerOpenError",
    "JobFailedError",
    "ShardError",
    "ConfigurationError",
]
