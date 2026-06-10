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
    >>> from robusta_queue import Queue, Worker
    >>> queue = Queue()
    >>> queue.enqueue({"task": "send_email"})
    >>> worker = Worker("worker-1", lambda p: print(p), queue)
    >>> worker.start()
"""

from robusta_queue.core.circuit_breaker import CircuitBreaker, CircuitState
from robusta_queue.core.queue import Queue
from robusta_queue.core.rate_limiter import RateLimitUnit, RateLimiter
from robusta_queue.core.worker import AsyncWorker, Worker, WorkerPool, WorkerState
from robusta_queue.core.decorator import periodic_task, retryable_task, task
from robusta_queue.exceptions import (
    CircuitBreakerOpenError,
    ConfigurationError,
    JobFailedError,
    QueueError,
    RateLimitError,
    ShardError,
)
from robusta_queue.models.job import Job, JobPriority, JobResult, JobStatus

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
