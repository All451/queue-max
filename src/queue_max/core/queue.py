"""Main Queue class for Queue Max.

Provides the primary API for enqueuing, processing, and managing jobs
with support for sharding, rate limiting, circuit breaker, and monitoring.
"""

import logging
import os
import random
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, List, Optional

from queue_max.core.circuit_breaker import CircuitBreaker
from queue_max.core.database import DATA_DIR, NUM_SHARDS, ShardManager
from queue_max.core.rate_limiter import RateLimiter
from queue_max.exceptions import QueueError
from queue_max.models.job import Job
from queue_max.utils.helpers import (
    determine_shard,
    get_env_int,
    validate_payload,
    validate_priority,
)

logger = logging.getLogger("queue_max")

class Queue:
    """Main queue for managing and processing jobs.

    Provides a persistent, sharded task queue backed by SQLite
    with rate limiting, circuit breaker, and automatic recovery.

    Example:
        >>> queue = Queue(rate_limit=100)
        >>> queue.enqueue({"task": "send_email", "to": "user@example.com"})
        >>> stats = queue.get_stats()
    """

    def __init__(
        self,
        shards: Optional[int] = None,
        rate_limit: Optional[int] = None,
        max_retries: Optional[int] = None,
        data_dir: Optional[str] = None,
        circuit_breaker_threshold: Optional[int] = None,
        circuit_breaker_timeout: Optional[float] = None,
    ):
        """Initialize the queue.

        Args:
            shards: Number of shards (default: NUM_SHARDS env or 6).
            rate_limit: Requests per minute (default: RATE_LIMIT_MAX env or 160).
            max_retries: Maximum retry attempts (default: QUEUE_MAX_RETRIES env or 3).
            data_dir: Directory for shard files (default: DATA_DIR env or ./data).
            circuit_breaker_threshold: Failures before circuit opens (default: 5).
            circuit_breaker_timeout: Seconds before recovery attempt (default: 60).
        """
        self.num_shards = shards or get_env_int("NUM_SHARDS", NUM_SHARDS)
        effective_rate_limit = rate_limit or get_env_int("RATE_LIMIT_MAX", 160)
        self.max_retries = max_retries or get_env_int("QUEUE_MAX_RETRIES", 3)
        self.data_dir = data_dir or os.environ.get("DATA_DIR", DATA_DIR)

        self.shard_manager = ShardManager(self.num_shards, self.data_dir)
        self.rate_limiter = RateLimiter(effective_rate_limit)
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=circuit_breaker_threshold or 5,
            recovery_timeout=circuit_breaker_timeout or 60.0,
        )
        self._start_time = time.time()
        self._pop_lock = threading.Lock()

        self._events: Dict[str, List[Callable]] = {
            "job_enqueued": [],
            "job_completed": [],
            "job_failed": [],
            "job_retried": [],
            "alert": [],
        }

    @property
    def is_healthy(self) -> bool:
        """Check if queue is healthy (circuit not open)."""
        try:
            return self.circuit_breaker.state.value != "open"
        except Exception:
            return False

    def on(self, event: str, callback: Callable) -> "Queue":
        """Register an event listener.

        Args:
            event: Event name ('job_enqueued', 'job_completed', 'job_failed', 'alert').
            callback: Function to call when event is emitted.

        Returns:
            Self for method chaining.
        """
        if event not in self._events:
            raise ValueError(f"Unknown event: {event}. Valid events: {list(self._events.keys())}")
        self._events[event].append(callback)
        return self

    def _emit(self, event: str, **data: Any) -> None:
        """Emit an event to all registered listeners."""
        for callback in self._events[event]:
            try:
                callback(**data)
            except Exception:
                logger.exception(f"Error in event handler for {event}")

    @contextmanager
    def batch(self) -> Generator[None, None, None]:
        """Context manager for batch operations (disables events temporarily)."""
        original_emit = self._emit
        self._emit = lambda event, **data: None
        try:
            yield
        finally:
            self._emit = original_emit

    def enqueue(
        self,
        payload: Dict[str, Any],
        pagina_id: Optional[int] = None,
        priority: int = 0,
        max_retries: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Enqueue a job.

        Args:
            payload: Job payload (JSON-serializable dict).
            pagina_id: Optional ID for consistent sharding.
            priority: Priority (0=low, 1=medium, 2=high).
            max_retries: Maximum retry attempts (default: instance default).

        Returns:
            Dict with 'id' (job ID) and 'shard_id' (assigned shard).

        Raises:
            ValueError: If payload is not a dict or priority is invalid.
        """
        payload = validate_payload(payload)
        priority = validate_priority(priority)
        max_retries = max_retries or self.max_retries

        shard_id = determine_shard(pagina_id, self.num_shards)
        job_id = self.shard_manager.insert_job(shard_id, payload, pagina_id, priority, max_retries)

        self._emit("job_enqueued", job_id=job_id, shard_id=shard_id)

        alert_threshold = get_env_int("QUEUE_ALERT_THRESHOLD", 1000)
        pending = self.shard_manager.get_stats(shard_id)["pending"]
        if pending > alert_threshold:
            self._emit("alert", type="QUEUE_SIZE", pending=pending, threshold=alert_threshold)

        return {"id": job_id, "shard_id": shard_id}

    def enqueue_batch(self, jobs: List[Dict[str, Any]]) -> Dict[str, int]:
        """Enqueue multiple jobs in a batch.

        Each job dict must contain at least 'payload'.
        Optional keys: 'pagina_id', 'priority', 'max_retries'.

        Returns:
            Dict with 'total' enqueued count.
        """
        total = 0
        with self.batch():
            for job in jobs:
                self.enqueue(
                    payload=job["payload"],
                    pagina_id=job.get("pagina_id"),
                    priority=job.get("priority", 0),
                    max_retries=job.get("max_retries"),
                )
                total += 1
        return {"total": total}

    def enqueue_from_file(self, filepath: str, fmt: str = "jsonl") -> Dict[str, int]:
        """Enqueue jobs from a file (JSON Lines format).

        Args:
            filepath: Path to the file.
            fmt: File format ('jsonl' or 'csv').

        Returns:
            Dict with 'total' enqueued count.
        """
        import csv
        import json

        total = 0
        if fmt == "jsonl":
            with open(filepath) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        job = json.loads(line)
                        self.enqueue(**job)
                        total += 1
        elif fmt == "csv":
            with open(filepath) as f:
                for row in csv.DictReader(f):
                    payload = {k: v for k, v in row.items() if k != "priority"}
                    priority = int(row.get("priority", 0))
                    self.enqueue(payload=payload, priority=priority)
                    total += 1
        else:
            raise ValueError(f"Unsupported format: {fmt}")
        return {"total": total}

    def pop_job(self, worker_id: str) -> Optional[Job]:
        """Atomically pop the next job from the queue.

        Scans shards in random order. Respects rate limiting and circuit breaker.

        Args:
            worker_id: Unique identifier for the calling worker.

        Returns:
            A Job if one is available, None if the queue is empty.
        """
        try:
            self.rate_limiter.acquire()
        except Exception:
            return None
        try:
            self.circuit_breaker.call(lambda: None)
        except Exception:
            return None

        with self._pop_lock:
            shard_order = list(range(self.num_shards))
            random.shuffle(shard_order)

            for shard_id in shard_order:
                try:
                    job = self.shard_manager.pop_job(shard_id, worker_id)
                    if job is not None:
                        return job
                except Exception:
                    logger.exception(f"Error popping from shard {shard_id}")
                    continue
        return None

    def complete_job(self, job_id: int, shard_id: int) -> None:
        """Mark a job as completed and remove it from the queue."""
        self.shard_manager.complete_job(shard_id, job_id)
        self._emit("job_completed", job_id=job_id, shard_id=shard_id)

    def fail_job(
        self,
        job_id: int,
        shard_id: int,
        error: Exception,
        permanent: bool = False,
    ) -> None:
        """Mark a job as failed with retry/backoff logic.

        Args:
            job_id: ID of the job to fail.
            shard_id: Shard containing the job.
            error: The exception that caused the failure.
            permanent: If True, fail immediately without retry.
        """
        self.shard_manager.fail_job(shard_id, job_id, error, permanent=permanent)
        if permanent:
            self._emit("job_failed", job_id=job_id, shard_id=shard_id, error=str(error))
        else:
            self._emit("job_retried", job_id=job_id, shard_id=shard_id, error=str(error))

    def retry_failed_jobs(self, shard_id: Optional[int] = None) -> int:
        """Re-queue all failed jobs.

        Args:
            shard_id: Specific shard, or None for all shards.

        Returns:
            Total number of jobs retried.
        """
        total = 0
        if shard_id is not None:
            total = self.shard_manager.retry_failed_jobs(shard_id)
        else:
            for sid in range(self.num_shards):
                total += self.shard_manager.retry_failed_jobs(sid)
        return total

    def cleanup_old_jobs(self, days: int = 7) -> int:
        """Remove old jobs from the queue.

        Args:
            days: Remove jobs older than this many days (default: 7).

        Returns:
            Total number of jobs removed.
        """
        total = 0
        for shard_id in range(self.num_shards):
            total += self.shard_manager.cleanup_old_jobs(shard_id, days)
        return total

    def purge_queue(self, status: Optional[str] = None) -> int:
        """Purge jobs from the queue.

        Args:
            status: Filter by status, or None for all.

        Returns:
            Number of jobs purged.
        """
        total = 0
        for shard_id in range(self.num_shards):
            conn = self.shard_manager._get_connection(shard_id)
            if status:
                cursor = conn.execute("DELETE FROM fila WHERE status=?", (status,))
            else:
                cursor = conn.execute("DELETE FROM fila")
            total += cursor.rowcount
            conn.commit()
        return total

    def get_failed_jobs(self, limit: int = 100) -> List[Job]:
        """Get all failed jobs across all shards."""
        jobs: List[Job] = []
        for shard_id in range(self.num_shards):
            jobs.extend(self.shard_manager.get_failed_jobs(shard_id, limit))
        jobs.sort(key=lambda j: j.id, reverse=True)
        return jobs

    def get_processing_jobs(self) -> List[Job]:
        """Get all currently processing jobs."""
        jobs: List[Job] = []
        for shard_id in range(self.num_shards):
            jobs.extend(self.shard_manager.get_processing_jobs(shard_id))
        return jobs

    def get_pending_count(self) -> int:
        """Get total number of pending jobs."""
        return self.get_stats().get("pending", 0)

    def is_empty(self) -> bool:
        """Check if queue has no pending jobs."""
        return self.get_pending_count() == 0

    def heartbeat(self, shard_id: int, worker_id: str) -> None:
        """Update heartbeat for a worker."""
        self.shard_manager.heartbeat(shard_id, worker_id)

    def recover_orphans(self) -> int:
        """Recover orphaned jobs (stuck in processing state).

        Returns:
            Number of jobs recovered.
        """
        total = 0
        stuck_timeout = get_env_int("STUCK_TIMEOUT", 30000)
        for shard_id in range(self.num_shards):
            total += self.shard_manager.recover_orphans(shard_id, stuck_timeout)
        return total

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive queue statistics.

        Returns:
            Dict with pending, processing, failed, and configuration stats.
        """
        shard_stats = self.shard_manager.get_all_stats()
        rate_stats = self.rate_limiter.get_stats()
        cb_stats = self.circuit_breaker.get_stats()
        return {
            **shard_stats,
            "tokens_available": rate_stats.get("tokens_remaining", 0),
            "circuit_state": cb_stats.get("state", "unknown"),
            "circuit_failures": cb_stats.get("failure_count", 0),
            "num_shards": self.num_shards,
            "rate_limit": rate_stats.get("rate_limit", 0),
            "max_retries": self.max_retries,
            "uptime_seconds": round(time.time() - self._start_time, 2),
            "is_healthy": self.is_healthy,
        }

    def wait_until_empty(self, timeout: Optional[float] = None) -> bool:
        """Wait until the queue is empty.

        Returns:
            True if queue became empty, False if timeout.
        """
        start = time.time()
        while True:
            if self.is_empty():
                return True
            if timeout and (time.time() - start) > timeout:
                return False
            time.sleep(1)

    def wait_for_jobs(self, count: int = 1, timeout: Optional[float] = None) -> bool:
        """Wait for at least N jobs in the queue."""
        start = time.time()
        while True:
            if self.get_pending_count() >= count:
                return True
            if timeout and (time.time() - start) > timeout:
                return False
            time.sleep(0.5)

    def close(self) -> None:
        """Close all database connections."""
        self.shard_manager.close_all()

    def __enter__(self) -> "Queue":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"Queue(shards={self.num_shards}, rate_limit={self.rate_limiter.rate_limit})"
