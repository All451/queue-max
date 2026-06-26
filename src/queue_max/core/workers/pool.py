"""Worker pool for managing multiple workers with auto-scaling."""

import logging
import threading
import time
from typing import Any, Callable, Optional

from queue_max.core.workers.base import Worker

logger = logging.getLogger("queue_max.worker")


class WorkerPool:
    """Manages multiple Worker instances for parallel processing.

    Supports auto-scaling based on queue depth with hysteresis and
    proportional scale-up to avoid thrashing.

    Attributes:
        workers: list of Worker instances.
        auto_scale: Whether to automatically adjust worker count.
    """

    def __init__(
        self,
        workers: Optional[list[Worker]] = None,
        worker_factory: Optional[Callable[..., Worker]] = None,
        auto_scale: bool = False,
        min_workers: int = 1,
        max_workers: int = 10,
        scale_up_threshold: int = 100,
        scale_down_threshold: int = 10,
        scale_check_interval: float = 60.0,
        scale_cooldown: float = 30.0,
    ):
        """Initialize the worker pool.

        Args:
            workers: Initial list of Worker instances.
            worker_factory: Callable that returns a Worker instance.
                Receives the same ``**kwargs`` as ``Worker.__init__``.
                If omitted, a default factory is used that copies the
                first worker's ``process_function`` and ``queue``.
            auto_scale: Whether to automatically adjust worker count.
            min_workers: Minimum number of workers (default: 1).
            max_workers: Maximum number of workers (default: 10).
            scale_up_threshold: Pending jobs above this triggers scale-up.
            scale_down_threshold: Pending jobs below this triggers scale-down.
            scale_check_interval: Seconds between scale checks (default: 60).
            scale_cooldown: Seconds to wait after a scale change before
                allowing another (default: 30, reduces thrashing).
        """
        self.workers: list[Worker] = workers or []
        self.worker_factory = worker_factory
        self.auto_scale = auto_scale
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold
        self.scale_check_interval = scale_check_interval
        self.scale_cooldown = scale_cooldown

        self._queue = self.workers[0].queue if self.workers else None
        self._scale_thread: Optional[threading.Thread] = None
        self._stop_scale = threading.Event()
        self._worker_counter = len(self.workers)
        self._last_scale_time = 0.0
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_worker(self, worker: Worker) -> None:
        """Add a worker to the pool.

        The worker is NOT automatically started — call ``start_all()``
        or ``worker.start()`` separately.
        """
        with self._lock:
            self.workers.append(worker)

    def remove_worker(self, worker: Worker, timeout: float = 10.0) -> None:
        """Remove and stop a worker from the pool.

        Stops the worker gracefully, waiting up to ``timeout`` seconds
        for the current job (if any) to finish. Unfinished jobs are
        returned to the queue via orphan recovery.
        """
        with self._lock:
            if worker in self.workers:
                self.workers.remove(worker)
        worker.stop(timeout=timeout)

    def start_all(self) -> None:
        """Start all workers and, if ``auto_scale`` is enabled, the scaler."""
        with self._lock:
            for worker in self.workers:
                worker.start()
        if self.auto_scale and self._queue:
            self._start_auto_scaling()

    def stop_all(self, timeout: float = 10.0) -> None:
        """Stop all workers and the auto-scaler.

        Args:
            timeout: Max seconds per worker to wait for graceful stop.
        """
        self._stop_auto_scaling()
        with self._lock:
            for worker in self.workers:
                worker.stop(timeout=timeout)

    def wait_for_idle(self, timeout: Optional[float] = None) -> bool:
        """Wait until all workers are idle.

        Args:
            timeout: Max seconds to wait (None = forever).

        Returns:
            True if all workers became idle, False if timed out.
        """
        deadline = time.monotonic() + timeout if timeout else float("inf")
        while time.monotonic() < deadline:
            with self._lock:
                busy = sum(
                    1 for w in self.workers if w.get_current_job() is not None
                )
                if busy == 0:
                    return True
            time.sleep(0.5)
        return False

    # ------------------------------------------------------------------
    # Auto-scaling
    # ------------------------------------------------------------------

    def _start_auto_scaling(self) -> None:
        self._stop_scale.clear()
        self._scale_thread = threading.Thread(target=self._scale_loop, daemon=True)
        self._scale_thread.start()

    def _stop_auto_scaling(self) -> None:
        self._stop_scale.set()
        if self._scale_thread and self._scale_thread.is_alive():
            self._scale_thread.join(timeout=5)

    def _scale_loop(self) -> None:
        while not self._stop_scale.is_set():
            try:
                self._check_and_scale()
            except Exception as e:
                logger.error("Auto-scaling error: %s", e)
            self._stop_scale.wait(timeout=self.scale_check_interval)

    def _check_and_scale(self) -> None:
        if not self._queue:
            return

        now = time.monotonic()
        # Cooldown: prevent thrashing by limiting how often we scale
        if now - self._last_scale_time < self.scale_cooldown:
            return

        stats = self._queue.get_stats()
        pending = stats.get("pending", 0)

        with self._lock:
            current = len(self.workers)

        if pending > self.scale_up_threshold and current < self.max_workers:
            # Proportional scaling: bump by a factor of backlog / threshold
            target = min(
                self.max_workers,
                current + max(1, pending // self.scale_up_threshold),
            )
            self._scale_to(target, f"pending={pending}")
        elif pending < self.scale_down_threshold and current > self.min_workers:
            target = max(self.min_workers, current - 1)
            self._scale_to(target, f"pending={pending}")

    def _scale_to(self, target: int, reason: str) -> None:
        with self._lock:
            current = len(self.workers)
            if target == current:
                return
            logger.info(
                "Scaling workers %d -> %d: %s", current, target, reason
            )
            if target > current:
                self._scale_up(target)
            else:
                self._scale_down(target)
            self._last_scale_time = time.monotonic()

    def _scale_up(self, target: int) -> None:
        """Add workers to reach ``target`` count (caller must hold _lock)."""
        for _ in range(len(self.workers), target):
            self._worker_counter += 1
            w = self._build_worker(f"pool-worker-{self._worker_counter}")
            w.start()
            self.workers.append(w)

    def _scale_down(self, target: int) -> None:
        """Remove workers down to ``target`` count (caller must hold _lock)."""
        for w in self.workers[target:]:
            w.stop()
        del self.workers[target:]

    def _build_worker(self, worker_id: str) -> Worker:
        """Create a new Worker using the factory or a sensible default.

        If a ``worker_factory`` was provided, it is called with the
        same keyword arguments as ``Worker.__init__``.  Otherwise a
        default Worker is created using the first pool worker's config.
        """
        if self.worker_factory is not None:
            return self.worker_factory(
                worker_id=worker_id,
                process_function=self.workers[0].process_function,
                queue=self._queue,
            )
        return Worker(
            worker_id=worker_id,
            process_function=self.workers[0].process_function,
            queue=self._queue,
        )

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def get_stats(self) -> dict[str, Any]:
        with self._lock:
            per_worker = [w.get_stats() for w in self.workers]
            total = len(self.workers)
        return {
            "workers": per_worker,
            "total_workers": total,
            "total_processed": sum(w["processed"] for w in per_worker),
            "total_failed": sum(w["failed"] for w in per_worker),
            "total_retried": sum(w["retried"] for w in per_worker),
            "auto_scale_enabled": self.auto_scale,
        }

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.stop_all()
