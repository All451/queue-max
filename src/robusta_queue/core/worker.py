"""Worker implementation for Robusta Queue.

Workers run in their own thread, polling the queue for jobs,
executing the processing function, and managing job lifecycle.
Includes state machine, callbacks, async support, and auto-scaling pool.
"""

import asyncio
import logging
import threading
import time
import traceback
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

from robusta_queue.core.queue import Queue
from robusta_queue.utils.helpers import get_env_int, is_retryable_error, now_iso

logger = logging.getLogger("robusta_queue.worker")


class WorkerState(Enum):
    """Worker state machine states."""
    INITIALIZED = "initialized"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class WorkerStats:
    """Statistics for a worker."""
    worker_id: str = ""
    state: str = "initialized"
    processed: int = 0
    failed: int = 0
    retried: int = 0
    started_at: Optional[str] = None
    stopped_at: Optional[str] = None
    last_heartbeat_at: Optional[str] = None
    last_error: Optional[str] = None
    total_runtime_seconds: float = 0.0
    current_job_id: Optional[int] = None
    throughput_jobs_per_hour: float = 0.0
    uptime_seconds: float = 0.0


class Worker:
    """Worker for processing queue jobs.

    Runs a processing loop in a dedicated thread with state machine,
    heartbeats, callbacks, and graceful shutdown.

    Attributes:
        worker_id: Unique identifier for this worker.
        process_function: Function that processes job payloads.
        queue: Queue instance to pull jobs from.
    """

    def __init__(
        self,
        worker_id: str,
        process_function: Callable[[Dict[str, Any]], Any],
        queue: Optional[Queue] = None,
        on_job_start: Optional[Callable] = None,
        on_job_complete: Optional[Callable] = None,
        on_job_error: Optional[Callable] = None,
        poll_interval: float = 1.0,
        job_timeout: Optional[float] = None,
    ):
        """Initialize a worker.

        Args:
            worker_id: Unique identifier for the worker.
            process_function: Function that processes job payloads.
            queue: Queue instance (creates a new one if None).
            on_job_start: Callback when a job starts processing.
            on_job_complete: Callback when a job completes.
            on_job_error: Callback when a job fails.
            poll_interval: Seconds between polls when queue is empty.
            job_timeout: Max seconds for job execution (Unix only).
        """
        self.worker_id = worker_id
        self.process_function = process_function
        self.queue = queue or Queue()
        self.on_job_start = on_job_start
        self.on_job_complete = on_job_complete
        self.on_job_error = on_job_error
        self.poll_interval = poll_interval
        self.job_timeout = job_timeout

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._state = WorkerState.INITIALIZED
        self._current_job: Optional[Any] = None
        self._current_job_start: Optional[float] = None
        self._job_mutex = threading.Lock()
        self._stats: WorkerStats = WorkerStats(worker_id=worker_id)
        self._last_heartbeat_time = 0.0
        self._heartbeat_interval = get_env_int("HEARTBEAT_INTERVAL", 5000) / 1000.0
        self._start_time: Optional[float] = None

    @property
    def state(self) -> str:
        return self._state.value

    @property
    def is_running(self) -> bool:
        return self._state == WorkerState.RUNNING

    def start(self) -> None:
        """Start the worker in a background thread."""
        if self._state in (WorkerState.RUNNING, WorkerState.STARTING):
            logger.warning(f"Worker {self.worker_id} is already {self._state.value}")
            return
        self._state = WorkerState.STARTING
        self._start_time = time.monotonic()
        self._stats.started_at = now_iso()
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"Worker-{self.worker_id}",
            daemon=True,
        )
        self._thread.start()
        self._state = WorkerState.RUNNING
        logger.info(f"Worker {self.worker_id} started")

    def stop(self, timeout: float = 10.0) -> None:
        """Gracefully stop the worker.

        Args:
            timeout: Max seconds to wait for the worker to stop.
        """
        if self._state not in (WorkerState.RUNNING, WorkerState.STARTING):
            return
        self._state = WorkerState.STOPPING
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
            self._state = WorkerState.STOPPED if not self._thread.is_alive() else WorkerState.ERROR
        self._stats.stopped_at = now_iso()
        logger.info(f"Worker {self.worker_id} stopped ({self._state.value})")

    def _run_loop(self) -> None:
        """Main worker processing loop."""
        while not self._stop_event.is_set():
            try:
                job = self.queue.pop_job(self.worker_id)
            except Exception as e:
                logger.exception(f"Worker {self.worker_id}: pop error: {e}")
                self._stats.last_error = str(e)
                self._idle_wait()
                continue
            if job is None:
                self._idle_wait()
                continue
            self._process_job(job)
            self._send_heartbeat()

    def _process_job(self, job: Any) -> None:
        """Process a single job with timeout, callbacks, and error handling."""
        start_time = time.monotonic()
        with self._job_mutex:
            self._current_job = job
            self._current_job_start = start_time
            self._stats.current_job_id = job.id

        if self.on_job_start:
            try:
                self.on_job_start(worker_id=self.worker_id, job_id=job.id, payload=job.payload)
            except Exception as e:
                logger.error(f"Worker {self.worker_id}: on_job_start error: {e}")

        try:
            if self.job_timeout:
                result = self._execute_with_timeout(job.payload)
            else:
                result = self.process_function(job.payload)
            self.queue.complete_job(job.id, job.shard_id)
            self._stats.processed += 1
            self._stats.total_runtime_seconds += time.monotonic() - start_time
            if self.on_job_complete:
                try:
                    self.on_job_complete(worker_id=self.worker_id, job_id=job.id, result=result)
                except Exception as e:
                    logger.error(f"Worker {self.worker_id}: on_job_complete error: {e}")
        except Exception as e:
            permanent = not is_retryable_error(e)
            self.queue.fail_job(job.id, job.shard_id, e, permanent=permanent)
            if permanent or job.tentativas + 1 >= job.max_tentativas:
                self._stats.failed += 1
            else:
                self._stats.retried += 1
            self._stats.last_error = str(e)
            self._stats.total_runtime_seconds += time.monotonic() - start_time
            if self.on_job_error:
                try:
                    self.on_job_error(worker_id=self.worker_id, job_id=job.id, error=str(e), permanent=permanent)
                except Exception as cb_err:
                    logger.error(f"Worker {self.worker_id}: on_job_error error: {cb_err}")
        finally:
            with self._job_mutex:
                self._current_job = None
                self._current_job_start = None
                self._stats.current_job_id = None

    def _execute_with_timeout(self, payload: Dict) -> Any:
        """Execute function with job_timeout using SIGALRM (Unix only)."""
        import signal as _signal

        class _TimeoutError(Exception):
            pass

        def _handler(signum, frame):
            raise _TimeoutError(f"Job exceeded {self.job_timeout}s timeout")

        old = _signal.signal(_signal.SIGALRM, _handler)
        _signal.alarm(int(self.job_timeout))
        try:
            return self.process_function(payload)
        finally:
            _signal.alarm(0)
            _signal.signal(_signal.SIGALRM, old)

    def _idle_wait(self) -> None:
        self._stop_event.wait(timeout=self.poll_interval)

    def _send_heartbeat(self) -> None:
        now = time.monotonic()
        if now - self._last_heartbeat_time >= self._heartbeat_interval:
            try:
                for sid in range(self.queue.num_shards):
                    self.queue.heartbeat(sid, self.worker_id)
                self._last_heartbeat_time = now
                self._stats.last_heartbeat_at = now_iso()
            except Exception as e:
                logger.exception(f"Worker {self.worker_id}: heartbeat error: {e}")

    def get_current_job(self) -> Optional[Any]:
        with self._job_mutex:
            return self._current_job

    def get_stats(self) -> Dict[str, Any]:
        runtime_hours = self._stats.total_runtime_seconds / 3600
        processed = self._stats.processed
        throughput = round(processed / runtime_hours, 2) if runtime_hours > 0 else 0.0
        uptime = time.monotonic() - self._start_time if self._start_time else 0
        return {
            "worker_id": self.worker_id,
            "state": self._state.value,
            "is_running": self._state == WorkerState.RUNNING,
            "processed": processed,
            "failed": self._stats.failed,
            "retried": self._stats.retried,
            "started_at": self._stats.started_at,
            "last_heartbeat_at": self._stats.last_heartbeat_at,
            "total_runtime_seconds": round(self._stats.total_runtime_seconds, 2),
            "throughput_jobs_per_hour": throughput,
            "uptime_seconds": round(uptime, 2),
            "current_job_id": self._stats.current_job_id,
        }

    def __repr__(self) -> str:
        return f"Worker(id='{self.worker_id}', state={self._state.value})"


class AsyncWorker(Worker):
    """Worker that supports async/await process functions."""

    def _run_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        while not self._stop_event.is_set():
            try:
                job = self.queue.pop_job(self.worker_id)
            except Exception as e:
                logger.exception(f"AsyncWorker {self.worker_id}: pop error: {e}")
                self._stats.last_error = str(e)
                time.sleep(self.poll_interval)
                continue
            if job is None:
                self._idle_wait()
                continue
            self._loop.run_until_complete(self._process_async(job))
            self._send_heartbeat()

    async def _process_async(self, job: Any) -> None:
        start_time = time.monotonic()
        try:
            if asyncio.iscoroutinefunction(self.process_function):
                result = await self.process_function(job.payload)
            else:
                result = self.process_function(job.payload)
            self.queue.complete_job(job.id, job.shard_id)
            self._stats.processed += 1
            self._stats.total_runtime_seconds += time.monotonic() - start_time
        except Exception as e:
            permanent = not is_retryable_error(e)
            self.queue.fail_job(job.id, job.shard_id, e, permanent=permanent)
            if permanent or job.tentativas + 1 >= job.max_tentativas:
                self._stats.failed += 1
            else:
                self._stats.retried += 1
            self._stats.total_runtime_seconds += time.monotonic() - start_time
            self._stats.last_error = str(e)


class WorkerPool:
    """Manages multiple Worker instances for parallel processing.

    Supports auto-scaling based on queue depth.

    Attributes:
        workers: List of Worker instances.
    """

    def __init__(
        self,
        workers: Optional[List[Worker]] = None,
        auto_scale: bool = False,
        min_workers: int = 1,
        max_workers: int = 10,
        scale_up_threshold: int = 100,
        scale_down_threshold: int = 10,
        scale_check_interval: float = 60.0,
    ):
        self.workers: List[Worker] = workers or []
        self.auto_scale = auto_scale
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold
        self.scale_check_interval = scale_check_interval
        self._queue = workers[0].queue if workers else None
        self._scale_thread: Optional[threading.Thread] = None
        self._stop_scale = threading.Event()

    def add_worker(self, worker: Worker) -> None:
        self.workers.append(worker)

    def remove_worker(self, worker: Worker) -> None:
        if worker in self.workers:
            worker.stop()
            self.workers.remove(worker)

    def start_all(self) -> None:
        for worker in self.workers:
            worker.start()
        if self.auto_scale and self._queue:
            self._start_auto_scaling()

    def stop_all(self, timeout: float = 10.0) -> None:
        self._stop_auto_scaling()
        for worker in self.workers:
            worker.stop(timeout=timeout)

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
                logger.error(f"Auto-scaling error: {e}")
            self._stop_scale.wait(timeout=self.scale_check_interval)

    def _check_and_scale(self) -> None:
        if not self._queue:
            return
        stats = self._queue.get_stats()
        pending = stats.get("pending", 0)
        current = len(self.workers)
        if pending > self.scale_up_threshold and current < self.max_workers:
            self._scale_to(min(self.max_workers, current + 1), f"pending={pending}")
        elif pending < self.scale_down_threshold and current > self.min_workers:
            self._scale_to(max(self.min_workers, current - 1), f"pending={pending}")

    def _scale_to(self, target: int, reason: str) -> None:
        if target == len(self.workers):
            return
        logger.info(f"Scaling workers {len(self.workers)} -> {target}: {reason}")
        if target > len(self.workers):
            for i in range(len(self.workers), target):
                w = Worker(
                    worker_id=f"pool-worker-{i+1}",
                    process_function=self.workers[0].process_function,
                    queue=self._queue,
                )
                w.start()
                self.workers.append(w)
        else:
            for w in self.workers[target:]:
                w.stop()
            self.workers = self.workers[:target]

    def get_stats(self) -> Dict[str, Any]:
        per_worker = [w.get_stats() for w in self.workers]
        return {
            "workers": per_worker,
            "total_workers": len(self.workers),
            "total_processed": sum(w["processed"] for w in per_worker),
            "total_failed": sum(w["failed"] for w in per_worker),
            "total_retried": sum(w["retried"] for w in per_worker),
            "auto_scale_enabled": self.auto_scale,
        }

    def wait_for_idle(self, timeout: Optional[float] = None) -> bool:
        start = time.time()
        while True:
            busy = sum(1 for w in self.workers if w.get_current_job() is not None)
            if busy == 0:
                return True
            if timeout and (time.time() - start) > timeout:
                return False
            time.sleep(0.5)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.stop_all()
