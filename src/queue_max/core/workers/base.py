"""Worker implementation for Queue Max.

Workers run in their own thread, polling the queue for jobs,
executing the processing function, and managing job lifecycle.
Includes state machine, callbacks, and stats tracking.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional

from queue_max.core.queue.queue import Queue
from queue_max.utils.helpers import get_env_int, is_retryable_error, now_iso

logger = logging.getLogger("queue_max.worker")


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
        process_function: Callable[[dict[str, Any]], Any],
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
        self._last_job_shard_id: Optional[int] = None
        self._executor = (
            ThreadPoolExecutor(max_workers=1) if job_timeout else None
        )

    @property
    def state(self) -> str:
        return self._state.value

    def start(self) -> None:
        """Start the worker in a background thread."""
        if self._state in (WorkerState.RUNNING, WorkerState.STARTING):
            logger.warning("Worker %s is already %s", self.worker_id, self._state.value)
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
        logger.info("Worker %s started", self.worker_id)

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
        if self._executor:
            self._executor.shutdown(wait=False)
        logger.info("Worker %s stopped (%s)", self.worker_id, self._state.value)

    def _run_loop(self) -> None:
        """Main worker processing loop."""
        self._state = WorkerState.RUNNING
        while not self._stop_event.is_set():
            try:
                job = self.queue.pop_job(self.worker_id)
            except Exception as e:
                logger.exception("Worker %s: pop error: %s", self.worker_id, e)
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

        self._last_job_shard_id = job.shard_id

        if self.on_job_start:
            try:
                self.on_job_start(worker_id=self.worker_id, job_id=job.id, payload=job.payload)
            except Exception as e:
                logger.error("Worker %s: on_job_start error: %s", self.worker_id, e)

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
                    logger.error("Worker %s: on_job_complete error: %s", self.worker_id, e)
        except Exception as e:
            permanent = not is_retryable_error(e)
            self.queue.fail_job(job.id, job.shard_id, e, permanent=permanent)
            if permanent or job.attempts + 1 >= job.max_attempts:
                self._stats.failed += 1
            else:
                self._stats.retried += 1
            self._stats.last_error = str(e)
            self._stats.total_runtime_seconds += time.monotonic() - start_time
            if self.on_job_error:
                try:
                    self.on_job_error(worker_id=self.worker_id, job_id=job.id, error=str(e), permanent=permanent)
                except Exception as cb_err:
                    logger.error("Worker %s: on_job_error error: %s", self.worker_id, cb_err)
        finally:
            with self._job_mutex:
                self._current_job = None
                self._current_job_start = None
                self._stats.current_job_id = None

    def _execute_with_timeout(self, payload: dict) -> Any:
        """Execute function with job_timeout using ThreadPoolExecutor."""
        from concurrent.futures import TimeoutError as FuturesTimeoutError

        future = self._executor.submit(self.process_function, payload)
        try:
            return future.result(timeout=self.job_timeout)
        except FuturesTimeoutError:
            raise TimeoutError(f"Job exceeded {self.job_timeout}s timeout")

    def _idle_wait(self) -> None:
        self._stop_event.wait(timeout=self.poll_interval)

    def _send_heartbeat(self) -> None:
        now = time.monotonic()
        if now - self._last_heartbeat_time >= self._heartbeat_interval:
            try:
                sid = self._last_job_shard_id
                if sid is not None:
                    self.queue.heartbeat(sid, self.worker_id)
                self._last_heartbeat_time = now
                self._stats.last_heartbeat_at = now_iso()
            except Exception as e:
                logger.exception("Worker %s: heartbeat error: %s", self.worker_id, e)

    def get_current_job(self) -> Optional[Any]:
        with self._job_mutex:
            return self._current_job

    def get_stats(self) -> dict[str, Any]:
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
