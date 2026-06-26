"""Async worker that supports async/await process functions."""

import asyncio
import logging
import time
from typing import Any

from queue_max.core.workers.base import Worker, WorkerState
from queue_max.utils.helpers import is_retryable_error

logger = logging.getLogger("queue_max.worker")


class AsyncWorker(Worker):
    """Worker that supports async/await process functions."""

    def _run_loop(self) -> None:
        self._state = WorkerState.RUNNING
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            while not self._stop_event.is_set():
                try:
                    job = self.queue.pop_job(self.worker_id)
                except Exception as e:
                    logger.exception("AsyncWorker %s: pop error: %s", self.worker_id, e)
                    self._stats.last_error = str(e)
                    time.sleep(self.poll_interval)
                    continue
                if job is None:
                    self._idle_wait()
                    continue
                self._loop.run_until_complete(self._process_async(job))
                self._send_heartbeat()
        finally:
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            self._loop.close()
            self._loop = None

    async def _process_async(self, job: Any) -> None:
        start_time = time.monotonic()
        self._last_job_shard_id = job.shard_id

        if self.on_job_start:
            try:
                self.on_job_start(worker_id=self.worker_id, job_id=job.id, payload=job.payload)
            except Exception as e:
                logger.error("AsyncWorker %s: on_job_start error: %s", self.worker_id, e)

        try:
            if asyncio.iscoroutinefunction(self.process_function):
                result = await self.process_function(job.payload)
            else:
                result = self.process_function(job.payload)
            self.queue.complete_job(job.id, job.shard_id)
            self._stats.processed += 1
            self._stats.total_runtime_seconds += time.monotonic() - start_time
            if self.on_job_complete:
                try:
                    self.on_job_complete(worker_id=self.worker_id, job_id=job.id, result=result)
                except Exception as e:
                    logger.error("AsyncWorker %s: on_job_complete error: %s", self.worker_id, e)
        except Exception as e:
            permanent = not is_retryable_error(e)
            self.queue.fail_job(job.id, job.shard_id, e, permanent=permanent)
            if permanent or job.attempts + 1 >= job.max_attempts:
                self._stats.failed += 1
            else:
                self._stats.retried += 1
            self._stats.total_runtime_seconds += time.monotonic() - start_time
            self._stats.last_error = str(e)
            if self.on_job_error:
                try:
                    self.on_job_error(
                        worker_id=self.worker_id, job_id=job.id, error=str(e), permanent=permanent
                    )
                except Exception as cb_err:
                    logger.error("AsyncWorker %s: on_job_error error: %s", self.worker_id, cb_err)
        finally:
            with self._job_mutex:
                self._current_job = None
                self._current_job_start = None
                self._stats.current_job_id = None
