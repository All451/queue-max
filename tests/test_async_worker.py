"""Tests for AsyncWorker."""

import asyncio
import threading

import pytest

from queue_max import Queue, AsyncWorker


@pytest.fixture
def async_queue(data_dir):
    return Queue(shards=1, rate_limit=5000, data_dir=data_dir)


class TestAsyncWorker:
    def test_async_worker_initialization(self, async_queue):
        async def process(payload):
            return "ok"

        w = AsyncWorker("async-test", process, async_queue)
        assert w.worker_id == "async-test"

    def test_async_worker_processes_async_job(self, async_queue):
        """AsyncWorker processes an async process function."""
        results = []
        done = threading.Event()

        async def process(payload):
            await asyncio.sleep(0.05)
            results.append(payload["data"])
            done.set()
            return "ok"

        async_queue.enqueue({"data": "async-hello"}, priority=2)
        w = AsyncWorker("async-worker-1", process, async_queue, poll_interval=0.05)
        w.start()
        assert done.wait(timeout=5), "AsyncWorker did not process job"
        w.stop()

        stats = w.get_stats()
        assert results == ["async-hello"]
        assert stats["processed"] >= 1

    def test_async_worker_processes_sync_job(self, async_queue):
        """AsyncWorker also handles sync process functions."""
        results = []
        done = threading.Event()

        def process(payload):
            results.append(payload["data"])
            done.set()
            return "ok"

        async_queue.enqueue({"data": "sync-in-async"}, priority=2)
        w = AsyncWorker("async-worker-2", process, async_queue, poll_interval=0.05)
        w.start()
        assert done.wait(timeout=5), "AsyncWorker did not process sync job"
        w.stop()

        assert results == ["sync-in-async"]

    def test_async_worker_multiple_jobs(self, async_queue):
        """AsyncWorker processes all enqueued jobs."""
        results = []
        done = threading.Event()

        async def process(payload):
            await asyncio.sleep(0.02)
            results.append(payload["seq"])
            if len(results) == 5:
                done.set()
            return "ok"

        for i in range(5):
            async_queue.enqueue({"seq": i}, priority=2)

        w = AsyncWorker("async-batch", process, async_queue, poll_interval=0.05)
        w.start()
        assert done.wait(timeout=10), "Not all async jobs processed"
        w.stop()

        stats = w.get_stats()
        assert stats["processed"] == 5
        assert stats["failed"] == 0

    def test_async_worker_handles_failure(self, async_queue):
        """AsyncWorker increments retry count when job raises retryable error."""
        done = threading.Event()

        async def process(payload):
            done.set()
            raise ValueError("async fail")

        async_queue.enqueue({"data": "fail"}, priority=2)
        w = AsyncWorker("async-fail", process, async_queue, poll_interval=0.05)
        w.start()
        assert done.wait(timeout=5), "Job did not execute"
        w.stop()

        stats = w.get_stats()
        # ValueError is retryable by default → counted as retried, not failed
        assert stats["retried"] >= 1

    def test_async_worker_stop_idle(self, async_queue):
        """Stopping an idle AsyncWorker is safe."""
        async def process(payload):
            return "ok"

        w = AsyncWorker("async-idle", process, async_queue)
        w.start()
        w.stop()
        assert w.state in ("stopped", "error")
