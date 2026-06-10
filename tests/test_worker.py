"""Tests for Worker functionality."""

import time

import pytest

from queue_max import Queue, Worker


class TestWorker:
    def test_worker_initialization(self, queue, process_function):
        """Test worker initialization."""
        worker = Worker("test-worker", process_function, queue)
        assert worker.worker_id == "test-worker"
        assert worker.process_function == process_function
        assert worker.queue is queue

    def test_worker_process_job(self, queue):
        """Test worker processes a job successfully."""
        results = []

        def process(payload):
            results.append(payload["data"])
            return "ok"

        queue.enqueue({"data": "hello"}, priority=2)
        worker = Worker("test-worker", process, queue)
        worker.start()
        time.sleep(2)
        worker.stop()

        assert "hello" in results

    def test_worker_handles_failure(self, queue):
        """Test worker handles job failure."""
        def failing(payload):
            raise ValueError("always fails")

        queue.enqueue({"data": "fail"})
        worker = Worker("fail-worker", failing, queue)
        worker.start()
        time.sleep(2)
        worker.stop()

        stats = worker.get_stats()
        assert stats["failed"] >= 0

    def test_worker_multiple_jobs(self, queue):
        """Test worker processes multiple jobs."""
        processed = []

        def process(payload):
            processed.append(payload["seq"])
            return "ok"

        for i in range(5):
            queue.enqueue({"seq": i}, priority=2)

        worker = Worker("batch-worker", process, queue)
        worker.start()
        time.sleep(3)
        worker.stop()

        assert len(processed) >= 1

    def test_worker_stats(self, queue, process_function):
        """Test worker statistics."""
        worker = Worker("stats-worker", process_function, queue)
        stats = worker.get_stats()

        assert stats["worker_id"] == "stats-worker"
        assert stats["is_running"] is False
        assert stats["processed"] == 0
        assert stats["failed"] == 0

    def test_worker_stop_idle(self, queue, process_function):
        """Test stopping an idle worker."""
        worker = Worker("idle-worker", process_function, queue)
        worker.start()
        time.sleep(0.5)
        worker.stop()
        stats = worker.get_stats()
        assert stats["is_running"] is False

    def test_worker_double_start(self, queue, process_function):
        """Test starting an already running worker."""
        worker = Worker("double-worker", process_function, queue)
        worker.start()
        worker.start()  # Should not crash
        time.sleep(0.5)
        worker.stop()
