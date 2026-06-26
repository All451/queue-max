"""Tests for Worker functionality.

Uses ``threading.Event`` for synchronization instead of ``time.sleep()``
to keep tests fast and deterministic.
"""

import threading

import pytest

from queue_max import Queue, Worker


class TestWorker:
    def test_worker_initialization(self, queue, process_function):
        worker = Worker("test-worker", process_function, queue)
        assert worker.worker_id == "test-worker"
        assert worker.process_function == process_function
        assert worker.queue is queue

    def test_worker_process_job(self, queue):
        """Worker processes exactly the enqueued job."""
        done = threading.Event()
        results = []

        def process(payload):
            results.append(payload["data"])
            done.set()
            return "ok"

        queue.enqueue({"data": "hello"}, priority=2)
        worker = Worker("test-worker", process, queue)
        worker.start()
        assert done.wait(timeout=5), "Worker did not process job in time"
        worker.stop()

        assert results == ["hello"]

    def test_worker_processes_all_enqueued_jobs(self, queue):
        """Worker drains all enqueued jobs."""
        counter = {"n": 0}
        done = threading.Event()

        def process(payload):
            counter["n"] += 1
            if counter["n"] == 5:
                done.set()
            return "ok"

        for i in range(5):
            queue.enqueue({"seq": i}, priority=2)

        worker = Worker("batch-worker", process, queue, poll_interval=0.05)
        worker.start()
        assert done.wait(timeout=10), "Not all jobs were processed"
        worker.stop()

        stats = worker.get_stats()
        assert stats["processed"] == 5
        assert stats["failed"] == 0

    def test_worker_handles_failure_and_continues(self, queue):
        """A failing job is retried; remaining jobs still run."""
        results = []
        fail_event = threading.Event()
        done = threading.Event()

        def process(payload):
            if payload.get("fail"):
                fail_event.set()
                raise ValueError("always fails")
            results.append(payload["seq"])
            if len(results) == 3:
                done.set()
            return "ok"

        queue.enqueue({"seq": 1, "fail": True})
        for i in range(2, 5):
            queue.enqueue({"seq": i}, priority=2)

        worker = Worker("fail-worker", process, queue, poll_interval=0.05)
        worker.start()
        assert fail_event.wait(timeout=5), "Worker did not process failing job"
        assert done.wait(timeout=5), "Worker did not process remaining jobs"
        worker.stop()

        stats = worker.get_stats()
        # ValueError is retryable by default, so it counts as retried, not failed
        assert stats["processed"] >= 3
        assert stats["retried"] >= 1

    def test_worker_stats(self, queue, process_function):
        worker = Worker("stats-worker", process_function, queue)
        stats = worker.get_stats()

        assert stats["worker_id"] == "stats-worker"
        assert stats["is_running"] is False
        assert stats["processed"] == 0
        assert stats["failed"] == 0

    def test_worker_stop_idle(self, queue, process_function):
        """Stopping an idle worker does not error."""
        worker = Worker("idle-worker", process_function, queue)
        worker.start()
        worker.stop()
        stats = worker.get_stats()
        assert stats["is_running"] is False

    def test_worker_double_start_is_idempotent(self, queue, process_function):
        """Calling start() twice does not raise."""
        worker = Worker("double-worker", process_function, queue)
        worker.start()
        worker.start()  # Should not crash
        worker.stop()
        assert worker.state == "stopped"

    def test_worker_processes_by_priority(self, queue):
        """High-priority jobs are processed before low-priority ones."""
        order = []
        done = threading.Event()

        def process(payload):
            order.append(payload["prio"])
            if len(order) == 3:
                done.set()
            return "ok"

        queue.enqueue({"prio": 1}, priority=1, pagina_id=0)
        queue.enqueue({"prio": 2}, priority=2, pagina_id=0)
        queue.enqueue({"prio": 0}, priority=0, pagina_id=0)

        worker = Worker("priority-worker", process, queue, poll_interval=0.05)
        worker.start()
        assert done.wait(timeout=5), "Not all jobs were processed"
        worker.stop()

        assert order == [2, 1, 0]

    def test_worker_stop_during_processing(self, queue):
        """Stopping a worker while it processes a job does not hang."""
        started = threading.Event()
        can_finish = threading.Event()

        def process(payload):
            started.set()
            assert can_finish.wait(timeout=10), "Timed out before can_finish"
            return "ok"

        queue.enqueue({"data": "slow"})
        worker = Worker("stop-worker", process, queue, poll_interval=0.05)
        worker.start()
        assert started.wait(timeout=5), "Worker did not start processing"
        # Stop with a short timeout — the job is blocked so we expect a forceful stop
        worker.stop(timeout=1)
        # Release the blocked job (so it doesn't hang thread forever)
        can_finish.set()
        assert worker.state in ("stopped", "error")

    def test_job_is_not_processed_twice(self, queue):
        """A completed job is removed from the queue — no double processing."""
        processed = []
        done = threading.Event()
        lock = threading.Lock()

        def process(payload):
            with lock:
                processed.append((payload["seq"], "done"))
                if len(processed) == 3:
                    done.set()
            return "ok"

        for i in range(3):
            queue.enqueue({"seq": i}, priority=2)

        worker = Worker("exactly-once", process, queue, poll_interval=0.05)
        worker.start()
        assert done.wait(timeout=10), "Did not process all jobs"
        worker.stop()

        # Each job processed exactly once
        ids = [p[0] for p in processed]
        assert sorted(ids) == [0, 1, 2]
        assert len(ids) == len(set(ids)), "Duplicate processing detected"

    def test_empty_queue_does_not_block_worker(self, queue, process_function):
        """Worker handles empty queue gracefully without error."""
        worker = Worker("empty-worker", process_function, queue, poll_interval=0.05)
        worker.start()
        worker.stop(timeout=2)
        stats = worker.get_stats()
        assert stats["processed"] == 0
        assert stats["failed"] == 0
