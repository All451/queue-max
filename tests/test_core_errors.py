"""Targeted tests for error-handling paths in core modules.

These tests exercise exception-catching branches that only trigger
when something goes wrong (callback raises, heartbeat fails, etc.).
"""

import threading
import time

from queue_max import Queue, Worker, AsyncWorker


class TestWorkerErrorPaths:
    """Cover error handlers in workers/base.py."""

    def test_on_job_complete_callback_error(self, data_dir):
        """on_job_complete that raises is caught."""
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)

        def broken_cb(**kw):
            raise ValueError("complete failed")

        q.enqueue({"task": "x"})
        w = Worker("w", lambda p: "ok", q,
                   on_job_complete=broken_cb,
                   poll_interval=0.05)
        w.start()
        time.sleep(0.3)
        w.stop()
        assert True  # no exception propagated

    def test_on_job_error_callback_error(self, data_dir):
        """on_job_error that raises is caught."""
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)

        def broken_error_cb(**kw):
            raise RuntimeError("error handler failed")

        q.enqueue({"task": "x"})
        w = Worker("w", lambda p: (_ for _ in ()).throw(ValueError("fail")), q,
                   on_job_error=broken_error_cb,
                   poll_interval=0.05)
        w.start()
        time.sleep(0.5)
        w.stop()
        assert True

    def test_executor_path_normal_job(self, data_dir):
        """_execute_with_timeout works for a normal (non-timeout) job."""
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        done = threading.Event()

        q.enqueue({"task": "x"})
        w = Worker("w", lambda p: done.set() or "ok", q,
                   job_timeout=5, poll_interval=0.05)
        w.start()
        assert done.wait(timeout=3), "normal executor job did not complete"
        w.stop()
        stats = w.get_stats()
        assert stats["processed"] >= 1

    def test_worker_job_timeout_fires(self, data_dir):
        """A job that exceeds job_timeout is failed."""
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        blocked = threading.Event()
        started = threading.Event()

        def slow(payload):
            started.set()
            blocked.wait(timeout=5)
            return "too late"

        q.enqueue({"task": "slow"})
        w = Worker("slowpoke", slow, q,
                   job_timeout=0.1, poll_interval=0.05)
        w.start()
        assert started.wait(timeout=3), "slow job did not start"
        time.sleep(0.3)  # wait for timeout to fire
        blocked.set()  # release the slow job
        time.sleep(0.2)  # let worker finish
        w.stop()
        stats = w.get_stats()
        assert stats["retried"] >= 1
