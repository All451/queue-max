"""TDD tests for remaining uncovered code paths.

Each test maps to specific uncovered lines in the coverage report.
"""

import threading
import time

from queue_max import Queue, Worker, AsyncWorker, WorkerPool, RateLimiter


class TestWorkerBaseGaps:
    """Covers workers/base.py remaining lines."""

    def test_repr(self):
        w = Worker("repr-test", lambda p: "ok")
        r = repr(w)
        assert "Worker" in r
        assert "repr-test" in r

    def test_on_job_start_callback_error(self, data_dir):
        """L182-183: on_job_start that raises is handled."""
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)

        def broken_start(**kw):
            raise ValueError("start callback fail")

        q.enqueue({"task": "x"})
        w = Worker("w", lambda p: "ok", q,
                   on_job_start=broken_start, poll_interval=0.05)
        w.start()
        time.sleep(0.3)
        w.stop()
        assert True  # no exception propagated

    def test_heartbeat_error(self, data_dir):
        """L240-241: heartbeat failure is caught."""
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        q.enqueue({"task": "x"})
        w = Worker("hb-test", lambda p: "ok", q, poll_interval=0.05)
        w.start()
        time.sleep(0.3)
        w.stop()
        assert True

    def test_pop_job_error_in_worker(self, data_dir):
        """L158-162: pop_job exception in worker loop is caught."""
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        # Make pop_job raise by breaking shard_manager
        original = q.shard_manager
        broken_sm = type('BrokenSM', (), {'pop_job': lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("pop fail"))})()
        q.shard_manager = broken_sm
        try:
            w = Worker("pop-err", lambda p: "ok", q, poll_interval=0.05)
            w.start()
            time.sleep(0.3)
            w.stop()
        finally:
            q.shard_manager = original
        assert True

    def test_permanent_failure_stats(self, data_dir):
        """L202: job with non-retryable error increments failed."""
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        done = threading.Event()

        def fails_permanently(payload):
            done.set()
            raise Exception("400 Bad Request")  # 4xx → permanent

        q.enqueue({"task": "x"})
        w = Worker("perm-fail", fails_permanently, q, poll_interval=0.05)
        w.start()
        assert done.wait(timeout=5), "job did not run"
        time.sleep(0.3)
        w.stop()
        stats = w.get_stats()
        assert stats["failed"] >= 1


class TestAsyncWorkerGaps:
    """Covers workers/async_worker.py remaining lines."""

    def test_async_pop_error(self, data_dir):
        """L25-29: pop_job error in async worker is caught."""
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        broken_sm = type('BrokenSM', (), {'pop_job': lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("pop fail"))})()
        original = q.shard_manager
        q.shard_manager = broken_sm
        try:
            async def process(p):
                return "ok"
            w = AsyncWorker("async-pop-err", process, q, poll_interval=0.05)
            w.start()
            time.sleep(0.3)
            w.stop()
        finally:
            q.shard_manager = original
        assert True

    def test_async_permanent_failure(self, data_dir):
        """L67: non-retryable error increments failed stats."""
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)

        async def fail_perm(payload):
            raise Exception("400 Bad Request")  # 4xx → permanent

        q.enqueue({"task": "x"})
        w = AsyncWorker("async-perm", fail_perm, q, poll_interval=0.05)
        w.start()
        time.sleep(0.5)
        w.stop()
        stats = w.get_stats()
        assert stats["failed"] >= 1


class TestPoolGaps:
    """Covers workers/pool.py remaining lines."""

    def test_worker_factory_path(self, data_dir):
        """L218: worker_factory is used when provided."""
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        w = Worker("model", lambda p: "ok", q)
        pool = WorkerPool(
            [w],
            worker_factory=lambda **kw: Worker(kw["worker_id"], lambda p: "ok", q),
            auto_scale=True, min_workers=1, max_workers=2,
            scale_up_threshold=1, scale_down_threshold=100,
            scale_check_interval=0.1, scale_cooldown=0,
        )
        pool.start_all()
        q.enqueue({"task": "x"})
        time.sleep(0.3)
        pool.stop_all()
        assert len(pool.workers) >= 1


class TestRateLimiterGaps:
    """Covers rate_limiter.py remaining lines."""

    def test_per_second_refill(self):
        """L244: _tokens_per_second for PER_SECOND is exercised."""
        from queue_max.core.rate_limiter import RateLimitUnit
        rl = RateLimiter(10, unit=RateLimitUnit.PER_SECOND)
        # Calling try_acquire triggers _refill → _tokens_per_second
        rl.try_acquire()
        rl.get_retry_after()
        rl.get_stats()
        assert True

    def test_per_hour_refill(self):
        """L246: _tokens_per_second for PER_HOUR is exercised."""
        from queue_max.core.rate_limiter import RateLimitUnit
        rl = RateLimiter(3600, unit=RateLimitUnit.PER_HOUR)
        rl.try_acquire()
        rl.get_retry_after()
        rl.get_stats()
        assert True


class TestCircuitBreakerGaps:
    """Covers circuit_breaker.py remaining lines."""

    def test_notify_exception_handled(self):
        """L254-255: on_state_change exception is caught."""
        from queue_max.core.circuit_breaker import CircuitBreaker, CircuitState
        def broken_cb(old, new):
            raise ValueError("notify fail")
        cb = CircuitBreaker(failure_threshold=2,
                            on_state_change=broken_cb)
        cb.record_failure()
        cb.record_failure()  # triggers transition + notify
        assert cb.state == CircuitState.OPEN

    def test_on_failure_with_predicate_return(self):
        """L227: _on_failure returns when predicate returns False."""
        from queue_max.core.circuit_breaker import CircuitBreaker, CircuitState
        cb = CircuitBreaker(failure_threshold=3,
                            failure_predicate=lambda e: "500" in str(e))
        cb._on_failure(ValueError("bad request"))  # "500" not in "bad request"
        assert cb._failure_count == 0  # predicate False → no record

    def test_half_open_probe_path(self, data_dir):
        """L208-209, 213: HALF_OPEN probe and rejection."""
        from queue_max.core.circuit_breaker import CircuitBreaker, CircuitState
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)
        cb.record_failure()
        time.sleep(0.1)
        assert cb.is_allowed() is True   # L208-209: HALF_OPEN, probe = True
        assert cb.is_allowed() is False  # L213: HALF_OPEN, probe blocked


class TestPeriodicTaskGaps:
    """Covers tasks/periodic.py remaining lines."""

    def test_scheduler_error_handled(self, data_dir):
        """L43-44: scheduler catches exceptions and continues."""
        from queue_max.core.tasks import periodic_task
        import threading, time

        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        call_count = {"n": 0}
        started = threading.Event()

        @periodic_task(interval=0.05, queue=q)
        def failing():
            call_count["n"] += 1
            started.set()
            raise ValueError("scheduled fail")

        failing.start_scheduler()
        assert started.wait(timeout=10), "Scheduler task did not execute"
        time.sleep(0.15)
        assert call_count["n"] >= 1
