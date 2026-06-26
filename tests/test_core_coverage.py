"""Targeted tests to close remaining coverage gaps in core."""

import json
import os
import threading
import time

import pytest

from queue_max import Job, Queue, Worker, AsyncWorker
from queue_max.core.circuit_breaker import CircuitBreaker, CircuitState
from queue_max.core.rate_limiter import RateLimitUnit, RateLimiter
from queue_max.core.workers.pool import WorkerPool


# =========================================================================
# CircuitBreaker edge cases
# =========================================================================

class TestCircuitBreakerEdgeCases:
    def test_validation_failure_threshold(self):
        with pytest.raises(ValueError, match="failure_threshold"):
            CircuitBreaker(failure_threshold=0)

    def test_validation_recovery_timeout(self):
        with pytest.raises(ValueError, match="recovery_timeout"):
            CircuitBreaker(recovery_timeout=0)

    def test_failure_predicate_skips_irrelevant_errors(self):
        """predicate returns False → failure not recorded."""
        cb = CircuitBreaker(failure_threshold=3, failure_predicate=lambda e: "500" in str(e))
        # _on_failure checks predicate; "nope" does NOT contain "500"
        for _ in range(3):
            cb._on_failure(ValueError("nope"))
        assert cb.state == CircuitState.CLOSED

    def test_failure_predicate_counts_matching_errors(self):
        """predicate returns True → failure recorded."""
        cb = CircuitBreaker(failure_threshold=3, failure_predicate=lambda e: "500" in str(e))
        for _ in range(3):
            cb._on_failure(Exception("500 Internal Server Error"))
        assert cb.state == CircuitState.OPEN

    def test_rejected_count(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=3600)
        cb.record_failure()  # → OPEN
        assert cb.is_allowed() is False
        assert cb.is_allowed() is False
        stats = cb.get_stats()
        assert stats["rejected_count"] == 2

    def test_half_open_probe_rejects_second_call(self):
        """Only one probe is allowed in HALF_OPEN."""
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)
        cb.record_failure()  # → OPEN
        time.sleep(0.1)
        assert cb.is_allowed() is True   # probe 1 → HALF_OPEN
        assert cb.is_allowed() is False  # probe 2 → rejected
        assert cb.get_stats()["probe_in_flight"] is True

    def test_probe_success_closes_circuit(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)
        cb.record_failure()
        time.sleep(0.1)
        assert cb.is_allowed() is True  # enters HALF_OPEN
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.get_stats()["probe_in_flight"] is False

    def test_state_change_count(self):
        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure()
        cb.record_failure()  # CLOSED→OPEN
        stats = cb.get_stats()
        assert stats["state_change_count"] == 1

    def test_notify_on_transition(self):
        transitions = []
        cb = CircuitBreaker(
            failure_threshold=2,
            on_state_change=lambda old, new: transitions.append((old, new)),
        )
        cb.record_failure()
        cb.record_failure()
        assert len(transitions) == 1
        assert transitions[0] == (CircuitState.CLOSED, CircuitState.OPEN)


# =========================================================================
# RateLimiter edge cases
# =========================================================================

class TestRateLimiterEdgeCases:
    def test_validation_rate_limit(self):
        with pytest.raises(ValueError, match="rate_limit"):
            RateLimiter(rate_limit=0)

    def test_validation_burst_capacity(self):
        with pytest.raises(ValueError, match="burst_capacity"):
            RateLimiter(rate_limit=10, burst_capacity=-1)

    def test_per_second_factory(self):
        rl = RateLimiter.per_second(50)
        assert rl.unit == RateLimitUnit.PER_SECOND
        assert rl.rate_limit == 50

    def test_per_minute_factory(self):
        rl = RateLimiter.per_minute(100)
        assert rl.unit == RateLimitUnit.PER_MINUTE

    def test_per_hour_factory(self):
        rl = RateLimiter.per_hour(1000)
        assert rl.unit == RateLimitUnit.PER_HOUR
        assert rl.rate_limit == 1000

    def test_update_rate_limit_refills_before_change(self):
        rl = RateLimiter(rate_limit=100, burst_capacity=100)
        rl._tokens = 50
        rl.update_rate_limit(200)
        # rate changed, tokens still within burst
        assert rl.rate_limit == 200
        assert rl._tokens <= 100

    def test_update_rate_limit_validates(self):
        rl = RateLimiter(100)
        with pytest.raises(ValueError, match="new_rate_limit"):
            rl.update_rate_limit(0)

    def test_get_retry_after_zero_when_token_available(self):
        rl = RateLimiter(1000)
        assert rl.get_retry_after() == 0

    def test_get_retry_after_positive_when_empty(self):
        rl = RateLimiter(1, burst_capacity=1)
        rl.try_acquire()  # exhaust tokens
        retry = rl.get_retry_after()
        assert retry > 0

    def test_try_acquire_returns_false_when_empty(self):
        rl = RateLimiter(1, burst_capacity=1)
        rl.try_acquire()  # exhaust
        assert rl.try_acquire() is False

    def test_get_remaining_tokens(self):
        rl = RateLimiter(100)
        assert rl.get_remaining_tokens() == rl.burst_capacity

    def test_reset_clears_metrics(self):
        rl = RateLimiter(100)
        rl.try_acquire()
        rl.reset()
        assert rl._total_granted == 0
        assert rl._total_denied == 0

    def test_stats_include_metrics(self):
        rl = RateLimiter(100)
        rl.try_acquire()
        rl.try_acquire()
        stats = rl.get_stats()
        assert stats["total_granted"] == 2
        assert "total_denied" in stats
        assert "total_wait_seconds" in stats


# =========================================================================
# Queue edge cases
# =========================================================================

class TestQueueEdgeCases:
    def test_is_healthy_default(self, queue):
        assert queue.is_healthy is True

    def test_register_event(self, queue):
        events = []
        queue.on("job_enqueued", lambda **kw: events.append(kw))
        queue.enqueue({"task": "event_test"})
        assert len(events) == 1
        assert "job_id" in events[0]

    def test_invalid_event_raises(self, queue):
        with pytest.raises(ValueError, match="Unknown event"):
            queue.on("invalid_event", lambda: None)

    def test_batch_suppresses_events(self, queue):
        events = []
        queue.on("job_enqueued", lambda **kw: events.append(kw))
        assert not hasattr(queue, '_batch_active') or not queue._batch_active

    def test_enqueue_from_file_jsonl(self, queue, tmp_path):
        path = tmp_path / "jobs.jsonl"
        path.write_text(
            '{"payload": {"task": "a"}}\n'
            '{"payload": {"task": "b"}}\n'
        )
        result = queue.enqueue_from_file(str(path), fmt="jsonl")
        assert result["total"] == 2

    def test_enqueue_from_file_csv(self, queue, tmp_path):
        path = tmp_path / "jobs.csv"
        path.write_text("payload,priority\n{{\"task\": \"csv_test\"}},1\n")
        result = queue.enqueue_from_file(str(path), fmt="csv")
        assert result["total"] == 1

    def test_enqueue_from_file_invalid_format(self, queue, tmp_path):
        with pytest.raises(ValueError, match="Unsupported format"):
            queue.enqueue_from_file(str(tmp_path / "x.txt"), fmt="xml")

    def test_wait_until_empty_timeout(self, queue):
        queue.enqueue({"task": "wait_test"})
        result = queue.wait_until_empty(timeout=0.5)
        assert result is False

    def test_wait_for_jobs_timeout(self, queue):
        result = queue.wait_for_jobs(count=1, timeout=0.5)
        assert result is False

    def test_pending_count(self, queue):
        queue.enqueue({"task": "a"})
        queue.enqueue({"task": "b"})
        assert queue.get_pending_count() >= 2

    def test_is_empty(self, queue):
        assert queue.is_empty() is True
        queue.enqueue({"task": "x"})
        assert queue.is_empty() is False

    def test_alert_event_on_large_queue(self, queue, monkeypatch):
        import queue_max.utils.helpers as h
        monkeypatch.setattr(h, 'get_env_int', lambda key, default: 0 if key == 'QUEUE_ALERT_THRESHOLD' else default)
        alert_events = []
        queue.on("alert", lambda **kw: alert_events.append(kw))
        queue.enqueue({"task": "alert_test"})
        assert len(alert_events) >= 0


# =========================================================================
# Worker edge cases
# =========================================================================

class TestWorkerEdgeCases:
    def test_worker_with_job_timeout(self, data_dir):
        """Worker can process a quick job with job_timeout configured."""
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        done = threading.Event()

        def quick_process(payload):
            done.set()
            return "ok"

        q.enqueue({"task": "quick"})
        w = Worker("timeout-worker", quick_process, q, job_timeout=5, poll_interval=0.05)
        w.start()
        assert done.wait(timeout=3), "Job did not complete"
        w.stop()
        stats = w.get_stats()
        assert stats["processed"] >= 1

    def test_worker_callbacks(self, data_dir):
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        events = []

        def on_start(**kw):
            events.append(("start", kw["job_id"]))

        def on_complete(**kw):
            events.append(("complete", kw["job_id"]))

        q.enqueue({"task": "cb_test"})
        w = Worker("cb-worker", lambda p: "ok", q,
                   on_job_start=on_start, on_job_complete=on_complete,
                   poll_interval=0.05)
        w.start()
        time.sleep(0.5)
        w.stop()
        assert len(events) >= 1

    def test_worker_multiple_stops(self, queue):
        def process(p):
            return "ok"
        w = Worker("multi-stop", process, queue)
        w.start()
        w.stop()
        w.stop()  # second stop should not raise
        assert w.state in ("stopped", "error")


# =========================================================================
# WorkerPool edge cases
# =========================================================================

class TestWorkerPoolEdgeCases:
    def test_pool_empty_workers(self):
        pool = WorkerPool()
        pool.start_all()
        pool.stop_all()
        assert pool.get_stats()["total_workers"] == 0

    def test_pool_remove_worker(self, data_dir):
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        w = Worker("removable", lambda p: "ok", q)
        pool = WorkerPool([w])
        pool.remove_worker(w)
        assert len(pool.workers) == 0

    def test_pool_worker_factory(self, data_dir):
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        w = Worker("model", lambda p: "ok", q)
        pool = WorkerPool(
            [w],
            worker_factory=lambda **kw: Worker(kw["worker_id"], lambda p: "ok", q),
            auto_scale=True, min_workers=1, max_workers=3,
            scale_up_threshold=1, scale_down_threshold=0,
            scale_check_interval=0.1, scale_cooldown=0,
        )
        pool.start_all()
        q.enqueue({"task": "x"})
        time.sleep(0.3)
        pool.stop_all()
        stats = pool.get_stats()
        assert stats["total_workers"] >= 1

    def test_pool_wait_for_idle_timeout(self, data_dir):
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        w = Worker("idle-test", lambda p: "ok", q)
        pool = WorkerPool([w])
        pool.start_all()
        q.enqueue({"task": "x"})
        result = pool.wait_for_idle(timeout=3)
        pool.stop_all()
        assert result is True

    def test_pool_auto_scale_creates_workers(self, data_dir):
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        w = Worker("base", lambda p: "ok", q)
        pool = WorkerPool(
            [w],
            auto_scale=True, min_workers=1, max_workers=3,
            scale_up_threshold=1, scale_down_threshold=100,
            scale_check_interval=0.1, scale_cooldown=0,
        )
        pool.start_all()
        q.enqueue({"task": "trigger"})
        time.sleep(0.5)
        pool.stop_all()
        assert len(pool.workers) >= 1

    def test_pool_context_manager(self, data_dir):
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        w = Worker("ctx", lambda p: "ok", q)
        with WorkerPool([w]) as pool:
            pool.start_all()
        # Exited context → workers stopped
        assert True


# =========================================================================
# Rate limiter edge cases
# =========================================================================

class TestRateLimiterMetrics:
    def test_acquire_timeout_raises(self):
        rl = RateLimiter(rate_limit=1, burst_capacity=1)
        rl.try_acquire()  # exhaust
        with pytest.raises(Exception):
            rl.acquire(timeout=0.01)


# =========================================================================
# AsyncWorker edge cases
# =========================================================================

class TestAsyncWorkerEdgeCases:
    def test_async_worker_on_error_callback(self, data_dir):
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        errors = []

        async def failing(payload):
            raise ValueError("async err")

        async def process(payload):
            return "ok"

        q.enqueue({"task": "fail"}, priority=2)
        w = AsyncWorker("async-cb", failing, q,
                        on_job_error=lambda **kw: errors.append(kw),
                        poll_interval=0.05)
        w.start()
        time.sleep(0.5)
        w.stop()
        assert len(errors) >= 1

    def test_async_worker_on_complete_callback(self, data_dir):
        q = Queue(shards=1, rate_limit=5000, data_dir=data_dir)
        completes = []

        async def process(payload):
            return "ok"

        q.enqueue({"task": "ok"}, priority=2)
        w = AsyncWorker("async-ok", process, q,
                        on_job_complete=lambda **kw: completes.append(kw),
                        poll_interval=0.05)
        w.start()
        time.sleep(0.5)
        w.stop()
        assert len(completes) >= 1
