"""Tests for @task, @periodic_task, and @retryable_task decorators."""

import threading
import time

import pytest

from queue_max import Queue
from queue_max.core.tasks import task, periodic_task, retryable_task


@pytest.fixture
def tq(data_dir):
    return Queue(shards=1, rate_limit=5000, data_dir=data_dir)


class TestTaskDecorator:
    def test_task_delay_enqueues_job(self, tq):
        @task(queue=tq)
        def my_func(x: int) -> int:
            return x * 2

        result = my_func.delay(21)
        assert "id" in result
        assert "shard_id" in result

    def test_task_direct_call(self, tq):
        @task(queue=tq)
        def my_func(x: int) -> int:
            return x * 2

        assert my_func(21) == 42

    def test_task_without_queue_creates_one(self):
        @task()
        def my_func(x: int) -> int:
            return x

        assert my_func(1) == 1

    def test_schedule_at(self, tq):
        @task(queue=tq)
        def future_task(x: int) -> int:
            return x

        from datetime import datetime, timedelta, timezone
        future = datetime.now(timezone.utc) + timedelta(seconds=10)
        result = future_task.schedule_at(future, 42)
        assert "id" in result

    def test_schedule_at_past_raises(self, tq):
        @task(queue=tq)
        def past_task():
            pass

        from datetime import datetime, timedelta, timezone
        past = datetime.now(timezone.utc) - timedelta(seconds=10)
        with pytest.raises(ValueError, match="in the past"):
            past_task.schedule_at(past)

    def test_schedule_in(self, tq):
        @task(queue=tq)
        def later_task(x: int) -> int:
            return x

        result = later_task.schedule_in(60, 99)
        assert "id" in result

    def test_map_enqueues_multiple(self, tq):
        @task(queue=tq)
        def map_task(x: int) -> int:
            return x * 2

        results = map_task.map([1, 2, 3])
        assert len(results) == 3
        for r in results:
            assert "id" in r

    def test_bulk_delay(self, tq):
        @task(queue=tq)
        def bulk_task(a: int, b: int) -> int:
            return a + b

        results = bulk_task.bulk_delay([((1, 2), {}), ((3, 4), {})])
        assert len(results) == 2

    def test_timeout_kills_long_task(self, tq):
        @task(queue=tq, timeout=0.1)
        def slow_task():
            time.sleep(5)
            return "done"

        with pytest.raises(TimeoutError):
            slow_task()

    def test_version_metadata(self, tq):
        @task(queue=tq, version=2)
        def versioned():
            pass

        assert versioned.version == 2

    def test_get_stats(self, tq):
        @task(queue=tq)
        def stats_task():
            pass

        s = stats_task.get_stats()
        assert "task_name" in s
        assert "queue_stats" in s

    def test_get_queue(self, tq):
        @task(queue=tq)
        def queue_task():
            pass

        assert queue_task.get_queue() is tq


class TestPeriodicTask:
    def test_periodic_task_decorator(self):
        @periodic_task(interval=3600)
        def cleanup():
            pass

        assert hasattr(cleanup, "delay")
        assert hasattr(cleanup, "start_scheduler")
        assert hasattr(cleanup, "schedule_at")


class TestRetryableTask:
    def test_retryable_task_decorator(self):
        @retryable_task(max_retries=3)
        def flaky():
            return "ok"

        assert hasattr(flaky, "delay")
        assert flaky() == "ok"

    def test_retryable_task_retries_on_failure(self):
        attempt = {"n": 0}

        @retryable_task(max_retries=3, retry_delay=0.01)
        def flaky():
            attempt["n"] += 1
            if attempt["n"] < 3:
                raise ValueError("not yet")
            return "success"

        result = flaky()
        assert result == "success"
        assert attempt["n"] == 3

    def test_retryable_task_exhausts_retries(self):
        attempt = {"n": 0}

        @retryable_task(max_retries=2, retry_delay=0.01)
        def always_fails():
            attempt["n"] += 1
            raise ValueError("always")

        with pytest.raises(ValueError):
            always_fails()
        assert attempt["n"] == 3  # 1 original + 2 retries

    def test_retryable_task_has_delay(self, tq):
        @retryable_task(queue=tq, max_retries=3)
        def flaky():
            pass

        assert hasattr(flaky, "delay")
        result = flaky.delay()
        assert "id" in result

    def test_periodic_task_delays(self, tq):
        @periodic_task(interval=3600, queue=tq)
        def cleanup():
            pass

        result = cleanup.delay()
        assert "id" in result
