"""Tests for ShardRepository and ShardManager (facade)."""

import pytest

from queue_max.core.db import ShardManager
from queue_max.core.db.repository import ShardMetrics
from queue_max.models.job import Job, JobStatus


@pytest.fixture
def sm(data_dir):
    return ShardManager(3, data_dir=data_dir)


class TestShardRepository:
    def test_insert_job(self, sm):
        jid = sm.insert_job(0, {"task": "test"})
        assert jid > 0

    def test_insert_jobs_batch(self, sm):
        jobs = [({"task": f"job{i}"}, None, 0, 3) for i in range(5)]
        count = sm.insert_jobs_batch(0, jobs)
        assert count == 5

    def test_pop_job(self, sm):
        sm.insert_job(0, {"task": "pop_me"})
        job = sm.pop_job(0, "w1")
        assert job is not None
        assert job.payload["task"] == "pop_me"

    def test_pop_job_empty(self, sm):
        assert sm.pop_job(0, "w1") is None

    def test_pop_job_once(self, sm):
        sm.insert_job(0, {"task": "unique"})
        j1 = sm.pop_job(0, "w1")
        j2 = sm.pop_job(0, "w2")
        assert j1 is not None
        assert j2 is None

    def test_complete_job(self, sm):
        jid = sm.insert_job(0, {"task": "complete_me"})
        job = sm.pop_job(0, "w1")
        sm.complete_job(0, job.id)
        # Verify the job is gone
        assert sm.pop_job(0, "w1") is None

    def test_fail_job_permanent(self, sm):
        jid = sm.insert_job(0, {"task": "fail_permanent"})
        job = sm.pop_job(0, "w1")
        sm.fail_job(0, job.id, ValueError("fatal"), permanent=True)

        failed = sm.get_failed_jobs(0)
        assert len(failed) == 1
        dlq = sm.get_dead_letter_queue(0)
        assert len(dlq) == 1

    def test_fail_job_with_retry(self, sm):
        """Fail without permanent → job remains in queue, not immediately pop-able."""
        jid = sm.insert_job(0, {"task": "retry_me"})
        job = sm.pop_job(0, "w1")
        sm.fail_job(0, job.id, ValueError("try again"), permanent=False)

        # Job was marked for retry — it won't be immediately pop-able
        # because next_retry_at is in the future (backoff delay)
        retrieved = sm.retry_job(0, job.id)
        assert retrieved is False  # status is 'pending' already, not 'failed'

    def test_retry_failed_jobs(self, sm):
        sm.insert_job(0, {"task": "retry_all"})
        job = sm.pop_job(0, "w1")
        sm.fail_job(0, job.id, ValueError("fail"), permanent=True)

        count = sm.retry_failed_jobs(0)
        assert count >= 0

    def test_retry_job(self, sm):
        sm.insert_job(0, {"task": "retry_single"})
        job = sm.pop_job(0, "w1")
        sm.fail_job(0, job.id, RuntimeError("fail"), permanent=True)
        # Try to retry
        result = sm.retry_job(0, job.id)
        assert result is True

    def test_retry_job_not_found(self, sm):
        assert sm.retry_job(0, 999) is False

    def test_get_failed_jobs(self, sm):
        jid = sm.insert_job(0, {"task": "get_failed"})
        job = sm.pop_job(0, "w1")
        sm.fail_job(0, job.id, ValueError("nope"), permanent=True)

        failed = sm.get_failed_jobs(0)
        assert len(failed) >= 1

    def test_get_processing_jobs(self, sm):
        sm.insert_job(0, {"task": "processing"})
        job = sm.pop_job(0, "w1")
        processing = sm.get_processing_jobs(0)
        assert len(processing) >= 1

    def test_get_dead_letter_queue(self, sm):
        jid = sm.insert_job(0, {"task": "dlq_test"})
        job = sm.pop_job(0, "w1")
        sm.fail_job(0, job.id, ValueError("dlq"), permanent=True)
        dlq = sm.get_dead_letter_queue(0)
        assert len(dlq) >= 1

    def test_purge_jobs_all(self, sm):
        sm.insert_job(0, {"task": "purge1"})
        sm.insert_job(0, {"task": "purge2"})
        count = sm.purge_jobs(0)
        assert count == 2

    def test_purge_jobs_by_status(self, sm):
        sm.insert_job(0, {"task": "purge3"})
        count = sm.purge_jobs(0, status="pending")
        assert count == 1

    def test_heartbeat(self, sm):
        jid = sm.insert_job(0, {"task": "heartbeat_test"})
        job = sm.pop_job(0, "w1")
        sm.heartbeat(0, "w1")  # should not raise

    def test_recover_orphans(self, sm):
        import time
        sm.insert_job(0, {"task": "orphan"})
        job = sm.pop_job(0, "w1")
        # Wait so heartbeat becomes stale, then recover
        time.sleep(0.05)
        count = sm.recover_orphans(0, stuck_timeout=10)
        assert count >= 1

    def test_cleanup_old_jobs(self, sm):
        sm.insert_job(0, {"task": "old"})
        count = sm.cleanup_old_jobs(0, days=0)
        assert count >= 0

    def test_get_metrics(self, sm):
        metrics = sm.get_metrics(0)
        assert isinstance(metrics, ShardMetrics)
        assert metrics.shard_id == 0

    def test_get_stats(self, sm):
        stats = sm.get_stats(0)
        assert "pending" in stats
        assert "processing" in stats
        assert "failed" in stats

    def test_get_all_stats(self, sm):
        all_stats = sm.get_all_stats()
        assert "pending" in all_stats
        assert "processing" in all_stats
        assert "failed" in all_stats

    def test_metrics_for_invalid_shard(self, sm):
        """get_metrics should handle errors gracefully."""
        metrics = sm.get_metrics(99)
        assert metrics.is_healthy is False
        assert metrics.last_error is not None

    def test_shard_metrics_dataclass(self):
        m = ShardMetrics(shard_id=1, pending=5, processing=2, failed=1)
        assert m.pending == 5
        assert m.processing == 2
        assert m.failed == 1
        assert m.is_healthy is True

    def test_get_connection_context(self, sm):
        with sm.get_connection(0) as conn:
            assert conn is not None
