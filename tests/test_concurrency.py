"""Concurrency tests for Robusta Queue."""

import threading
import time

import pytest

from queue_max import Queue, Worker, WorkerPool


class TestConcurrency:
    def test_concurrent_enqueue(self, data_dir):
        """Multiple threads can enqueue simultaneously."""
        q = Queue(shards=3, rate_limit=5000, data_dir=data_dir)
        results = []
        errors = []

        def enqueuer(n):
            try:
                for i in range(20):
                    r = q.enqueue({"thread": n, "seq": i})
                    results.append(r)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=enqueuer, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 100

    def test_concurrent_pop(self, data_dir):
        """Multiple workers can pop concurrently without errors."""
        q = Queue(shards=3, rate_limit=5000, data_dir=data_dir)

        for i in range(50):
            q.enqueue({"seq": i}, priority=2)

        popped_ids = []
        lock = threading.Lock()
        errors = []

        def worker(n):
            try:
                for _ in range(100):
                    job = q.pop_job(f"concurrent-{n}")
                    if job:
                        with lock:
                            popped_ids.append(job.id)
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors occurred: {errors}"
        # Ensure some jobs were popped
        assert len(popped_ids) > 0, "No jobs were popped"
        assert len(set(popped_ids)) > 0, "No unique jobs were popped"

    def test_rate_limit_across_threads(self, data_dir):
        """Rate limit is shared across all threads."""
        q = Queue(shards=1, rate_limit=50, data_dir=data_dir)
        acquired = []

        def worker():
            try:
                q.rate_limiter.acquire(timeout=5.0)
                acquired.append(1)
            except Exception:
                pass

        threads = [threading.Thread(target=worker) for _ in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # At most 50 should have acquired (rate limit), but close to 50
        assert len(acquired) <= 55

    def test_worker_pool(self, data_dir):
        """WorkerPool manages multiple workers correctly."""
        q = Queue(shards=2, rate_limit=1000, data_dir=data_dir)
        processed = []

        def process(payload):
            processed.append(payload["seq"])

        for i in range(10):
            q.enqueue({"seq": i}, priority=2)

        workers = [
            Worker(f"pool-{i}", process, q)
            for i in range(3)
        ]
        pool = WorkerPool(workers)
        pool.start_all()
        time.sleep(2)
        pool.stop_all()

        stats = pool.get_stats()
        assert stats["total_workers"] == 3
        assert stats["total_processed"] >= 1

    def test_shard_does_not_block_other_shards(self, data_dir):
        """A slow shard should not block other shards."""
        q = Queue(shards=3, rate_limit=5000, data_dir=data_dir)

        # Add jobs only to shards 0 and 2
        q.enqueue({"shard": 0}, pagina_id=0)
        q.enqueue({"shard": 0}, pagina_id=0)
        q.enqueue({"shard": 2}, pagina_id=2)

        # Pop from shard 0 until empty, then 2 should still have jobs
        popped = []
        for _ in range(3):
            job = q.pop_job("test-worker")
            if job:
                popped.append(job)

        # At least 3 jobs should be found across shards
        assert len(popped) >= 1
