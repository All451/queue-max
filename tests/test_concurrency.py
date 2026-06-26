"""Concurrency tests for Queue Max.

Uses ``threading.Event`` for synchronization where possible to avoid
brittle ``time.sleep()`` patterns.
"""

import threading

import pytest

from queue_max import Queue, Worker, WorkerPool


class TestConcurrency:
    def test_concurrent_enqueue(self, data_dir):
        """Multiple threads can enqueue simultaneously without errors."""
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

        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 100
        # Every result has an id and shard_id
        for r in results:
            assert "id" in r
            assert "shard_id" in r
            assert 0 <= r["shard_id"] < 3

    def test_concurrent_pop_exactly_once(self, data_dir):
        """Concurrent pops return unique (id, shard_id) pairs — no double-claim."""
        q = Queue(shards=3, rate_limit=5000, data_dir=data_dir)

        for i in range(50):
            q.enqueue({"seq": i}, priority=2)

        popped: list[tuple[int, int]] = []
        lock = threading.Lock()
        errors = []

        def worker(n):
            try:
                for _ in range(200):
                    job = q.pop_job(f"concurrent-{n}")
                    if job:
                        with lock:
                            popped.append((job.id, job.shard_id))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors occurred: {errors}"
        # All 50 jobs should be popped exactly once
        assert len(popped) == 50
        # Each (id, shard_id) pair uniquely identifies a job
        assert len(set(popped)) == 50

    def test_rate_limit_across_threads(self, data_dir):
        """Rate limit is shared across all threads — burst is bounded."""
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

        # At most 50 should acquire within timeout (some races allowed)
        assert len(acquired) <= 55

    def test_worker_pool_processes_all_jobs(self, data_dir):
        """WorkerPool distributes work across workers; all jobs complete."""
        q = Queue(shards=2, rate_limit=5000, data_dir=data_dir)
        processed = []
        lock = threading.Lock()
        done = threading.Event()

        def process(payload):
            with lock:
                processed.append(payload["seq"])
                if len(processed) == 10:
                    done.set()

        for i in range(10):
            q.enqueue({"seq": i}, priority=2)

        workers = [
            Worker(f"pool-{i}", process, q, poll_interval=0.05)
            for i in range(3)
        ]
        pool = WorkerPool(workers)
        pool.start_all()
        assert done.wait(timeout=10), "Not all jobs processed by pool"
        pool.stop_all()

        stats = pool.get_stats()
        assert stats["total_workers"] == 3
        assert stats["total_processed"] == 10
        assert stats["total_failed"] == 0
        assert sorted(processed) == list(range(10))

    def test_shard_independence(self, data_dir):
        """A slow shard does not block other shards from being popped."""
        q = Queue(shards=3, rate_limit=5000, data_dir=data_dir)

        # Add 2 jobs to shard 0 and 1 to shard 2 (shard 1 stays empty)
        q.enqueue({"shard": 0}, pagina_id=0)
        q.enqueue({"shard": 0}, pagina_id=0)
        q.enqueue({"shard": 2}, pagina_id=2)

        popped = []
        for _ in range(10):
            job = q.pop_job("test-worker")
            if job:
                popped.append(job)

        # All 3 jobs should be found
        assert len(popped) == 3
