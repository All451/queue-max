#!/usr/bin/env python3
"""Stress test for Queue Max.

Usage:
    python tests/stress_test.py              # default: 10k jobs, 8 workers, 4 shards
    python tests/stress_test.py --jobs 50000 --workers 16 --shards 8
    python tests/stress_test.py --quick      # 1k jobs, 4 workers, 2 shards
"""

import argparse
import os
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from queue_max import Queue, Worker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class Counter:
    """Thread-safe counter and event collector."""

    def __init__(self):
        self.lock = threading.Lock()
        self.processed = []
        self.errors = []
        self.started_at = 0.0

    def add_processed(self, job_id: int, shard_id: int):
        with self.lock:
            self.processed.append((job_id, shard_id))

    def add_error(self, msg: str):
        with self.lock:
            self.errors.append(msg)

    @property
    def unique_count(self) -> int:
        with self.lock:
            return len(set(self.processed))

    @property
    def total_count(self) -> int:
        with self.lock:
            return len(self.processed)

    @property
    def duplicate_count(self) -> int:
        return self.total_count - self.unique_count


def build_payloads(n: int) -> list[dict]:
    """Generate ``n`` unique job payloads."""
    return [{"seq": i, "data": f"payload_{i}", "nested": {"idx": i}} for i in range(n)]


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

def phase_enqueue(q: Queue, payloads: list[dict], num_threads: int = 4) -> float:
    """Enqueue all payloads using ``num_threads`` concurrent enqueuers.

    Returns elapsed seconds.
    """
    chunk_size = (len(payloads) + num_threads - 1) // num_threads
    errors = []
    lock = threading.Lock()

    def enqueuer(chunk):
        for p in chunk:
            try:
                q.enqueue(p, priority=1)
            except Exception as e:
                with lock:
                    errors.append(str(e))

    threads = []
    start = time.monotonic()
    for i in range(num_threads):
        chunk = payloads[i * chunk_size : (i + 1) * chunk_size]
        t = threading.Thread(target=enqueuer, args=(chunk,), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    elapsed = time.monotonic() - start

    if errors:
        print(f"  ❌  Enqueue errors: {len(errors)}")
        for e in errors[:5]:
            print(f"       {e}")

    return elapsed


def phase_process(
    q: Queue,
    counter: Counter,
    num_workers: int,
    poll_interval: float = 0.05,
    timeout: float = 120,
) -> float:
    """Process all jobs with ``num_workers`` workers.

    Returns elapsed seconds.
    """
    def process_fn(payload):
        counter.add_processed(payload["seq"], 0)
        return "ok"

    workers = [
        Worker(f"stress-{i}", process_fn, q, poll_interval=poll_interval)
        for i in range(num_workers)
    ]
    for w in workers:
        w.start()

    start = time.monotonic()
    deadline = start + timeout
    while time.monotonic() < deadline:
        if q.is_empty():
            time.sleep(0.5)
            # Double-check after drain
            if q.is_empty():
                break
        time.sleep(0.5)
    elapsed = time.monotonic() - start

    for w in workers:
        w.stop(timeout=5)

    return elapsed


def phase_recover_orphans(q: Queue) -> int:
    """Recover orphaned jobs and return count."""
    return q.recover_orphans()


def phase_cleanup(q: Queue) -> int:
    """Remove old jobs and return count."""
    return q.cleanup_old_jobs(days=0)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_report(
    total_jobs: int,
    enqueue_time: float,
    process_time: float,
    counter: Counter,
    q: Queue,
):
    stats = q.get_stats()
    print()
    print("=" * 56)
    print("  STRESS TEST REPORT")
    print("=" * 56)
    print(f"  Jobs enqueued:       {total_jobs}")
    print(f"  Enqueue time:        {enqueue_time:.2f}s")
    print(f"  Enqueue rate:        {total_jobs / enqueue_time:.0f} jobs/s")
    print(f"  Process time:        {process_time:.2f}s")
    print(f"  Process rate:        {counter.total_count / process_time:.0f} jobs/s")
    print(f"  Workers:             {stats.get('num_workers', '?')}")
    print(f"  Shards:              {stats.get('num_shards', '?')}")
    print()
    print(f"  Total processed:     {counter.total_count}")
    print(f"  Unique processed:    {counter.unique_count}")
    print(f"  Duplicates:          {counter.duplicate_count}")
    print(f"  Pending in queue:    {stats.get('pending', '?')}")
    print(f"  Processing in queue: {stats.get('processing', '?')}")
    print(f"  Failed in queue:     {stats.get('failed', '?')}")
    print()
    print(f"  Errors:              {len(counter.errors)}")
    if counter.errors:
        for e in counter.errors[:5]:
            print(f"    {e}")
    print()

    passed = (
        counter.total_count == total_jobs
        and counter.duplicate_count == 0
        and stats.get("pending", 1) == 0
        and stats.get("processing", 1) == 0
        and len(counter.errors) == 0
    )

    if passed:
        print("  ✅  STRESS TEST PASSED")
    else:
        print("  ❌  STRESS TEST FAILED")
        if counter.total_count != total_jobs:
            print(f"       Expected {total_jobs} processed, got {counter.total_count}")
        if counter.duplicate_count > 0:
            print(f"       Found {counter.duplicate_count} duplicate jobs")
        if stats.get("pending", 0) > 0:
            print(f"       Queue still has {stats['pending']} pending jobs")
    print("=" * 56)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Stress test Queue Max")
    parser.add_argument("--jobs", type=int, default=10_000, help="Number of jobs (default: 10k)")
    parser.add_argument("--workers", type=int, default=8, help="Number of workers (default: 8)")
    parser.add_argument("--shards", type=int, default=4, help="Number of shards (default: 4)")
    parser.add_argument("--quick", action="store_true", help="Quick run: 1k jobs, 4 workers, 2 shards")
    parser.add_argument("--timeout", type=float, default=120, help="Max process wait in seconds")

    args = parser.parse_args()

    if args.quick:
        args.jobs = 1_000
        args.workers = 4
        args.shards = 2

    print()
    print(f"  Queue Max Stress Test")
    print(f"  Jobs:    {args.jobs}")
    print(f"  Workers: {args.workers}")
    print(f"  Shards:  {args.shards}")
    print(f"  Timeout: {args.timeout}s")
    print()

    data_dir = tempfile.mkdtemp(prefix="queue_max_stress_")
    print(f"  Data dir: {data_dir}")
    print()

    # --- Setup ---
    print("  [setup]  Creating queue...")
    q = Queue(shards=args.shards, rate_limit=50000, data_dir=data_dir)
    counter = Counter()
    payloads = build_payloads(args.jobs)

    # --- Enqueue ---
    print("  [phase 1] Enqueuing jobs...")
    et = phase_enqueue(q, payloads, num_threads=min(8, args.workers))

    # --- Verify enqueue ---
    enqueued_total = sum(
        q.shard_manager.get_stats(s)["pending"] + q.shard_manager.get_stats(s)["processing"]
        for s in range(args.shards)
    )
    print(f"            {enqueued_total} jobs visible in shards")

    # --- Process ---
    print(f"  [phase 2] Processing with {args.workers} workers...")
    pt = phase_process(q, counter, args.workers, timeout=args.timeout)

    # --- Recover orphans ---
    print("  [phase 3] Recovering orphans...")
    orphans = phase_recover_orphans(q)
    if orphans:
        print(f"            {orphans} orphans recovered — reprocessing...")
        pt2 = phase_process(q, counter, args.workers, timeout=30)
        pt += pt2

    # --- Report ---
    orphan_count = phase_recover_orphans(q)
    if orphan_count:
        counter.add_error(f"{orphan_count} orphans remaining after recovery")

    print_report(args.jobs, et, pt, counter, q)

    # --- Cleanup ---
    q.close()
    import shutil
    shutil.rmtree(data_dir, ignore_errors=True)

    sys.exit(0 if (
        counter.total_count == args.jobs
        and counter.duplicate_count == 0
        and len(counter.errors) == 0
    ) else 1)


if __name__ == "__main__":
    main()
