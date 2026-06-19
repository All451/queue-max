"""Teste de scaling: shards × workers."""
import os, sys, tempfile, time, threading

sys.path.insert(0, "src")
from queue_max import Queue, Worker

RESULTS = []
LOCK = threading.Lock()

def process(payload):
    time.sleep(0.001)
    with LOCK:
        RESULTS.append(payload.get("seq"))


def test_config(num_shards, num_workers, num_jobs=10000):
    """Testa uma configuração específica de shards/workers."""
    global RESULTS
    RESULTS = []

    data_dir = tempfile.mkdtemp(prefix=f"s{num_shards}w{num_workers}_")
    queue = Queue(shards=num_shards, rate_limit=50000, data_dir=data_dir)

    # Enfileira
    for i in range(num_jobs):
        queue.enqueue({"seq": i}, priority=i % 3)
    print(f"    Enfileirados {num_jobs} jobs        ", end="\r")

    # Workers
    workers = [
        Worker(f"w{i}", process, queue, poll_interval=0.01)
        for i in range(num_workers)
    ]
    for w in workers:
        w.start()

    start = time.time()
    queue.wait_until_empty(timeout=120)
    elapsed = time.time() - start

    for w in workers:
        w.stop()
    queue.close()

    throughput = num_jobs / elapsed if elapsed > 0 else 0
    return {"shards": num_shards, "workers": num_workers,
            "throughput": round(throughput, 0), "time": round(elapsed, 2)}


if __name__ == "__main__":
    print("=" * 65)
    print("  SCALING TEST - Shards × Workers")
    print("=" * 65)

    configs = [
        (4, 4), (4, 8), (4, 16),
        (8, 4), (8, 8), (8, 16),
        (16, 8), (16, 16), (16, 32),
        (32, 16), (32, 32),
    ]

    results = []
    for shards, workers in configs:
        print(f"\n  Shards: {shards:2} | Workers: {workers:2}")
        r = test_config(shards, workers)
        results.append(r)
        print(f"  → {r['throughput']:5.0f} jobs/s  ({r['time']:.1f}s)")

    print("\n" + "=" * 65)
    print("  RESUMO")
    print("=" * 65)
    print(f"  {'Shards':>6} | {'Workers':>7} | {'jobs/s':>7} | {'Tempo':>6}")
    print(f"  {'-'*6}-+-{'-'*7}-+-{'-'*7}-+-{'-'*6}")
    for r in results:
        print(f"  {r['shards']:6} | {r['workers']:7} | {r['throughput']:7.0f} | {r['time']:6.1f}s")
    print("=" * 65)
