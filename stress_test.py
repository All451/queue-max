"""Stress test for Queue Max."""
import os, sys, tempfile, time, threading, json
sys.path.insert(0, "src")
from queue_max import Queue, Worker, WorkerPool
from collections import Counter

DATA = tempfile.mkdtemp(prefix="stress_")
RESULTS = []
LOCK = threading.Lock()
ERRORS = []

def process(payload):
    time.sleep(0.001)  # simulate minimal work
    with LOCK:
        RESULTS.append(payload.get("seq"))

def flaky_process(payload):
    """50% fail rate to test retry/circuit breaker."""
    import random
    time.sleep(0.001)
    if random.random() < 0.5:
        raise ConnectionError("simulated timeout")
    with LOCK:
        RESULTS.append(payload.get("seq"))

print("=" * 60)
print("STRESS TEST - Robusta Queue")
print("=" * 60)

# 1. THROUGHPUT TEST
print("\n[1/5] Throughput: 5000 jobs, 8 workers...")
q = Queue(shards=8, rate_limit=5000, data_dir=f"{DATA}/throughput")
for i in range(5000):
    q.enqueue({"seq": i}, priority=i % 3)
print(f"  Enqueued 5000 jobs")
start = time.time()
workers = [Worker(f"t{i}", process, q) for i in range(8)]
for w in workers: w.start()
while q.get_stats()["pending"] > 0:
    time.sleep(0.5)
for w in workers: w.stop()
elapsed = time.time() - start
print(f"  Processed {len(RESULTS)} jobs in {elapsed:.1f}s")
print(f"  Throughput: {len(RESULTS)/elapsed:.0f} jobs/sec")

# 2. CONCURRENT ENQUEUE
print("\n[2/5] Concurrent enqueue: 5 threads, 2000 jobs...")
q2 = Queue(shards=4, rate_limit=5000, data_dir=f"{DATA}/concurrent_enq")
errors = []
def enqueuer(n):
    for i in range(400):
        try:
            q2.enqueue({"thread": n, "seq": i})
        except Exception as e:
            errors.append(str(e))
threads = [threading.Thread(target=enqueuer, args=(i,)) for i in range(5)]
for t in threads: t.start()
for t in threads: t.join()
stats = q2.get_stats()
print(f"  Pending: {stats['pending']}, Errors: {len(errors)}")

# 3. FLAKY JOBS (retry + circuit breaker)
print("\n[3/5] Flaky jobs (50% fail rate): 1000 jobs, 4 workers...")
RESULTS.clear()
q3 = Queue(shards=4, rate_limit=5000, max_retries=3, data_dir=f"{DATA}/flaky")
for i in range(1000):
    q3.enqueue({"seq": i})
workers = [Worker(f"f{i}", flaky_process, q3) for i in range(4)]
for w in workers: w.start()
time.sleep(15)
for w in workers: w.stop()
s3 = q3.get_stats()
print(f"  Processed: {len(RESULTS)}, Failed: {s3['failed']}, Pending: {s3['pending']}")
print(f"  Circuit state: {s3['circuit_state']}")

# 4. RECOVERY TEST
print("\n[4/5] Orphan recovery: crash workers mid-job...")
q4 = Queue(shards=2, rate_limit=5000, data_dir=f"{DATA}/recovery")
for i in range(200):
    q4.enqueue({"seq": i})
def crash(payload):
    time.sleep(10)  # long job, will be orphaned
crash_worker = Worker("crash", crash, q4)
crash_worker.start()
time.sleep(1)  # let it claim some jobs
crash_worker.stop()  # hard stop simulates crash
orphans = q4.recover_orphans()
print(f"  Orphans recovered: {orphans}")
# jobs should be back to pending
s4 = q4.get_stats()
print(f"  After recovery - Pending: {s4['pending']}, Processing: {s4['processing']}")

# 5. PERSISTENCE
print("\n[5/5] Persistence: survive restart...")
q5 = Queue(shards=3, data_dir=f"{DATA}/persist")
q5.enqueue({"survive": True})
del q5
q5b = Queue(shards=3, data_dir=f"{DATA}/persist")
job = q5b.pop_job("restored")
print(f"  Job survived restart: {job is not None}")
q5b.close()

print("=" * 60)
print("STRESS TEST COMPLETE")
print("=" * 60)
