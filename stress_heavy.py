"""Heavy stress test - Robusta Queue"""
import os, sys, tempfile, time, threading
sys.path.insert(0, "src")
from robusta_queue import Queue, Worker, WorkerPool
from collections import Counter

DATA = tempfile.mkdtemp(prefix="stress_heavy_")
RESULTS = []; ERRORS = []; LOCK = threading.Lock()

def process(payload):
    time.sleep(0.0005)
    with LOCK: RESULTS.append(payload.get("seq"))

def flaky(payload):
    import random; time.sleep(0.0005)
    if random.random() < 0.3: raise ConnectionError("fail")
    with LOCK: RESULTS.append(payload.get("seq"))

print("="*60)
print("HEAVY STRESS - Robusta Queue")
print("="*60)

# 1. MAX THROUGHPUT: 50k jobs, 12 workers
print("\n[1] 50.000 jobs - 12 workers - 12 shards")
q = Queue(shards=12, rate_limit=10000, data_dir=f"{DATA}/t1")
t0 = time.time()
for batch in range(50):
    jobs = [{"payload": {"seq": i}, "priority": i%3} for i in range(batch*1000, (batch+1)*1000)]
    q.enqueue_batch(jobs)
print(f"  Enqueue: {time.time()-t0:.1f}s")
t0 = time.time()
workers = [Worker(f"t1-{i}", process, q) for i in range(12)]
for w in workers: w.start()
while q.get_stats()["pending"] > 0: time.sleep(0.5)
for w in workers: w.stop()
t = time.time()-t0
print(f"  Processed {len(RESULTS)} jobs in {t:.1f}s = {len(RESULTS)/t:.0f} jobs/sec")

# 2. CONTENTION: single shard, many workers
print("\n[2] 10.000 jobs - 1 shard - 10 workers (contention test)")
RESULTS.clear()
q2 = Queue(shards=1, rate_limit=20000, data_dir=f"{DATA}/t2")
for i in range(10000): q2.enqueue({"seq": i})
t0 = time.time()
workers = [Worker(f"t2-{i}", process, q2) for i in range(10)]
for w in workers: w.start()
while q2.get_stats()["pending"] > 0: time.sleep(0.5)
for w in workers: w.stop()
t = time.time()-t0
print(f"  Processed {len(RESULTS)} jobs in {t:.1f}s = {len(RESULTS)/t:.0f} jobs/sec")

# 3. CHAOS MONKEY: 30% fail rate, 5000 jobs, 8 workers
print("\n[3] 5.000 jobs - 30% fail rate - 8 workers (chaos)")
RESULTS.clear()
q3 = Queue(shards=8, rate_limit=10000, max_retries=2, data_dir=f"{DATA}/t3")
for i in range(5000): q3.enqueue({"seq": i})
workers = [Worker(f"t3-{i}", flaky, q3) for i in range(8)]
for w in workers: w.start()
time.sleep(30)
for w in workers: w.stop()
s3 = q3.get_stats()
print(f"  OK: {len(RESULTS)} | Pending: {s3['pending']} | Failed: {s3['failed']} | Circuit: {s3['circuit_state']}")

# 4. NO-LIMIT BURST: max rate
print("\n[4] 20.000 jobs - 20 workers - no rate limit (burst)")
q4 = Queue(shards=10, rate_limit=50000, data_dir=f"{DATA}/t4")
for i in range(20000): q4.enqueue({"seq": i})
t0 = time.time()
workers = [Worker(f"t4-{i}", process, q4) for i in range(20)]
for w in workers: w.start()
while q4.get_stats()["pending"] > 0: time.sleep(0.5)
for w in workers: w.stop()
t = time.time()-t0
print(f"  Processed 20000 jobs in {t:.1f}s = {20000/t:.0f} jobs/sec")

print("\n"+ "="*60)
print("HEAVY STRESS COMPLETE")
print("="*60)
