"""Basic usage example for Robusta Queue."""

from robusta_queue import Queue, Worker


def process_task(payload: dict) -> str:
    """Process a task payload."""
    print(f"  Processing: {payload}")
    return f"Done: {payload.get('task', 'unknown')}"


def main():
    # 1. Create a queue (3 shards, 100 req/min)
    queue = Queue(shards=3, rate_limit=100)

    # 2. Enqueue some jobs
    for i in range(5):
        result = queue.enqueue(
            payload={"task": f"job-{i}", "data": f"payload-{i}"},
            priority=i % 3,  # Mix of priorities
            pagina_id=i % 3,  # Spread across shards
        )
        print(f"  Enqueued job {result['id']} in shard {result['shard_id']}")

    # 3. Process with a worker
    worker = Worker("example-worker", process_task, queue)
    worker.start()

    # 4. Let it process for a bit
    import time

    time.sleep(3)

    # 5. Check stats
    stats = queue.get_stats()
    print(f"\n  Queue stats: {stats}")

    worker.stop()
    print("  Done!")


if __name__ == "__main__":
    main()
