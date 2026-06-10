"""Webhook processing example with Robusta Queue."""

import json
import time

from robusta_queue import Queue, Worker

# Queue configured for webhook processing
queue = Queue(shards=4, rate_limit=160, max_retries=3)


def process_webhook(payload: dict) -> dict:
    """Process a webhook payload.

    In real usage, this would call an external API, send emails, etc.
    """
    print(f"  Processing webhook: {payload.get('event', 'unknown')}")
    print(f"  Account: {payload.get('account_id', 'N/A')}")

    # Simulate API call
    time.sleep(0.5)

    # Simulate occasional failure
    if payload.get("simulate_error"):
        raise ConnectionError("External API timeout")

    return {"status": "processed"}


def main():
    # Enqueue sample webhooks
    webhooks = [
        {"event": "user.created", "account_id": 101, "user": "alice"},
        {"event": "payment.received", "account_id": 102, "amount": 99.90},
        {"event": "order.shipped", "account_id": 101, "order_id": "ORD-123"},
        {"event": "user.deleted", "account_id": 103, "user": "bob"},
    ]

    for wh in webhooks:
        result = queue.enqueue(
            payload=wh,
            priority=2,  # Webhooks are high priority
            pagina_id=wh.get("account_id"),  # Consistent sharding per account
        )
        print(f"  Enqueued webhook: {wh['event']} (job {result['id']})")

    # Start worker
    worker = Worker("webhook-worker", process_webhook, queue)
    worker.start()

    print("\n  Processing webhooks...")
    time.sleep(5)

    # Stats
    stats = queue.get_stats()
    worker_stats = worker.get_stats()
    print(f"\n  Queue: {stats['pending']} pending, {stats['failed']} failed")
    print(f"  Worker: {worker_stats['processed']} processed")

    worker.stop()
    print("  Done!")


if __name__ == "__main__":
    main()
