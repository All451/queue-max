"""AI/LLM API usage example with rate limiting.

Shows how to use Queue Max to handle rate-limited API calls
for AI/LLM services like OpenAI, Anthropic, etc.
"""

import time

from queue_max import Queue, Worker

# Configure queue with rate limit matching your API tier
# e.g., OpenAI free tier: 3 RPM, Tier 1: 500 RPM
queue = Queue(shards=2, rate_limit=10, max_retries=3)


def call_llm_api(payload: dict) -> dict:
    """Simulate calling an LLM API with rate limiting.

    Replace with actual API call:
        import openai
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": payload['prompt']}]
        )
    """
    prompt = payload.get("prompt", "")
    print(f"  Calling LLM API with prompt: {prompt[:50]}...")

    # Simulate API latency
    time.sleep(1.0)

    # Simulate rate limit error
    import random

    if random.random() < 0.1:
        raise Exception("429 Too Many Requests")

    return {"response": f"Response to: {prompt}", "tokens": len(prompt)}


def main():
    prompts = [
        "Explain quantum computing in simple terms",
        "Write a Python decorator for logging",
        "Summarize the theory of relativity",
        "Create a recipe for vegan chocolate cake",
        "Translate 'hello world' to Japanese",
        "Write a haiku about programming",
        "Explain how databases work",
        "Describe the water cycle",
    ]

    print("  Enqueuing LLM API calls...")
    for prompt in prompts:
        result = queue.enqueue(
            payload={"prompt": prompt, "model": "gpt-4"},
            priority=1,
        )
        print(f"    Enqueued: {prompt[:30]}... (job {result['id']})")

    # Start worker with rate-limited processing
    worker = Worker("ai-worker", call_llm_api, queue)
    worker.start()

    print("\n  Processing with rate limiting (10 req/min)...")
    time.sleep(15)

    worker_stats = worker.get_stats()
    print(f"\n  Processed: {worker_stats['processed']}")
    print(f"  Failed: {worker_stats['failed']}")
    print(f"  Retried: {worker_stats['retried']}")

    worker.stop()
    print("  Done!")


if __name__ == "__main__":
    main()
