"""Tests for the RateLimiter."""

import time

import pytest

from queue_max import RateLimitError
from queue_max.core.rate_limiter import RateLimiter


class TestRateLimiter:
    def test_initialization(self):
        """Test default rate limit configuration."""
        rl = RateLimiter()
        assert rl.rate_limit == 160
        assert rl.interval == 60.0 / 160

    def test_custom_rate_limit(self):
        """Test custom rate limit."""
        rl = RateLimiter(rate_limit=50)
        assert rl.rate_limit == 50

    def test_acquire_token(self):
        """Test basic token acquisition."""
        rl = RateLimiter(rate_limit=1000)  # High limit
        result = rl.acquire(timeout=1.0)
        assert result is True

    def test_get_stats(self):
        """Test statistics reporting."""
        rl = RateLimiter(rate_limit=100)
        stats = rl.get_stats()
        assert stats["rate_limit"] == 100
        assert stats["tokens_remaining"] > 0
        assert stats["interval"] > 0

    def test_reset(self):
        """Test reset restores tokens."""
        rl = RateLimiter(rate_limit=100)
        rl.acquire(timeout=1.0)
        rl.acquire(timeout=1.0)
        rl.acquire(timeout=1.0)
        rl.reset()
        stats = rl.get_stats()
        assert stats["tokens_remaining"] == 100

    def test_rate_limit_enforced(self):
        """Test that rate limit is enforced."""
        rl = RateLimiter(rate_limit=5)
        # Consume all 5 tokens
        for _ in range(5):
            rl.acquire(timeout=1.0)

        # Next acquisition should fail quickly (no tokens, short timeout)
        start = time.monotonic()
        with pytest.raises(RateLimitError):
            rl.acquire(timeout=0.5)
        elapsed = time.monotonic() - start
        assert elapsed < 5.0  # Should fail quickly, not wait full interval

    def test_thread_safety(self):
        """Test that rate limiter is thread-safe."""
        import threading

        rl = RateLimiter(rate_limit=200)
        errors = []

        def worker():
            try:
                for _ in range(10):
                    rl.acquire(timeout=5.0)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0

    def test_token_recovery(self):
        """Test that tokens recover over time."""
        rl = RateLimiter(rate_limit=60)  # 1 token per second
        # Use all tokens
        while True:
            try:
                rl.acquire(timeout=0.1)
            except RateLimitError:
                break

        stats = rl.get_stats()
        assert stats["tokens_remaining"] < 1

        # Wait for 1 token to refill
        time.sleep(1.1)
        stats = rl.get_stats()
        assert stats["tokens_remaining"] >= 0.9
