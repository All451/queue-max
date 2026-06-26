"""Token bucket rate limiter for Queue Max.

Thread-safe rate limiter with configurable rate limits, burst capacity,
and adaptive rate adjustment. Uses a classic token bucket algorithm.

Features:
- Per-minute, per-second, and per-hour limits
- Burst capacity control
- Dynamic rate adjustment
- Condition-based waiting (no polling)
- Pre-configured limiters for popular APIs
"""

import random
import threading
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from queue_max.exceptions import RateLimitError


class RateLimitUnit(Enum):
    """Time unit for rate limiting."""
    PER_MINUTE = "per_minute"
    PER_SECOND = "per_second"
    PER_HOUR = "per_hour"


class RateLimiter:
    """Token bucket rate limiter.

    Thread-safe rate limiter that distributes requests uniformly over time.
    Uses ``threading.Condition`` so waiters are woken when a token becomes
    available, not by busy-polling.

    Attributes:
        rate_limit: Maximum requests per unit.
        unit: Time unit for rate limiting.
        burst_capacity: Maximum tokens that can accumulate (independent of rate).
    """

    def __init__(
        self,
        rate_limit: int = 160,
        unit: RateLimitUnit = RateLimitUnit.PER_MINUTE,
        burst_capacity: Optional[int] = None,
    ):
        """Initialize the rate limiter.

        Args:
            rate_limit: Maximum requests per unit (default: 160, minimum 1).
            unit: Time unit (PER_MINUTE, PER_SECOND, PER_HOUR).
            burst_capacity: Maximum tokens that can accumulate
                (default: same as rate_limit, minimum 1).

        Raises:
            ValueError: If rate_limit < 1 or burst_capacity < 1.
        """
        if rate_limit < 1:
            raise ValueError(f"rate_limit must be >= 1, got {rate_limit}")

        self.rate_limit = rate_limit
        self.unit = unit
        self.burst_capacity = burst_capacity or rate_limit
        if self.burst_capacity < 1:
            raise ValueError(
                f"burst_capacity must be >= 1, got {self.burst_capacity}"
            )

        self.interval = _compute_interval(rate_limit, unit)
        self._tokens = float(self.burst_capacity)
        self._last_refill = time.monotonic()
        self._total_granted = 0
        self._total_denied = 0
        self._total_wait_seconds = 0.0
        self._mutex = threading.Lock()
        self._cond = threading.Condition(self._mutex)
        self._rng = random.Random()

    @classmethod
    def per_second(cls, rate_limit: int, **kwargs) -> "RateLimiter":
        """Create a per-second rate limiter."""
        return cls(rate_limit, unit=RateLimitUnit.PER_SECOND, **kwargs)

    @classmethod
    def per_minute(cls, rate_limit: int, **kwargs) -> "RateLimiter":
        """Create a per-minute rate limiter."""
        return cls(rate_limit, unit=RateLimitUnit.PER_MINUTE, **kwargs)

    @classmethod
    def per_hour(cls, rate_limit: int, **kwargs) -> "RateLimiter":
        """Create a per-hour rate limiter."""
        return cls(rate_limit, unit=RateLimitUnit.PER_HOUR, **kwargs)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def acquire(self, timeout: float = 30.0) -> bool:
        """Acquire a token, blocking until one is available.

        Uses a ``Condition`` to wait efficiently — the caller is woken
        when new tokens are refilled instead of busy-polling.

        Args:
            timeout: Maximum time to wait in seconds (default: 30.0, must be > 0).

        Returns:
            True if a token was acquired.

        Raises:
            RateLimitError: If unable to acquire a token within the timeout.
        """
        deadline = time.monotonic() + timeout
        with self._cond:
            self._refill()
            while self._tokens < 1.0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._total_denied += 1
                    raise RateLimitError(
                        f"Rate limit exceeded: could not acquire token within "
                        f"{timeout}s (limit: {self.rate_limit}/{self.unit.value})"
                    )
                wait_start = time.monotonic()
                self._cond.wait(timeout=remaining)
                self._total_wait_seconds += time.monotonic() - wait_start
                self._refill()
            self._tokens -= 1.0
            self._total_granted += 1
            return True

    def try_acquire(self) -> bool:
        """Try to acquire a token without blocking.

        Returns:
            True if a token was acquired immediately, False otherwise.
        """
        with self._cond:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                self._total_granted += 1
                return True
            self._total_denied += 1
            return False

    def update_rate_limit(self, new_rate_limit: int) -> None:
        """Dynamically update the rate limit.

        Calls ``_refill()`` *before* changing the rate so no accumulated
        tokens are lost. ``burst_capacity`` is left independent — it is
        NOT clamped to the new rate.

        Args:
            new_rate_limit: New requests per unit (must be >= 1).
        """
        if new_rate_limit < 1:
            raise ValueError(
                f"new_rate_limit must be >= 1, got {new_rate_limit}"
            )
        with self._mutex:
            self._refill()
            self.rate_limit = new_rate_limit
            self.interval = _compute_interval(new_rate_limit, self.unit)
            self._tokens = min(self._tokens, self.burst_capacity)

    def reset(self) -> None:
        """Reset the rate limiter to full tokens."""
        with self._mutex:
            self._tokens = float(self.burst_capacity)
            self._last_refill = time.monotonic()
            self._total_granted = 0
            self._total_denied = 0
            self._total_wait_seconds = 0.0

    # ------------------------------------------------------------------
    # Info
    # ------------------------------------------------------------------

    def get_remaining_tokens(self) -> float:
        """Get the number of tokens currently available."""
        with self._mutex:
            self._refill()
            return self._tokens

    def get_retry_after(self) -> float:
        """Get recommended retry delay in seconds.

        Returns:
            Seconds to wait before next attempt (0 if token available now).
        """
        with self._mutex:
            self._refill()
            if self._tokens >= 1:
                return 0
            tps = self._tokens_per_second()
            return (1 - self._tokens) / tps if tps > 0 else self.interval

    def get_stats(self) -> dict:
        """Get current rate limiter statistics.

        Returns:
            Dict with token count, config, and cumulative metrics.
        """
        with self._mutex:
            self._refill()
            return {
                "tokens_remaining": round(self._tokens, 2),
                "rate_limit": self.rate_limit,
                "burst_capacity": self.burst_capacity,
                "interval": round(self.interval, 4),
                "unit": self.unit.value,
                "total_granted": self._total_granted,
                "total_denied": self._total_denied,
                "total_wait_seconds": round(self._total_wait_seconds, 2),
            }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _refill(self) -> None:
        """Refill tokens based on elapsed time.

        Notifies waiters when tokens are added so ``acquire()`` can
        wake up without polling.
        """
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed <= 0:
            return
        tokens_to_add = elapsed * self._tokens_per_second()
        if tokens_to_add > 0:
            self._tokens = min(self.burst_capacity, self._tokens + tokens_to_add)
            self._last_refill = now
            self._cond.notify_all()

    def _tokens_per_second(self) -> float:
        """Return the token refill rate as tokens per second."""
        if self.unit == RateLimitUnit.PER_SECOND:
            return float(self.rate_limit)
        elif self.unit == RateLimitUnit.PER_HOUR:
            return self.rate_limit / 3600.0
        return self.rate_limit / 60.0


def _compute_interval(rate_limit: int, unit: RateLimitUnit) -> float:
    """Return the average seconds between tokens for a given rate."""
    if unit == RateLimitUnit.PER_SECOND:
        return 1.0 / rate_limit
    elif unit == RateLimitUnit.PER_HOUR:
        return 3600.0 / rate_limit
    return 60.0 / rate_limit
