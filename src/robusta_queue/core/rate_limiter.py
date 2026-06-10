"""Token bucket rate limiter for Robusta Queue.

Thread-safe rate limiter with configurable rate limits, burst capacity,
and adaptive rate adjustment.

Features:
- Per-minute, per-second, and per-hour limits
- Burst capacity control
- Configurable jitter
- Dynamic rate adjustment
- Pre-configured limiters for popular APIs
"""

import threading
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Optional

from robusta_queue.exceptions import RateLimitError


class RateLimitUnit(Enum):
    """Time unit for rate limiting."""
    PER_MINUTE = "per_minute"
    PER_SECOND = "per_second"
    PER_HOUR = "per_hour"


class RateLimiter:
    """Token bucket rate limiter.

    Thread-safe rate limiter that distributes requests uniformly over time.

    Attributes:
        rate_limit: Maximum requests per unit.
        unit: Time unit for rate limiting.
        burst_capacity: Maximum tokens that can accumulate.
    """

    def __init__(
        self,
        rate_limit: int = 160,
        unit: RateLimitUnit = RateLimitUnit.PER_MINUTE,
        burst_capacity: Optional[int] = None,
        enable_jitter: bool = True,
    ):
        """Initialize the rate limiter.

        Args:
            rate_limit: Maximum requests per unit (default: 160).
            unit: Time unit (PER_MINUTE, PER_SECOND, PER_HOUR).
            burst_capacity: Maximum tokens that can accumulate (default: rate_limit).
            enable_jitter: Add jitter to refill (default: True).
        """
        self.rate_limit = rate_limit
        self.unit = unit
        self.burst_capacity = burst_capacity or rate_limit
        self.enable_jitter = enable_jitter

        if unit == RateLimitUnit.PER_SECOND:
            self.interval = 1.0 / rate_limit
        elif unit == RateLimitUnit.PER_HOUR:
            self.interval = 3600.0 / rate_limit
        else:
            self.interval = 60.0 / rate_limit

        self._tokens = float(self.burst_capacity)
        self._last_refill = time.monotonic()
        self._mutex = threading.Lock()

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

    def acquire(self, timeout: float = 30.0) -> bool:
        """Acquire a token, blocking until one is available.

        Args:
            timeout: Maximum time to wait in seconds (default: 30.0).

        Returns:
            True if a token was acquired.

        Raises:
            RateLimitError: If unable to acquire a token within the timeout.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._try_acquire():
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(self.interval * 0.5, remaining * 0.1))
        raise RateLimitError(
            f"Rate limit exceeded: could not acquire token within {timeout}s "
            f"(limit: {self.rate_limit}/{self.unit.value})"
        )

    def try_acquire(self) -> bool:
        """Try to acquire a token without blocking.

        Returns:
            True if a token was acquired.
        """
        return self._try_acquire()

    def _try_acquire(self) -> bool:
        """Try to acquire a token without blocking."""
        with self._mutex:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        if self.unit == RateLimitUnit.PER_SECOND:
            refill_rate = self.rate_limit
        elif self.unit == RateLimitUnit.PER_HOUR:
            refill_rate = self.rate_limit / 3600.0
        else:
            refill_rate = self.rate_limit / 60.0
        tokens_to_add = elapsed * refill_rate
        if self.enable_jitter and tokens_to_add > 0:
            jitter = tokens_to_add * 0.1 * ((hash(str(now)) % 100) / 100.0)
            tokens_to_add += jitter
        self._tokens = min(self.burst_capacity, self._tokens + tokens_to_add)
        self._last_refill = now

    def update_rate_limit(self, new_rate_limit: int) -> None:
        """Dynamically update the rate limit.

        Args:
            new_rate_limit: New requests per unit limit.
        """
        with self._mutex:
            ratio = new_rate_limit / self.rate_limit if self.rate_limit > 0 else 1
            self._tokens = min(self.burst_capacity, self._tokens * ratio)
            self.rate_limit = new_rate_limit
            if self.burst_capacity > new_rate_limit:
                self.burst_capacity = new_rate_limit
                self._tokens = min(self._tokens, self.burst_capacity)
            if self.unit == RateLimitUnit.PER_SECOND:
                self.interval = 1.0 / new_rate_limit
            elif self.unit == RateLimitUnit.PER_HOUR:
                self.interval = 3600.0 / new_rate_limit
            else:
                self.interval = 60.0 / new_rate_limit

    def reset(self) -> None:
        """Reset the rate limiter to full tokens."""
        with self._mutex:
            self._tokens = float(self.burst_capacity)
            self._last_refill = time.monotonic()

    def get_remaining_tokens(self) -> float:
        """Get the number of tokens currently available."""
        with self._mutex:
            self._refill()
            return self._tokens

    def get_retry_after(self) -> float:
        """Get recommended retry delay in seconds.

        Returns:
            Seconds to wait before next attempt (0 if token available).
        """
        with self._mutex:
            if self._tokens >= 1:
                return 0
            if self.unit == RateLimitUnit.PER_SECOND:
                tps = self.rate_limit
            elif self.unit == RateLimitUnit.PER_HOUR:
                tps = self.rate_limit / 3600.0
            else:
                tps = self.rate_limit / 60.0
            return (1 - self._tokens) / tps if tps > 0 else self.interval

    def get_stats(self) -> Dict[str, float]:
        """Get current rate limiter statistics."""
        with self._mutex:
            now = time.monotonic()
            elapsed = now - self._last_refill
            if self.unit == RateLimitUnit.PER_SECOND:
                refill_rate = self.rate_limit
            elif self.unit == RateLimitUnit.PER_HOUR:
                refill_rate = self.rate_limit / 3600.0
            else:
                refill_rate = self.rate_limit / 60.0
            tokens = min(self.burst_capacity, self._tokens + elapsed * refill_rate)
            return {
                "tokens_remaining": round(tokens, 2),
                "rate_limit": self.rate_limit,
                "burst_capacity": self.burst_capacity,
                "interval": round(self.interval, 4),
                "unit": self.unit.value,
            }
