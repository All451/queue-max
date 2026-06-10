"""Tests for the CircuitBreaker."""

import time

import pytest

from queue_max import CircuitBreakerOpenError
from queue_max.core.circuit_breaker import CircuitBreaker, CircuitState


class TestCircuitBreaker:
    def test_initial_state_closed(self):
        """Test circuit breaker starts in CLOSED state."""
        cb = CircuitBreaker()
        assert cb.state == CircuitState.CLOSED

    def test_successful_call(self):
        """Test successful calls keep circuit closed."""
        cb = CircuitBreaker()

        def success():
            return "ok"

        result = cb.call(success)
        assert result == "ok"
        assert cb.state == CircuitState.CLOSED

    def test_open_after_threshold(self):
        """Test circuit opens after failure threshold."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60)

        def fail():
            raise ValueError("fail")

        for _ in range(3):
            with pytest.raises(ValueError):
                cb.call(fail)

        assert cb.state == CircuitState.OPEN

    def test_open_blocks_requests(self):
        """Test open circuit rejects requests."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=60)

        def fail():
            raise ValueError("fail")

        for _ in range(2):
            with pytest.raises(ValueError):
                cb.call(fail)

        with pytest.raises(CircuitBreakerOpenError):
            cb.call(lambda: "ok")

    def test_half_open_after_timeout(self):
        """Test circuit transitions to HALF_OPEN after recovery timeout."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1)

        def fail():
            raise ValueError("fail")

        for _ in range(2):
            with pytest.raises(ValueError):
                cb.call(fail)

        assert cb.state == CircuitState.OPEN

        # After timeout, the next attempt transitions to HALF_OPEN
        time.sleep(0.2)
        # _try_call is called inside call(), checks timeout, sets HALF_OPEN, returns True
        # The function succeeds since lambda doesn't raise
        result = cb.call(lambda: "recovered")
        assert result == "recovered"
        assert cb.state == CircuitState.CLOSED  # Success in HALF_OPEN closes circuit

    def test_half_open_success_closes(self):
        """Test HALF_OPEN -> CLOSED on success."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1)

        def fail():
            raise ValueError("fail")

        for _ in range(2):
            with pytest.raises(ValueError):
                cb.call(fail)

        time.sleep(0.2)

        result = cb.call(lambda: "recovered")
        assert result == "recovered"
        assert cb.state == CircuitState.CLOSED

    def test_half_open_failure_reopens(self):
        """Test HALF_OPEN -> OPEN on failure."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1)

        def fail():
            raise ValueError("fail")

        for _ in range(2):
            with pytest.raises(ValueError):
                cb.call(fail)

        time.sleep(0.2)

        with pytest.raises(ValueError):
            cb.call(fail)

        assert cb.state == CircuitState.OPEN

    def test_reset(self):
        """Test reset restores closed state."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=60)

        def fail():
            raise ValueError("fail")

        for _ in range(2):
            with pytest.raises(ValueError):
                cb.call(fail)

        cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb.get_stats()["failure_count"] == 0

    def test_get_stats(self):
        """Test statistics reporting."""
        cb = CircuitBreaker(failure_threshold=5)
        stats = cb.get_stats()
        assert stats["state"] == "closed"
        assert stats["failure_count"] == 0
        assert stats["failure_threshold"] == 5

    def test_on_state_change_callback(self):
        """Test state change callback is invoked."""
        changes = []

        def callback(old, new):
            changes.append((old.value, new.value))

        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1, on_state_change=callback)

        def fail():
            raise ValueError("fail")

        for _ in range(2):
            with pytest.raises(ValueError):
                cb.call(fail)

        assert ("closed", "open") in [c for c in changes]
