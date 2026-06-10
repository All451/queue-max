"""Circuit breaker implementation for Robusta Queue.

Implements the standard circuit breaker pattern with three states:
- CLOSED: Normal operation, requests pass through
- OPEN: Circuit is tripped, requests are rejected immediately
- HALF_OPEN: Testing state, allows one request through to check if service recovered
"""

import threading
import time
from enum import Enum
from typing import Callable, Optional


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker for external service calls.

    Prevents cascading failures by failing fast when a service is unhealthy.

    Attributes:
        failure_threshold: Number of consecutive failures to trip the circuit.
        recovery_timeout: Seconds to wait before attempting recovery (HALF_OPEN).
        state: Current circuit state.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        on_state_change: Optional[Callable[[CircuitState, CircuitState], None]] = None,
    ):
        """Initialize the circuit breaker.

        Args:
            failure_threshold: Consecutive failures before opening (default: 5).
            recovery_timeout: Seconds before attempting recovery (default: 60).
            on_state_change: Optional callback for state transitions.
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.on_state_change = on_state_change

        self.state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._mutex = threading.Lock()

    def call(self, func: Callable, *args, **kwargs):
        """Execute a function with circuit breaker protection.

        Args:
            func: The function to execute.
            *args: Arguments for the function.
            **kwargs: Keyword arguments for the function.

        Returns:
            The return value of the function.

        Raises:
            CircuitBreakerOpenError: If the circuit is open.
            Exception: Re-raises any exception from the called function.
        """
        if not self._try_call():
            from queue_max.exceptions import CircuitBreakerOpenError

            raise CircuitBreakerOpenError(
                f"Circuit breaker is OPEN (state: {self.state.value}). "
                f"Recovery in {self._recovery_remaining():.0f}s. "
                f"Failures: {self._failure_count}/{self.failure_threshold}"
            )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _try_call(self) -> bool:
        """Check if a call should be allowed through.

        Returns:
            True if the call is allowed, False if rejected.
        """
        with self._mutex:
            if self.state == CircuitState.CLOSED:
                return True
            elif self.state == CircuitState.OPEN:
                if time.monotonic() - self._last_failure_time >= self.recovery_timeout:
                    self._set_state(CircuitState.HALF_OPEN)
                    return True
                return False
            elif self.state == CircuitState.HALF_OPEN:
                return True
            return False

    def _on_success(self) -> None:
        """Handle a successful call."""
        with self._mutex:
            if self.state == CircuitState.HALF_OPEN:
                self._set_state(CircuitState.CLOSED)
            self._failure_count = 0

    def _on_failure(self) -> None:
        """Handle a failed call."""
        with self._mutex:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            if self.state == CircuitState.HALF_OPEN:
                self._set_state(CircuitState.OPEN)
            elif (
                self.state == CircuitState.CLOSED
                and self._failure_count >= self.failure_threshold
            ):
                self._set_state(CircuitState.OPEN)

    def _set_state(self, new_state: CircuitState) -> None:
        """Set the circuit state and trigger callback if configured.

        Args:
            new_state: The new circuit state.
        """
        old_state = self.state
        self.state = new_state
        if self.on_state_change and old_state != new_state:
            self.on_state_change(old_state, new_state)

    def _recovery_remaining(self) -> float:
        """Calculate seconds remaining until recovery attempt."""
        remaining = self.recovery_timeout - (time.monotonic() - self._last_failure_time)
        return max(0.0, remaining)

    def reset(self) -> None:
        """Reset the circuit breaker to closed state."""
        with self._mutex:
            self._set_state(CircuitState.CLOSED)
            self._failure_count = 0
            self._last_failure_time = 0.0

    def get_stats(self) -> dict:
        """Get current circuit breaker statistics.

        Returns:
            Dict with 'state', 'failure_count', 'failure_threshold', etc.
        """
        with self._mutex:
            return {
                "state": self.state.value,
                "failure_count": self._failure_count,
                "failure_threshold": self.failure_threshold,
                "recovery_timeout": self.recovery_timeout,
                "recovery_remaining": round(self._recovery_remaining(), 1),
            }
