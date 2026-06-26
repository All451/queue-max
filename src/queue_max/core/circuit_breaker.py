"""Circuit breaker implementation for Queue Max.

Implements the standard circuit breaker pattern with three states:
- CLOSED: Normal operation, requests pass through
- OPEN: Circuit is tripped, requests are rejected immediately
- HALF_OPEN: Testing state, allows exactly one probe to check if service recovered
"""

import threading
import time
from enum import Enum
from typing import Callable, Optional, TypeVar

from queue_max.exceptions import CircuitBreakerOpenError

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker for external service calls.

    Prevents cascading failures by failing fast when a service is unhealthy.
    Thread-safe; guarantees at most one probe in HALF_OPEN state.

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
        failure_predicate: Optional[Callable[[Exception], bool]] = None,
    ):
        """Initialize the circuit breaker.

        Args:
            failure_threshold: Consecutive failures before opening (default: 5, minimum 1).
            recovery_timeout: Seconds before attempting recovery (default: 60, must be > 0).
            on_state_change: Optional callback for state transitions (invoked outside lock).
            failure_predicate: Optional callable that returns True if an exception
                should count toward the failure count. If None, all exceptions count.

        Raises:
            ValueError: If failure_threshold < 1 or recovery_timeout <= 0.
        """
        if failure_threshold < 1:
            raise ValueError(
                f"failure_threshold must be >= 1, got {failure_threshold}"
            )
        if recovery_timeout <= 0:
            raise ValueError(
                f"recovery_timeout must be > 0, got {recovery_timeout}"
            )

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.on_state_change = on_state_change
        self.failure_predicate = failure_predicate

        self.state = CircuitState.CLOSED
        self._failure_count = 0
        self._rejected_count = 0
        self._last_failure_time = 0.0
        self._last_state_change_time: float = 0.0
        self._total_time_open: float = 0.0
        self._state_change_count: int = 0
        self._probe_in_flight = False
        self._pending_notify: Optional[tuple[CircuitState, CircuitState]] = None
        self._mutex = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_allowed(self) -> bool:
        """Check if a call is currently allowed through the circuit.

        Returns:
            True if the call is allowed, False if circuit is open.
        """
        result = self._try_call()
        self._flush_pending_notify()
        return result

    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute a function with circuit breaker protection.

        Args:
            func: The function to execute.
            *args: Arguments for the function.
            **kwargs: Keyword arguments for the function.

        Returns:
            The return value of the function (typed).

        Raises:
            CircuitBreakerOpenError: If the circuit is open.
            Exception: Re-raises any exception from the called function.
        """
        if not self.is_allowed():
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
            self._on_failure(e)
            raise

    def record_success(self) -> None:
        """Record a successful call, resetting the failure count."""
        with self._mutex:
            if self.state == CircuitState.HALF_OPEN:
                self._set_state_unlocked(CircuitState.CLOSED)
            self._failure_count = 0
            self._probe_in_flight = False
            notify = self._pending_notify
            self._pending_notify = None
        self._notify(notify)

    def record_failure(self) -> None:
        """Record a failed call, potentially tripping the circuit."""
        with self._mutex:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            self._probe_in_flight = False
            if self.state == CircuitState.HALF_OPEN:
                self._set_state_unlocked(CircuitState.OPEN)
            elif (
                self.state == CircuitState.CLOSED
                and self._failure_count >= self.failure_threshold
            ):
                self._set_state_unlocked(CircuitState.OPEN)
            notify = self._pending_notify
            self._pending_notify = None
        self._notify(notify)

    def reset(self) -> None:
        """Reset the circuit breaker to closed state."""
        with self._mutex:
            if self.state != CircuitState.CLOSED:
                self._set_state_unlocked(CircuitState.CLOSED)
            self._failure_count = 0
            self._rejected_count = 0
            self._last_failure_time = 0.0
            self._probe_in_flight = False
            notify = self._pending_notify
            self._pending_notify = None
        self._notify(notify)

    def get_stats(self) -> dict:
        """Get current circuit breaker statistics.

        Returns:
            Dict with state, counters, and timing info.
        """
        with self._mutex:
            return {
                "state": self.state.value,
                "failure_count": self._failure_count,
                "failure_threshold": self.failure_threshold,
                "rejected_count": self._rejected_count,
                "recovery_timeout": self.recovery_timeout,
                "recovery_remaining": round(self._recovery_remaining(), 1),
                "total_time_open_seconds": round(self._total_time_open, 1),
                "state_change_count": self._state_change_count,
                "probe_in_flight": self._probe_in_flight,
            }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _try_call(self) -> bool:
        """Check if a call should be allowed through."""
        with self._mutex:
            if self.state == CircuitState.CLOSED:
                return True

            if self.state == CircuitState.OPEN:
                if time.monotonic() - self._last_failure_time >= self.recovery_timeout:
                    self._set_state_unlocked(CircuitState.HALF_OPEN)
                    self._probe_in_flight = True
                    return True
                self._rejected_count += 1
                return False

            # HALF_OPEN — at most one concurrent probe
            if self.state == CircuitState.HALF_OPEN:
                if not self._probe_in_flight:
                    self._probe_in_flight = True
                    return True
                self._rejected_count += 1
                return False

            return False

    def _on_success(self) -> None:
        self.record_success()

    def _on_failure(self, exception: Exception) -> None:
        if self.failure_predicate is not None and not self.failure_predicate(exception):
            return
        self.record_failure()

    def _set_state_unlocked(self, new_state: CircuitState) -> None:
        """Set circuit state (caller must hold _mutex)."""
        old_state = self.state
        if old_state == new_state:
            return

        now = time.monotonic()
        if old_state == CircuitState.OPEN:
            self._total_time_open += now - self._last_state_change_time

        self.state = new_state
        self._last_state_change_time = now
        self._state_change_count += 1
        self._pending_notify = (old_state, new_state)

    def _flush_pending_notify(self) -> None:
        """Invoke any deferred state-change notification outside the lock."""
        with self._mutex:
            notify = self._pending_notify
            self._pending_notify = None
        self._notify(notify)

    def _notify(
        self, transition: Optional[tuple[CircuitState, CircuitState]]
    ) -> None:
        """Invoke the on_state_change callback if a transition happened."""
        if transition is None or self.on_state_change is None:
            return
        old_state, new_state = transition
        try:
            self.on_state_change(old_state, new_state)
        except Exception:
            pass

    def _recovery_remaining(self) -> float:
        remaining = self.recovery_timeout - (time.monotonic() - self._last_failure_time)
        return max(0.0, remaining)
