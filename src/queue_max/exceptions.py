"""Custom exceptions for the Robusta Queue library."""


class QueueError(Exception):
    """Base exception for all queue-related errors."""


class RateLimitError(QueueError):
    """Raised when rate limit is exceeded and the operation times out."""


class CircuitBreakerOpenError(QueueError):
    """Raised when the circuit breaker is open and rejecting requests."""


class JobFailedError(QueueError):
    """Raised when a job has permanently failed."""


class ShardError(QueueError):
    """Raised when there is an error related to a specific shard."""


class ConfigurationError(QueueError):
    """Raised when there is an invalid configuration."""
