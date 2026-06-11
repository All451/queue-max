"""Utility helpers for Robusta Queue."""

import json
import os
import random
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def now_iso() -> str:
    """Return current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO timestamp string to datetime."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def validate_payload(payload: Any) -> Dict[str, Any]:
    """Validate and sanitize a job payload.

    Args:
        payload: The payload to validate.

    Returns:
        The validated payload as a dict.

    Raises:
        ValueError: If payload is not a dict, too large, or too deep.
    """
    if not isinstance(payload, dict):
        raise ValueError(f"Payload must be a dict, got {type(payload).__name__}")

    try:
        raw = json.dumps(payload)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Payload must be JSON-serializable: {e}")
    if len(raw) > 10 * 1024 * 1024:
        raise ValueError(f"Payload too large: {len(raw)} bytes (max 10MB)")

    def _check_depth(obj, depth=0):
        if depth > 20:
            raise ValueError("Payload nesting exceeds 20 levels")
        if isinstance(obj, dict):
            for v in obj.values():
                _check_depth(v, depth + 1)
        elif isinstance(obj, (list, tuple)):
            for v in obj:
                _check_depth(v, depth + 1)

    _check_depth(payload)
    return payload


def validate_priority(priority: int) -> int:
    """Validate priority value.

    Args:
        priority: Priority value (0, 1, or 2).

    Returns:
        The validated priority.

    Raises:
        ValueError: If priority is not 0, 1, or 2.
    """
    if priority not in (0, 1, 2):
        raise ValueError(f"Priority must be 0, 1, or 2, got {priority}")
    return priority


def get_env_int(name: str, default: int) -> int:
    """Get an integer from an environment variable with a fallback default."""
    try:
        return int(os.environ.get(name, default))
    except (ValueError, TypeError):
        return default


def backoff_delay(tentativa: int, base_delay: int = 60, max_delay: int = 3600) -> float:
    """Calculate exponential backoff delay with jitter.

    Formula: delay = base * 2^(tentativa - 1)
    Jitter: +-20%
    Cap: max_delay (default 3600s = 1 hour)

    Args:
        tentativa: Current attempt number (1-based).
        base_delay: Base delay in seconds (default: 60).
        max_delay: Maximum delay in seconds (default: 3600).

    Returns:
        Delay in seconds with jitter applied.
    """
    delay = base_delay * (2 ** (tentativa - 1))
    jitter = delay * 0.2
    delay = delay + random.uniform(-jitter, jitter)
    return min(delay, max_delay)


def determine_shard(
    pagina_id: Optional[int],
    num_shards: int,
) -> int:
    """Determine which shard a job should go to.

    If pagina_id is provided, shard = pagina_id % num_shards for consistency.
    Otherwise, a random shard is chosen.

    Args:
        pagina_id: Optional ID for consistent sharding.
        num_shards: Total number of shards.

    Returns:
        Shard ID (0 to num_shards - 1).
    """
    if pagina_id is not None:
        return pagina_id % num_shards
    return random.randint(0, num_shards - 1)


def is_retryable_error(error: Exception) -> bool:
    """Determine if an error should trigger a retry.

    4xx errors (client) are permanent failures.
    5xx errors (server), 429 (rate limit), timeouts, and connection errors are retryable.

    Args:
        error: The exception to evaluate.

    Returns:
        True if the job should be retried, False for permanent failure.
    """
    import re

    error_str = str(error).lower()
    error_type = type(error).__name__.lower()

    # 4xx status codes (except 429) are permanent client errors
    http_code_match = re.search(r"\b(4\d\d)\b", error_str)
    if http_code_match and http_code_match.group(1) != "429":
        return False

    # Connection, timeout, rate-limit, and server errors are retryable
    retryable_keywords = ["timeout", "connection", "temporary", "retryable", "500", "502", "503", "429"]
    for keyword in retryable_keywords:
        if keyword in error_str or keyword in error_type:
            return True

    return True
