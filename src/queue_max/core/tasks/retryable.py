"""@retryable_task decorator for Queue Max."""

import functools
import logging
from typing import Any, Callable, Optional

from queue_max.core.tasks.base import task

logger = logging.getLogger("queue_max.task")


def retryable_task(
    max_retries: int = 5,
    retry_on: Optional[list[Exception]] = None,
    **task_kwargs,
) -> Callable:
    """Decorator for tasks that should retry synchronously on specific exceptions.

    Args:
        max_retries: Max retry attempts.
        retry_on: list of exception types to retry on (default: all).
        **task_kwargs: Arguments passed through to @task.

    Returns:
        Decorated function with .delay() and synchronous retry logic.
    """
    def decorator(func: Callable) -> Callable:
        task_kwargs.setdefault("max_retries", max_retries)
        decorated = task(**task_kwargs)(func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retry_on_exceptions = retry_on or (Exception,)
            attempt = 0
            while attempt <= max_retries:
                try:
                    return func(*args, **kwargs)
                except retry_on_exceptions as e:
                    attempt += 1
                    if attempt > max_retries:
                        raise
                    import time as _time
                    delay = task_kwargs.get("retry_delay", 60) * (2 ** (attempt - 1))
                    logger.warning(
                        "Retry %d/%d for %s in %.1fs: %s",
                        attempt, max_retries, func.__name__, delay, e,
                    )
                    _time.sleep(delay)

        wrapper.delay = decorated.delay
        wrapper.schedule_at = decorated.schedule_at
        wrapper.schedule_in = decorated.schedule_in
        wrapper.map = decorated.map
        wrapper.task_name = decorated.task_name
        return wrapper

    return decorator
