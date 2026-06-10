"""Django integration for Robusta Queue.

Provides a @task decorator that integrates with Django models
and management commands for queue operations.

Usage:
    # settings.py
    INSTALLED_APPS = [
        ...
        'queue_max.contrib.django',
    ]

    ROBUSTA_QUEUE = {
        'SHARDS': 4,
        'RATE_LIMIT': 160,
        'MAX_RETRIES': 3,
    }

    # tasks.py
    from queue_max.contrib.django import task

    @task
    def send_welcome_email(user_id):
        from myapp.models import User
        user = User.objects.get(id=user_id)
        # send email
"""

from typing import Any, Callable, Dict, Optional

from queue_max import Queue as BaseQueue
from queue_max import task as base_task


def _get_django_queue() -> BaseQueue:
    """Get or create a Queue configured from Django settings."""
    try:
        from django.conf import settings
    except ImportError:
        return BaseQueue()

    config = getattr(settings, "ROBUSTA_QUEUE", {})
    return BaseQueue(
        shards=config.get("SHARDS"),
        rate_limit=config.get("RATE_LIMIT"),
        max_retries=config.get("MAX_RETRIES"),
        data_dir=config.get("DATA_DIR"),
    )


def task(
    queue: Optional[BaseQueue] = None,
    priority: int = 0,
    max_retries: Optional[int] = None,
) -> Callable:
    """Django-aware @task decorator.

    Uses Django settings if available for queue configuration.
    """
    _queue = queue or _get_django_queue()
    return base_task(queue=_queue, priority=priority, max_retries=max_retries)
