"""Django management command to purge old jobs."""

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    """Remove old jobs from the queue."""

    help = "Remove old jobs from Queue Max"

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=7, help="Age threshold (days)")

    def handle(self, *args, **options):
        from queue_max.contrib.django import _get_django_queue

        queue = _get_django_queue()
        removed = queue.cleanup_old_jobs(days=options["days"])
        self.stdout.write(self.style.SUCCESS(f"Removed {removed} old job(s)"))
