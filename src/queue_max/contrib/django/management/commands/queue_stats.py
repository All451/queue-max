"""Django management command to display queue statistics."""

import json

from django.core.management.base import BaseCommand

from queue_max import Queue


class Command(BaseCommand):
    """Display Robusta Queue statistics."""

    help = "Display Robusta Queue statistics"

    def add_arguments(self, parser):
        parser.add_argument("--shard", type=int, default=None, help="Specific shard")
        parser.add_argument("--json", action="store_true", help="Output as JSON")

    def handle(self, *args, **options):
        from queue_max.contrib.django import _get_django_queue

        queue = _get_django_queue()

        if options["shard"] is not None:
            stats = queue.shard_manager.get_stats(options["shard"])
        else:
            stats = queue.get_stats()

        if options["json"]:
            self.stdout.write(json.dumps(stats, indent=2))
            return

        self.stdout.write("\n  Robusta Queue Statistics")
        self.stdout.write("=" * 40)
        self.stdout.write(f"  Pending:     {stats.get('pending', 0)}")
        self.stdout.write(f"  Processing:  {stats.get('processing', 0)}")
        self.stdout.write(f"  Failed:      {stats.get('failed', 0)}")
        self.stdout.write(f"  Shards:      {stats.get('num_shards', queue.num_shards)}")
        self.stdout.write(f"  Circuit:     {stats.get('circuit_state', 'closed')}")
