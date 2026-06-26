"""Shard grouping for efficient pop_job scanning.

Groups shards so workers scan at most shards_per_group per attempt,
reducing contention when many workers compete for jobs.
"""

import random


class ShardGroup:
    """Groups shards to optimize pop_job scanning.

    Instead of randomly scanning all N shards (O(N) worst case),
    workers scan groups of shards — checking at most shards_per_group
    before moving to the next group.

    Group size adapts to the number of shards:
    - ≤4 shards:  single group (same as flat random scan)
    -  8 shards:  4 groups of 2
    - 16 shards:  4 groups of 4
    - 32 shards:  8 groups of 4

    The adaptive sizing ensures at least 4 groups, keeping contention
    low when many workers compete for jobs.
    """

    def __init__(self, num_shards: int):
        self.num_shards = num_shards
        self.shards_per_group = (
            max(1, min(4, num_shards // 4)) if num_shards >= 8 else num_shards
        )
        self.groups: list[list[int]] = [
            list(range(
                g * self.shards_per_group,
                min((g + 1) * self.shards_per_group, num_shards),
            ))
            for g in range(
                (num_shards + self.shards_per_group - 1) // self.shards_per_group
            )
        ]

    def randomized_groups(self) -> list[list[int]]:
        """Return all groups in random order for pop_job scanning."""
        groups = list(self.groups)
        random.shuffle(groups)
        return groups

    def group_for_shard(self, shard_id: int) -> list[int]:
        """Return the group that contains the given shard ID."""
        group_idx = shard_id // self.shards_per_group
        return self.groups[group_idx] if group_idx < len(self.groups) else []

    def __len__(self) -> int:
        return len(self.groups)

    def __repr__(self) -> str:
        return (
            f"ShardGroup({self.num_shards} shards, "
            f"{len(self.groups)} groups of ~{self.shards_per_group})"
        )
