"""Tests for ShardGroup."""

from queue_max.core.db import ShardGroup


class TestShardGroup:
    def test_single_group_for_few_shards(self):
        """≤4 shards → single group with all shards."""
        sg = ShardGroup(3)
        assert len(sg) == 1
        assert sg.groups[0] == [0, 1, 2]

    def test_four_shards_single_group(self):
        sg = ShardGroup(4)
        assert len(sg) == 1
        assert sg.groups[0] == [0, 1, 2, 3]

    def test_eight_shards_four_groups(self):
        sg = ShardGroup(8)
        assert len(sg) == 4
        assert sg.groups == [[0, 1], [2, 3], [4, 5], [6, 7]]

    def test_sixteen_shards_four_groups(self):
        sg = ShardGroup(16)
        assert len(sg) == 4
        assert sg.groups == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11], [12, 13, 14, 15]]

    def test_randomized_groups_returns_all_shards(self):
        sg = ShardGroup(6)
        all_shards = set()
        for group in sg.randomized_groups():
            for s in group:
                all_shards.add(s)
        assert all_shards == {0, 1, 2, 3, 4, 5}

    def test_randomized_groups_are_shuffled(self):
        sg = ShardGroup(8)
        results = set()
        for _ in range(50):
            g = sg.randomized_groups()
            results.add(tuple(g[0]))
        # At least 2 different orderings seen (probabilistic)
        assert len(results) >= 2

    def test_group_for_shard(self):
        sg = ShardGroup(8)
        assert sg.group_for_shard(0) == [0, 1]
        assert sg.group_for_shard(3) == [2, 3]
        assert sg.group_for_shard(7) == [6, 7]

    def test_group_for_shard_out_of_range(self):
        sg = ShardGroup(8)
        assert sg.group_for_shard(999) == []

    def test_repr(self):
        sg = ShardGroup(6)
        r = repr(sg)
        assert "ShardGroup" in r
        assert "6 shards" in r
