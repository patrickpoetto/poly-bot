"""Unit tests for the WS sharding partitioner."""

from __future__ import annotations

from poly_bot.catalog.ws_daemon import _partition


def test_partition_empty() -> None:
    assert _partition([], shard_size=500, max_shards=16) == []


def test_partition_under_one_shard() -> None:
    tokens = [f"t{i:03d}" for i in range(120)]
    groups = _partition(tokens, shard_size=500, max_shards=16)
    assert len(groups) == 1
    assert groups[0] == sorted(tokens)


def test_partition_balanced() -> None:
    tokens = [f"t{i:04d}" for i in range(3300)]
    groups = _partition(tokens, shard_size=500, max_shards=16)
    # 3300 / 500 = ceil 7 shards
    assert len(groups) == 7
    sizes = [len(g) for g in groups]
    assert max(sizes) - min(sizes) <= 1
    flat = [t for g in groups for t in g]
    assert sorted(flat) == sorted(tokens)


def test_partition_max_shards_cap() -> None:
    tokens = [f"t{i:05d}" for i in range(50_000)]
    groups = _partition(tokens, shard_size=500, max_shards=8)
    assert len(groups) == 8
    sizes = [len(g) for g in groups]
    assert max(sizes) - min(sizes) <= 1


def test_partition_is_stable_for_sorted_input() -> None:
    a = ["b", "a", "c", "d"]
    b = ["d", "c", "b", "a"]
    assert _partition(a, shard_size=2, max_shards=4) == _partition(
        b, shard_size=2, max_shards=4
    )
