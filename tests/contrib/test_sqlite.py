from pathlib import Path

import pytest

from stageflow.contrib.storage.sqlite import SqliteStorage


@pytest.fixture
def store(tmp_path: Path) -> SqliteStorage:
    return SqliteStorage(tmp_path / "kv.db")


def test_put_get_roundtrip(store):
    store.put("runs/x", {"a": 1, "b": [1, 2], "c": "中文"})
    assert store.get("runs/x") == {"a": 1, "b": [1, 2], "c": "中文"}


def test_get_missing_returns_none(store):
    assert store.get("nonexistent") is None


def test_put_overwrites(store):
    store.put("k", {"v": 1})
    store.put("k", {"v": 2})
    assert store.get("k") == {"v": 2}


def test_delete_existing(store):
    store.put("k", {"v": 1})
    store.delete("k")
    assert store.get("k") is None


def test_delete_missing_is_silent(store):
    # 与 stageflow.core FileStorage.delete 一致
    store.delete("never-existed")


def test_list_keys_prefix(store):
    store.put("a/1", {"x": 1})
    store.put("a/2", {"x": 2})
    store.put("b/1", {"x": 3})
    assert store.list_keys("a/") == ["a/1", "a/2"]


def test_list_keys_returns_sorted(store):
    store.put("z", {"x": 1})
    store.put("a", {"x": 2})
    store.put("m", {"x": 3})
    assert store.list_keys("") == ["a", "m", "z"]


def test_key_safety(store):
    with pytest.raises(ValueError):
        store.put("../bad", {})
    with pytest.raises(ValueError):
        store.put("with space", {})


def test_schema_idempotent(store, tmp_path):
    store.put("k", {"v": 1})
    store2 = SqliteStorage(tmp_path / "kv.db")
    assert store2.get("k") == {"v": 1}


def test_unicode_safe(store):
    data = {"emoji": "🎉", "chinese": "中文 ok"}
    store.put("u", data)
    assert store.get("u") == data
