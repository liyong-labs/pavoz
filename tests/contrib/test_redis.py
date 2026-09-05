import pytest

from stageflow.contrib.storage.redis import RedisStorage


@pytest.fixture
def fake_redis(monkeypatch):
    fakeredis = pytest.importorskip("fakeredis")
    fake = fakeredis.FakeRedis()
    monkeypatch.setattr("redis.Redis", lambda *a, **kw: fake)
    return fake


@pytest.fixture
def store(fake_redis) -> RedisStorage:
    return RedisStorage(url="redis://test/0", prefix="sf:")


def test_put_get_roundtrip(store):
    store.put("k", {"a": 1, "b": "中文"})
    assert store.get("k") == {"a": 1, "b": "中文"}


def test_get_missing_returns_none(store):
    assert store.get("nope") is None


def test_put_overwrites(store):
    store.put("k", {"v": 1})
    store.put("k", {"v": 2})
    assert store.get("k") == {"v": 2}


def test_delete_silent_on_missing(store):
    store.delete("never-existed")


def test_list_keys_prefix(store):
    store.put("a/1", {})
    store.put("a/2", {})
    store.put("b/1", {})
    assert store.list_keys("a/") == ["a/1", "a/2"]


def test_prefix_isolation(store, fake_redis):
    store.put("k", {"v": 1})
    raw = fake_redis.get("sf:k")
    assert raw is not None


def test_unicode_safe(store):
    data = {"emoji": "🎉", "中文": "ok"}
    store.put("u", data)
    assert store.get("u") == data
