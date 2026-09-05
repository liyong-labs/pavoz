"""RedisStorage — redis-py sync driver. URL; prefix 隔离; SCAN-based list_keys."""
from __future__ import annotations

try:
    import redis
except ImportError as _e:
    raise ImportError(
        "RedisStorage 需要 redis. 安装: pip install 'stageflow[redis]'"
    ) from _e

from stageflow.storage import StorageBackend

from ._base import _decode_payload, _encode_payload, _validate_key

__all__ = ["RedisStorage"]


class RedisStorage(StorageBackend):
    def __init__(self, url: str = "redis://localhost:6379/0", *, prefix: str = "stageflow:"):
        self._client = redis.Redis(url)
        self.prefix = prefix

    def put(self, key: str, data: dict) -> None:
        _validate_key(key)
        self._client.set(self.prefix + key, _encode_payload(data))

    def get(self, key: str) -> dict | None:
        _validate_key(key)
        raw = self._client.get(self.prefix + key)
        if raw is None:
            return None
        return _decode_payload(raw)

    def list_keys(self, prefix: str) -> list[str]:
        full_prefix = self.prefix + prefix
        out: list[str] = []
        cursor = 0
        while True:
            cursor, batch = self._client.scan(
                cursor=cursor, match=full_prefix + "*", count=100
            )
            for raw_key in batch:
                if isinstance(raw_key, bytes):
                    raw_key = raw_key.decode("utf-8")
                if raw_key.startswith(self.prefix):
                    out.append(raw_key[len(self.prefix) :])
            if cursor == 0:
                break
        return sorted(out)

    def delete(self, key: str) -> None:
        _validate_key(key)
        self._client.delete(self.prefix + key)
