"""SqliteStorage — stdlib sqlite3, 无第三方依赖. 单文件 DB; 默认表 stageflow_kv."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from stageflow.storage import StorageBackend

from ._base import _decode_payload, _encode_payload, _validate_key

__all__ = ["SqliteStorage"]


class SqliteStorage(StorageBackend):
    def __init__(self, path: str | Path, *, table: str = "stageflow_kv"):
        self.path = str(path)
        self.table = table
        self._conn = sqlite3.connect(self.path)
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} (key TEXT PRIMARY KEY, value BLOB NOT NULL)"
        )
        self._conn.commit()

    def put(self, key: str, data: dict) -> None:
        _validate_key(key)
        self._conn.execute(
            f"INSERT OR REPLACE INTO {self.table} (key, value) VALUES (?, ?)",
            (key, _encode_payload(data)),
        )
        self._conn.commit()

    def get(self, key: str) -> dict | None:
        _validate_key(key)
        cur = self._conn.execute(f"SELECT value FROM {self.table} WHERE key = ?", (key,))
        row = cur.fetchone()
        if row is None:
            return None
        return _decode_payload(row[0])

    def list_keys(self, prefix: str) -> list[str]:
        cur = self._conn.execute(
            f"SELECT key FROM {self.table} WHERE key LIKE ? ORDER BY key",
            (prefix + "%",),
        )
        return [r[0] for r in cur.fetchall()]

    def delete(self, key: str) -> None:
        _validate_key(key)
        self._conn.execute(f"DELETE FROM {self.table} WHERE key = ?", (key,))
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
