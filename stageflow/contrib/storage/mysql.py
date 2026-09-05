"""MySQLStorage — PyMySQL (纯 Python). DSN; 默认表 stageflow_kv; LONGTEXT."""
from __future__ import annotations

try:
    import pymysql
except ImportError as _e:
    raise ImportError(
        "MySQLStorage 需要 pymysql. 安装: pip install 'stageflow[mysql]'"
    ) from _e

from stageflow.storage import StorageBackend

from ._base import _decode_payload, _encode_payload, _validate_key

__all__ = ["MySQLStorage"]


def _parse_dsn(dsn: str) -> dict:
    from urllib.parse import urlparse

    dsn = dsn.removeprefix("mysql+pymysql://")
    p = urlparse(f"//{dsn}")
    return {
        "host": p.hostname or "localhost",
        "port": p.port or 3306,
        "user": p.username or "root",
        "password": p.password or "",
        "database": (p.path or "/").lstrip("/") or "test",
    }


class MySQLStorage(StorageBackend):
    def __init__(self, dsn: str, *, table: str = "stageflow_kv"):
        cfg = _parse_dsn(dsn)
        self.table = table
        self._conn = pymysql.connect(
            **cfg,
            charset="utf8mb4",
            autocommit=False,
            cursorclass=pymysql.cursors.DictCursor,
        )
        with self._conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS `{table}` "
                "(key VARCHAR(512) PRIMARY KEY, value LONGTEXT NOT NULL) "
                "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            )
        self._conn.commit()

    def put(self, key: str, data: dict) -> None:
        _validate_key(key)
        with self._conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO `{self.table}` (key, value) VALUES (%s, %s) "
                f"ON DUPLICATE KEY UPDATE value = VALUES(value)",
                (key, _encode_payload(data).decode("utf-8")),
            )
        self._conn.commit()

    def get(self, key: str) -> dict | None:
        _validate_key(key)
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT value FROM `{self.table}` WHERE key = %s", (key,))
            row = cur.fetchone()
        if row is None:
            return None
        return _decode_payload(row["value"].encode("utf-8"))

    def list_keys(self, prefix: str) -> list[str]:
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT key FROM `{self.table}` WHERE key LIKE %s ORDER BY key",
                (prefix + "%",),
            )
            return [r["key"] for r in cur.fetchall()]

    def delete(self, key: str) -> None:
        _validate_key(key)
        with self._conn.cursor() as cur:
            cur.execute(f"DELETE FROM `{self.table}` WHERE key = %s", (key,))
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
