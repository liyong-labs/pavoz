"""PostgresStorage — psycopg3 sync driver. DSN; 默认表 stageflow_kv; JSONB."""
from __future__ import annotations

try:
    import psycopg
    import psycopg.rows
except ImportError as _e:
    raise ImportError(
        "PostgresStorage 需要 psycopg. 安装: pip install 'stageflow[postgres]'"
    ) from _e

from stageflow.storage import StorageBackend

from ._base import _validate_key  # psycopg3 自动 dict<->jsonb, 不需 _encode/decode

__all__ = ["PostgresStorage"]


class PostgresStorage(StorageBackend):
    def __init__(self, dsn: str, *, table: str = "stageflow_kv", schema: str = "public"):
        self._dsn = dsn
        self.table = table
        self.schema = schema
        self._conn = psycopg.connect(dsn, autocommit=False, row_factory=psycopg.rows.dict_row)
        self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} "
            "(key TEXT PRIMARY KEY, value JSONB NOT NULL)"
        )
        self._conn.commit()

    def put(self, key: str, data: dict) -> None:
        _validate_key(key)
        with self._conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {self.schema}.{self.table} (key, value) VALUES (%s, %s) "
                f"ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                (key, data),
            )
        self._conn.commit()

    def get(self, key: str) -> dict | None:
        _validate_key(key)
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT value FROM {self.schema}.{self.table} WHERE key = %s", (key,)
            )
            row = cur.fetchone()
        if row is None:
            return None
        return row["value"]

    def list_keys(self, prefix: str) -> list[str]:
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT key FROM {self.schema}.{self.table} "
                f"WHERE key LIKE %s ORDER BY key",
                (prefix + "%",),
            )
            return [r["key"] for r in cur.fetchall()]

    def delete(self, key: str) -> None:
        _validate_key(key)
        with self._conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self.schema}.{self.table} WHERE key = %s", (key,)
            )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
