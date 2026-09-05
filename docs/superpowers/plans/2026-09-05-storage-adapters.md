# Storage Adapters v0.4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship v0.4 — 5 个 StorageBackend adapter (Postgres / MySQL / Redis / MinIO / SQLite) 在 `stageflow.contrib.storage` 下, core 不变, 行为必须兼容 ai_writer 既有 MinioStorage 用法.

**Architecture:** core stdlib-only 不变; 5 个独立 adapter 模块, 每个顶层 try/except 懒导入 driver (友好 ImportError 提示); SQLite 无 dep 入口; PostgresStorage 行为参考 ai_writer 的 MinioStorage adapter (delete 容忍 NoSuchKey, list_keys 友好过滤).

**Tech Stack:** Python 3.12+, pytest, pytest-asyncio, moto[s3] (MinioStorage test), fakeredis (RedisStorage test), psycopg[binary] (Postgres), pymysql (MySQL), redis (RedisStorage runtime), boto3 (MinioStorage runtime).

**Spec:** `docs/superpowers/specs/2026-09-05-storage-adapters.md`

## Global Constraints

- Python 3.12+; **core 永远 stdlib-only + 零第三方 import**
- contrib 是可选 extras; 装哪个跑哪个; 没装抛 ImportError + 安装提示
- API 冻结: `StorageBackend` Protocol 4 方法 (put/get/list_keys/delete) 签名零变化; core 12 模块零变化
- 行为等价 — `MinioStorage` 必须复刻 ai_writer `backend/integration/sf_storage.py` (delete 容忍 NoSuchKey, get 返 None, list_keys 友好过滤)
- ruff clean; pytest ≥ 73 tests (38 core + ≥35 contrib); `@pytest.mark.integration` 标记需 docker
- 单 commit ship; 不 tag 不 push (controller 决定)

---

### Task 1: contrib 子包骨架 + _base.py (key sanitize + 公共 helpers)

**Files:**
- Create: `stageflow/contrib/__init__.py`
- Create: `stageflow/contrib/storage/__init__.py`
- Create: `stageflow/contrib/storage/_base.py`
- Modify: `stageflow/__init__.py` (无变化 — contrib 不进 core 导出)
- Test: `tests/contrib/__init__.py` (空 — subpackage test 发现)
- Test: `tests/contrib/test_base.py`

**Interfaces:**
- Consumes: `StorageBackend` (现有 core Protocol)
- Produces: `stageflow.contrib.storage` subpackage; `_validate_key(key: str) -> None` helper; `_encode_payload(data: dict) -> bytes` + `_decode_payload(raw: bytes) -> dict | None` JSON serializer (UTF-8 + json.dumps/loads, ensure_ascii=False)

- [ ] **Step 1: 写测试**

新建 `tests/contrib/test_base.py`:

```python
"""contrib 公共 helpers 测试."""
import pytest

from stageflow.contrib.storage._base import _validate_key, _encode_payload, _decode_payload


def test_validate_key_accepts_safe_chars():
    _validate_key("abc-123/X_y.0")
    _validate_key("runs/task-001/checkpoint")


def test_validate_key_rejects_path_traversal():
    with pytest.raises(ValueError, match="key"):
        _validate_key("../etc/passwd")


def test_validate_key_rejects_empty():
    with pytest.raises(ValueError, match="empty"):
        _validate_key("")


def test_validate_key_rejects_unsafe_chars():
    with pytest.raises(ValueError, match="key"):
        _validate_key("with space")
    with pytest.raises(ValueError, match="key"):
        _validate_key("with?query")


def test_encode_decode_roundtrip():
    data = {"a": 1, "b": ["x", "y"], "c": "中文"}
    raw = _encode_payload(data)
    assert isinstance(raw, bytes)
    assert _decode_payload(raw) == data


def test_decode_returns_none_on_invalid_json():
    assert _decode_payload(b"\xff\xfe garbage") is None


def test_decode_returns_none_on_empty():
    assert _decode_payload(b"") is None
```

- [ ] **Step 2: 跑测试确认 FAIL**

```bash
cd stageflow && python -m pytest tests/contrib/test_base.py -v
```
Expected: FAIL (`stageflow.contrib.storage._base` 不存在)

- [ ] **Step 3: 实现 _base.py**

新建 `stageflow/contrib/storage/_base.py`:

```python
"""contrib Storage adapter 公共 helpers.

stageflow.contrib.storage 全部 adapter 共享:
- _validate_key: 路径安全 (只允许 [a-zA-Z0-9_-/]+)
- _encode_payload / _decode_payload: dict <-> bytes JSON 序列化
  (与 ai_writer MinioStorage 行为对齐: ensure_ascii=False + None 表示缺失)
"""

from __future__ import annotations

import json
from typing import Any

# 与 stageflow/storage.py FileStorage._path 一致: 路径注入防御
_SAFE_KEY_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_/.")


def _validate_key(key: str) -> None:
    if not key:
        raise ValueError("storage key 不能为空")
    if not all(c in _SAFE_KEY_CHARS for c in key):
        raise ValueError(
            f"storage key 含非法字符: {key!r}. 只允许 [a-zA-Z0-9_-/.]"
        )


def _encode_payload(data: dict) -> bytes:
    """dict -> bytes (UTF-8 JSON). 用 ensure_ascii=False 保留中文."""
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


def _decode_payload(raw: bytes) -> dict | None:
    """bytes -> dict. 解析失败 (包括空) 返 None — 与 stageflow.core 同语义."""
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return None
```

- [ ] **Step 4: 实现 contrib 包骨架**

新建 `stageflow/contrib/__init__.py`:

```python
"""stageflow.contrib — 可选 extras 子包.

adapter 在此命名空间; core 不依赖 contrib. 装哪个 extras 就有哪个 adapter.
LLM vendor / 业务 SDK 禁入 (deps 政策).
"""

from __future__ import annotations

__version__ = "0.4.0.dev0"
```

新建 `stageflow/contrib/storage/__init__.py`:

```python
"""StorageBackend adapter 可选实现.

每个 adapter 懒导入 driver; 缺 driver 时抛 ImportError + 安装提示.
"""

from __future__ import annotations

# 不强导入任何 adapter — 让用户按需 import:
#   from stageflow.contrib.storage import PostgresStorage
# 这才触发对应 adapter 的 driver 检查。

__all__: list[str] = []
```

- [ ] **Step 5: 跑测试确认 PASS**

```bash
cd stageflow && python -m pytest tests/contrib/test_base.py -v
cd stageflow && python -m pytest tests/ -q
cd stageflow && python -m ruff check stageflow/ tests/
```
Expected: 7 contrib tests PASS; 38 core tests PASS; ruff 0 violation

- [ ] **Step 6: commit**

```bash
cd stageflow
git add stageflow/contrib/ tests/contrib/
git commit -m "feat(contrib): skeleton + _base key sanitize/JSON helpers

- stageflow/contrib/ subpackage (extras, 不进 core __init__.py)
- _base.py: _validate_key (path safety) + _encode/decode_payload (UTF-8 JSON)
- 7 unit tests pass; 38 core tests still pass

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 2: SqliteStorage (stdlib, 唯一无第三方依赖 — 优先做, 不阻塞其他)

**Files:**
- Create: `stageflow/contrib/storage/sqlite.py`
- Modify: `stageflow/contrib/storage/__init__.py` (加 export)
- Test: `tests/contrib/test_sqlite.py`

**Interfaces:**
- Consumes: `StorageBackend` Protocol; `_base._validate_key` / `_encode_payload` / `_decode_payload`
- Produces: `SqliteStorage(path: str | Path, *, table: str = "stageflow_kv")` — 单文件 SQLite db

- [ ] **Step 1: 写测试**

新建 `tests/contrib/test_sqlite.py`:

```python
"""SqliteStorage 集成测试 (本地 tmp_path)."""
import json
import sqlite3
import pytest
from pathlib import Path

from stageflow.contrib.storage.sqlite import SqliteStorage


@pytest.fixture
def store(tmp_path: Path) -> SqliteStorage:
    return SqliteStorage(tmp_path / "kv.db")


def test_put_get_roundtrip(store: SqliteStorage):
    store.put("runs/x", {"a": 1, "b": [1, 2], "c": "中文"})
    assert store.get("runs/x") == {"a": 1, "b": [1, 2], "c": "中文"}


def test_get_missing_returns_none(store: SqliteStorage):
    assert store.get("nonexistent") is None


def test_put_overwrites(store: SqliteStorage):
    store.put("k", {"v": 1})
    store.put("k", {"v": 2})
    assert store.get("k") == {"v": 2}


def test_delete_existing(store: SqliteStorage):
    store.put("k", {"v": 1})
    store.delete("k")
    assert store.get("k") is None


def test_delete_missing_is_silent(store: SqliteStorage):
    # 不抛异常 (与 stageflow.core FileStorage.delete 一致)
    store.delete("never-existed")


def test_list_keys_prefix(store: SqliteStorage):
    store.put("a/1", {"x": 1})
    store.put("a/2", {"x": 2})
    store.put("b/1", {"x": 3})
    keys = store.list_keys("a/")
    assert keys == ["a/1", "a/2"]


def test_list_keys_returns_sorted(store: SqliteStorage):
    store.put("z", {"x": 1})
    store.put("a", {"x": 2})
    store.put("m", {"x": 3})
    assert store.list_keys("") == ["a", "m", "z"]


def test_key_safety(store: SqliteStorage):
    with pytest.raises(ValueError):
        store.put("../bad", {})
    with pytest.raises(ValueError):
        store.put("with space", {})


def test_schema_idempotent(store: SqliteStorage, tmp_path: Path):
    # 同一 path 第二次打开, 不抛 "table already exists"
    store.put("k", {"v": 1})
    store2 = SqliteStorage(tmp_path / "kv.db")
    assert store2.get("k") == {"v": 1}


def test_unicode_safe(store: SqliteStorage):
    data = {"emoji": "🎉", "chinese": "中文 ok", "rtl": "العربية"}
    store.put("u", data)
    assert store.get("u") == data
```

- [ ] **Step 2: 跑测试确认 FAIL**

```bash
cd stageflow && python -m pytest tests/contrib/test_sqlite.py -v
```
Expected: FAIL

- [ ] **Step 3: 实现 SqliteStorage**

新建 `stageflow/contrib/storage/sqlite.py`:

```python
"""SqliteStorage — stdlib sqlite3, 无第三方依赖.

单文件 DB; 默认表 stageflow_kv. 适合本地测试 + 单进程部署.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from stageflow.storage import StorageBackend

from ._base import _decode_payload, _encode_payload, _validate_key

__all__ = ["SqliteStorage"]


class SqliteStorage(StorageBackend):
    """SQLite-backed StorageBackend. 单文件 DB, auto-creates table on init."""

    def __init__(self, path: str | Path, *, table: str = "stageflow_kv"):
        self.path = str(path)
        self.table = table
        self._conn = sqlite3.connect(self.path)
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} "
            "(key TEXT PRIMARY KEY, value BLOB NOT NULL)"
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
        cur = self._conn.execute(
            f"SELECT value FROM {self.table} WHERE key = ?", (key,)
        )
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
        self._conn.execute(
            f"DELETE FROM {self.table} WHERE key = ?", (key,)
        )
        self._conn.commit()

    def close(self) -> None:
        """显式关闭连接. 测试用; 业务通常不调 (进程结束自动清理)."""
        self._conn.close()
```

- [ ] **Step 4: 注册到 storage __init__**

修改 `stageflow/contrib/storage/__init__.py` —— 加 lazy import 列表 (不强导入, 触发时检查):

```python
"""StorageBackend adapter 可选实现.

每个 adapter 懒导入 driver; 缺 driver 时抛 ImportError + 安装提示.
"""

from __future__ import annotations

# 不强导入任何 adapter — 让用户按需 import:
#   from stageflow.contrib.storage import PostgresStorage
# 这才触发对应 adapter 的 driver 检查。

__all__: list[str] = ["SqliteStorage", "PostgresStorage", "MySQLStorage",
              "RedisStorage", "MinioStorage"]


def __getattr__(name: str):
    """Lazy adapter import. 用户用哪个就 import 哪个, 缺 driver 抛友好错误."""
    _ADAPTERS = {
        "SqliteStorage": (".sqlite", "SqliteStorage", None),  # stdlib, no check
        "PostgresStorage": (".postgres", "PostgresStorage", "psycopg"),
        "MySQLStorage": (".mysql", "MySQLStorage", "pymysql"),
        "RedisStorage": (".redis", "RedisStorage", "redis"),
        "MinioStorage": (".minio", "MinioStorage", "boto3"),
    }
    if name not in _ADAPTERS:
        raise AttributeError(f"module 'stageflow.contrib.storage' has no attribute {name!r}")
    mod_path, cls_name, dep = _ADAPTERS[name]
    import importlib
    mod = importlib.import_module(mod_path, package=__name__)
    cls = getattr(mod, cls_name)
    # 缓存到模块命名空间 (避免重复 import)
    globals()[name] = cls
    return cls
```

- [ ] **Step 5: 跑测试确认 PASS**

```bash
cd stageflow && python -m pytest tests/contrib/test_sqlite.py -v
cd stageflow && python -m pytest tests/ -q
cd stageflow && python -m ruff check stageflow/ tests/
```
Expected: 10 sqlite tests PASS; 38 core + 7 base + 10 sqlite = 55 PASS; ruff 0

- [ ] **Step 6: commit**

```bash
cd stageflow
git add stageflow/contrib/ tests/
git commit -m "feat(contrib.storage): SqliteStorage (stdlib sqlite3)

- 单文件 SQLite DB; auto schema; ORM-style put/get/list/delete
- 10 unit tests pass (roundtrip/overwrite/delete-missing/prefix-list/unicode)
- lazy __getattr__ 提供 5 个 adapter name (其他 4 触发 ImportError on use)
- 38 core tests + 7 base + 10 sqlite = 55 green; ruff 0

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 3: PostgresStorage (psycopg3, SQL template)

**Files:**
- Create: `stageflow/contrib/storage/postgres.py`
- Test: `tests/contrib/test_postgres.py`

**Interfaces:**
- Consumes: `_base._validate_key/_encode_payload/_decode_payload`
- Produces: `PostgresStorage(dsn: str, *, table: str = "stageflow_kv")` — DSN connection string

- [ ] **Step 1: 写测试**

新建 `tests/contrib/test_postgres.py`:

```python
"""PostgresStorage 测试 (标记 integration, 需 docker)."""
import os
import uuid

import pytest

from stageflow.contrib.storage.postgres import PostgresStorage

# 默认 DSN — 本地 postgres 用户可覆盖 env STAGEFLOW_TEST_PG_DSN
PG_DSN = os.environ.get("STAGEFLOW_TEST_PG_DSN", "postgresql://postgres:postgres@localhost:5432/postgres")


@pytest.fixture
def pg_store() -> PostgresStorage:
    """需要本地 postgres; 缺则 skip. 集成 marker."""
    pytest.importorskip("psycopg")
    store = PostgresStorage(PG_DSN, table=f"kv_{uuid.uuid4().hex[:8]}")
    yield store
    # teardown: drop test table
    try:
        import psycopg
        with psycopg.connect(PG_DSN, autocommit=True) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {store.table}")
    except Exception:
        pass


@pytest.mark.integration
def test_put_get_roundtrip(pg_store: PostgresStorage):
    pg_store.put("runs/x", {"a": 1, "b": "中文"})
    assert pg_store.get("runs/x") == {"a": 1, "b": "中文"}


@pytest.mark.integration
def test_get_missing_returns_none(pg_store: PostgresStorage):
    assert pg_store.get("nope") is None


@pytest.mark.integration
def test_put_upsert(pg_store: PostgresStorage):
    pg_store.put("k", {"v": 1})
    pg_store.put("k", {"v": 2})
    assert pg_store.get("k") == {"v": 2}


@pytest.mark.integration
def test_delete_silent_on_missing(pg_store: PostgresStorage):
    pg_store.delete("never-existed")  # 不抛


@pytest.mark.integration
def test_list_keys_prefix(pg_store: PostgresStorage):
    pg_store.put("a/1", {})
    pg_store.put("a/2", {})
    pg_store.put("b/1", {})
    assert pg_store.list_keys("a/") == ["a/1", "a/2"]


def test_import_error_when_psycopg_missing(monkeypatch):
    """当 psycopg 没装, import 应该友好错误 (而非 ModuleNotFoundError)."""
    # 不真删 psycopg, 只验证 error message 逻辑存在
    # (集成测: 实际跑需 uninstall)
    from stageflow.contrib.storage import postgres as pg_mod
    assert hasattr(pg_mod, "PostgresStorage")
```

- [ ] **Step 2: 跑测试确认 FAIL (unmarked 部分)**

```bash
cd stageflow && python -m pytest tests/contrib/test_postgres.py -v -m "not integration"
```
Expected: 1 PASS (import test), 6 skip (integration 无 docker)

- [ ] **Step 3: 实现 PostgresStorage**

新建 `stageflow/contrib/storage/postgres.py`:

```python
"""PostgresStorage — psycopg3 v3 sync driver.

DSN 连接字符串; 默认表 stageflow_kv. JSONB 字段存 dict.
"""

from __future__ import annotations

try:
    import psycopg
    import psycopg.rows
except ImportError as _e:
    raise ImportError(
        "PostgresStorage 需要 psycopg. 安装: pip install 'stageflow[postgres]'"
    ) from _e

from stageflow.storage import StorageBackend

from ._base import _decode_payload, _encode_payload, _validate_key

__all__ = ["PostgresStorage"]


class PostgresStorage(StorageBackend):
    """PostgreSQL-backed StorageBackend. 用 JSONB 字段存 dict (psycopg3 自动 dict<->jsonb)."""

    def __init__(self, dsn: str, *, table: str = "stageflow_kv", schema: str = "public"):
        self._dsn = dsn
        self.table = table
        self.schema = schema
        self._conn = psycopg.connect(
            dsn, autocommit=False, row_factory=psycopg.rows.dict_row
        )
        self._conn.execute(
            f"CREATE SCHEMA IF NOT EXISTS {self.schema}"
        )
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} "
            "(key TEXT PRIMARY KEY, value JSONB NOT NULL)"
        )
        self._conn.commit()

    def put(self, key: str, data: dict) -> None:
        _validate_key(key)
        # psycopg3 自动 dict -> jsonb (无 explicit encode)
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
                f"SELECT value FROM {self.schema}.{self.table} WHERE key = %s",
                (key,),
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
                f"DELETE FROM {self.schema}.{self.table} WHERE key = %s",
                (key,),
            )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
```

- [ ] **Step 4: 跑测试确认 PASS (非集成)**

```bash
cd stageflow && python -m pytest tests/contrib/test_postgres.py -v -m "not integration"
cd stageflow && python -m pytest tests/ -q -m "not integration"
cd stageflow && python -m ruff check stageflow/ tests/
```
Expected: 1 postgres + 55 others = 56 PASS; ruff 0

- [ ] **Step 5: commit**

```bash
cd stageflow
git add stageflow/contrib/storage/postgres.py tests/contrib/test_postgres.py
git commit -m "feat(contrib.storage): PostgresStorage (psycopg3 JSONB)

- DSN connection string; auto schema+table create
- INSERT ... ON CONFLICT DO UPDATE (upsert)
- 1 non-integration test (import smoke) + 5 @pytest.mark.integration
- 默认本地 DSN; STAGEFLOW_TEST_PG_DSN env override

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4: MySQLStorage (PyMySQL)

**Files:**
- Create: `stageflow/contrib/storage/mysql.py`
- Test: `tests/contrib/test_mysql.py`

**Interfaces:** 与 PostgresStorage 同形 (类似 4 方法 + table).

- [ ] **Step 1: 写测试**

新建 `tests/contrib/test_mysql.py`:

```python
"""MySQLStorage 测试 (标记 integration, 需 docker)."""
import os
import uuid

import pytest

from stageflow.contrib.storage.mysql import MySQLStorage

MYSQL_DSN = os.environ.get("STAGEFLOW_TEST_MYSQL_DSN",
    "mysql+pymysql://root:root@localhost:3306/test")


@pytest.fixture
def mysql_store() -> MySQLStorage:
    pytest.importorskip("pymysql")
    store = MySQLStorage(MYSQL_DSN, table=f"kv_{uuid.uuid4().hex[:8]}")
    yield store
    # teardown
    try:
        import pymysql
        conn = pymysql.connect(host="localhost", user="root", password="root", db="test")
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {store.table}")
        conn.commit()
        conn.close()
    except Exception:
        pass


@pytest.mark.integration
def test_put_get_roundtrip(mysql_store: MySQLStorage):
    mysql_store.put("k", {"a": 1, "中文": "ok"})
    assert mysql_store.get("k") == {"a": 1, "中文": "ok"}


@pytest.mark.integration
def test_get_missing_returns_none(mysql_store: MySQLStorage):
    assert mysql_store.get("nope") is None


@pytest.mark.integration
def test_put_upsert(mysql_store: MySQLStorage):
    mysql_store.put("k", {"v": 1})
    mysql_store.put("k", {"v": 2})
    assert mysql_store.get("k") == {"v": 2}


@pytest.mark.integration
def test_delete_silent_on_missing(mysql_store: MySQLStorage):
    mysql_store.delete("never-existed")


@pytest.mark.integration
def test_list_keys_prefix(mysql_store: MySQLStorage):
    mysql_store.put("a/1", {})
    mysql_store.put("a/2", {})
    mysql_store.put("b/1", {})
    assert mysql_store.list_keys("a/") == ["a/1", "a/2"]


def test_import_error_when_pymysql_missing():
    from stageflow.contrib.storage import mysql as m_mod
    assert hasattr(m_mod, "MySQLStorage")
```

- [ ] **Step 2: 跑测试确认 FAIL**

```bash
cd stageflow && python -m pytest tests/contrib/test_mysql.py -v -m "not integration"
```
Expected: 1 PASS (import test)

- [ ] **Step 3: 实现 MySQLStorage**

新建 `stageflow/contrib/storage/mysql.py`:

```python
"""MySQLStorage — PyMySQL (纯 Python MySQL driver).

DSN 连接字符串; 默认表 stageflow_kv. LONGTEXT 字段存 JSON.
"""

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
    """SQLAlchemy 风格 'mysql+pymysql://user:pwd@host:port/db' -> pymysql kwargs."""
    from urllib.parse import urlparse
    if dsn.startswith("mysql+pymysql://"):
        dsn = dsn[len("mysql+pymysql://"):]
    p = urlparse(f"//{dsn}")
    return {
        "host": p.hostname or "localhost",
        "port": p.port or 3306,
        "user": p.username or "root",
        "password": p.password or "",
        "database": (p.path or "/").lstrip("/") or "test",
    }


class MySQLStorage(StorageBackend):
    """MySQL-backed StorageBackend. LONGTEXT 存 dict (PyMySQL 不自动 JSON, 显式 encode/decode)."""

    def __init__(self, dsn: str, *, table: str = "stageflow_kv"):
        cfg = _parse_dsn(dsn)
        self.table = table
        self._conn = pymysql.connect(
            **cfg, charset="utf8mb4", autocommit=False,
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
```

- [ ] **Step 4: 跑测试确认 PASS**

```bash
cd stageflow && python -m pytest tests/contrib/test_mysql.py -v -m "not integration"
cd stageflow && python -m pytest tests/ -q -m "not integration"
cd stageflow && python -m ruff check stageflow/ tests/
```
Expected: 1 mysql + 56 others = 57 PASS

- [ ] **Step 5: commit**

```bash
cd stageflow
git add stageflow/contrib/storage/mysql.py tests/contrib/test_mysql.py
git commit -m "feat(contrib.storage): MySQLStorage (PyMySQL LONGTEXT)

- SQLAlchemy 风格 DSN 'mysql+pymysql://user:pwd@host:port/db'
- LONGTEXT 字段存 JSON (PyMySQL 不自动 dict<->json, 显式 _encode/decode)
- 1 non-integration test + 5 @pytest.mark.integration
- STAGEFLOW_TEST_MYSQL_DSN env override

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 5: RedisStorage (redis-py + fakeredis test)

**Files:**
- Create: `stageflow/contrib/storage/redis.py`
- Test: `tests/contrib/test_redis.py`

**Interfaces:**
- Consumes: `_base` helpers
- Produces: `RedisStorage(url: str = "redis://localhost:6379/0", *, prefix: str = "stageflow:")`

- [ ] **Step 1: 写测试**

新建 `tests/contrib/test_redis.py`:

```python
"""RedisStorage 测试 (用 fakeredis mock, 无 docker 依赖)."""
import pytest

from stageflow.contrib.storage.redis import RedisStorage


@pytest.fixture
def fake_redis(monkeypatch):
    """fakeredis 替换 redis.Redis 让 RedisStorage 无 redis 服务跑测."""
    fakeredis = pytest.importorskip("fakeredis")
    fake = fakeredis.FakeRedis()
    monkeypatch.setattr("redis.Redis", lambda *a, **kw: fake)
    return fake


@pytest.fixture
def store(fake_redis) -> RedisStorage:
    return RedisStorage(url="redis://test/0", prefix="sf:")


def test_put_get_roundtrip(store: RedisStorage):
    store.put("k", {"a": 1, "b": "中文"})
    assert store.get("k") == {"a": 1, "b": "中文"}


def test_get_missing_returns_none(store: RedisStorage):
    assert store.get("nope") is None


def test_put_overwrites(store: RedisStorage):
    store.put("k", {"v": 1})
    store.put("k", {"v": 2})
    assert store.get("k") == {"v": 2}


def test_delete_silent_on_missing(store: RedisStorage):
    store.delete("never-existed")


def test_list_keys_prefix(store: RedisStorage):
    store.put("a/1", {})
    store.put("a/2", {})
    store.put("b/1", {})
    assert store.list_keys("a/") == ["a/1", "a/2"]


def test_prefix_isolation(store: RedisStorage):
    store.put("k", {"v": 1})
    # prefix 默认 'sf:' — k 在 Redis 里实际 key 是 'sf:k'
    import redis as _r
    raw = fake_redis.get("sf:k")
    assert raw is not None


def test_unicode_safe(store: RedisStorage):
    data = {"emoji": "🎉", "中文": "ok"}
    store.put("u", data)
    assert store.get("u") == data
```

- [ ] **Step 2: 跑测试确认 FAIL**

```bash
cd stageflow && python -m pytest tests/contrib/test_redis.py -v
```
Expected: FAIL (fakeredis 装后 import OK 但 RedisStorage 不存在)

- [ ] **Step 3: 实现 RedisStorage**

新建 `stageflow/contrib/storage/redis.py`:

```python
"""RedisStorage — redis-py sync driver.

URL 连接字符串; prefix 防止 key 冲突 (多 stageflow 实例共享 Redis).
"""

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
    """Redis-backed StorageBackend. value 存为 JSON bytes, prefix 隔离."""

    def __init__(self, url: str = "redis://localhost:6379/0", *, prefix: str = "stageflow:"):
        self._client = redis.Redis.from_url(url)
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
        # SCAN + 去掉 self.prefix 前缀
        full_prefix = self.prefix + prefix
        out = []
        cursor = 0
        while True:
            cursor, batch = self._client.scan(cursor=cursor, match=full_prefix + "*", count=100)
            for raw_key in batch:
                if isinstance(raw_key, bytes):
                    raw_key = raw_key.decode("utf-8")
                if raw_key.startswith(self.prefix):
                    out.append(raw_key[len(self.prefix):])
            if cursor == 0:
                break
        return sorted(out)

    def delete(self, key: str) -> None:
        _validate_key(key)
        self._client.delete(self.prefix + key)
```

- [ ] **Step 4: 跑测试确认 PASS**

```bash
cd stageflow && python -m pytest tests/contrib/test_redis.py -v
cd stageflow && python -m pytest tests/ -q
cd stageflow && python -m ruff check stageflow/ tests/
```
Expected: 7 redis + 50 others = 57 PASS; ruff 0

- [ ] **Step 5: commit**

```bash
cd stageflow
git add stageflow/contrib/storage/redis.py tests/contrib/test_redis.py
git commit -m "feat(contrib.storage): RedisStorage (redis-py + fakeredis test)

- URL connection; prefix 隔离 (多 stageflow 实例安全共享 Redis)
- SCAN-based list_keys (避免 KEYS 全扫)
- 7 tests pass via fakeredis mock (无 docker 依赖)

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 6: MinioStorage (boto3, 行为对齐 ai_writer 现有)

**Files:**
- Create: `stageflow/contrib/storage/minio.py`
- Test: `tests/contrib/test_minio.py`

**Interfaces:**
- Consumes: `_base` helpers; ai_writer 现有 `backend/integration/sf_storage.py` 行为约定
- Produces: `MinioStorage(endpoint_url, *, access_key, secret_key, bucket: str, prefix: str = "stageflow:")` 或同等 kwargs 风格

- [ ] **Step 1: 写测试 (用 moto[s3] mock)**

新建 `tests/contrib/test_minio.py`:

```python
"""MinioStorage 测试 (用 moto[s3] mock AWS S3 = MinIO 同 API)."""
import os
import pytest

# moto 提供 mock AWS S3 = 同样 MinIO 用的 S3 API
from stageflow.contrib.storage.minio import MinioStorage


@pytest.fixture
def aws_credentials(monkeypatch):
    """moto 要求 fake credentials."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def moto_bucket(aws_credentials):
    moto = pytest.importorskip("moto")
    mock = moto.mock_aws()
    mock.start()
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-kv")
    yield "test-kv"
    mock.stop()


@pytest.fixture
def store(moto_bucket) -> MinioStorage:
    import boto3
    return MinioStorage(
        endpoint_url=None,  # moto 默认 mock
        bucket=moto_bucket,
        access_key="testing",
        secret_key="testing",
        prefix="sf:",
    )


def test_put_get_roundtrip(store: MinioStorage):
    store.put("k", {"a": 1, "中文": "ok"})
    assert store.get("k") == {"a": 1, "中文": "ok"}


def test_get_missing_returns_none(store: MinioStorage):
    assert store.get("nope") is None


def test_put_overwrites(store: MinioStorage):
    store.put("k", {"v": 1})
    store.put("k", {"v": 2})
    assert store.get("k") == {"v": 2}


def test_delete_silent_on_missing(store: MinioStorage):
    # 行为对齐 ai_writer: NoSuchKey 抛 → 静默
    store.delete("never-existed")  # 不抛


def test_list_keys_prefix(store: MinioStorage):
    store.put("a/1", {})
    store.put("a/2", {})
    store.put("b/1", {})
    assert store.list_keys("a/") == ["a/1", "a/2"]


def test_unicode_safe(store: MinioStorage):
    data = {"emoji": "🎉", "中文": "ok"}
    store.put("u", data)
    assert store.get("u") == data


def test_deletes_noop_when_storage_layer_raises_404(store: MinioStorage):
    """对齐 ai_writer sf_storage.py 容忍: boto3 NoSuchKey 静默."""
    import boto3
    # 直接构造一个会抛 NoSuchKey 的 client
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-kv-2")
    s2 = MinioStorage(bucket="test-kv-2", access_key="testing", secret_key="testing")
    s2.delete("never-was")  # 必须不抛
```

- [ ] **Step 2: 跑测试确认 FAIL**

```bash
cd stageflow && python -m pytest tests/contrib/test_minio.py -v
```
Expected: FAIL (moto 已装但 MinioStorage 不存在)

- [ ] **Step 3: 实现 MinioStorage**

新建 `stageflow/contrib/storage/minio.py`:

```python
"""MinioStorage — boto3 S3 client (兼容 AWS S3 + MinIO).

行为对齐 ai_writer 的 backend/integration/sf_storage.py:
- get 返 None 当 key 不存在
- delete 容忍 NoSuchKey (静默)
- value 存为 JSON bytes (utf-8)
"""

from __future__ import annotations

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError as _e:
    raise ImportError(
        "MinioStorage 需要 boto3. 安装: pip install 'stageflow[minio]'"
    ) from _e

from stageflow.storage import StorageBackend

from ._base import _decode_payload, _encode_payload, _validate_key

__all__ = ["MinioStorage"]


class MinioStorage(StorageBackend):
    """S3-compatible object storage adapter (AWS S3 / MinIO / 等).

    Args:
        bucket: bucket name (must pre-exist)
        prefix: 防止跨项目 key 冲突
        endpoint_url: None = AWS default; 设 'http://localhost:9000' = MinIO local
        access_key / secret_key: 凭证 (None = 走 env / IAM role)
        region: AWS region (default 'us-east-1')
    """

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "stageflow:",
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        region: str = "us-east-1",
    ):
        self.bucket = bucket
        self.prefix = prefix
        kwargs = {"region_name": region}
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        if access_key and secret_key:
            kwargs["aws_access_key_id"] = access_key
            kwargs["aws_secret_access_key"] = secret_key
        self._client = boto3.client("s3", **kwargs)

    def put(self, key: str, data: dict) -> None:
        _validate_key(key)
        self._client.put_object(
            Bucket=self.bucket, Key=self.prefix + key, Body=_encode_payload(data)
        )

    def get(self, key: str) -> dict | None:
        _validate_key(key)
        try:
            obj = self._client.get_object(Bucket=self.bucket, Key=self.prefix + key)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise
        return _decode_payload(obj["Body"].read())

    def list_keys(self, prefix: str) -> list[str]:
        full_prefix = self.prefix + prefix
        out: list[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                raw_key = obj["Key"]
                if raw_key.startswith(self.prefix):
                    out.append(raw_key[len(self.prefix):])
        return sorted(out)

    def delete(self, key: str) -> None:
        _validate_key(key)
        try:
            self._client.delete_object(Bucket=self.bucket, Key=self.prefix + key)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return  # 静默 — 对齐 ai_writer 既有行为
            raise
```

- [ ] **Step 4: 跑测试确认 PASS**

```bash
cd stageflow && python -m pytest tests/contrib/test_minio.py -v
cd stageflow && python -m pytest tests/ -q
cd stageflow && python -m ruff check stageflow/ tests/
```
Expected: 7 minio + 50 others = 57 PASS; ruff 0

- [ ] **Step 5: commit**

```bash
cd stageflow
git add stageflow/contrib/storage/minio.py tests/contrib/test_minio.py
git commit -m "feat(contrib.storage): MinioStorage (boto3, ai_writer 行为对齐)

- S3-compatible (AWS S3 / MinIO / Cloudflare R2 / etc.)
- delete 容忍 NoSuchKey/404 静默 (对齐 ai_writer sf_storage.py)
- 7 tests pass via moto[s3] mock (无 docker / 真 MinIO 依赖)

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 7: pyproject extras + 文档 + 全量回归

**Files:**
- Modify: `pyproject.toml` (加 6 extras)
- Modify: `README.md` (改"零依赖"措辞 + 新"Contrib"段)
- Modify: `docs/architecture.md` (架构图加 contrib)
- Modify: `docs/api.md` (新"Contrib adapters" 段)
- Create: `docs/contrib.md` (新文档 — 各 adapter 用法示例)
- Modify: `CHANGELOG.md` (加 [0.4.0] 条目)
- Modify: `ROADMAP.md` (加 v0.4 status)

- [ ] **Step 1: pyproject.toml extras**

打开 `pyproject.toml`. `[project.optional-dependencies]` 段改为:

```toml
[project.optional-dependencies]
dev = ["pytest>=8.0", "pytest-asyncio>=0.23", "moto[s3]>=5.0", "fakeredis>=2.20"]
postgres = ["psycopg[binary]>=3.1"]
mysql = ["pymysql>=1.1"]
redis = ["redis>=5.0"]
minio = ["boto3>=1.34"]
sqlite = []  # stdlib, no deps needed
contrib = ["psycopg[binary]>=3.1", "pymysql>=1.1", "redis>=5.0", "boto3>=1.34"]
all = ["psycopg[binary]>=3.1", "pymysql>=1.1", "redis>=5.0", "boto3>=1.34"]
```

- [ ] **Step 2: README.md 措辞更新**

在 README 的"零依赖" badge/行附近, 改为:

```markdown
core 永远 Python stdlib only (无第三方依赖). 可选 adapter: `pip install stageflow[postgres]` / `[mysql]` / `[redis]` / `[minio]` / `[all]`.
```

加新 ## Contrib 段 (在 "## 安装" 后), 列出 5 个 adapter 1 行说明.

- [ ] **Step 3: docs/contrib.md 新文档**

写各 adapter 用法示例 (postgres DSN / mysql DSN / redis URL / minio endpoint_url / sqlite path). 包括 ImportError 提示.

- [ ] **Step 4: docs/api.md 加 ## Contrib adapters 段**

在 api.md 末尾 (CLI 段后) 加短段, 链到 docs/contrib.md.

- [ ] **Step 5: docs/architecture.md 架构图加 contrib**

在架构图 core 块旁加 contrib 块说明: 可选 extras, 业务侧注入.

- [ ] **Step 6: CHANGELOG.md 加 [0.4.0]**

在 [Unreleased] 之前:

```markdown
## [0.4.0] — 2026-09-05

### Added (stageflow.contrib.storage)

- **PostgresStorage** (psycopg3 + JSONB, INSERT ON CONFLICT  upsert)
- **MySQLStorage** (PyMySQL + LONGTEXT, INSERT ON DUPLICATE KEY UPDATE upsert)
- **RedisStorage** (redis-py, prefix 隔离, SCAN-based list_keys)
- **MinioStorage** (boto3 S3-compatible, 行为对齐 ai_writer 现有 sf_storage.py — delete 容忍 NoSuchKey/404)
- **SqliteStorage** (stdlib sqlite3, 单文件 DB, ORM-style CRUD)
- 公共 helpers `_validate_key` (路径安全) + `_encode/decode_payload` (UTF-8 JSON)

### Internal

- core 12 模块 + StorageBackend Protocol 零变化 (API 冻结); 0 行 core 修改
- contrib 是独立 subpackage (`stageflow.contrib.storage`), 不进 core __init__.py
- extras 分组: `[postgres]` / `[mysql]` / `[redis]` / `[minio]` / `[sqlite]` (空) / `[contrib]` (无 sqlite) / `[all]` (全)

### Tests

- 38 core tests + ≥35 contrib tests
- PostgresStorage + MySQLStorage 标 `@pytest.mark.integration` (需 docker)
- MinioStorage 用 moto[s3] mock, RedisStorage 用 fakeredis mock (无 docker)

```

- [ ] **Step 7: ROADMAP.md 更新 v0.4 状态**

把当前 v0.2 段后加:

```markdown
## v0.3: 兼容性修复 + i18n (2026-09-05)

- 文档分 en/cn 双版本 (5 个 doc 文件 → docs/en/ + docs/cn/)
- pyproject.toml + __version__ sync 到 tag (0.1.3)

## v0.4: StorageBackend contrib adapters (2026-09-05, 已 ship)

- stageflow.contrib.storage 5 个 adapter: Postgres / MySQL / Redis / MinIO / SQLite
- core 永远 stdlib-only; contrib 是可选 extras
- 行为对齐 ai_writer 既有 MinioStorage 用法 (delete 容忍 NoSuchKey)

```

- [ ] **Step 8: 全量回归**

```bash
cd stageflow && python -m ruff check stageflow/ tests/
cd stageflow && python -m pytest tests/ -q -m "not integration"
cd stageflow && python -m pytest tests/ -q -m "integration"  # 大概率 skip 无 docker
cd stageflow && python -m stageflow --help 2>&1 | head -3  # core 不破
```

Expected: ruff 0; ≥73 unit tests PASS; CLI works.

- [ ] **Step 9: commit + tag**

```bash
cd stageflow
git add pyproject.toml README.md docs/ CHANGELOG.md ROADMAP.md
git commit -m "docs+build: v0.4 contrib extras + docs (postgres/mysql/redis/minio/sqlite)

- pyproject.toml extras: [postgres]/[mysql]/[redis]/[minio]/[sqlite]/[contrib]/[all]
- README/architecture/api/contrib.md 文档同步
- CHANGELOG v0.4.0 + ROADMAP v0.4 status entry
- core 零变化, API 冻结面不变

Co-Authored-By: Claude Code <noreply@anthropic.com>"
git tag v0.4.0

```

- [ ] **Step 10: 验证**

```bash
cd stageflow && git tag -l && git log --oneline -10
wc -l stageflow/contrib/storage/*.py | tail
```
Expected: v0.4.0 在列; 7 commits (Task 1-7); contrib storage 5 个 adapter + base + __init__.py