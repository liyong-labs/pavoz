import os
import uuid

import pytest

from stageflow.contrib.storage.postgres import PostgresStorage

PG_DSN = os.environ.get(
    "STAGEFLOW_TEST_PG_DSN",
    "postgresql://postgres:postgres@localhost:5432/postgres",
)


@pytest.fixture
def pg_store():
    pytest.importorskip("psycopg")
    store = PostgresStorage(PG_DSN, table=f"kv_{uuid.uuid4().hex[:8]}")
    yield store
    try:
        import psycopg

        with psycopg.connect(PG_DSN, autocommit=True) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {store.table}")
    except Exception:  # noqa: BLE001, S110 — cleanup, no logger needed
        pass


@pytest.mark.integration
def test_put_get_roundtrip(pg_store):
    pg_store.put("runs/x", {"a": 1, "b": "中文"})
    assert pg_store.get("runs/x") == {"a": 1, "b": "中文"}


@pytest.mark.integration
def test_get_missing_returns_none(pg_store):
    assert pg_store.get("nope") is None


@pytest.mark.integration
def test_put_upsert(pg_store):
    pg_store.put("k", {"v": 1})
    pg_store.put("k", {"v": 2})
    assert pg_store.get("k") == {"v": 2}


@pytest.mark.integration
def test_delete_silent_on_missing(pg_store):
    pg_store.delete("never-existed")  # 不抛


@pytest.mark.integration
def test_list_keys_prefix(pg_store):
    pg_store.put("a/1", {})
    pg_store.put("a/2", {})
    pg_store.put("b/1", {})
    assert pg_store.list_keys("a/") == ["a/1", "a/2"]


def test_import_smoke():
    from stageflow.contrib.storage import postgres as pg_mod

    assert hasattr(pg_mod, "PostgresStorage")
