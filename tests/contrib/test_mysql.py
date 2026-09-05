import os
import uuid

import pytest

pytest.importorskip("pymysql")  # skip whole module when pymysql absent (no docker + no driver)

from stageflow.contrib.storage.mysql import MySQLStorage

MYSQL_DSN = os.environ.get(
    "STAGEFLOW_TEST_MYSQL_DSN",
    "mysql+pymysql://root:root@localhost:3306/test",
)


@pytest.fixture
def mysql_store():
    pytest.importorskip("pymysql")
    store = MySQLStorage(MYSQL_DSN, table=f"kv_{uuid.uuid4().hex[:8]}")
    yield store
    try:
        import pymysql

        conn = pymysql.connect(
            host="localhost", user="root", password="root", db="test"
        )
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{store.table}`")
        conn.commit()
        conn.close()
    except Exception:  # noqa: BLE001, S110 — cleanup, no logger needed
        pass


@pytest.mark.integration
def test_put_get_roundtrip(mysql_store):
    mysql_store.put("k", {"a": 1, "中文": "ok"})
    assert mysql_store.get("k") == {"a": 1, "中文": "ok"}


@pytest.mark.integration
def test_get_missing_returns_none(mysql_store):
    assert mysql_store.get("nope") is None


@pytest.mark.integration
def test_put_upsert(mysql_store):
    mysql_store.put("k", {"v": 1})
    mysql_store.put("k", {"v": 2})
    assert mysql_store.get("k") == {"v": 2}


@pytest.mark.integration
def test_delete_silent_on_missing(mysql_store):
    mysql_store.delete("never-existed")


@pytest.mark.integration
def test_list_keys_prefix(mysql_store):
    mysql_store.put("a/1", {})
    mysql_store.put("a/2", {})
    mysql_store.put("b/1", {})
    assert mysql_store.list_keys("a/") == ["a/1", "a/2"]


def test_import_smoke():
    from stageflow.contrib.storage import mysql as m_mod

    assert hasattr(m_mod, "MySQLStorage")
