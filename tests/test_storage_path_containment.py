"""FileStorage 必须把 key 关在 root 内 — 路径穿越防护.

背景: 2026-10 审计发现 `_path` 只有字符集过滤 (`[a-z0-9_/.-]`), 而 `.` 在允许
集内, 所以 `..` 段畅通。实测 (修复前):

    fs.get("../../xxx/secret")     → 读出 root 之外的文件
    fs.delete("../../xxx/secret")  → 删掉 root 之外的文件

`put` 当时看似被挡住, 但那是 os.makedirs 逐段创建时撞上字面量 `..` 目录而**偶然**
失败 (PermissionError), 不是防护 —— 且只挡 put, 挡不住 get/delete。

现在 `_path` 把路径解析成 realpath 后断言仍落在 root 内, 对三个操作一视同仁。
本测试用真实 FileStorage 验证越界被拒, 且不依赖"文件系统恰好报错"。
"""
from __future__ import annotations

import os

import pytest

from pavoz import FileStorage


@pytest.fixture
def roots(tmp_path):
    """(storage_root, root 之外的受害目录, 受害文件路径)。"""
    root = tmp_path / "store"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = outside / "secret.json"
    victim.write_text('{"stolen": "top-secret"}', encoding="utf-8")
    return root, outside, victim


def _escaping_key(root, victim) -> str:
    """构造一个从 storage root 出发能走到受害文件的 key (去掉 .json 后缀)。"""
    rel = os.path.relpath(str(victim), str(root))
    return rel[: -len(".json")]


def test_get_cannot_read_outside_root(roots):
    root, _outside, victim = roots
    fs = FileStorage(str(root))
    with pytest.raises(ValueError, match="越界"):
        fs.get(_escaping_key(root, victim))


def test_delete_cannot_remove_outside_root(roots):
    root, _outside, victim = roots
    fs = FileStorage(str(root))
    with pytest.raises(ValueError, match="越界"):
        fs.delete(_escaping_key(root, victim))
    assert victim.exists(), "越界 delete 必须被拒, 受害文件仍在"


def test_put_cannot_write_outside_root(roots):
    root, outside, _victim = roots
    fs = FileStorage(str(root))
    rel = os.path.relpath(str(outside / "new.json"), str(root))[: -len(".json")]
    with pytest.raises(ValueError, match="越界"):
        fs.put(rel, {"x": 1})
    assert not (outside / "new.json").exists()


def test_list_keys_cannot_walk_outside_root(roots):
    root, _outside, victim = roots
    fs = FileStorage(str(root))
    with pytest.raises(ValueError, match="越界"):
        fs.list_keys(_escaping_key(root, victim))


def test_dotdot_task_id_cannot_escape_via_checkpoint_store(roots):
    """端到端: task_id='..' 经 CheckpointStore 落盘, 不得写到 root 之外。

    实测修复前 task_id='..' 会让 key 变成 runs/../<run_id>/checkpoint。
    """
    import asyncio

    from pavoz import DAG, CheckpointStore, Runtime

    root, outside, _victim = roots
    dag = DAG("t")

    @dag.stage()
    async def s_a(ctx):
        return {"x": 1}

    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(root))))
    r = asyncio.run(rt.run(dag, task_id=".."))
    assert r.status == "done"
    # 一切仍在 root 内
    assert all(str(root) in str(p) for p in root.rglob("*.json"))


def test_normal_keys_still_work(tmp_path):
    """防护不能误伤正常 key (含 runs/ 前缀的多段 key)。"""
    root = tmp_path / "store"
    fs = FileStorage(str(root))
    key = "runs/t1/abc123/checkpoint"
    fs.put(key, {"ok": True})
    assert fs.get(key) == {"ok": True}
    assert key in fs.list_keys("runs/t1")
    fs.delete(key)
    assert fs.get(key) is None


def test_illegal_character_still_rejected(tmp_path):
    root = tmp_path / "store"
    fs = FileStorage(str(root))
    with pytest.raises(ValueError, match="非法 storage key"):
        fs.get("runs/t1/evil key")
