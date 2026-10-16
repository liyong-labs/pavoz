"""`trace` 的输出要足以**避免误判**。

`trace` 只显示 latest 那一个 run。而输出里原先**没有任何迹象**表明这是个多 run
的 task —— 一个跑过 5 次的 task, 和一个只跑过 1 次的, 输出长得一模一样。用户据此
以为"这就是全部", 尤其在 `diff_runs` 需要两个 run_id、而 CLI 层没有命令能列出它们
的时候 (`prune --dry-run` 能, 但那是另一回事)。

跟之前修的 `state --key` 缺 key (输出 `null` + rc=0, 与"值就是 null"逐字节相同)
是同一族问题: **输出不足以让调用方判断自己看到的是不是全部**。

判据靠**变异**验证: 把 `total_runs` 恒填 1 (等于"这是个单 run 的 task"那种误导),
本文件必须转红。

⚠️ 本文件的用例是**同步**的: `cli.main` 内部自己 `asyncio.run()`, 在 async 用例里
调它会撞上运行中的 event loop。这个坑本会话已经踩过一次 (export-input 那批用例),
写之前该先查同目录下别的 CLI 测试怎么写的 —— 我这次还是先写后撞。
"""

from __future__ import annotations

import asyncio
import json

import pytest

import pavoz.cli as cli
from pavoz import DAG, CheckpointStore, FileStorage, Runtime


@pytest.fixture(autouse=True)
def _isolated_storage(tmp_path, monkeypatch):
    """指向 tmp 的 storage, 且**必须用 SPEC** 而不是裸的 PAVOZ_STORAGE。

    裸的 `PAVOZ_STORAGE` 在 `pavoz.cli` **import 期**就被固化成 `DEFAULT_STORAGE`
    了, 之后 monkeypatch 它来不及 —— 症状是 CLI 去读真实的 `~/.pavoz/data`,
    报"task T 无 checkpoint", 看起来像被测行为坏了。`PAVOZ_STORAGE_SPEC` 是
    运行时读的, 才切得动。同目录的 store 也要用它 (见各用例)。
    """
    root = tmp_path / "store"
    monkeypatch.setenv("PAVOZ_STORAGE_SPEC", "pavoz.storage:FileStorage")
    monkeypatch.setenv("PAVOZ_STORAGE_KWARGS",
                       json.dumps({"root_dir": str(root)}))
    monkeypatch.setenv("PAVOZ_STORAGE", str(root))


def _dag() -> DAG:
    dag = DAG("trace_probe")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    return dag


def _trace(capsys) -> dict:
    rc = cli.main(["trace", "--task-id", "T"])
    out, _err = capsys.readouterr()
    assert rc == 0, f"trace 应成功, 实得 rc={rc}: {out}"
    return json.loads(out)


def test_total_runs_makes_multi_run_tasks_visible(tmp_path, capsys):
    """★ 核心: 同一个 task 跑 1 次与跑 3 次, 输出必须**可区分**。"""
    store = CheckpointStore(FileStorage(tmp_path / "store"))

    def _run_once():
        return asyncio.run(Runtime(checkpoint_store=store).run(_dag(), "T"))

    _run_once()
    first = _trace(capsys)
    assert first["total_runs"] == 1, f"单 run 时 total_runs=1, 实得 {first['total_runs']}"

    for _ in range(2):
        _run_once()
    third = _trace(capsys)
    assert third["total_runs"] == 3, (
        f"★ 跑了 3 次的 task, 输出必须说 total_runs=3 —— 否则用户以为看到的是"
        f"全部, 实得 {third['total_runs']}")


def test_is_latest_marks_the_pointer_target(tmp_path, capsys):
    """输出要标明这是 latest —— checkpoint 会变 (fork 会让 latest 指向新 run)。"""
    store = CheckpointStore(FileStorage(tmp_path / "store"))

    def _run_once():
        return asyncio.run(Runtime(checkpoint_store=store).run(_dag(), "T"))

    _run_once()
    first = _trace(capsys)
    assert first["is_latest"] is True

    _run_once()
    second = _trace(capsys)
    assert second["is_latest"] is True
    assert second["run_id"] != first["run_id"], (
        "前置条件: 第二次 run 应产生新 run_id (否则说明 latest 没更新, 那是另一个问题)")


def test_added_fields_do_not_break_the_existing_contract(tmp_path, capsys):
    """反向约束: 原有字段一个都不能少、不能改名 —— 加字段是纯增。"""
    store = CheckpointStore(FileStorage(tmp_path / "store"))
    asyncio.run(Runtime(checkpoint_store=store).run(_dag(), "T"))

    out = _trace(capsys)
    for key in ("task_id", "run_id", "dag", "workflow_hash",
                "stage_statuses", "done_stages", "state_keys"):
        assert key in out, f"原有字段 {key} 丢了 —— 这是 breaking change"
    assert out["task_id"] == "T"
    assert out["dag"] == "trace_probe"
    assert out["done_stages"] == ["s_a"]
    assert out["state_keys"] == ["a"]


def test_trace_still_reports_missing_checkpoint_clearly(tmp_path, capsys):
    """无 checkpoint 时仍是友好消息 + rc=1, 不受本次改动影响。"""
    rc = cli.main(["trace", "--task-id", "NOPE"])
    out, err = capsys.readouterr()
    assert rc == 1
    assert "无 checkpoint" in out
    assert "Traceback" not in err


def test_legacy_checkpoint_without_run_listing_still_traces(tmp_path, capsys,
                                                                monkeypatch):
    """存储后端枚举不了 run 时, `total_runs` 退成 null, **整条命令不能挂**。

    那条 try/except 是有意的: `list_runs` 是可选能力, 而 `trace` 的本职是打印
    latest checkpoint —— 因一个附加字段失败而让整条命令报错, 是本末倒置。
    """
    store = CheckpointStore(FileStorage(tmp_path / "store"))
    asyncio.run(Runtime(checkpoint_store=store).run(_dag(), "T"))

    def _boom(_task_id):
        raise NotImplementedError("这个后端不支持枚举 run")

    monkeypatch.setattr(CheckpointStore, "list_runs", _boom)
    out = _trace(capsys)
    assert out["total_runs"] is None, (
        f"枚举失败时应退成 null, 实得 {out['total_runs']!r}")
    assert out["run_id"], "核心字段仍要在"
