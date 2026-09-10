"""R1 prune / R2 error_class / R3 set_progress — 2026-09-10 ai-research PR 验收测试.

评审条件逐条落测: latest 指针保护 / dry-run / error 字符串格式不变 / cancelled→None /
stage_progress 不落 checkpoint / 同 fraction 去抖 / 无 on_event no-op.
"""
from __future__ import annotations

import asyncio
import json

import pytest

from pavoz import DAG, Runtime
from pavoz.checkpoint import Checkpoint, CheckpointStore
from pavoz.storage import FileStorage
from pavoz.types import StageError


@pytest.fixture()
def store(tmp_path):
    return CheckpointStore(FileStorage(str(tmp_path / "cp")))


def _cp(task_id, run_id, ts):
    return Checkpoint(
        task_id=task_id, run_id=run_id, dag_name="d", workflow_hash="h",
        stage_statuses={"s": "done"}, done_stages=["s"], stage_ts={"s": ts},
    )


# ── R1: prune ───────────────────────────────────────────────

def test_prune_keeps_newest_and_protects_latest(store):
    for rid, ts in [("r-old", 100.0), ("r-mid", 200.0), ("r-new", 300.0)]:
        store.save(_cp("t", rid, ts))
    rep = store.prune("t", keep_last=1)
    assert {d["run_id"] for d in rep["deleted"]} == {"r-old", "r-mid"}
    assert rep["kept"] == ["r-new"]
    assert rep["protected_latest"] == "r-new"  # save 时指针已指向最后保存的 r-new
    assert rep["approx_bytes_freed"] > 0
    assert store.load("t", "r-old") is None
    assert store.load("t", "r-new") is not None


def test_prune_protects_latest_even_when_old(store):
    # 指针指向最旧的 run (手工构造) — 也绝不许删
    for rid, ts in [("r-old", 100.0), ("r-new", 200.0)]:
        store.save(_cp("t", rid, ts))
    store.storage.put("runs/t/latest", {"run_id": "r-old"})
    rep = store.prune("t", keep_last=1)
    assert rep["deleted"] == []  # r-old 受指针保护, r-new 在 keep_last 内
    assert sorted(rep["kept"]) == ["r-new", "r-old"]


def test_prune_dry_run_lists_without_deleting(store):
    for rid, ts in [("r-old", 1.0), ("r-new", 2.0)]:
        store.save(_cp("t", rid, ts))
    rep = store.prune("t", keep_last=1, dry_run=True)
    assert [d["run_id"] for d in rep["deleted"]] == ["r-old"]
    assert rep["dry_run"] is True
    assert store.load("t", "r-old") is not None  # 什么都没删


def test_prune_no_stage_ts_falls_back_oldest(store):
    # 无 stage_ts 的 cp 视为最旧 (排序兜底), 不抛异常
    store.save(_cp("t", "r-a", 0.0))
    cp_b = _cp("t", "r-b", 5.0)
    cp_b.stage_ts = {}
    store.save(cp_b)
    rep = store.prune("t", keep_last=1)
    assert [d["run_id"] for d in rep["deleted"]] == ["r-a"]


def test_prune_unknown_task_noop(store):
    rep = store.prune("ghost", keep_last=3)
    assert rep["deleted"] == [] and rep["protected_latest"] is None


def test_prune_negative_keep_last_raises(store):
    with pytest.raises(ValueError):
        store.prune("t", keep_last=-1)


def test_prune_isolated_per_task(store):
    store.save(_cp("t1", "r1", 1.0))
    store.save(_cp("t2", "r2", 2.0))
    store.prune("t1", keep_last=1)
    assert store.load("t2", "r2") is not None  # 跨 task 不触碰


# ── R2: RunResult.error_class ───────────────────────────────

class PipelineError(RuntimeError):
    """模拟消费者业务异常 (不继承 pavoz 类型 — 反向耦合被禁止)."""


def _dag_failing(exc):
    dag = DAG("t_err")

    @dag.stage()
    async def s_boom(ctx):
        raise exc

    return dag


def test_error_class_business_exception_preserved(tmp_path):
    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(tmp_path / "cp"))))
    res = asyncio.run(rt.run(_dag_failing(PipelineError("搜索 0 结果")), "t1"))
    assert res.status == "failed"
    assert res.error_class == "PipelineError"  # 原始类名保真 — 分诊的关键
    assert res.error == "FatalError: 搜索 0 结果"  # 字符串格式不变 (API 冻结)


def test_error_class_real_bug(tmp_path):
    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(tmp_path / "cp"))))
    res = asyncio.run(rt.run(_dag_failing(AttributeError("no attr")), "t2"))
    assert res.error_class == "AttributeError"


def test_error_class_stage_error(tmp_path):
    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(tmp_path / "cp"))))
    res = asyncio.run(rt.run(_dag_failing(StageError("bad stage")), "t3"))
    assert res.error_class == "StageError"
    assert res.error == "bad stage"  # StageError 分支消息无前缀 (既有语义)


def test_error_class_done_and_cancel_are_none(tmp_path):
    events: list[tuple] = []
    dag = DAG("t_ok")

    @dag.stage()
    async def s_ok(ctx):
        return {"v": 1}

    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(tmp_path / "cp"))),
                 on_event=lambda e, d: events.append((e, d)))
    res = asyncio.run(rt.run(dag, "t4"))
    assert res.status == "done" and res.error_class is None

    dag2 = DAG("t_cancel")

    @dag2.stage()
    async def s_slow(ctx):
        await asyncio.sleep(30)

    rt2 = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(tmp_path / "cp2"))),
                  cancel_check=lambda tid: True,
                  on_event=lambda e, d: events.append((e, d)))
    res2 = asyncio.run(rt2.run(dag2, "t5"))
    assert res2.status == "cancelled" and res2.error_class is None  # 取消 ≠ 异常


# ── R3: ctx.set_progress ────────────────────────────────────

def test_set_progress_events_and_debounce(tmp_path):
    progress: list[dict] = []
    dag = DAG("t_prog")

    @dag.stage()
    async def s_long(ctx):
        ctx.set_progress(0.3, "batch 1/3")
        ctx.set_progress(0.3, "batch 1/3 again")  # 同 fraction → 去抖
        ctx.set_progress(0.9, "batch 3/3")
        return {"done": True}

    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(tmp_path / "cp"))),
                 on_event=lambda e, d: progress.append(d) if e == "stage_progress" else None)
    res = asyncio.run(rt.run(dag, "t6"))
    assert res.status == "done"
    assert len(progress) == 2  # 去抖生效
    assert progress[0]["fraction"] == 0.3 and progress[0]["note"] == "batch 1/3"
    assert progress[1]["fraction"] == 0.9
    assert progress[0]["stage"] == "s_long" and progress[0]["task_id"] == "t6"


def test_set_progress_not_in_checkpoint_and_noop_without_events(tmp_path):
    # 1) 不落 checkpoint: checkpoint 文件里无 progress 痕迹
    cp_dir = tmp_path / "cp"
    dag = DAG("t_pers")

    @dag.stage()
    async def s_p(ctx):
        ctx.set_progress(0.5)
        return {"v": 1}

    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(cp_dir))))
    res = asyncio.run(rt.run(dag, "t7"))
    assert "v" in res.state
    cps = list((cp_dir / "runs" / "t7").glob("*/*.json"))
    assert cps, "checkpoint 应存在"
    blob = cps[0].read_text()
    assert "fraction" not in blob and "stage_progress" not in blob

    # 2) 未启用 on_event → no-op 不炸
    dag2 = DAG("t_noop")

    @dag2.stage()
    async def s_n(ctx):
        ctx.set_progress(0.1)
        return {"ok": True}

    rt2 = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(tmp_path / "cp2"))))
    assert asyncio.run(rt2.run(dag2, "t8")).status == "done"
