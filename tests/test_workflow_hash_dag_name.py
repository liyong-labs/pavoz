"""DAG 名必须计入结构指纹 —— 否则两条流水线可以静默混流.

背景 (2026-10-16 巡检): `workflow_hash` 只覆盖 stage 名 + 依赖 + retries +
timeout + 条件边, **不含 dag 名**。于是结构相同、名字不同的两个 DAG (两条不同
流水线) 拿到同一个 hash; task_id 撞车时 resume 会拿错 cp 且毫无提示。实测:

    ① dag "extract" 首跑, 末 stage 失败 → cp 部分完成 (by=EXTRACT)
    ② dag "classify" resume 同一 task → status=done
       state = {'by': 'EXTRACT', ..., 'final': 'EXTRACT'}
       RunResult.dag_name = 'classify'      ← 报告是 classify, 装的是 extract 的数据

RunResult.dag_name 撒谎, state 跨流水线污染, 而 task_id 是幂等键 —— 拿它续跑的
前提本就是"同一个工作流", 名字是工作流身份的一部分。

本测试钉住三件事: 名字进指纹 (不同名 → 不同 hash)、名字进指纹的**代价** (改名
= 旧 cp 不能续跑, 这是刻意的)、以及跨 DAG resume 真的会被 CheckpointMismatchError
拦住 —— 而不是只测 hash 值。
"""
from __future__ import annotations

import tempfile

import pytest

from pavoz import (
    DAG,
    CheckpointMismatchError,
    CheckpointStore,
    FileStorage,
    Runtime,
    StageError,
    TestPipe,
    workflow_hash,
)


def _build(name: str, label: str, *, fail_last: bool):
    dag = DAG(name)

    @dag.stage()
    async def s_a(ctx):
        return {"by": label}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        if fail_last:
            raise StageError("b 失败", category="SEARCH_NO_RESULT")
        return {"final": ctx.state["by"]}

    return dag


def test_dag_name_is_part_of_the_fingerprint():
    """结构相同、名字不同 → 指纹必须不同 (这是本轮修复的全部内容)。"""
    h_extract = workflow_hash(_build("extract", "X", fail_last=False))
    h_classify = workflow_hash(_build("classify", "Y", fail_last=False))
    assert h_extract != h_classify, "dag 名没进指纹 → 两条流水线指纹相同"


def test_same_dag_name_same_structure_is_still_stable():
    """反向约束: 同一名字同一结构 → 指纹稳定 (不许把名字变成随机盐)。"""
    assert (workflow_hash(_build("extract", "A", fail_last=False))
            == workflow_hash(_build("extract", "B", fail_last=False)))


async def test_renaming_a_dag_invalidates_old_checkpoint():
    """改名的**代价**, 刻意如此: 宁可拒绝, 不可静默混流。"""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    r = await rt.run(_build("extract", "EXTRACT", fail_last=True), "t1")
    assert r.status == "failed", "前提: 先造一个部分完成的 cp"

    with pytest.raises(CheckpointMismatchError, match="≠ 当前 DAG hash"):
        await rt.run(_build("extract_v2", "EXTRACT", fail_last=True), "t1", resume=True)


async def test_cross_dag_resume_is_rejected_instead_of_mixing_state():
    """核心防线: 换一条流水线来 resume → 响, 而不是静默产出混合 state。"""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    await rt.run(_build("extract", "EXTRACT", fail_last=True), "t2")

    with pytest.raises(CheckpointMismatchError):
        await rt.run(_build("classify", "CLASSIFY", fail_last=False), "t2", resume=True)


async def test_cross_dag_fork_run_is_rejected_too():
    """fork_run 走另一条入口, 同样要拦。"""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    await rt.run(_build("extract", "EXTRACT", fail_last=True), "t3")

    with pytest.raises(CheckpointMismatchError):
        await rt.fork_run(_build("classify", "CLASSIFY", fail_last=False),
                          "t3", from_stage="s_a")


def test_replay_from_rejects_cross_dag_checkpoint(tmp_path):
    """TestPipe.replay_from 是第三条入口 —— 同样要拦, 且消息共用同一构造点。"""
    import asyncio

    async def _go():
        store = CheckpointStore(FileStorage(tmp_path / "cp"))
        rt = Runtime(checkpoint_store=store)
        await rt.run(_build("extract", "EXTRACT", fail_last=True), "t4")
        return store.load_latest("t4")

    cp = asyncio.run(_go())
    with pytest.raises(CheckpointMismatchError, match="extract"):
        TestPipe.replay_from(cp, _build("classify", "CLASSIFY", fail_last=False))


async def test_mismatch_message_names_both_dags_when_they_differ():
    """名字不同是最常见的 mismatch 原因, 消息要点名双方 (别让人自己猜)。"""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    await rt.run(_build("extract", "EXTRACT", fail_last=True), "t5")

    with pytest.raises(CheckpointMismatchError) as ei:
        await rt.run(_build("classify", "CLASSIFY", fail_last=False), "t5", resume=True)
    msg = str(ei.value)
    assert "extract" in msg and "classify" in msg, f"消息该点名两个 dag 名: {msg}"
    assert "不能 resume" in msg
