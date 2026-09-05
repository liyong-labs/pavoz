"""Checkpoint: 落盘 / resume 续跑 / workflow hash mismatch 拒 resume."""

import pytest

from stageflow import DAG, CheckpointMismatchError, CheckpointStore, Runtime
from stageflow.checkpoint import workflow_hash
from stageflow.storage import FileStorage


def _dag():
    dag = DAG("cp")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": ctx.state["a"] + 1}

    return dag


def _make_store(tmp_path):
    return CheckpointStore(FileStorage(str(tmp_path)))


async def test_workflow_hash_stable_across_instances(tmp_path):
    d1, d2 = _dag(), _dag()
    assert workflow_hash(d1) == workflow_hash(d2)


def test_workflow_hash_changes_with_structure(tmp_path):
    dag = _dag()
    h1 = workflow_hash(dag)

    dag2 = DAG("cp")
    # 同 stage 但 retries 不同 → hash 变
    @dag2.stage(retries=1)
    async def s_a(ctx):
        return {"a": 1}

    assert workflow_hash(dag2) != h1


async def test_resume_skips_done_stages(tmp_path):
    calls = {"a": 0, "b": 0}
    dag = DAG("cp")

    @dag.stage()
    async def s_a(ctx):
        calls["a"] += 1
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        calls["b"] += 1
        return {"b": ctx.state["a"] + 1}

    store = _make_store(tmp_path)
    rt = Runtime(checkpoint_store=store)

    # 手动构造 mid-run checkpoint: a done, b 没跑 (模拟 a 后进程断)
    from stageflow import Checkpoint
    from stageflow.checkpoint import workflow_hash as wh

    store.save(Checkpoint(
        task_id="t-cp", run_id="run-cp", dag_name="cp", workflow_hash=wh(dag),
        stage_statuses={"s_a": "done"}, state={"a": 1}, done_stages=["s_a"],
    ))

    # resume: 应只跑 s_b, 不重跑 s_a
    result = await rt.run(dag, "t-cp", resume=True)
    assert result.status == "done"
    assert calls["a"] == 0  # 没重跑
    assert calls["b"] == 1
    assert result.state == {"a": 1, "b": 2}


async def test_resume_hash_mismatch_rejected(tmp_path):
    store = _make_store(tmp_path)
    dag1 = _dag()
    rt = Runtime(checkpoint_store=store)

    result = await rt.run(dag1, "t-mismatch")
    assert result.status == "done"
    # 跑完 checkpoint 已清 → 无 cp, resume 从头
    result2 = await rt.run(dag1, "t-mismatch", resume=True)
    assert result2.status == "done"

    # 构造一个中途 cp, 然后改 DAG 结构 (retries) → mismatch
    from stageflow import Checkpoint
    from stageflow.checkpoint import workflow_hash as wh

    store.save(Checkpoint(
        task_id="t-mid", run_id="run-mid", dag_name="cp", workflow_hash=wh(dag1),
        stage_statuses={"s_a": "done"}, state={"a": 1}, done_stages=["s_a"],
    ))
    dag2 = DAG("cp")

    @dag2.stage(retries=3)  # 结构变了
    async def s_a(ctx):
        return {"a": 1}

    @dag2.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 1}

    with pytest.raises(CheckpointMismatchError, match="hash"):
        await rt.run(dag2, "t-mid", resume=True)


async def test_done_clears_checkpoint(tmp_path):
    store = _make_store(tmp_path)
    rt = Runtime(checkpoint_store=store)
    await rt.run(_dag(), "t-clear")
    assert store.load("t-clear") is None
