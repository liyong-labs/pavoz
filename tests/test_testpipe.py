"""TestPipe: mock stage / 跑全图 / 调用记录 / 还原 / replay_from (真实 cp 回归)."""

import tempfile

import pytest

from stageflow import DAG, Checkpoint, CheckpointMismatchError, Runtime, TestPipe, workflow_hash
from stageflow.checkpoint import CheckpointStore
from stageflow.storage import FileStorage


def _dag():
    dag = DAG("tp")

    @dag.stage()
    async def s_fetch(ctx):
        raise AssertionError("真实现不该被跑 (mock 掉了)")

    @dag.stage(depends_on=["s_fetch"])
    async def s_process(ctx):
        return {"processed": len(ctx.state["items"])}

    return dag


def _run_dag(retries: int = 0):
    """真实可跑完的 2-stage DAG (replay_from 首跑用 — _dag 的 s_fetch 真跑会炸)."""
    dag = DAG("tp-run")

    @dag.stage(retries=retries)
    async def s_fetch(ctx):
        return {"fetched": sorted(ctx.state["items"])}

    @dag.stage(depends_on=["s_fetch"])
    async def s_process(ctx):
        return {"processed": len(ctx.state["fetched"])}

    return dag


async def test_mock_replaces_stage():
    pipe = TestPipe(_dag(), initial_state={"items": [1, 2, 3]})
    pipe.mock("s_fetch", lambda state: {"fetched": True})  # items 已在 initial_state, 不重写
    result = await pipe.run()
    assert result.status == "done"
    assert result.state["fetched"] is True
    assert result.state["processed"] == 3


async def test_calls_recorded_with_order():
    dag = _dag()
    pipe = TestPipe(dag, initial_state={"items": [1]})
    pipe.mock("s_fetch", lambda state: {"items": state["items"]})
    await pipe.run()
    calls = pipe.calls()
    assert [c["stage"] for c in calls] == ["s_fetch"]
    assert calls[0]["mock"] is True


async def test_mock_restored_after_run():
    dag = _dag()
    pipe = TestPipe(dag)
    pipe.mock("s_fetch", lambda state: {"items": []})
    await pipe.run()
    # 还原后真实现还在 (再跑会 raise AssertionError)
    assert dag.stages["s_fetch"].fn.__name__ == "s_fetch"


async def test_mock_unknown_stage_raises():
    pipe = TestPipe(_dag())
    try:
        pipe.mock("s_nope", lambda state: {})
        assert False, "应该 raise"
    except KeyError:
        pass


async def test_mock_can_be_async():
    async def _async_fetch(state):
        return {"items": ["x"]}

    pipe = TestPipe(_dag())
    pipe.mock("s_fetch", _async_fetch)
    result = await pipe.run()
    assert result.status == "done"
    assert result.state["processed"] == 1


async def test_replay_from_reproduces_full_state():
    """从真实 cp 全 mock 重放 → 终态与 cp.state 一致 (回归: 图行为没坏)."""
    dag = _run_dag()
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        first = await runtime.run(dag, task_id="t-replay", initial_state={"items": [3, 1, 2]})
        assert first.status == "done"
        cp = store.load_latest("t-replay")
        assert cp is not None

        # 把真实现换成会炸的 — replay 若漏 mock 任一已完成 stage 会立刻暴露
        original = {n: dag.stages[n].fn for n in dag.stages}

        async def _boom(ctx):
            raise AssertionError("replay 不该跑真实现")

        for n in dag.stages:
            dag.stages[n].fn = _boom
        try:
            pipe = TestPipe.replay_from(cp, dag)
            result = await pipe.run()
        finally:
            for n, fn in original.items():
                dag.stages[n].fn = fn

        assert result.status == "done"
        assert result.state == cp.state
        calls = pipe.calls()
        assert [c["stage"] for c in calls] == ["s_fetch", "s_process"]
        assert all(c["mock"] for c in calls)


async def test_replay_from_rejects_mismatched_and_legacy_cp():
    """DAG 结构变了的 cp / v0.5.0 无 stage_deltas 的 cp → 明确报错 (不静默错配/真跑)."""
    dag = _run_dag()
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        await runtime.run(dag, task_id="t-1", initial_state={"items": [1]})
        cp = store.load_latest("t-1")
        assert cp is not None

        # DAG 结构变了 (s_fetch retries 0 → 1) → hash mismatch
        with pytest.raises(CheckpointMismatchError, match="DAG hash"):
            TestPipe.replay_from(cp, _run_dag(retries=1))

        # v0.5.0 旧 cp: 有 done_stages/state, 无 stage_deltas/initial_state
        store.save(Checkpoint(
            task_id="t-1",
            run_id="legacy1",
            dag_name=dag.name,
            workflow_hash=workflow_hash(dag),
            stage_statuses={"s_fetch": "done", "s_process": "done"},
            state={"items": [1], "fetched": [1], "processed": 1},
            done_stages=["s_fetch", "s_process"],
        ))
        legacy = store.load_latest("t-1")
        assert legacy is not None
        with pytest.raises(RuntimeError, match="stage_deltas"):
            TestPipe.replay_from(legacy, dag)
