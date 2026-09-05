"""M3: Runtime.run_stage 单 stage 重放 (不落 cp)."""
import tempfile

import pytest

from stageflow import Runtime
from stageflow.checkpoint import CheckpointStore
from stageflow.dag import DAG
from stageflow.storage import FileStorage


def _dag():
    dag = DAG("d")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": ctx.state["a"] + 1}

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        return {"c": ctx.state["b"] + 1}

    return dag


async def _run_full(dag, task_id="t-1"):
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        result = await runtime.run(dag, task_id=task_id, initial_state={"seed": 0})
        assert result.status == "done"
        return store, result


def _temp_store():
    tmp = tempfile.TemporaryDirectory()
    return CheckpointStore(FileStorage(root_dir=tmp)), tmp


async def test_full_run_saves_deltas():
    """run() 落盘的 cp 含 stage_deltas (M2 存储打通 runtime)."""
    dag = _dag()
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        await runtime.run(dag, task_id="t-1", initial_state={"seed": 0})
        cp = store.load_latest("t-1")
        assert cp.initial_state == {"seed": 0}
        assert set(cp.stage_deltas.keys()) == {"s_a", "s_b", "s_c"}


async def test_run_stage_replays_single_stage():
    """run_stage('s_b') 用重建 state, 只跑 s_b."""
    dag = _dag()
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        await runtime.run(dag, task_id="t-1", initial_state={"seed": 0})

        # 改 s_b 的实现 (模拟调 prompt/参数)
        calls = []
        original = dag.stages["s_b"].fn

        async def s_b_v2(ctx):
            calls.append(ctx.state["a"])  # 应看到 s_a 的输出 1
            return {"b": ctx.state["a"] * 10}

        dag.stages["s_b"].fn = s_b_v2
        try:
            result = await runtime.run_stage(dag, task_id="t-1", stage_name="s_b")
        finally:
            dag.stages["s_b"].fn = original

        assert result.status == "done"
        assert result.state["b"] == 10  # 新实现生效
        assert calls == [1]  # ctx.state.a = 1 (s_a 的 delta, 不是终态 3)


async def test_run_stage_does_not_touch_checkpoint():
    """run_stage 不落 cp 不动 pointer (A6/A7 防污染)."""
    dag = _dag()
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        await runtime.run(dag, task_id="t-1", initial_state={"seed": 0})
        cp_before = store.load_latest("t-1")
        runs_before = store.list_runs("t-1")

        await runtime.run_stage(dag, task_id="t-1", stage_name="s_b")

        cp_after = store.load_latest("t-1")
        assert cp_after.run_id == cp_before.run_id  # pointer 没动
        assert store.list_runs("t-1") == runs_before  # 没新增 run


async def test_run_stage_missing_cp_raises():
    dag = _dag()
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        with pytest.raises(RuntimeError, match="无 checkpoint"):
            await runtime.run_stage(dag, task_id="nobody", stage_name="s_a")


async def test_run_stage_unknown_stage_raises():
    dag = _dag()
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        await runtime.run(dag, task_id="t-1", initial_state={"seed": 0})
        with pytest.raises(KeyError):
            await runtime.run_stage(dag, task_id="t-1", stage_name="s_nope")
