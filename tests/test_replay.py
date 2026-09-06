"""M3: Runtime.run_stage 单 stage 重放 (不落 cp)."""
import tempfile

import pytest

from stageflow import Checkpoint, Runtime, StageError, workflow_hash
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
        assert result.state == {"seed": 0, "a": 1, "b": 10}  # 精确集合: rebuilt 是 {seed,a:1}, 若误用终态会含 b:2/c:3
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


def _dag_fail_sb():
    """s_b 首跑必失败 (StageError) 的 DAG — 失败重放 / 依赖未完成 guard 用."""
    dag = DAG("d")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        raise StageError("boom")

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        return {"c": ctx.state["b"] + 1}

    return dag


async def test_run_stage_replays_failed_stage():
    """原始 run 失败的 stage: 依赖已完成 → rebuild_state 全量 merge 后可重放."""
    dag = _dag_fail_sb()
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        result = await runtime.run(dag, task_id="t-1", initial_state={"seed": 0})
        assert result.status == "failed"
        cp = store.load_latest("t-1")
        assert cp.done_stages == ["s_a"]  # s_b 失败, 只完成了 s_a

        # 修好 s_b 再重放 (模拟改 prompt 后只重跑该 stage)
        seen = []
        original = dag.stages["s_b"].fn

        async def s_b_fixed(ctx):
            seen.append(dict(ctx.state))  # 应看到重建输入 {seed:0, a:1}
            return {"b": ctx.state["a"] + 1}

        dag.stages["s_b"].fn = s_b_fixed
        try:
            replayed = await runtime.run_stage(dag, task_id="t-1", stage_name="s_b")
        finally:
            dag.stages["s_b"].fn = original

        assert replayed.status == "done"
        assert replayed.state == {"seed": 0, "a": 1, "b": 2}
        assert seen == [{"seed": 0, "a": 1}]  # ctx 看到的是重建 pre-state


async def test_run_stage_legacy_cp_without_deltas_raises():
    """v0.5.0 旧 cp (无 stage_deltas) → 明确报错, 不静默用空输入重放."""
    dag = _dag()
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        # 手工构造 pre-M2 cp: done_stages/state 有, stage_deltas/initial_state 空
        store.save(Checkpoint(
            task_id="t-1",
            run_id="legacy1",
            dag_name="d",
            workflow_hash=workflow_hash(dag),
            stage_statuses={"s_a": "done", "s_b": "done", "s_c": "done"},
            done_stages=["s_a", "s_b", "s_c"],
        ))
        with pytest.raises(RuntimeError, match="stage_deltas"):
            await runtime.run_stage(dag, task_id="t-1", stage_name="s_b")


async def test_run_stage_deps_incomplete_raises():
    """target 未完成且依赖未全完成 → 明确报错 (重建输入残缺)."""
    dag = _dag_fail_sb()
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        await runtime.run(dag, task_id="t-1", initial_state={"seed": 0})
        # s_b 失败 → s_c 的依赖 [s_b] 未完成 → 明确错误
        with pytest.raises(RuntimeError, match="依赖未完成"):
            await runtime.run_stage(dag, task_id="t-1", stage_name="s_c")


def _dag_chain_overwrite():
    """chain-overwrite DAG: s_a→a / s_b→b / s_c→a=99 (a 的终态 producer = s_c)."""
    dag = DAG("d")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": ctx.state["a"] + 1}

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        return {"a": 99}  # chain-overwrite: 覆盖 s_a 写的 a

    return dag


async def test_run_stage_chain_overwrite_uses_rebuilt_producers():
    """chain-overwrite DAG 重放中间 stage: 覆盖判定用执行时点 producer (非终态).

    原始 run: s_a→a=1 / s_b→b=2 / s_c→a=99 → cp 终态 producers["a"]="s_c".
    重放 s_b 且新实现改写 a: s_b 执行时 a 的 producer 是 s_a (其祖先) → 允许覆盖,
    必须 done. 若误用终态 producers (a→s_c), reachable("s_c","s_b")=False →
    误报 StateConflictError (reviewer live-reproduced bug).
    """
    dag = _dag_chain_overwrite()
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        await runtime.run(dag, task_id="t-1")
        cp = store.load_latest("t-1")
        assert cp.producers["a"] == "s_c"  # 确认 DAG 形状: 终态 producer 是后写者

        original = dag.stages["s_b"].fn

        async def s_b_v2(ctx):  # 模拟调 prompt: 新实现额外改写 a
            return {"b": ctx.state["a"] + 1, "a": ctx.state["a"] * 10}

        dag.stages["s_b"].fn = s_b_v2
        try:
            result = await runtime.run_stage(dag, task_id="t-1", stage_name="s_b")
        finally:
            dag.stages["s_b"].fn = original

        assert result.status == "done"
        assert result.state == {"a": 10, "b": 2}
