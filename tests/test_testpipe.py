"""TestPipe: mock stage / 跑全图 / 调用记录 / 还原 / replay_from (真实 cp 回归)."""

import tempfile

import pytest

from pavoz import DAG, Checkpoint, CheckpointMismatchError, Runtime, TestPipe, workflow_hash
from pavoz.checkpoint import CheckpointStore
from pavoz.storage import FileStorage


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
    with pytest.raises(KeyError, match="不在 DAG"):
        pipe.mock("s_nope", lambda state: {})


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
            done_stages=["s_fetch", "s_process"],
        ))
        legacy = store.load_latest("t-1")
        assert legacy is not None
        with pytest.raises(RuntimeError, match="stage_deltas"):
            TestPipe.replay_from(legacy, dag)

# ── 2026-10-16 巡检补: 夹具自身的契约 (此前零覆盖) ─────────────────────────

async def test_calls_returns_the_actual_keys_not_the_old_args_key():
    """calls() 的 docstring 曾写 [{stage, mock, args}], 实际键是 state_keys。

    照旧文档写 `call["args"]` 会 KeyError —— 断言直接崩, 而不是断言失败。
    """
    pipe = TestPipe(_dag(), initial_state={"items": [1, 2, 3]})
    pipe.mock("s_fetch", lambda st: {"fetched": [1]})
    assert (await pipe.run()).status == "done"

    recs = pipe.calls()
    assert recs, "mock 过的 stage 应留下调用记录"
    assert set(recs[0]) == {"stage", "mock", "state_keys"}, (
        f"calls() 的键变了, docstring 与断言都要跟着改: {sorted(recs[0])}")
    assert "args" not in recs[0], "args 这个键从来就不存在 (docstring 写错了)"


async def test_calls_state_keys_reflects_visible_state():
    """state_keys 是该 stage 被调用时**看到的** state 顶层 key —— 能用来断言
    '下游确实拿到了上游的产出'。"""
    pipe = TestPipe(_dag(), initial_state={"items": [1, 2]})
    seen: list[str] = []

    def _record(st):
        seen.extend(st.keys())
        return {"processed": 0}

    pipe.mock("s_fetch", lambda st: {"fetched": [1, 2]})   # _dag 的真实现会炸, 必须也 mock
    pipe.mock("s_process", _record)
    assert (await pipe.run()).status == "done"

    rec = [r for r in pipe.calls() if r["stage"] == "s_process"][0]
    # 注意: 记录的是**插入序**不是排序序, 所以直接比 seen 不比 sorted(seen)
    assert rec["state_keys"] == seen, "调用记录的 state_keys 应与 mock 看到的一致"
    assert "fetched" in rec["state_keys"], "上游产出的 key 应出现在下游的 state 里"


async def test_unmocked_stage_is_not_recorded_in_calls():
    """反向约束: 只有被 mock 的 stage 留调用记录 (docstring 现在明说了这一点)。"""
    pipe = TestPipe(_dag(), initial_state={"items": [1]})
    pipe.mock("s_fetch", lambda st: {"fetched": [1]})
    await pipe.run()

    assert [r["stage"] for r in pipe.calls()] == ["s_fetch"], (
        "未 mock 的 stage 走真实现, 不该出现在 calls() 里")


async def test_run_rejects_resume_with_an_actionable_message():
    """resume=True 永远失败 (TestPipe 不落盘) —— 但要说清替代路径是什么。

    修复前它一路传到 Runtime 才炸, 报的是内部不变量 "resume=True requires
    checkpoint_store", 与调用方毫无关系。
    """
    pipe = TestPipe(_dag(), initial_state={"items": [1]})
    pipe.mock("s_fetch", lambda st: {"fetched": [1]})

    with pytest.raises(ValueError) as ei:
        await pipe.run(resume=True)
    msg = str(ei.value)
    assert "replay_from" in msg, f"报错应指出替代路径, 实得: {msg}"
    assert "checkpoint_store" in msg, f"报错应说明为什么不支持, 实得: {msg}"


async def test_run_default_still_works_without_resume_kwarg():
    """反向约束: 默认调用一字不变 (不带 resume 参数)。"""
    pipe = TestPipe(_dag(), initial_state={"items": [1]})
    pipe.mock("s_fetch", lambda st: {"fetched": [1]})
    assert (await pipe.run()).status == "done"
