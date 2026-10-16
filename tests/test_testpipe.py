"""TestPipe: mock stage / 跑全图 / 调用记录 / 还原 / replay_from (真实 cp 回归)."""

import tempfile

import pytest

from pavoz import (
    DAG,
    Checkpoint,
    CheckpointMismatchError,
    RetryableError,
    Runtime,
    TestPipe,
    workflow_hash,
)
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


# ── 记录的生命周期 ──────────────────────────────────────────────────────────
#
# `calls()` 是这个夹具的主要断言面, 所以它的生命周期必须是"每次 run 独立"。
# `run()` 的 finally 明说"实例可复用", 而记录此前跨 run 累积 —— 复用实例跑第二遍
# 时会拿到第一遍的记录。
#
# 失败形态是**假红**而非假绿 (该红的红), 但"只跑了一遍却拿到两条记录"没法用直觉
# 排除, 用户得先怀疑自己的用法才查得到。测试工具出假红仍然算缺陷: 它消耗的是
# 别人的信任。

async def test_calls_are_per_run_not_cumulative():
    """★ 复用实例跑第二遍, `calls()` 只该有第二遍的记录。"""
    dag = DAG("tp_reuse")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 2}

    pipe = TestPipe(dag)
    pipe.mock("s_a", lambda st: {"a": 10})
    pipe.mock("s_b", lambda st: {"b": 20})

    await pipe.run()
    assert [c["stage"] for c in pipe.calls()] == ["s_a", "s_b"], (
        f"第一遍: {[c['stage'] for c in pipe.calls()]}")

    await pipe.run()
    assert [c["stage"] for c in pipe.calls()] == ["s_a", "s_b"], (
        f"第二遍只该有它自己的两条记录, 实得 "
        f"{[c['stage'] for c in pipe.calls()]} —— 记录跨 run 累积了")


async def test_reusing_the_pipe_with_different_mocks_tracks_the_latest():
    """改 mock 再跑第二遍 → 记录反映第二遍的调用与 state。"""
    dag = DAG("tp_reuse2")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 2}

    pipe = TestPipe(dag)
    pipe.mock("s_a", lambda st: {"a": 10})
    await pipe.run()
    assert pipe.calls()[-1]["state_keys"] == []

    # 第二遍: 多给一个初始 key, 下游看到的 state_keys 就该变
    pipe.initial_state = {"extra": 1}
    pipe.mock("s_b", lambda st: {"b": 20})
    await pipe.run()
    # ⚠️ mock 是**累积**的 (dict 语义): 第一遍设的 s_a 在第二遍依然生效, 所以
    # 这里期望两条记录 —— 我第一版按"再 mock 一次就换掉"写, 期望成一条, 红了。
    # 顺带记一笔: `mock()` 没有 unmock, 想让某 stage 走真实现只能新建一个 pipe。
    assert [c["stage"] for c in pipe.calls()] == ["s_a", "s_b"], (
        f"第二遍 s_a 的 mock 仍然生效, 实得 {[c['stage'] for c in pipe.calls()]}")
    # 按 stage 找记录, 不靠下标 —— 下标依赖调用序, 读断言的人得先在脑子里重跑一遍
    # 才能确认指的是谁 (我第一版写成 calls()[0], 指向的是 s_a, 于是"少了 a")
    s_b_call = next(c for c in pipe.calls() if c["stage"] == "s_b")
    assert s_b_call["state_keys"] == ["extra", "a"], (
        f"s_b 应看到 extra 与 s_a 的产出 a, 实得 {s_b_call['state_keys']}")


async def test_snapshot_before_second_run_if_you_want_cross_run_history():
    """反向约束/用法说明: 跨轮次累计要自己取快照 —— `calls()` 返回的是拷贝。

    同时钉住"返回的是拷贝不是内部列表": 直接 `pipe.calls().append(...)` 不该
    影响后续记录 (否则测试之间会互相污染)。
    """
    dag = DAG("tp_snapshot")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    pipe = TestPipe(dag)
    pipe.mock("s_a", lambda st: {"a": 1})
    await pipe.run()
    snap = pipe.calls()

    snap.append({"stage": "伪造", "mock": True, "state_keys": []})
    assert len(pipe.calls()) == 1, "calls() 必须返回拷贝, 否则调用方能改坏内部状态"

    await pipe.run()
    assert [c["stage"] for c in pipe.calls()] == ["s_a"], (
        "跨 run 累计请自己留着快照 —— calls() 只给最近一次")
    assert len(snap) == 2, "快照本身不受影响 (它就是调用方自己的列表)"


# ── replay_from 的环图断言: docstring 说了, 但一直没人验过 ────────────────
#
# `replay_from` 的 docstring 写着"多数场景下终态仍与 cp.state 相等 (因为终值本身
# 就是终点), 但轮次敏感的 stage (计数/累加) 会算错"。前半句是一个**未经实测的
# 承诺** —— 而它正是用户照着写 `assert result.state == cp.state` 的依据。
# 后半句也要验, 否则那句警告可能是在防一个不存在的问题。

async def test_loop_replay_final_state_matches_cp_state():
    """★ 累加型 (轮次敏感) 的环图上, replay 的**终态**仍与 cp.state 相等。

    形状上 s_work 同时产出 `n` (计数) 和 `acc` (累加), 两者对轮次都敏感。replay
    把每轮压成"返回最后一次 visit 的 delta", 所以过程一定不同 —— 这里钉的是
    **终态相等**这个 docstring 承诺。
    """
    def _dag(cap: int) -> DAG:
        dag = DAG("tp_loop_replay")

        @dag.stage()
        async def s_start(ctx):
            return {}

        @dag.stage(depends_on=["s_start"])
        async def s_work(ctx):
            n = ctx.state.get("n", 0) + 1
            return {"n": n, "acc": ctx.state.get("acc", 0) + n}

        @dag.stage(depends_on=["s_work"])
        async def s_router(ctx):
            return {}

        dag.add_conditional_edges(
            "s_router", lambda s: "more" if s.get("n", 0) < cap else "stop",
            {"more": "s_work", "stop": "s_end"}, max_visits=12)

        @dag.stage()
        async def s_end(ctx):
            return {"rounds": ctx.state["n"]}

        return dag

    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    real = await Runtime(checkpoint_store=store).run(_dag(4), "TP")
    cp = store.load_latest("TP")

    replayed = await TestPipe.replay_from(cp, _dag(4)).run()

    assert real.state == cp.state, "前置条件: 活跑与 cp 应一致"
    assert replayed.state == cp.state, (
        f"docstring 承诺环图 replay 的终态与 cp.state 相等, 实得 "
        f"replay={replayed.state} vs cp={cp.state}")
    assert replayed.state["rounds"] == 4
    assert replayed.state["acc"] == 10, f"累加值应被保留, 实得 {replayed.state}"


# ── calls() 在 mock 抛异常时是**空的** — 而那正是你要看它的时刻 ─────────────
#
# `TestPipe.calls()` 的 docstring 说它"用于断言调用顺序与次数", 且
# "仅 mock 过的 stage 会记进这里"。但 `_wrapped` 的记录写在 `out = _fn(ctx.state)`
# **之后**: mock 一抛异常, append 那行根本走不到。
#
# 后果不是数字不准, 是**调试夹具在需要它的时候瞎掉**:
#   · "我的 mock 到底被调了几次?" —— 抛异常时恒答 0 次, 哪怕真的调了 1 次
#   · 配合 `retries`: 一个总失败的 mock 被重试 N 次, calls() 仍是一条都没有
#   · stage 有 mock 但 calls() 里查无此 stage, 第一反应是"它根本没被调用" ——
#     于是去查 DAG 的调度, 而真正的原因在 mock 里
#
# 而且失败路径不止抛异常: mock 返回非 dict 时抛的 TypeError 也在同一点之后,
# 同样不记。


async def test_calls_records_the_attempt_even_when_the_mock_raises():
    """★ mock 抛异常也要留下"被调用过"的记录。"""
    dag = DAG("TPboom")

    @dag.stage(retries=2)          # 重试 2 次 → 共 3 次尝试
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 2}

    # ★ 必须抛 RetryableError 而不是 ValueError: 引擎只重试 RetryableError /
    # TimeoutError (其余走 FatalError 不重试), 抛 ValueError 的话 attempts=1,
    # "记录了几次" 这件事就没得验了。这本身也是一条该记住的引擎契约。
    def boom(ctx):
        raise RetryableError("mock 炸了")

    pipe = TestPipe(dag).mock("s_a", boom)
    r = await pipe.run()
    assert r.status == "failed", f"前置: mock 恒失败 ⇒ run 应 failed, 实得 {r.status}"

    calls = pipe.calls()
    assert calls, (
        "★ mock 抛异常时 calls() 是空的 —— 而 calls() 的用途正是断言调用了几次。"
        f"此刻它恒答 0 次, 会把排查方向误导到 DAG 调度上 (retries={dag.stages['s_a'].retries})")
    assert [c["stage"] for c in calls] == ["s_a"] * 3, (
        f"被 mock 的 stage 应留下每次尝试的记录 (retries=2 → 3 次), 实得 {calls}")
    assert all(c["mock"] is True for c in calls)
    # state_keys 在异常路径上也应拿得到 —— 那是"它被调用时看到什么"的证据
    assert all("state_keys" in c for c in calls)


async def test_calls_records_before_returning_non_dict():
    """相邻路径: mock 返回非 dict。**这条本来就过** —— 类型检查在 append 之后。

    钉它是因为下面的修复要把 append 挪到更前面: 挪的时候别把这个"本来就对"的
    邻居弄坏 (两条路径的差别只在异常来自 mock 内部还是来自返回值检查)。
    """
    dag = DAG("TPbad")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    pipe = TestPipe(dag).mock("s_a", lambda ctx: ["不是 dict"])
    r = await pipe.run()
    assert r.status == "failed", f"前置: mock 返回非 dict 应 failed, 实得 {r.status}"
    assert pipe.calls(), "返回非 dict 也是一次真实调用, 必须记录"
    assert [c["stage"] for c in pipe.calls()] == ["s_a"]
