"""`done_stages` 与 `stage_deltas_visits` 必须**逐条对齐** —— 重建靠位置配对.

背景 (2026-10-16, 对抗性审查发现 + 本地独立复现): `_run_inner` 里 append delta
曾带 `if delta:` 前置, 而 `done_stages.append` 是无条件的。于是"stage 返回 `{}`
或 `None`"的那次执行**只进 done 不进 deltas**, 在两个列表之间留下一条永久错位。
`Checkpoint._rebuild_state` 按**序号位置**配对 (顺序/尾对齐都建立在这个前提上),
错位之后就会把某次 visit 的 delta 配到另一次 visit 上。

实测 (纯非 fork run 就能中招, 触发条件: 循环图 + 某次访问返回空 delta + 其间有
链式覆盖同名 key):

    活跑   {'n': 2, 'k': 'W2', 'report': 'final=W2'}
    cp     {'k': 'P',  'n': 2, 'report': 'final=W2'}      ← cp 自相矛盾
    $ pavoz state --key k      → "P"
    $ pavoz state --key report → "final=W2"               ← 同一份文件两种答案

无异常、无日志。`report` 说 k=W2 而 `k` 字段是 P —— 不存在任何一次执行能同时产出
这两个值。

本文件钉住: 任何 run/fork 之后, 每个 stage 的 done 条目数 == delta 条目数, 且
`cp.state == 活跑 state`。
"""
from __future__ import annotations

import tempfile
from pathlib import Path

from pavoz import DAG, CheckpointStore, FileStorage, Runtime


def _assert_aligned(cp, label):
    """done_stages 与 stage_deltas_visits 的对应关系 —— 重建正确性的前提.

    **两种形态, 别混**:
    - 非 fork cp: 逐条 1:1, `done 条目数 == delta 条目数`。这一条就是本文件的靶心:
      之前空 delta 只进 done 不进 deltas ⇒ 永久错位。
    - fork cp: `done_stages` 截到 keep, 而 `stage_deltas_visits` **携带全量历史**
      (fork_run 刻意如此), 所以 delta 条目数可以**多于** done 条目数 —— 边界由
      `fork_keep_counts` 标定 (哪几条属于截断点之前)。此时只要求 delta 不少于 done。
      我第一版在这里直接写等号, 结果在 fork 用例上误报 (s_router 6 done / 8 delta),
      恰恰因为 fork 的历史是有意多带的 —— 判据写错会让人怀疑修复, 得不偿失。
    """
    is_fork = bool(cp.fork_keep_counts)
    for name in set(cp.done_stages):
        n_done = cp.done_stages.count(name)
        n_delta = len(cp.stage_deltas_visits.get(name) or [])
        if is_fork:
            assert n_delta >= n_done, (
                f"{label}: stage {name!r} 的 delta 条目 {n_delta} 条 < done {n_done} 条 "
                "—— fork cp 里 delta 应含全量历史, 少于 done 说明丢了对齐依据")
        else:
            assert n_done == n_delta, (
                f"{label}: stage {name!r} 的 done 条目 {n_done} 条但 delta {n_delta} 条 "
                "—— 重建按位置配对, 错位会把某次 visit 的 delta 配到另一次上")


async def _run_and_fetch(build, task_id, **kw):
    root = Path(tempfile.mkdtemp())
    store = CheckpointStore(FileStorage(root))
    r = await Runtime(checkpoint_store=store).run(build(), task_id, **kw)
    return store, r


def _loop_with_empty_delta(name: str):
    """循环图: s_work 第 1 轮返回空 delta, 第 2 轮返回真值; s_router 轮间链式覆盖 k."""
    counters = {"w": 0, "r": 0}

    def build():
        dag = DAG(name)

        @dag.stage()
        async def s_work(ctx):
            counters["w"] += 1
            if counters["w"] == 1:
                return {}                        # 空 delta: 不该造成错位
            return {"k": f"W{counters['w']}"}

        @dag.stage(depends_on=["s_work"])
        async def s_router(ctx):
            counters["r"] += 1
            n = ctx.state.get("n", 0) + 1
            return {"n": n, "k": "P"} if counters["r"] == 1 else {"n": n}

        dag.add_conditional_edges(
            "s_router", lambda s: "more" if s["n"] < 2 else "stop",
            {"more": "s_work", "stop": "s_final"}, max_visits=6)

        @dag.stage()
        async def s_final(ctx):
            return {"report": f"final={ctx.state.get('k')}"}
        return dag

    return build


async def test_empty_delta_does_not_desync_done_from_deltas():
    """★ 核心回归: 返回 {} 的那次执行也必须在 deltas 里占一条。"""
    build = _loop_with_empty_delta("lockstep_plain")
    store, r = await _run_and_fetch(build, "t1")

    cp = store.load_latest("t1")
    assert cp.done_stages.count("s_work") == 2
    assert len(cp.stage_deltas_visits["s_work"]) == 2, (
        f"修复前这里只有 1 条: {cp.stage_deltas_visits['s_work']}")
    _assert_aligned(cp, "非 fork run")


async def test_checkpoint_state_matches_live_run_despite_empty_delta():
    """cp 不能自相矛盾: report 与 k 必须指向同一个值。"""
    build = _loop_with_empty_delta("lockstep_plain2")
    store, r = await _run_and_fetch(build, "t2")

    cp = store.load_latest("t2")
    assert cp.state == r.state, (
        f"cp.state 与活跑不一致\n  活跑 = {r.state}\n  cp   = {cp.state}")
    assert cp.state["k"] == "W2", (
        f"最后一次真执行说了算, 不该被 earlier visit 的 delta 覆盖: {cp.state}")


async def test_empty_delta_after_fork_truncation_leaves_no_ghost_key():
    """fork 路径: 截断点之后若有访问返回空 delta, 重建不得**复活**截断点前的
    历史 delta (幽灵 key) —— 尾对齐同样建立在"逐条对齐"这个前提上。"""
    def build():
        dag = DAG("lockstep_fork")

        @dag.stage()
        async def s_work(ctx):
            # 第 1 轮真值、第 2 轮真值 (留下两条历史), fork 后这次返回空
            n = ctx.state.get("n", 0) + 1
            if ctx.state.get("blank"):
                return {}
            return {"n": n, "k": f"W{n}"}

        @dag.stage(depends_on=["s_work"])
        async def s_router(ctx):
            return {}

        # route 用 .get: fork 后第一轮返回空 delta, n 还不存在 —— 写 s["n"] 会 KeyError
        dag.add_conditional_edges(
            "s_router", lambda s: "more" if s.get("n", 0) < 2 else "stop",
            {"more": "s_work", "stop": "s_final"}, max_visits=6)

        @dag.stage()
        async def s_final(ctx):
            return {"rounds": ctx.state.get("n", 0),
                    "saw_k": ctx.state.get("k")}
        return dag

    root = Path(tempfile.mkdtemp())
    store = CheckpointStore(FileStorage(root))
    rt = Runtime(checkpoint_store=store)
    assert (await rt.run(build(), "t3")).status == "done"   # 造历史

    r2 = await rt.fork_run(build(), "t3", from_stage="s_work",
                           overrides={"blank": True})
    cp = store.load_latest("t3")

    _assert_aligned(cp, "fork")
    assert cp.state == r2.state, (
        f"fork 后 cp 与活跑不一致\n  活跑 = {r2.state}\n  cp   = {cp.state}")
    assert "k" not in cp.state or r2.state.get("k") == cp.state.get("k"), (
        f"幽灵 key: cp 里出现了本次 run 没产生过的 {cp.state.get('k')!r}")


async def test_stage_returning_empty_dict_leaves_state_untouched():
    """反向约束: 空 delta 本来就不该改 state, 修复也不能让它凭空改。"""
    dag = DAG("empty_is_noop")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_nothing(ctx):
        return {}

    @dag.stage(depends_on=["s_nothing"])
    async def s_c(ctx):
        return {"seen": sorted(ctx.state.keys())}

    store, r = await _run_and_fetch(lambda: dag, "t4")
    assert r.status == "done"
    assert r.state == {"a": 1, "seen": ["a"]}, f"空 delta 不该新增 key: {r.state}"
    cp = store.load_latest("t4")
    _assert_aligned(cp, "空 delta 是 noop")
    assert cp.state == r.state


async def test_stage_returning_none_is_treated_as_empty():
    """`return None` 与 `return {}` 同义 (_run_stage 归一成 {}) —— 也必须占一条。"""
    dag = DAG("none_delta")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_none(ctx):
        return None

    store, r = await _run_and_fetch(lambda: dag, "t5")
    assert r.status == "done"
    cp = store.load_latest("t5")
    _assert_aligned(cp, "None delta")
    assert cp.state == r.state
