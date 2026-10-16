"""不变量: 每一步之后, `cp.state` 必须等于**活跑返回的** state.

`cp.state` 是从 `initial_state + stage_deltas_visits + fork_overrides` 重建的
(id=3241 那类数据完整性问题的现场), 而 Runtime 走的是另一条路 (运行时直接把值
给 stage)。两条路必须给出同一个答案 —— 否则"fork_run 当场返回的是对的, 但此后
任何读该 cp 的路径 (resume / load / diff_runs / replay_from) 拿到的是错的"。

单一用例只钉一种形状不够: e8cec96 修的是重建逻辑里最精细的一段, 它自己就是
最该被怀疑的代码。本文件把不变式铺到多种形状上 —— 线性链连续 fork / 从第一个
stage fork / fork + skip_unchanged / **循环图**(条件边) / fork 后 run_stage 重放,
每步都比对一次。

形状 4 是本文件发现的第二个缺陷的现场 (见 test_fork_override_lost_when_rerun_
stage_overwrites_it): override 打在**会被 from_stage 自己改写**的 key 上。
"""
from __future__ import annotations

import tempfile

import pytest

from pavoz import DAG, CheckpointStore, FileStorage, Runtime

FAILS: list[str] = []


def _assert_consistent(label, cp, live_state, *, note=""):
    if cp.state != live_state:
        FAILS.append(label)
        pytest.fail(f"{label}: cp.state 与活跑结果不一致\n"
                    f"       cp.state   = {cp.state}\n"
                    f"       活跑 state = {live_state}"
                    + (f"\n       ({note})" if note else ""))


# ── 形状 1: 线性链 ────────────────────────────────────────────────────────

def _linear_dag(name: str = "lin") -> DAG:
    dag = DAG(name)

    @dag.stage()
    async def s_a(ctx):
        return {"a": "A"}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": "B"}

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        tag = ctx.state.get("tag", "none")
        return {"c": f"C[{tag}]", "tag": tag}

    @dag.stage(depends_on=["s_c"])
    async def s_d(ctx):
        return {"d": ctx.state["c"]}

    return dag


async def test_linear_chain_repeated_forks_stay_consistent():
    """连续三次 fork 同一 from_stage (id=3241 的原始形状, 加长版)."""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)

    r = await rt.run(_linear_dag("lin1"), "L1")
    _assert_consistent("首跑", store.load_latest("L1"), r.state)

    for tag in ("F1", "F2", "F3"):
        r = await rt.fork_run(_linear_dag("lin1"), "L1", from_stage="s_c",
                              overrides={"tag": tag})
        _assert_consistent(f"fork → {tag}", store.load_latest("L1"), r.state,
                           note=f"期望 c=C[{tag}]")


async def test_fork_from_first_stage_with_untouched_stage_stays_consistent():
    """从**第一个** stage fork (keep 为空), 且该 stage 会改写 override 的 key。

    注意 s_a 永远返回字面量 'A' —— 所以 override 打到 s_a 自己产出的 key 上时,
    runtime 的语义是"注入后 s_a 重跑并覆盖回去", 而 cp 重建必须复现同一个结果。
    """
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    await rt.run(_linear_dag("lin2"), "L2")

    r = await rt.fork_run(_linear_dag("lin2"), "L2", from_stage="s_a",
                          overrides={"a": "A9"})
    _assert_consistent("fork from s_a", store.load_latest("L2"), r.state,
                       note="keep 为空, 且 override 的 key 会被 from_stage 改写")


async def test_fork_with_skip_unchanged_stays_consistent():
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    await rt.run(_linear_dag("lin3"), "L3")

    r = await rt.fork_run(_linear_dag("lin3"), "L3", from_stage="s_b",
                          skip_unchanged=True)
    _assert_consistent("fork + skip_unchanged", store.load_latest("L3"), r.state)


# ── 形状 2: 循环图 (条件边) ───────────────────────────────────────────────

def _loop_dag(name: str = "lp") -> DAG:
    dag = DAG(name)

    @dag.stage()
    async def s_work(ctx):
        n = ctx.state.get("n", 0) + 1
        return {"n": n, "seen": ctx.state.get("seen", []) + [n]}

    @dag.stage(depends_on=["s_work"])
    async def s_router(ctx):
        return {}

    dag.add_conditional_edges(
        "s_router", lambda s: "more" if s["n"] < s.get("limit", 3) else "stop",
        {"more": "s_work", "stop": "s_final"}, max_visits=8)

    @dag.stage()
    async def s_final(ctx):
        return {"rounds": ctx.state["n"]}

    return dag


@pytest.mark.parametrize("limit", [2, 4, 5])
async def test_loop_graph_fork_stays_consistent(limit):
    """循环图上 fork from 环内 stage, 每次改轮数 —— visits 有多条, 对齐最容易错."""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    r = await rt.run(_loop_dag("lp1"), "LP")
    _assert_consistent("循环图首跑", store.load_latest("LP"), r.state)

    r = await rt.fork_run(_loop_dag("lp1"), "LP", from_stage="s_work",
                          overrides={"limit": limit})
    _assert_consistent(f"循环 fork → {limit} 轮", store.load_latest("LP"), r.state,
                       note=f"期望 rounds={limit}, 实得 {r.state['rounds']}")


# ── 形状 3: fork 之后用 run_stage 重放 (第三条读同一 cp 的路径) ─────────────

async def test_run_stage_after_fork_reads_a_consistent_checkpoint():
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    await rt.run(_linear_dag("lin4"), "L4")
    r = await rt.fork_run(_linear_dag("lin4"), "L4", from_stage="s_c",
                          overrides={"tag": "F7"})
    _assert_consistent("fork → F7", store.load_latest("L4"), r.state)

    rep = await rt.run_stage(_linear_dag("lin4"), "L4", "s_c")
    assert rep.status == "done"
    # 重放用 checkpoint 重建的"执行前 state", 与 cp 自身的 state 必须同源
    assert rep.state["c"] == r.state["c"], (
        f"run_stage 重放的基线与 cp 不一致: {rep.state['c']} vs {r.state['c']}")


# ── 形状 4: override 打在会被 from_stage 改写的 key 上 ──────────────────────

async def test_fork_override_lost_when_rerun_stage_overwrites_it():
    """★ 本文件发现的第二个缺陷 (fix 见 runtime/checkpoint 提交).

    fork_overrides 在 **runtime** 里是"从 from_stage 开始注入输入", 于是 from_stage
    重跑后可以覆盖它; 而 cp 重建里 overrides 是**最后** merge 的, 会盖掉 from_stage
    刚跑出来的值。同一把钥匙开两把锁: runtime 说 a='A', cp 说 a='A9'。
    """
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    await rt.run(_linear_dag("lin5"), "L5")

    r = await rt.fork_run(_linear_dag("lin5"), "L5", from_stage="s_a",
                          overrides={"a": "A9"})

    # runtime 侧: s_a 重跑并覆盖了注入值 —— 这是 fork 的既定语义
    assert r.state["a"] == "A", f"活跑的 s_a 应覆盖注入值, 实得 {r.state['a']}"

    cp = store.load_latest("L5")
    assert cp.state["a"] == r.state["a"], (
        f"cp 重建把 fork_overrides 又盖回去了: cp={cp.state['a']} vs 活跑={r.state['a']}"
        " —— override 是**注入输入**, 该被 from_stage 的重跑结果覆盖")


async def test_fork_override_survives_when_no_rerun_stage_touches_it():
    """反向约束: override 打在**没人改写**的 key 上时, 必须保留 (别顺手改坏)."""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    await rt.run(_linear_dag("lin6"), "L6")

    # tag 由 s_c **读**但 s_c 自己不产出 tag; override 打在 tag 上, s_c 会消费它
    r = await rt.fork_run(_linear_dag("lin6"), "L6", from_stage="s_c",
                          overrides={"tag": "KEEP"})
    assert r.state["tag"] == "KEEP", "s_c 不产出 tag, override 必须留着"
    _assert_consistent("override 无人改写", store.load_latest("L6"), r.state)
