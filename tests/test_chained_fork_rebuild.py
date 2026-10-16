"""链式 fork 后 checkpoint 重建必须取最近一次 visit (已修复, 本用例即回归防线).

来源 (ai@home id=3241, 2026-10-15 报告): 链式两次 fork 后, 活跑的值正确, 落盘
cp 重建出来的却是**最老**那次:

    run1 : C[none]   fork1: C[F1]   fork2: C[F2]     ← 活跑的值正确
    cp 中 s_c delta 序列: [0] C[none] [1] C[F1] [2] C[F2]
    cp.state 重建      : C[none]                        ← 取了最老那次

即**内存结果与落盘 checkpoint 不一致**: fork_run 当场返回 C[F2] (对), 但此后任何
读该 cp 的路径 (resume / load / diff_runs / TestPipe.replay_from) 拿到的都是 fork
**之前**的值。

根因两层 (2026-10-16 本地修复, 此前等 ai@home 推分支未到):
1. `_save_cp` 没有 `fork_keep_counts` 参数 → fork 续跑的每轮 save 都把它清空成 {}
2. 更根本: `fork_keep_counts` 的语义是"该 stage 的 done 条目里, 有几条来自 fork
   截断点**之前**", 而 `fork_run` 过去只给 keep 里的 stage 记条目 → from_stage
   及其下游一条都没有 → 重建时 `kc is None` 被当成"顺序索引" → 对截断点后的重跑集
   取 `deltas[0]` (最老那条)。重建逻辑现在区分"fork cp"与"缺条目 = 全部重跑 (kc=0)"。

本用例曾标 `xfail(strict=False)` 作为验收信号, 修复落地后 **XPASS** —— 该信号已经
触发, marker 已摘, 用例转为永久回归防线。姊妹用例 `test_fork_from_first_stage_*`
覆盖"从第一个 stage fork"(keep 为空)这个分支 —— 过去那里连"这是 fork cp"都识别
不出来 (kcounts 为空)。
"""

from __future__ import annotations

import tempfile

from pavoz import DAG, CheckpointStore, FileStorage, Runtime


def _chain_dag() -> DAG:
    dag = DAG("chain_fork_rebuild")

    @dag.stage()
    async def s_a(ctx):
        return {"a": "A"}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": "B"}

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        tag = ctx.state.get("tag", "none")      # overrides 注入 → 每次 fork 值可区分
        return {"c": f"C[{tag}]", "tag": tag}

    @dag.stage(depends_on=["s_c"])
    async def s_d(ctx):
        return {"d": ctx.state["c"]}

    return dag


async def test_chained_fork_checkpoint_rebuild_keeps_latest_visit():
    """两次 fork from s_c 后, 落盘 cp 的 state 必须是最近一次的 c。

    这条是 id=3241 的回归防线: 修复前它取的是 C[none] (最老那次)。
    """
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    dag = _chain_dag()

    r1 = await rt.run(dag, task_id="t1")
    assert r1.state["c"] == "C[none]"

    await rt.fork_run(dag, "t1", from_stage="s_c", overrides={"tag": "F1"})
    r3 = await rt.fork_run(dag, "t1", from_stage="s_c", overrides={"tag": "F2"})
    assert r3.state["c"] == "C[F2]", "活跑的值必须是最新的 (对照组)"

    cp = store.load_latest("t1")
    # 三次执行留下三条 delta, 顺序 = 历史
    assert [d["c"] for d in cp.stage_deltas_visits["s_c"]] == ["C[none]", "C[F1]", "C[F2]"]

    # ★ 这一步修复前取最老那条 (是本用例的判据所在)
    assert cp.state["c"] == "C[F2]"
    assert cp.state["d"] == "C[F2]"


# ── 姊妹分支: 从**第一个** stage fork (keep 为空) ─────────────────────────────
#
# 这个分支过去连"这是 fork cp"都识别不出来: `fork_keep_counts` 只给 keep 里的 stage
# 记条目, keep 为空时得到空 dict, 重建逻辑据此判定"不是 fork cp" → 走顺序索引 →
# from_stage 自己的新 visit 被跳过, 取到 fork 之前那条。现 fork_run 对**所有**带
# visits 的 stage 都记条目 (缺的记 0), 于是空 keep 也能标出"这是 fork cp"。


def _chain_dag_first_fork() -> DAG:
    """首个 stage 就能产出可区分结果的 DAG (与上面同型, from_stage 换成 s_a)."""
    dag = DAG("chain_fork_first")

    @dag.stage()
    async def s_a(ctx):
        tag = ctx.state.get("tag", "none")
        return {"a": f"A[{tag}]", "tag": tag}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": ctx.state["a"]}

    return dag


async def test_fork_from_first_stage_rebuilds_to_the_newest_visit():
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)

    r1 = await rt.run(_chain_dag_first_fork(), "f1")
    assert r1.state["a"] == "A[none]"

    r2 = await rt.fork_run(_chain_dag_first_fork(), "f1",
                           from_stage="s_a", overrides={"tag": "F1"})
    assert r2.state["a"] == "A[F1]", "活跑的值 (对照组)"

    cp = store.load_latest("f1")
    assert [d["a"] for d in cp.stage_deltas_visits["s_a"]] == ["A[none]", "A[F1]"]
    assert cp.fork_keep_counts == {"s_a": 0, "s_b": 0}, (
        "从第一个 stage fork 时 keep 为空, 但仍须记出条目 (值为 0) 来标明"
        "这是 fork cp")
    assert cp.state["a"] == "A[F1]", "落盘重建必须是最新的那次, 不是最老的"
    assert cp.state["b"] == "A[F1]"


async def test_plain_run_checkpoint_is_not_marked_as_fork():
    """反向约束: 普通 run 的 fork_keep_counts 恒空 —— 否则重建会误走尾对齐。"""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    await rt.run(_chain_dag_first_fork(), "p1")

    cp = store.load_latest("p1")
    assert cp.fork_keep_counts == {}, "普通 run 不该有任何 fork 标记"
    assert cp.state["a"] == "A[none]"


# ── 契约: 链式 fork **不继承**祖先的 overrides (2026-10-16) ─────────────────
#
# 这条是被一次对抗性审查的误报钉下来的 —— 它把"链式 fork 后祖先 override 消失"
# 报成缺陷。实测: 现象是真的, 但直觉修法 (新 cp 合并 cp.fork_overrides) 会
# **制造**一个新的不一致, 所以现有实现是对的, 缺的是契约本身。
#
# 反对合并的实测形状: 祖先 override 打在会被 from_stage 覆盖的 key 上, 而新 cp
# 的 keep 段**不重跑** —— 那些 stage 的产出已定死, 注入值没人覆盖得回来:

#     fork#1 from s_a, a=A9  →  s_a 重跑覆盖回 'A'   (overrides 是注入输入)
#     fork#2 from s_c        →  活跑 a='A'  (keep=[s_a,s_b], s_a 不重跑)
#     若合并祖先 overrides     →  cp.state a='A9'   ← 与活跑分叉

def _overwritten_override_dag() -> DAG:
    dag = DAG("chain_override_contract")

    @dag.stage()
    async def s_a(ctx):
        return {"a": "A"}  # 字面量: 永远覆盖回注入值, 制造"注入≠最终值"

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": "B"}

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        return {"c": "C"}

    return dag


async def test_chained_fork_does_not_carry_ancestor_overrides():
    """祖先 override 不被继承, 且**落盘与活跑一致** (两个断言缺一不可).

    只断言"不继承"会被一个把 ancestor 塞进 initial_state 的实现骗过; 只断言
    "一致"则分不清是不继承带来的还是恰好两边都错。故这里把"现象"与"不变式"
    分别钉住。
    """
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    await rt.run(_overwritten_override_dag(), "co")

    r1 = await rt.fork_run(_overwritten_override_dag(), "co",
                           from_stage="s_a", overrides={"a": "A9"})
    assert r1.state["a"] == "A", f"活跑的 s_a 应覆盖注入值, 实得 {r1.state['a']}"
    assert store.load_latest("co").fork_overrides == {"a": "A9"}

    r2 = await rt.fork_run(_overwritten_override_dag(), "co",
                           from_stage="s_c", overrides={"extra": "X"})
    cp = store.load_latest("co")

    assert cp.fork_overrides == {"extra": "X"}, (
        "新 cp 只带本次的 overrides —— 祖先那次是**注入输入**, 已被消费; "
        f"带上它会让重建声称一次本 run 没发生的注入, 实得 {cp.fork_overrides}")
    assert cp.state == r2.state, (
        f"落盘与活跑必须一致: cp={cp.state} vs 活跑={r2.state}")
    assert cp.state["a"] == "A", (
        f"keep 段的 s_a 不重跑, 它的产出 'A' 不该被祖先注入值盖成 'A9'; "
        f"实得 {cp.state['a']!r}")


async def test_carrying_ancestor_overrides_would_corrupt_kept_output():
    """★ 把"合并祖先"这个改法**自己**证伪 —— 用数据, 不用论证。

    ⚠️ 这里踩过一个坑, 记下来免得重走: 我最初以为合并祖先会破坏
    **`cp.state == 活跑`** 这个一致性不变式。手工造了个"合并后的 cp"跟活跑
    比, 确实分叉了 —— **但那个结论是假的**: 手工造的 cp 绕过了 runtime 侧,
    而真把代码改成合并之后, runtime 与重建**同源**, 两边会一起变成 'A9',
    依然一致 (变异实测: 一致性不变式两处都绿)。

    被真正破坏的是另一件事: **keep 段的产出是既成事实**。s_a 明明跑出 'A',
    合并后它在 state 里成了 'A9' —— 一个本次 run 里从未发生过的注入, 盖掉了已经
    定死的真实产出。后果是**结果依赖历史链**: 同一份 (dag, from_stage, overrides),
    fork 之前的历史不同, 结果就不同。

    所以判据钉在"可复现性"这个行为上, 而不是钉在某一种实现。
    """
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    await rt.run(_overwritten_override_dag(), "co3")
    await rt.fork_run(_overwritten_override_dag(), "co3",
                      from_stage="s_a", overrides={"a": "A9"})
    r_dirty = await rt.fork_run(_overwritten_override_dag(), "co3",
                                from_stage="s_c", overrides={"extra": "X"})

    store2 = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt2 = Runtime(checkpoint_store=store2)
    await rt2.run(_overwritten_override_dag(), "co4")   # 同样起点, 但不先 fork
    r_clean = await rt2.fork_run(_overwritten_override_dag(), "co4",
                                 from_stage="s_c", overrides={"extra": "X"})

    assert r_dirty.state == r_clean.state, (
        "同一次 fork (同 dag / from_stage / overrides) 的结果**不该**取决于之前 "
        f"fork 过什么: 有历史={r_dirty.state} vs 无历史={r_clean.state} —— "
        "合并祖先 overrides 会让'改一个参数'意外改到无关的 key")
    assert r_dirty.state["a"] == "A", (
        f"s_a 在 keep 段不重跑, 它的产出 'A' 是既成事实, 实得 {r_dirty.state['a']!r}")
