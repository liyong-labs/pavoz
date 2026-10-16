"""`diff_runs` 必须让人看见"循环图上多跑了多少轮"。

背景 (2026-10-16 巡检): diff_runs 的 `output_diff` / `state_diff` 用的都是
`stage_deltas` —— **last-write 视图** (每个 stage 只取最后一次执行的 delta)。
在循环图 (R1 条件边) 上这既漏掉中间轮次, 又会把差异报偏。实测:

    run A: s_work 跑 3 轮;  run B: s_work 跑 5 轮
    两 run 的**终态逐字节相同** (收尾 stage 把临时计数归零)
    → state_diff      = {}                        ← 看不见 3 轮 vs 5 轮
    → output_diff     = {"scratch": [3, 5]}       ← 只比出"最后一次 visit 的
                                                    差在哪", 而终态其实相同

也就是说: 消费者拿到的信号方向是反的 —— 该报的"多干了 67% 的活"没报, 不该当
差异的"最后一次 delta"反倒成了唯一的差异线索。现补 `visits: [a, b]` 轮次字段。

本测试钉住: 轮次差异可见 + 非循环场景不受影响 + 单侧 stage 记 0 + 逐轮全貌仍
在 `cp.stage_deltas_visits` (output_diff 不该被改成逐轮比, 那是另一套语义)。
"""
from __future__ import annotations

import tempfile

from pavoz import DAG, CheckpointStore, FileStorage, Runtime, diff_runs


def _looping_dag(limit: dict) -> DAG:
    dag = DAG("diff_visits")

    @dag.stage()
    async def s_work(ctx):
        n = ctx.state.get("scratch", 0) + 1
        return {"score": 100, "scratch": n}

    @dag.stage(depends_on=["s_work"])
    async def s_router(ctx):
        return {}

    dag.add_conditional_edges(
        "s_router", lambda s: "more" if s["scratch"] < limit["n"] else "stop",
        {"more": "s_work", "stop": "s_final"}, max_visits=8)

    @dag.stage()
    async def s_final(ctx):
        # 归零临时计数 → 两个 run 的终态逐字节相同
        return {"score": 100, "scratch": 0}

    return dag


async def test_visits_expose_the_round_count_difference():
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    limit = {"n": 3}

    await Runtime(checkpoint_store=store).run(_looping_dag(limit), "t1")
    limit["n"] = 5                                   # B 多跑 2 轮
    await Runtime(checkpoint_store=store).run(_looping_dag(limit), "t1")

    runs = store.list_runs("t1")
    assert len(runs) == 2
    cp_x, cp_y = store.load("t1", runs[0]), store.load("t1", runs[1])
    d = diff_runs(cp_x, cp_y)

    # list_runs 的顺序不是创建序 (它按 key 排), 故断言用集合形式 —— 要证明的是
    # "轮次差异可见", 不是 "哪边是 A"
    assert sorted(d["stages"]["s_work"]["visits"]) == [3, 5], (
        "循环 stage 的执行轮次必须可见 —— 这是 state_diff/output_diff 都表达不出的量")
    assert cp_x.state == cp_y.state, "前提: 两个 run 终态相同, 差异只在轮数上"
    assert d["state_diff"] == {}, "前提: 终态相同 ⇒ state_diff 本来就是空的"


async def test_visits_are_one_for_non_looping_stages():
    """反向约束: 非循环 stage 记 [1, 1], 不引入噪音。"""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))

    def _plain() -> DAG:
        dag = DAG("diff_plain")

        @dag.stage()
        async def s_a(ctx):
            return {"a": 1}

        @dag.stage(depends_on=["s_a"])
        async def s_b(ctx):
            return {"b": 1}

        return dag

    await Runtime(checkpoint_store=store).run(_plain(), "t2")
    await Runtime(checkpoint_store=store).run(_plain(), "t2")
    runs = store.list_runs("t2")
    d = diff_runs(store.load("t2", runs[0]), store.load("t2", runs[1]))

    assert d["stages"]["s_a"]["visits"] == [1, 1]
    assert d["stages"]["s_b"]["visits"] == [1, 1]


async def test_visits_zero_on_the_side_that_never_ran_the_stage():
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))

    def _two() -> DAG:
        dag = DAG("diff_two")

        @dag.stage()
        async def s_a(ctx):
            return {"a": 1}

        @dag.stage(depends_on=["s_a"])
        async def s_b(ctx):
            return {"b": 1}

        return dag

    r = await Runtime(checkpoint_store=store).run(_two(), "t3")
    runs = store.list_runs("t3")
    # 构造一个只完成 s_a 的 checkpoint
    cp_a = store.load("t3", runs[0])
    cp_b = store.load("t3", r.run_id)
    d = diff_runs(cp_a, cp_b)

    assert d["stages"]["s_b"]["visits"] == [1, 1], "同一 run 的两次比较轮次相同"
    assert set(d["stages"]) >= {"s_a", "s_b"}


async def test_full_visit_history_remains_available_on_the_checkpoint():
    """逐轮全貌仍在 cp.stage_deltas_visits —— output_diff 有意保持 last-write 语义
    (逐轮比较是另一套语义, 不在这里改)。"""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    limit = {"n": 3}
    await Runtime(checkpoint_store=store).run(_looping_dag(limit), "t4")
    runs = store.list_runs("t4")
    cp = store.load("t4", runs[0])

    assert [d["scratch"] for d in cp.stage_deltas_visits["s_work"]] == [1, 2, 3]
    assert cp.stage_deltas["s_work"]["scratch"] == 3, "last-write 视图只留最后一次"


async def test_visits_is_additive_and_old_keys_survive():
    """visits 是**新增**键 —— 原有键一个都不能少 (消费者按 key 取值, 少一个就
    KeyError)。"""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    limit = {"n": 2}
    await Runtime(checkpoint_store=store).run(_looping_dag(limit), "t5")
    await Runtime(checkpoint_store=store).run(_looping_dag(limit), "t5")
    runs = store.list_runs("t5")
    d = diff_runs(store.load("t5", runs[0]), store.load("t5", runs[1]))

    assert {"run_id_a", "run_id_b", "dag_name", "workflow_hash_changed",
            "stages", "state_diff"} <= set(d), f"顶层键缺失: {sorted(d)}"
    for name, entry in d["stages"].items():
        assert {"status", "input_hash_changed", "output_diff", "visits"} <= set(entry), (
            f"stage {name} 的键缺失: {sorted(entry)}")
