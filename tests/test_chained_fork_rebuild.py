"""链式 fork 后 checkpoint 重建取到最老 visit 而非最近一次 —— 已知 BUG (xfail).

来源 (ai@home id=3241, 2026-10-15 报告): 他们的两条修复 (0c1be01 fork_keep_counts
被 _save_cp 清空 / 9d9d8de visits 重基) 压在本地 4 天未推, 报告称在 5a3bc56 上
cherry-pick 无冲突、354 passed。我们**独立复现确认属实**, 且严重性比报告更高:

    run1 : C[none]   fork1: C[F1]   fork2: C[F2]     ← 活跑的值正确
    cp 中 s_c delta 序列: [0] C[none] [1] C[F1] [2] C[F2]
    cp.state 重建      : C[none]                        ← 取了最老那次

即**内存结果与落盘 checkpoint 不一致**: fork_run 当场返回 C[F2] (对), 但此后任何
读该 cp 的路径 (resume / load / diff_runs / TestPipe.replay_from) 拿到的都是 fork
**之前**的值。根因: fork 造 cp 时写了 fork_keep_counts, 但 _save_cp 根本没有这个
参数 → 每完成一个 stage 就被清空; 重建时 kcounts={} 退化成"按 visit 顺序索引",
对"截断点后重跑集"取到 deltas[0] 而非最后一条。

**本用例现在标 xfail**: 复现已确认, 但修复尚未进本仓 (等 ai@home 推分支)。
非 strict —— 修复落地后它会 XPASS 变成真通过, 那正是我们要的信号。
届时把 marker 去掉即可 (届时本用例即成为回归防线)。
"""
from __future__ import annotations

import tempfile

import pytest

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


@pytest.mark.xfail(
    strict=False,
    reason="已知 BUG (ai@home id=3241): _save_cp 不透传 fork_keep_counts → 重建按 "
           "visit 顺序索引, 对截断点后重跑集取最老 delta. 修复待合入, 届时本用例 XPASS",
)
async def test_chained_fork_checkpoint_rebuild_keeps_latest_visit():
    """两次 fork from s_c 后, 落盘 cp 的 state 必须是最近一次的 c。"""
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

    # ★ 这一步当前失败: 重建取最老那条
    assert cp.state["c"] == "C[F2]"
    assert cp.state["d"] == "C[F2]"
