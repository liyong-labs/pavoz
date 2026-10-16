"""`max_visits` 熔断后的出路 —— 文档写了, 代码就得钉住, 否则那只是愿望。

熔断后有三条路走不通, 只有一条走得通:

| 做法 | 结果 |
|------|------|
| `resume=True` | **一个 stage 都没跑**就再次 `MaxVisitsExceeded` (计数跨 resume 累计) |
| 调大 `max_visits` 再 `resume` | `CheckpointMismatchError` (max_visits 入 `workflow_hash`) |
| 调大 `max_visits` 再 `fork_run` | 同上 `CheckpointMismatchError` |
| **`resume=False`** (修好退出条件后) | ✅ 正常跑完 |

⚠️ 我第一版把这四条路试到第三条就下了"死锁"的结论, 差点去改 `workflow_hash` 让
`max_visits` 不入指纹 —— 那会让**全体旧 checkpoint 再次失效** (继 dag 名那次之后
又一次), 而且解决的是一个并不存在的问题。教训记在这儿: **穷举出路再下"死锁"的
判断**, 尤其是那种"看起来无解"的结论。

`reset_pending` 在熔断后确实会滞留 (`cp.reset_pending = [router]`), 但那是**正确**
状态 —— 那个 router 本来就待重跑, 而它滞留与否不影响出路 (`resume=False` 不读旧
cp)。所以滞留不是缺陷, 无须修。
"""

from __future__ import annotations

import pytest

from pavoz import DAG, CheckpointStore, FileStorage, Runtime


def _loop_dag(thresh_cap: int, max_visits: int, name: str = "mv") -> DAG:
    """router 在 p < cap 时回跳, 否则停。cap=99 + max_visits 小 ⇒ 必然熔断."""
    dag = DAG(name)

    @dag.stage()
    async def s_start(ctx):
        return {}

    @dag.stage(depends_on=["s_start"])
    async def s_p(ctx):
        return {"p": ctx.state.get("p", 0) + 1}

    @dag.stage(depends_on=["s_p"])
    async def s_router(ctx):
        return {}

    dag.add_conditional_edges(
        "s_router",
        lambda s: "again" if s.get("p", 0) < thresh_cap else "stop",
        {"again": "s_p", "stop": "s_end"},
        max_visits=max_visits,
    )

    @dag.stage()
    async def s_end(ctx):
        return {"out": "done"}

    return dag


async def test_exhausted_budget_fails_the_run(tmp_path):
    """前置条件: 退出条件永远不满足 ⇒ router 跑满 max_visits ⇒ 熔断。"""
    store = CheckpointStore(FileStorage(tmp_path / "cp"))
    r = await Runtime(checkpoint_store=store).run(_loop_dag(99, 3), "mv")

    assert r.status == "failed"
    assert r.error_class == "MaxVisitsExceeded"
    cp = store.load_latest("mv")
    assert len(cp.stage_deltas_visits.get("s_router", [])) >= 3, "router 确实跑满了预算"


async def test_resume_after_exhaustion_runs_nothing(tmp_path):
    """★ 熔断后 `resume` 一个 stage 都不跑 —— 这条是"文档写的死路"的行为钉子。

    不钉的话, 哪天有人把计数改成"每次 resume 归零", 这个测试会红, 从而把那次
    行为变更暴露出来 (那是可以讨论的改动, 但必须是有意识的)。
    """
    store = CheckpointStore(FileStorage(tmp_path / "cp"))
    await Runtime(checkpoint_store=store).run(_loop_dag(99, 3), "mv")
    before = store.load_latest("mv").done_stages

    r = await Runtime(checkpoint_store=store).run(_loop_dag(99, 3), "mv", resume=True)

    assert r.status == "failed"
    assert r.error_class == "MaxVisitsExceeded", (
        f"计数跨 resume 累计 ⇒ 预算已满, 实得 {r.error_class}")
    after = store.load_latest("mv").done_stages
    assert after == before, (
        f"一个 stage 都不该跑, 完成序却变了: {len(before)} → {len(after)}")


@pytest.mark.parametrize("entry", ["resume", "fork_run"])
async def test_raising_max_visits_is_refused_both_ways(tmp_path, entry):
    """★ 调大 `max_visits` 的两条路都撞指纹 —— 这正是"死锁"错觉的来源。

    也说明为什么解法不该是"把 max_visits 移出 workflow_hash": 那要动指纹算法,
    会让全体旧 checkpoint **再次**失效 (继 dag 名那次之后), 却只是解决一个其实
    有出路的问题。
    """
    from pavoz import CheckpointMismatchError

    store = CheckpointStore(FileStorage(tmp_path / "cp"))
    await Runtime(checkpoint_store=store).run(_loop_dag(99, 3), "mv")

    with pytest.raises(CheckpointMismatchError):
        if entry == "resume":
            await Runtime(checkpoint_store=store).run(
                _loop_dag(99, 50), "mv", resume=True)
        else:
            await Runtime(checkpoint_store=store).fork_run(
                _loop_dag(99, 50), "mv", from_stage="s_start")


async def test_resume_false_is_the_way_out(tmp_path):
    """★ 出路本身: 修好退出条件 + `resume=False` ⇒ 正常跑完。

    这条是整份文档的落脚点 —— "有出路"必须有代码保障, 否则只是纸上说明。
    `resume=False` 起新 run_id 且不读旧 cp, 所以 visits 从 0 开始。
    """
    store = CheckpointStore(FileStorage(tmp_path / "cp"))
    first = await Runtime(checkpoint_store=store).run(_loop_dag(99, 3), "mv")
    assert first.status == "failed", "前置条件: 首跑应熔断"

    r = await Runtime(checkpoint_store=store).run(_loop_dag(3, 50), "mv", resume=False)

    assert r.status == "done", f"出路断了: {r.status} / {r.error}"
    assert r.state["out"] == "done"
    assert r.state["p"] == 3, f"退出条件修正后应恰好 3 圈, 实得 {r.state}"


async def test_lingering_reset_pending_does_not_affect_the_way_out(tmp_path):
    """★ 熔断后滞留的 `reset_pending` 不影响出路 —— 它是**正确**状态, 不是缺陷。

    那个 router 本来就待重跑 (回跳标记了它), 而 `resume=False` 不读旧 cp, 所以滞留
    标记在新 run 里完全不出现。写这条是为了钉住"不需要为它改代码"这个判断 ——
    日后有人看到滞留标记会想"顺手清一清", 而清了反而丢信息。
    """
    store = CheckpointStore(FileStorage(tmp_path / "cp"))
    await Runtime(checkpoint_store=store).run(_loop_dag(99, 3), "mv")

    cp = store.load_latest("mv")
    assert cp.reset_pending, (
        "前置条件: 熔断时该 router 应在待重跑集里 (回跳标记了它, 没机会消费)")

    r = await Runtime(checkpoint_store=store).run(_loop_dag(3, 50), "mv", resume=False)

    assert r.status == "done"
    cp2 = store.load_latest("mv")
    assert not cp2.reset_pending, (
        f"新 run 不读旧 cp 的待重跑集, 实得 {cp2.reset_pending}")
