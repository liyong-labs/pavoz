"""条件边的**分支闸门**: 未被路由选中的分支, 其 stage 不得被 topo 抢先执行。

★ 本文件钉的是本项目最危险的一类缺陷: **静默错结果**。修复前 (2026-10-16):

    s_a → s_b → s_gate{ go → s_c, end → s_fin },  s_c → s_d

router 选 `end` 时实测执行序是 `s_a s_b s_d s_gate s_fin` —— **`s_d` 在它自己的
闸门 `s_gate` 之前就跑了**。后果叠了三层:

1. `s_c` 一次没跑, 而 `s_d` 读到的上游值是"从未执行"的默认值
2. run 报 `status=done`, **无异常无日志**
3. 最要命: `cp.state == 活跑 state` —— 落盘与活跑**一致**, 所以一致性类的不可变
   测试 (`test_rebuild_consistency` 那一族) 也抓不到它

第 3 条是这类缺陷能潜伏至今的原因: 它不是"内部状态矛盾", 是"整个系统一致地
算出了错误的答案"。所以判据不能只钉状态, 要钉**执行顺序**与**读到的值**。

根因: `_next_auto` 从 auto_order 表头扫第一个"没 done"的, **完全不看静态依赖**。
无环图靠拓扑序兜住 (上游没 done 时下游不会是第一个); 而 route-only 节点被从
auto_order 剔除后, 它的静态下游就**失去位置约束** —— Kahn 把 indeg=0 的
route-only 提前弹出, 拓扑序的兜底随之失效。
"""

from __future__ import annotations

import pytest

from pavoz import DAG, CheckpointStore, FileStorage, Runtime


def _branch_dag(ran: list[str]) -> DAG:
    """三层 + 一个只经路由进入的分支, 分支下游另有一级静态后继."""
    dag = DAG("branch_gate")

    @dag.stage()
    async def s_a(ctx):
        ran.append("s_a")
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        ran.append("s_b")
        return {"b": 1}

    @dag.stage(depends_on=["s_b"])
    async def s_gate(ctx):
        ran.append("s_gate")
        return {}

    @dag.stage()                       # 只经路由进入 (indeg=0 的 route-only)
    async def s_c(ctx):
        ran.append("s_c")
        return {"c": "c-ran"}

    @dag.stage(depends_on=["s_c"])     # ★ s_c 的静态下游, 闸门前不该跑
    async def s_d(ctx):
        ran.append("s_d")
        return {"d_from_c": ctx.state.get("c", "!!! s_c 从未执行过 !!!")}

    @dag.stage()
    async def s_fin(ctx):
        ran.append("s_fin")
        return {"fin": True}

    dag.add_conditional_edges(
        "s_gate",
        lambda s: "go" if s.get("want_go") else "end",
        {"go": "s_c", "end": "s_fin"},
        max_visits=4,
    )
    return dag


async def test_unselected_branch_never_runs(tmp_path):
    """★ router 选 end: `s_c` 与它的静态下游 `s_d` 都**一次都不能跑**。

    `s_d` 是这条用例的主角 —— 它自己是"合法"的 stage (拓扑序上排在 s_gate 之后),
    闸门失效就发生在它身上。断言同时钉住"没跑"与"没留下痕迹": state 里不该有
    `d_from_c`, 因为那个值本身就是"读了没跑过的上游"的产物。
    """
    ran: list[str] = []
    store = CheckpointStore(FileStorage(tmp_path / "cp"))

    r = await Runtime(checkpoint_store=store).run(
        _branch_dag(ran), "T", initial_state={"want_go": False})

    assert "s_c" not in ran, f"未选中的分支不该跑, 实得执行序 {ran}"
    assert "s_d" not in ran, (
        f"★ 未选中分支的**静态下游**跑到了自己闸门之前 —— 实得执行序 {ran}")
    assert ran.index("s_gate") < ran.index("s_fin"), f"闸门应在 s_fin 之前, 实得 {ran}"
    assert "d_from_c" not in r.state, (
        f"state 里留着 d_from_c={r.state.get('d_from_c')!r} —— 那是 s_d 读了"
        f"从未执行的上游留下的痕迹")


async def test_selected_branch_runs_fully_and_reads_real_upstream(tmp_path):
    """反向约束: 闸门不能修成"永远不放行" —— go 分支必须完整跑, 且 s_d 读到
    s_c 的**真实产出**。

    这条是本次修复里踩过的坑的直接防线: 我第一版把 route-only 的静态下游
    **踢出 auto_order**, 结果 go 分支下 s_d 永远不跑 (status 直接 failed)。
    正确的模型是"闸门状态检查"而非"永久排除"。
    """
    ran: list[str] = []
    store = CheckpointStore(FileStorage(tmp_path / "cp"))

    r = await Runtime(checkpoint_store=store).run(
        _branch_dag(ran), "T", initial_state={"want_go": True})

    assert ran == ["s_a", "s_b", "s_gate", "s_c", "s_d"], (
        f"go 分支应按闸门顺序完整执行, 实得 {ran}")
    assert r.status == "done"
    assert r.state["d_from_c"] == "c-ran", (
        f"s_d 必须读到 s_c 的真实产出, 实得 {r.state.get('d_from_c')!r}")


async def test_gate_holds_for_deep_chain_below_a_route_only_stage(tmp_path):
    """闸门要一路传递到**任意深度**的下游, 不只是直接后继。

    s_d 之后再挂两级静态后继: 闸门失效时它们也会抢跑, 而只查一层的话会漏。
    """
    ran: list[str] = []
    dag = DAG("branch_gate_deep")

    @dag.stage()
    async def s_head(ctx):
        ran.append("s_head")
        return {}

    @dag.stage(depends_on=["s_head"])
    async def s_gate(ctx):
        ran.append("s_gate")
        return {}

    @dag.stage()
    async def s_c(ctx):
        ran.append("s_c")
        return {"c": 1}

    @dag.stage(depends_on=["s_c"])
    async def s_d(ctx):
        ran.append("s_d")
        return {}

    @dag.stage(depends_on=["s_d"])
    async def s_e(ctx):
        ran.append("s_e")
        return {}

    @dag.stage()
    async def s_fin(ctx):
        ran.append("s_fin")
        return {}

    dag.add_conditional_edges("s_gate",
                              lambda s: "go" if s.get("want_go") else "end",
                              {"go": "s_c", "end": "s_fin"}, max_visits=4)

    r = await Runtime(checkpoint_store=CheckpointStore(
        FileStorage(tmp_path / "cp"))).run(dag, "D", initial_state={"want_go": False})

    assert ran == ["s_head", "s_gate", "s_fin"], (
        f"go 分支的整条静态链 (s_c→s_d→s_e) 都不该跑, 实得 {ran}")
    assert "s_e" not in ran


@pytest.mark.parametrize("want_go", [True, False])
async def test_no_plain_dag_behaviour_change(tmp_path, want_go):
    """反向约束: 闸门逻辑对**有条件边**的图生效, 但不能改变简单图的语义。

    钉的是"闸门前置集合"为空时 `_next_auto` 的行为与从前逐字节一致 —— 否则这次
    修复就是在动一个所有图共用的循环。
    """
    ran: list[str] = []
    dag = DAG(f"plain_{want_go}")

    @dag.stage()
    async def s_a(ctx):
        ran.append("s_a")
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        ran.append("s_b")
        return {"b": 2}

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        ran.append("s_c")
        return {"c": 3}

    r = await Runtime(checkpoint_store=CheckpointStore(
        FileStorage(tmp_path / "cp"))).run(dag, "P")

    assert ran == ["s_a", "s_b", "s_c"]
    assert r.status == "done"
    assert r.state == {"a": 1, "b": 2, "c": 3}
