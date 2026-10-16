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


def _deep_branch_dag(ran: list[str]) -> DAG:
    """分支链深两级 (s_c → s_d → s_e) —— 只豁免一层的实现会在 s_e 上露馅."""
    dag = DAG("branch_gate_deep2")

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

    @dag.stage()
    async def s_c(ctx):
        ran.append("s_c")
        return {"c": 1}

    @dag.stage(depends_on=["s_c"])
    async def s_d(ctx):
        ran.append("s_d")
        return {"d": 1}

    @dag.stage(depends_on=["s_d"])
    async def s_e(ctx):
        ran.append("s_e")
        return {"e": 1}

    @dag.stage()
    async def s_fin(ctx):
        ran.append("s_fin")
        return {"fin": True}

    dag.add_conditional_edges("s_gate",
                              lambda s: "go" if s.get("want_go") else "end",
                              {"go": "s_c", "end": "s_fin"}, max_visits=4)
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


async def test_unselected_branch_whole_chain_is_exempt_from_orphan_check(tmp_path):
    """★ 分支豁免要覆盖**整条静态链**, 不只是 router 直接声明的 target。

    这是 `_gated_by` 那条的另一半 (同源): 孤儿检查原本只豁免 `mapping` 的
    **直接 target**。于是"分支目标带静态下游"时, 那个下游既没人跑、又不在豁免
    名单里 → 被判 `OrphanStagesError`, 也就是说**"选了某分支"这件事本身就让
    run 永远 failed** —— 条件边分支的核心用途直接不可用。

    深链 (s_c → s_d → s_e) 是为了钉住"只豁免一层"的实现: 豁免一层的话 s_e
    仍会成孤儿。
    """
    ran: list[str] = []
    store = CheckpointStore(FileStorage(tmp_path / "cp"))
    r = await Runtime(checkpoint_store=store).run(
        _deep_branch_dag(ran), "D", initial_state={"want_go": False})

    assert ran == ["s_a", "s_b", "s_gate", "s_fin"], f"实得 {ran}"
    assert r.status == "done", (
        f"未选中分支整条链合法跳过时 run 必须能报 done, 实得 {r.status}: "
        f"{r.error} —— 这是 A1 的另一半, 孤儿豁免只做了一层")


async def test_stale_route_decision_still_fails_after_coverage_widened(tmp_path):
    """★ 反向约束: 豁免扩到闭包**不能**削弱 stale 检测。

    这里先记一个我自己踩过的坑: 我第一版的"真孤儿"形状是给一个无依赖的
    `s_orphan` —— 那是 indeg=0, 会被 topo 自动推进跑掉, **根本不是孤儿**, 断言
    自然失败。真孤儿走的是 **stale** 那条路: router 决策之后跑了一个不在所选
    目标可达闭包里的 stage ⇒ 旧决策已过期却无人重判。

    这条要证明的是: `covered` 扩到闭包之后, stale 那一步仍然独立生效
    (代码里 stale 先判、stale 时**不**更新 covered —— 顺序不能倒)。
    """
    ran: list[str] = []
    dag = DAG("stale_kept")

    @dag.stage()
    async def s_head(ctx):
        ran.append("s_head")
        return {}

    @dag.stage(depends_on=["s_head"])
    async def s_pre(ctx):
        ran.append("s_pre")
        return {"doc": "draft"}

    @dag.stage(depends_on=["s_pre"])
    async def s_review(ctx):                 # ★ 分支外, 且无出边 → 回路没闭合
        ran.append("s_review")
        return {"audit": "r1"}

    @dag.stage()
    async def s_gate(ctx):
        ran.append("s_gate")
        return {}

    @dag.stage()
    async def s_revision(ctx):
        ran.append("s_revision")
        return {"article_md": "v2"}

    @dag.stage()
    async def s_fin(ctx):
        ran.append("s_fin")
        return {}

    dag.add_conditional_edges("s_gate", lambda s: "revise",
                              {"revise": "s_revision", "ship": "s_fin"},
                              max_visits=3)

    r = await Runtime(checkpoint_store=CheckpointStore(
        FileStorage(tmp_path / "cp"))).run(dag, "S")

    assert "s_review" in ran, "前置条件: s_review 必须在决策后被执行 (stale 的来源)"
    assert r.status == "failed", (
        f"决策后执行分支外 stage 必须报 failed —— 豁免扩过头会把它放过, "
        f"实得 {r.status}")
    assert r.error_class == "OrphanStagesError"
    assert "s_review" in (r.error or ""), f"错误消息应点名它, 实得 {r.error}"

    # ⚠️ 一条**没能构造出来**的反向约束, 记在这儿免得下一个人再找一遍:
    # "covered 扩得过头 (比如扩到全部 stage) 会不会放过真孤儿" ——
    # **测不出来**。变异验证: 把 covered 换成 `order` (最大豁免), 本文件 +
    # test_conditional_edges.py 共 48 个用例**全绿**, 抓不到。
    # 理由 (推出来的, 也解释了为什么它推得通): 在**有** router 的图里, 一个
    # stage 既不被 topo 自动推进、又不被路由到达, 只可能因为它的静态上游链上
    # 存在 route-only 节点; 而 route-only 节点按定义就是某个已执行 router 的
    # target ⇒ 它们的可达闭包已经覆盖了全部这类 stage。于是"扩到闭包"与"扩到
    # 全部"在正常终止语义下**不可区分**, 没有任何形状能把两者分开。
    # 真正有判别力的是 stale 那一步 (本用例) —— 它在 covered 之前判, 且 stale
    # 时**不**更新 covered; 这个顺序不能倒, 否则决策过期的分支会互相豁免。


async def test_selected_branch_with_deep_chain_completes(tmp_path):
    """选中分支的整条链都要跑通 (对称性: 豁免不能误伤被选中的分支)。"""
    ran: list[str] = []
    r = await Runtime(checkpoint_store=CheckpointStore(
        FileStorage(tmp_path / "cp"))).run(
        _deep_branch_dag(ran), "DG", initial_state={"want_go": True})

    assert ran == ["s_a", "s_b", "s_gate", "s_c", "s_d", "s_e"], f"实得 {ran}"
    assert r.status == "done"
    assert r.state["e"] == 1


async def test_gate_check_does_not_block_loop_reentry(tmp_path):
    """★ 闸门检查**不能**拦住 `reset_pending` 驱动的回路重跑。

    这条钉的是闸门检查那个改动自身的回归风险: `s_judge` 的静态上游闭包里含
    `s_build` (条件边的 target ⇒ route-only), 所以 `_gated_by[s_judge]` 非空 ——
    它**真的会**经过闸门分支。而回路能继续靠的是 `reset_pending`: 回跳落点
    `s_build` 的静态下游 (含 s_review / s_judge) 被标记重跑。

    闸门若在这里误拦, 回路会在第一圈后停住 —— 表现是 `s_build` 只跑 1 次而 run
    报 done (静默少跑, 与本文件开头那个缺陷同一类形态)。

    形状是**闭合**的: `s_judge` 依赖 `s_review`, 所以第二圈它能被 auto 推进回
    判断链。写成回路未闭合的形状会得到"两边行为一致"的假对照 (实测踩过)。

    ⚠️ 定位要说准: 这个形状**不经过**闸门判断分支 —— `s_build` 是回路目标
    (有静态父 s_start), 按 `route_only_targets` 的定义它**不算** route-only,
    所以 `_gated_by` 算出来是空集 (下面有断言把这一点钉住, 免得日后有人以为
    它测了闸门)。它守住的是"闸门改动没有**间接**影响回路": `auto_order` 的
    构造变了、`_next_auto` 多了一个 continue, 回路重跑都得照样跑满。
    真正与闸门判断**交互**的形状见下一条。
    """
    ran: list[str] = []
    dag = DAG("closed_loop_gate")

    @dag.stage()
    async def s_start(ctx):
        ran.append("s_start")
        return {}

    @dag.stage(depends_on=["s_start"])
    async def s_build(ctx):
        ran.append(f"s_build:{ctx.state.get('rev', 0) + 1}")
        return {"rev": ctx.state.get("rev", 0) + 1}

    @dag.stage(depends_on=["s_build"])
    async def s_review(ctx):
        ran.append(f"s_review:{ctx.state['rev']}")
        return {"reviewed": ctx.state["rev"]}

    @dag.stage(depends_on=["s_review"])       # ← 闭合回路的关键依赖
    async def s_judge(ctx):
        ran.append(f"s_judge:{ctx.state['rev']}")
        return {}

    @dag.stage()
    async def s_fin(ctx):
        ran.append("s_fin")
        return {"out": "done"}

    dag.add_conditional_edges(
        "s_judge", lambda s: "revise" if s.get("rev", 0) < 3 else "ship",
        {"revise": "s_build", "ship": "s_fin"}, max_visits=12)

    r = await Runtime(checkpoint_store=CheckpointStore(
        FileStorage(tmp_path / "cp"))).run(dag, "C")

    counts = {}
    for entry in ran:
        counts[entry.split(":")[0]] = counts.get(entry.split(":")[0], 0) + 1

    assert r.status == "done", f"实测 {r.status}: {r.error}"
    assert counts.get("s_build") == 3, f"回跳落点应重跑 3 轮, 实得 {counts} —— 回路被闸门误拦了?"
    assert counts.get("s_review") == 3, f"s_review 应随回跳重跑 3 轮, 实得 {counts}"
    assert counts.get("s_judge") == 3, f"s_judge 应重判 3 轮, 实得 {counts}"
    assert counts.get("s_fin") == 1
    assert r.state["out"] == "done"

    # 把"这个形状不走闸门判断"**钉成断言** —— 定位写在 docstring 里没有约束力,
    # 日后有人误以为它测了闸门分支, 就不会再补真正的交互用例了。
    route_only = dag.route_only_targets()
    anc: set[str] = set()
    stack = list(dag.stages["s_judge"].depends_on)
    while stack:
        x = stack.pop()
        if x not in anc:
            anc.add(x)
            stack.extend(dag.stages[x].depends_on)
    assert route_only == {"s_fin"}, f"前置条件: 只有分支终点是 route-only, 实得 {route_only}"
    assert not (anc & route_only), (
        "回路目标有静态父 ⇒ 不算 route-only ⇒ 闸门前置为空。本用例若哪天开始"
        "经过闸门分支了, 说明形状变了, 要重新定位它到底在守什么")


async def test_route_branch_can_be_reentered_and_unlocks_its_gated_downstream(tmp_path):
    """★ 闸门判断与路由回跳的**交互**形状。

    前一条用例的闸门前置是空的 (回路目标有静态父 ⇒ 不算 route-only), 所以真正
    与闸门交互的形状在这里: `s_branch` 是**分支目标**(无静态父 ⇒ route-only),
    `s_down` 是它的静态下游 ⇒ `_gated_by[s_down] = {s_branch}` —— 回跳到
    `s_branch` 之前, `s_down` 必须被闸门**卡住**; `s_branch` 重跑完之后, 闸门
    必须**放行**。两个方向都要对: 早放行 = 抢跑 (本文件开头那个缺陷), 永不放行
    = 回路第二圈直接停住。
    """
    ran: list[str] = []
    dag = DAG("branch_reentry")

    @dag.stage()
    async def s_gate(ctx):
        ran.append(f"s_gate:{ctx.state.get('rev', 0)}")
        return {}

    @dag.stage()                                   # 无静态父 ⇒ route-only
    async def s_branch(ctx):
        r = ctx.state.get("rev", 0) + 1
        ran.append(f"s_branch:{r}")
        return {"rev": r}

    @dag.stage(depends_on=["s_branch"])            # ★ 闸门前置 = {s_branch}
    async def s_down(ctx):
        ran.append(f"s_down:{ctx.state['rev']}")
        return {"checked": ctx.state["rev"]}

    @dag.stage(depends_on=["s_down"])
    async def s_judge(ctx):
        ran.append(f"s_judge:{ctx.state['rev']}")
        return {}

    @dag.stage()
    async def s_end(ctx):
        ran.append("s_end")
        return {"out": "done"}

    dag.add_conditional_edges(
        "s_gate", lambda s: "again" if s.get("rev", 0) < 3 else "stop",
        {"again": "s_branch", "stop": "s_end"}, max_visits=12)
    # s_judge 也要能把控制送回 s_gate, 否则第一圈后就没得判了
    dag.add_conditional_edges(
        "s_judge", lambda s: "again" if s.get("rev", 0) < 3 else "stop",
        {"again": "s_gate", "stop": "s_end"}, max_visits=16)

    r = await Runtime(checkpoint_store=CheckpointStore(
        FileStorage(tmp_path / "cp"))).run(dag, "BR")

    counts: dict[str, int] = {}
    for e in ran:
        k = e.split(":")[0]
        counts[k] = counts.get(k, 0) + 1

    # s_branch 的每一轮, s_down 都必须紧跟着跑一次 (闸门在该放行时放行)
    for i in range(1, 4):
        assert f"s_branch:{i}" in ran, f"第 {i} 轮回跳没发生, 实得轨迹 {ran}"
        assert f"s_down:{i}" in ran, (
            f"★ 第 {i} 轮: s_branch 跑完后它被闸门卡住的下游 s_down 必须放行。"
            f"实得轨迹 {ran}")
    assert r.status == "done", f"实测 {r.status}: {r.error}"
    assert r.state["out"] == "done"
