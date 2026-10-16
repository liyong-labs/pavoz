"""形状矩阵: 空 delta × 环图 × 重叠 key × fork 点 的**交叉**。

现有测试各钉一个维度 (空 delta 钉 lockstep、环图钉 visit 对齐、fork 钉 override
落点), 但**交叉**没人钉 —— 而 bug 恰恰躲在交叉里。本文件把三条不变式铺到一批
形状上:

  1. lockstep: 每个 stage 的 done 次数 == 它的 visits 长度
  2. `cp.state` == 活跑返回的 state (两条路必须同源)
  3. `rebuild_state_before(s)` == s 首次执行时观测到的 `ctx.state`

**判据自己踩过两个坑, 都记在这里 —— 它们各自都给出过"漂亮的 0 问题", 而那个 0
是假的**:

- 用 `done_stages` 的**全局下标**去比该 stage 自己的 visits 长度 → 848 个形状
  里 576 个误报。计数必须是 per-stage 的。
- 遍历 `stage_deltas_visits` 的 key 找 lockstep 断 → **漏掉"有 done 无 delta"的
  stage**, 而那正是空 delta 的形态。环图族因此报 0/0, 连人工变异都抓不到。判据
  必须以 `done_stages` 为准遍历 (它是"理应有 delta"的那一侧)。

判据的有效性靠**变异**验证: 把 `_run_inner` 的 delta 追加改回 `if delta:` 前置
(即空 delta 那个 bug 的原始形态), 本文件必须大面积转红。绿了不算数, 要的是
"改坏就红"。

形状清单显式列出而不是全量笛卡尔积: 判别力最强的交叉就够, 全量是浪费 (而且
笛卡尔积里绝大多数形状是平凡的)。

**覆盖边界 (别拿它当全面保证, 都是实测过的)**:

- 本文件只在"该 stage **确实被调用过**"的前提下比 rebuild 与观测 (第三族)。
  因此"重跑中途失败"那一族**只**断言 lockstep 与 `cp.state == 活跑` —— 未跑到的
  stage 没有观测值, 拿什么去比 rebuild 都是判据自己编的。
- 对"截断点之后的 stage 从未完成 ⇒ `export-input` 丢 `fork_overrides`"这一类
  (a035821), **本矩阵实测无判别力**: 退回旧判据后 27 个用例照样全绿 —— 那些形状
  里唯一可比的是 from_stage, 而它**被调用过** (哪怕炸了), 所以 `stop_at` 照常命中,
  兜底注入那条路径根本没被走到。该缺陷钉在
  `tests/test_rebuild_consistency.py` 的"形状 5"。
- 对"环图 fork 的 visits 尾对齐" (id=3241) 判别力很强: 尾对齐退化成顺序索引时,
  本文件 6 个用例转红。
"""

from __future__ import annotations

import pytest

from pavoz import DAG, CheckpointStore, FileStorage, Runtime, StageError


# ── 判据 ────────────────────────────────────────────────────────────────────

def assert_lockstep(cp, label: str) -> None:
    """每个 stage 的 done 条目数必须与它的 visits 长度相等.

    仅对**非 fork** cp 成立 —— fork cp 的 `done_stages` 与 `stage_deltas_visits`
    本来就不等长 (截断点保留全量 deltas, 但只记 keep 段的 done), 那是尾对齐
    语义, 见 `fork_keep_counts`。
    """
    if cp.fork_keep_counts:
        return
    for st in set(cp.done_stages):          # ← 以 done 为准, 不是以 deltas 为准
        n_done = cp.done_stages.count(st)
        n_delta = len(cp.stage_deltas_visits.get(st, []))
        assert n_done == n_delta, (
            f"{label}: stage '{st}' 的 done 条目有 {n_done} 条, "
            f"但 stage_deltas_visits 只有 {n_delta} 条 —— 重建按序号位置配对, "
            f"两者不等长就会把某次 visit 的 delta 配到另一次上")


def assert_cp_matches_live(cp, live, label: str) -> None:
    assert cp.state == live, (
        f"{label}: 落盘与活跑分叉\n"
        f"       cp.state = {cp.state}\n"
        f"       活跑     = {live}")


# ── 线性链族: 空 delta 位置 × 重叠 key × fork 点 ───────────────────────────

def _chain(n: int, empty_at: frozenset[int], overlap: frozenset[int]) -> DAG:
    dag = DAG("matrix_chain")
    prev: str | None = None
    for i in range(n):
        async def body(ctx, i=i):
            if i in empty_at:
                return {}
            if i in overlap:
                return {"shared": f"from_s{i}"}   # 与相邻 stage 写同名 key
            return {f"k{i}": i * 10}

        body.__name__ = f"s{i}"
        if prev is None:
            dag.stage()(body)
        else:
            dag.stage(depends_on=[prev])(body)
        prev = f"s{i}"
    return dag


# 空 delta 位置是这族的关键维度: 空 delta 曾让 done/deltas 永久错位 (c5073af),
# 这里把每个位置都占一遍, 并叠加"相邻 stage 写同名 key" —— 空 delta + 链式覆盖
# 同名 key 正是当初那条 bug 变成"checkpoint 自相矛盾"的条件。
_CHAIN_CASES = [
    # (n, 空 delta 位置, 重叠 key 位置, fork 点 或 None)
    (2, frozenset(), frozenset(), None),
    (2, frozenset(), frozenset(), 1),
    (2, frozenset({0}), frozenset(), None),
    (2, frozenset({0}), frozenset(), 1),
    (2, frozenset({1}), frozenset(), None),
    (2, frozenset({0, 1}), frozenset(), None),
    (3, frozenset(), frozenset({1, 2}), 2),
    (3, frozenset({1}), frozenset({1, 2}), 2),
    (3, frozenset({1}), frozenset({1, 2}), None),
    (3, frozenset({0, 1}), frozenset(), 2),
    (3, frozenset({2}), frozenset({1, 2}), 1),
    (3, frozenset(), frozenset({0, 1}), 1),
]


@pytest.mark.parametrize("n,empty_at,overlap,fork", _CHAIN_CASES,
                         ids=[f"n{n}-空{sorted(e)}-重叠{sorted(o)}-fork{f}"
                              for n, e, o, f in _CHAIN_CASES])
async def test_chain_shape_matrix(n, empty_at, overlap, fork, tmp_path):
    store = CheckpointStore(FileStorage(tmp_path / "cp"))
    rt = Runtime(checkpoint_store=store)
    label = f"chain n={n} 空={sorted(empty_at)} 重叠={sorted(overlap)} fork={fork}"

    r = await rt.run(_chain(n, empty_at, overlap), "T")
    cp = store.load_latest("T")
    assert_lockstep(cp, f"{label} run")
    assert_cp_matches_live(cp, r.state, f"{label} run")

    if fork is not None:
        rf = await rt.fork_run(_chain(n, empty_at, overlap), "T",
                               from_stage=f"s{fork}", overrides={"ov": 1})
        cp = store.load_latest("T")
        assert_lockstep(cp, f"{label} fork@{fork}")
        assert_cp_matches_live(cp, rf.state, f"{label} fork@{fork}")


# ── 环图族: 空 router × 阈值 × fork 点 × override ──────────────────────────
#
# 环图是 visits 对齐最容易错的形状 (一个 stage 有多条 delta, 且历史段与重跑段
# 内容可以不同)。这里让 router 读 override 里的 tag ⇒ 两段 delta 必然不同,
# 顺序索引取错就会显形 —— 而不是像两段内容相同时那样"看着都对"。

def _loop(thresh: int, empty_router: bool) -> DAG:
    dag = DAG("matrix_loop")

    @dag.stage()
    async def s_w(ctx):
        return {"n": ctx.state.get("n", 0) + 1}

    @dag.stage(depends_on=["s_w"])
    async def s_r(ctx):
        if empty_router:
            return {}                        # 空 delta 在环内 stage 上
        return {"shared": f"R{ctx.state['n']}[{ctx.state.get('tag', 'none')}]",
                "lim": ctx.state.get("lim", thresh)}

    dag.add_conditional_edges(
        "s_r", lambda s: "more" if s["n"] < s.get("lim", thresh) else "stop",
        {"more": "s_w", "stop": "s_end"}, max_visits=12)

    @dag.stage()
    async def s_end(ctx):
        return {"rounds": ctx.state["n"]}

    return dag


_LOOP_CASES = [
    # (阈值, 空 router, fork 点 或 None, overrides)
    (2, False, None, {}),
    (3, False, "s_w", {}),
    (3, False, "s_w", {"tag": "F"}),
    (3, False, "s_r", {"tag": "F"}),
    (4, False, "s_end", {"tag": "F", "lim": 5}),   # override 改轮数 ⇒ 两段长度不同
    (2, True, None, {}),                            # 空 router + 无 fork
    (3, True, "s_w", {"tag": "F"}),                 # 空 router + fork
    (4, True, "s_r", {}),
    (3, True, "s_end", {"tag": "F"}),
]


@pytest.mark.parametrize("thresh,empty_router,fork,ov", _LOOP_CASES,
                         ids=[f"{t}轮-空r{e}-fork{f}-ov{sorted(o)}"
                              for t, e, f, o in _LOOP_CASES])
async def test_loop_shape_matrix(thresh, empty_router, fork, ov, tmp_path):
    store = CheckpointStore(FileStorage(tmp_path / "cp"))
    rt = Runtime(checkpoint_store=store)
    label = f"loop {thresh}轮 空router={empty_router} fork={fork} ov={sorted(ov)}"

    r = await rt.run(_loop(thresh, empty_router), "L")
    cp = store.load_latest("L")
    assert_lockstep(cp, f"{label} run")
    assert_cp_matches_live(cp, r.state, f"{label} run")

    if fork is not None:
        rf = await rt.fork_run(_loop(thresh, empty_router), "L",
                               from_stage=fork, overrides=ov)
        cp = store.load_latest("L")
        assert_lockstep(cp, f"{label} fork@{fork}")
        assert_cp_matches_live(cp, rf.state, f"{label} fork@{fork}")


# ── rebuild vs 观测 ─────────────────────────────────────────────────────────
#
# 这是第三条判据 (cp 能自洽不等于重建的**时点**对)。观测靠 stage 执行时记快照。
# 只在语义真正可比的两处比 —— 判据写错时点的教训见模块 docstring 与下面的注释。

async def test_rebuild_matches_observed_input_in_loop(tmp_path):
    """环图 run 之后: rebuild_state_before(s) == s 首次执行时真实看到的 state.

    非 fork cp 上两者同源 (done 序 = 执行序), 所以任意 stage 都能比 —— 这也是
    fork cp 上**不能**照搬的原因: 那里 done 序是 keep+重跑, 环内的 stage 会被
    条件边再调一次, 观测到的第一次已经不是重建所对应的那次了。
    """
    seen: dict[str, list[dict]] = {}

    dag = DAG("rebuild_obs")
    store = CheckpointStore(FileStorage(tmp_path / "cp"))

    @dag.stage()
    async def s_w(ctx):
        seen.setdefault("s_w", []).append(dict(ctx.state))
        return {"n": ctx.state.get("n", 0) + 1}

    @dag.stage(depends_on=["s_w"])
    async def s_r(ctx):
        seen.setdefault("s_r", []).append(dict(ctx.state))
        return {"shared": f"R{ctx.state['n']}"}

    dag.add_conditional_edges("s_r", lambda s: "more" if s["n"] < 3 else "stop",
                              {"more": "s_w", "stop": "s_end"}, max_visits=8)

    @dag.stage()
    async def s_end(ctx):
        seen.setdefault("s_end", []).append(dict(ctx.state))
        return {"rounds": ctx.state["n"]}

    await Runtime(checkpoint_store=store).run(dag, "RO")
    cp = store.load_latest("RO")

    for st, snaps in seen.items():
        assert cp.rebuild_state_before(st) == snaps[0], (
            f"rebuild({st}) 与该 stage 首次执行时观测到的输入不一致\n"
            f"       重建   = {cp.rebuild_state_before(st)}\n"
            f"       观测   = {snaps[0]}")


async def test_rebuild_of_fork_point_matches_observed_input(tmp_path):
    """fork cp 上只有 **from_stage 本身** 可比 (keep 到它就停 ⇒ 第一次必在重跑段)。

    环图里 keep 段的 stage (s_w) 会被条件边**再调一次**, 拿那次观测去比
    rebuild_state_before(s_w) 是时点错配 —— 那是判据的错, 不是被测行为的错
    (踩过: 54 个形状全是这类误报, 且正常代码与人工变异结果逐字相同)。
    """
    store = CheckpointStore(FileStorage(tmp_path / "cp"))
    rt = Runtime(checkpoint_store=store)
    await rt.run(_loop(3, False), "FR")

    seen: dict[str, list[dict]] = {}
    dag = _loop(3, False)
    real_r = dag.stages["s_r"].fn          # 换成记快照的版本
    async def s_r_spy(ctx):
        seen.setdefault("s_r", []).append(dict(ctx.state))
        return await real_r(ctx)
    s_r_spy.__name__ = "s_r"
    dag.stages["s_r"].fn = s_r_spy

    rf = await rt.fork_run(dag, "FR", from_stage="s_r", overrides={"tag": "F"})
    cp = store.load_latest("FR")

    assert seen["s_r"], "前置条件: fork 重跑必须真的调用了 s_r"
    assert cp.rebuild_state_before("s_r") == seen["s_r"][0], (
        "rebuild(from_stage) 与重跑时它真实看到的输入不一致\n"
        f"       重建 = {cp.rebuild_state_before('s_r')}\n"
        f"       观测 = {seen['s_r'][0]}")
    assert_cp_matches_live(cp, rf.state, "fork@s_r")


# ── 环图 + 重跑中途失败 ────────────────────────────────────────────────────
#
# 环图上"重跑失败"与"fork 重跑没跑完"是同一族: 截断点之后的 stage 会在
# stage_deltas 里留**历史**条目, 却不在本次 run 的 done_stages 里 (a035821 的
# 现场)。这一族只断言 lockstep 与 cp.state == 活跑 —— 未跑到的 stage 没有观测,
# rebuild 与谁比都是判据编的 (见模块 docstring 的覆盖边界)。
#
# 失败开关挂在模块级: fork 与 run 必须用**结构相同**的 DAG (fn 体不进
# workflow_hash), 才能让重跑在中途炸掉而首跑正常。

_FAIL = {"on": False}


def _loop_failable(thresh: int, fail_at: str | None = None) -> DAG:
    dag = DAG("matrix_loop_fail")

    @dag.stage()
    async def s_w(ctx):
        if _FAIL["on"] and fail_at == "s_w":
            raise StageError("worker 挂了", category="SEARCH_NO_RESULT")
        return {"n": ctx.state.get("n", 0) + 1}

    @dag.stage(depends_on=["s_w"])
    async def s_r(ctx):
        if _FAIL["on"] and fail_at == "s_r":
            raise StageError("router 挂了", category="SEARCH_NO_RESULT")
        return {"shared": f"R{ctx.state['n']}[{ctx.state.get('tag', 'none')}]",
                "lim": ctx.state.get("lim", thresh)}

    dag.add_conditional_edges(
        "s_r", lambda s: "more" if s["n"] < s.get("lim", thresh) else "stop",
        {"more": "s_w", "stop": "s_end"}, max_visits=12)

    @dag.stage()
    async def s_end(ctx):
        return {"rounds": ctx.state["n"]}

    return dag


@pytest.fixture(autouse=True)
def _reset_fail_flag():
    _FAIL["on"] = False
    yield
    _FAIL["on"] = False


_FAIL_CASES = [
    (2, "s_r"),      # 重跑在截断点本身炸 → 它的下游全没跑
    (3, "s_r"),
    (2, "s_w"),      # 截断点之后的环内 stage 炸
    (3, "s_w"),
]


@pytest.mark.parametrize("thresh,fail_at", _FAIL_CASES,
                         ids=[f"{t}轮-炸{f}" for t, f in _FAIL_CASES])
async def test_loop_fork_rerun_fails_midway(thresh, fail_at, tmp_path):
    store = CheckpointStore(FileStorage(tmp_path / "cp"))
    rt = Runtime(checkpoint_store=store)
    label = f"loop {thresh}轮 fork@s_r 重跑炸于 {fail_at}"

    # 首跑用**不带 fail_at 的**同结构 DAG (fn 体不进 workflow_hash, 结构一致)
    await rt.run(_loop_failable(thresh), "FL")

    _FAIL["on"] = True
    try:
        rf = await rt.fork_run(_loop_failable(thresh, fail_at), "FL",
                               from_stage="s_r", overrides={"tag": "F"})
    finally:
        _FAIL["on"] = False

    assert rf.status == "failed", (
        f"前置条件: 重跑应该在 {fail_at} 炸掉, 实得 {rf.status}")
    cp = store.load_latest("FL")
    assert_lockstep(cp, label)
    assert_cp_matches_live(cp, rf.state, label)
