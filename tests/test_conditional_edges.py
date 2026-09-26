"""R1 (0.5.5): conditional edge — 声明/校验/执行/循环/resume/观测 全量测试.

设计文档: pavoz-dev notes 2026-09-23-conditional-edge-r1-design.md (v4).
story 链 = ai@home 真实用例的等价最小形: s_write → s_review → {ship: s_save,
fix: s_revision → 回 s_review}.
"""

from __future__ import annotations

import pytest

from pavoz import (
    DAG,
    CheckpointMismatchError,
    CheckpointStore,
    CycleError,
    EventRecorder,
    FileStorage,
    MaxVisitsExceeded,
    Runtime,
    StageError,
    UnknownDepError,
    UnmappedRouteError,
    to_graph_json,
    to_mermaid,
    workflow_hash,
)


def build_story(counter: dict | None = None, *, pass_round: int = 2):
    """ai@home story 链等价最小形. counter 用于让 s_revision 首次必败 (resume 测试)."""
    dag = DAG("story")

    @dag.stage()
    async def s_write(ctx):
        return {"doc": "draft"}

    @dag.stage(depends_on=["s_write"])
    async def s_review(ctx):
        r = int(ctx.state.get("round", 0)) + 1
        verdict = "pass" if r >= pass_round else "fail"
        return {"round": r, "verdict": verdict}

    @dag.stage()
    async def s_revision(ctx):
        if counter is not None and not counter.get("rev_failed"):
            counter["rev_failed"] = True
            raise StageError("revise boom (首次必败)")
        return {"doc": "revised"}

    @dag.stage()
    async def s_save(ctx):
        return {"saved": True}

    dag.add_conditional_edges(
        "s_review",
        lambda s: "ship" if s["verdict"] == "pass" else "fix",
        {"ship": "s_save", "fix": "s_revision"},
        max_visits=5,
    )
    dag.add_conditional_edges(
        "s_revision",
        lambda s: "re_review",
        {"re_review": "s_review"},
        max_visits=5,
    )
    return dag


async def _run(dag, tmp_path, **kw):
    store = CheckpointStore(FileStorage(str(tmp_path / "cp")))
    rt = Runtime(checkpoint_store=store)
    r = await rt.run(dag, task_id=kw.pop("task_id", "t1"), **kw)
    return rt, store, r


# ── 声明与校验 ──────────────────────────────────────────

def test_v1_unknown_from_node():
    dag = DAG("d")

    @dag.stage()
    async def s_a(ctx):
        return {}

    dag.add_conditional_edges("s_ghost", lambda s: "k", {"k": "s_a"})
    with pytest.raises(UnknownDepError, match="s_ghost"):
        dag.validate()


def test_v1_unknown_mapping_target():
    dag = DAG("d")

    @dag.stage()
    async def s_a(ctx):
        return {}

    dag.add_conditional_edges("s_a", lambda s: "k", {"k": "s_ghost"})
    with pytest.raises(UnknownDepError, match="s_ghost"):
        dag.validate()


def test_v3_target_depends_on_router_rejected():
    dag = DAG("d")

    @dag.stage()
    async def s_a(ctx):
        return {}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {}

    dag.add_conditional_edges("s_a", lambda s: "go", {"go": "s_b"})
    with pytest.raises(ValueError, match="不可静态依赖 router"):
        dag.validate()


def test_v3_router_static_dependent_must_be_target():
    dag = DAG("d")

    @dag.stage()
    async def s_a(ctx):
        return {}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {}

    dag.add_conditional_edges("s_a", lambda s: "go", {"go": "s_a"})  # 自环绕开 s_b
    with pytest.raises(ValueError, match="静态旁路|s_b"):
        dag.validate()


def test_cycle_requires_max_visits():
    dag = DAG("d")

    @dag.stage()
    async def s_a(ctx):
        return {}

    @dag.stage()
    async def s_b(ctx):
        return {}

    dag.add_conditional_edges("s_a", lambda s: "go", {"go": "s_b"})
    dag.add_conditional_edges("s_b", lambda s: "back", {"back": "s_a"})  # 回路, 无上限
    with pytest.raises(ValueError, match="max_visits"):
        dag.validate()


def test_cycle_with_max_visits_ok_and_static_cycle_still_cycleerror():
    dag = DAG("d")

    @dag.stage()
    async def s_a(ctx):
        return {}

    @dag.stage()
    async def s_b(ctx):
        return {}

    dag.add_conditional_edges("s_a", lambda s: "go", {"go": "s_b"}, max_visits=3)
    dag.add_conditional_edges("s_b", lambda s: "back", {"back": "s_a"}, max_visits=3)
    dag.validate()  # 条件回路 + 显式上限 → 通过

    dag2 = DAG("d2")

    @dag2.stage()
    async def s_x(ctx):
        return {}

    @dag2.stage(depends_on=["s_x"])
    async def s_y(ctx):
        return {}

    dag2._stages["s_x"].depends_on = ("s_y",)  # 静态环 (绕过装饰器直接改, 模拟用户手写)
    with pytest.raises(CycleError):
        dag2.validate()


def test_entry_router_no_static_parent_auto_triggers():
    """环入口 = 首个声明的回路成员 (无静态父也 auto 触发; zeroflow
    default_entry_node 的自动版). 分支目标 / 循环体仍然 route-only."""
    dag = DAG("d")

    @dag.stage()
    async def s_head(ctx):
        return {"n": int(ctx.state.get("n", 0)) + 1}

    @dag.stage()
    async def s_tail(ctx):
        return {}

    dag.add_conditional_edges(
        "s_head", lambda s: "out" if s["n"] >= 2 else "loop",
        {"out": "s_tail", "loop": "s_head"}, max_visits=3)
    dag.validate()  # s_head 首声明 = 环入口, 无静态父也合法
    ro = dag.route_only_targets()
    assert "s_tail" in ro and "s_head" not in ro


def test_duplicate_router_and_bad_args():
    dag = DAG("d")

    @dag.stage()
    async def s_a(ctx):
        return {}

    @dag.stage()
    async def s_b(ctx):
        return {}

    with pytest.raises(ValueError, match="mapping"):
        dag.add_conditional_edges("s_a", lambda s: "k", {})
    with pytest.raises(ValueError, match="max_visits"):
        dag.add_conditional_edges("s_a", lambda s: "k", {"k": "s_a"}, max_visits=0)
    dag.add_conditional_edges("s_a", lambda s: "k", {"k": "s_a"}, max_visits=2)
    with pytest.raises(ValueError, match="已声明"):
        dag.add_conditional_edges("s_a", lambda s: "k", {"k": "s_a"})


def _route_go(s):
    return "go"


def _route_GO(s):
    return "GO"


def test_hash_includes_routing():
    def build(fn):
        dag = DAG("h")

        @dag.stage()
        async def s_a(ctx):
            return {}

        @dag.stage()
        async def s_b(ctx):
            return {}

        dag.add_conditional_edges("s_a", fn, {"go": "s_b"}, max_visits=2)
        return dag

    d1 = build(_route_go)
    d2 = build(_route_GO)  # 不同源码 → 不同 hash (route_fn 源码计入)
    assert workflow_hash(d1) != workflow_hash(d2)
    assert workflow_hash(d1) == workflow_hash(build(_route_go))  # 同源码同 hash


# ── 执行: 三大场景 ──────────────────────────────────────


async def test_pass_first_round_goes_straight_to_save(tmp_path):
    _rt, _store, r = await _run(build_story(pass_round=1), tmp_path)
    assert r.status == "done"
    assert r.stage_statuses == {
        "s_write": "done", "s_review": "done", "s_save": "done"}
    assert "s_revision" not in r.stage_statuses  # 未路由到 → 不执行不记录
    assert r.state["saved"] is True



async def test_fail_fix_pass(tmp_path):
    _rt, _store, r = await _run(build_story(pass_round=2), tmp_path)
    assert r.status == "done"
    assert r.stage_statuses["s_save"] == "done"
    assert r.state["round"] == 2
    # visit 计数: review 2 轮, revision 1 轮
    assert r.state["verdict"] == "pass"



async def test_force_save_by_route_fn_business_logic(tmp_path):
    """用例 3: 失败 2 轮强制 save — 轮次控制在 caller 的 route_fn/state 里."""
    dag = DAG("force")

    @dag.stage()
    async def s_review(ctx):
        r = int(ctx.state.get("round", 0)) + 1
        return {"round": r, "verdict": "fail"}  # 永远 fail

    @dag.stage()
    async def s_revision(ctx):
        return {"doc": "revised"}

    @dag.stage()
    async def s_save(ctx):
        return {"saved": True}

    dag.add_conditional_edges(
        "s_review",
        lambda s: "ship" if s["round"] >= 3 else "fix",  # 3 轮强制出
        {"ship": "s_save", "fix": "s_revision"},
        max_visits=5,
    )
    dag.add_conditional_edges(
        "s_revision", lambda s: "re", {"re": "s_review"}, max_visits=5)
    _rt, _store, r = await _run(dag, tmp_path)
    assert r.status == "done"
    assert r.state["round"] == 3 and r.state["saved"] is True



async def test_max_visits_exceeded_fails_run(tmp_path):
    dag = DAG("loop")

    @dag.stage()
    async def s_a(ctx):
        return {"n": int(ctx.state.get("n", 0)) + 1}

    @dag.stage()
    async def s_b(ctx):
        return {}

    dag.add_conditional_edges("s_a", lambda s: "loop", {"loop": "s_b"},
                              max_visits=2)
    dag.add_conditional_edges("s_b", lambda s: "back", {"back": "s_a"},
                              max_visits=9)
    _rt, _store, r = await _run(dag, tmp_path)
    assert r.status == "failed"
    assert r.error_class == MaxVisitsExceeded.__name__
    assert r.failed_stage == "s_a"



async def test_unmapped_route_key_fails_loud(tmp_path):
    # frozen 后无法改写 route_fn — 直接构造一个返回坏 key 的图
    dag2 = DAG("bad")

    @dag2.stage()
    async def s_a(ctx):
        return {"verdict": "???"}

    @dag2.stage()
    async def s_b(ctx):
        return {}

    dag2.add_conditional_edges(
        "s_a", lambda s: s["verdict"], {"pass": "s_b"}, max_visits=2)
    _rt, _store, r = await _run(dag2, tmp_path)
    assert r.status == "failed"
    assert r.error_class == UnmappedRouteError.__name__
    assert r.failed_stage == "s_a"
    assert "???" in (r.error or "")



async def test_route_fn_exception_propagates_class(tmp_path):
    dag2 = DAG("boom")

    @dag2.stage()
    async def s_a(ctx):
        return {}

    @dag2.stage()
    async def s_b(ctx):
        return {}

    def bad_route(s):
        raise ValueError("boom in router")

    dag2.add_conditional_edges("s_a", bad_route, {"go": "s_b"}, max_visits=2)
    _rt, _store, r = await _run(dag2, tmp_path)
    assert r.status == "failed"
    assert r.error_class == "ValueError"  # 原样保留, 编排器不包装
    assert r.failed_stage == "s_a"


# ── 观测: route 事件 / visit 序号 / timings 累计 ────────


async def test_route_event_and_visit_numbers(tmp_path):
    rec = EventRecorder()
    store = CheckpointStore(FileStorage(str(tmp_path / "cp")))
    rt = Runtime(checkpoint_store=store, on_event=rec)
    r = await rt.run(build_story(pass_round=2), task_id="t1")
    assert r.status == "done"

    routes = [d for e, d in rec.events if e == "route"]
    assert routes == [
        {"task_id": "t1", "run_id": r.run_id, "from": "s_review",
         "key": "fix", "to": "s_revision", "declared": ["fix", "ship"]},
        {"task_id": "t1", "run_id": r.run_id, "from": "s_revision",
         "key": "re_review", "to": "s_review", "declared": ["re_review"]},
        {"task_id": "t1", "run_id": r.run_id, "from": "s_review",
         "key": "ship", "to": "s_save", "declared": ["fix", "ship"]},
    ]
    visits = [d["visit"] for e, d in rec.events
              if e == "stage_end" and d["stage"] == "s_review" and d["status"] == "done"]
    assert visits == [1, 2]  # 循环重入第 2 轮



async def test_route_event_declared_keys_makes_skips_visible(tmp_path):
    """route 事件带 declared (mapping 全 key) — 本轮未选分支 = declared - [key] 可读,
    mis-wiring / 静默 skip 第一轮可见 (ai@home id=436 P1)."""
    rec = EventRecorder()
    store = CheckpointStore(FileStorage(str(tmp_path / "cp")))
    rt = Runtime(checkpoint_store=store, on_event=rec)
    r = await rt.run(build_story(pass_round=1), task_id="t1")
    assert r.status == "done"
    routes = [d for e, d in rec.events if e == "route"]
    assert all(set(d["declared"]) == {"fix", "ship"} for d in routes
               if d["from"] == "s_review")
    ship_route = [d for d in routes if d["key"] == "ship"][0]
    assert set(ship_route["declared"]) - {ship_route["key"]} == {"fix"}  # fix 分支本轮未走


async def test_stage_timings_accumulate_over_loop(tmp_path):
    _rt, _store, r = await _run(build_story(pass_round=2), tmp_path)
    assert r.stage_timings["s_review"] > 0
    # timings 是 last-write dict 上累计 — 键唯一 (无 [1] [2] 重复键)
    assert isinstance(r.stage_timings["s_review"], float)


# ── resume × loop ────────────────────────────────────────


async def test_resume_mid_loop(tmp_path):
    """循环中段失败 → resume 从最后完成 router 重路由, visit 续号."""
    counter: dict = {}
    dag = build_story(counter, pass_round=3)
    store = CheckpointStore(FileStorage(str(tmp_path / "cp")))
    rt = Runtime(checkpoint_store=store)

    r1 = await rt.run(dag, task_id="t1")
    assert r1.status == "failed" and r1.failed_stage == "s_revision"
    assert r1.state["round"] == 1  # review 第 1 轮完成, revision 首败

    r2 = await rt.run(dag, task_id="t1", resume=True)
    assert r2.status == "done"
    assert r2.state["round"] == 3 and r2.state["saved"] is True
    # review: r1 第 1 轮 + r2 第 2/3 轮 = 共 3 次执行
    d = store.diff_runs("t1", r1.run_id, r2.run_id)
    assert d["run_id_a"] == r1.run_id and d["workflow_hash_changed"] is False



async def test_hash_change_on_route_fn_refuses_resume(tmp_path):
    """改 route_fn 源码 = 结构变 → resume 拒续 (本特性最大的雷, 测试钉死)."""
    counter: dict = {}
    dag1 = build_story(counter, pass_round=3)
    store = CheckpointStore(FileStorage(str(tmp_path / "cp")))
    rt = Runtime(checkpoint_store=store)
    r1 = await rt.run(dag1, task_id="t1")
    assert r1.status == "failed"  # 中途失败, cp 留存 (done-guard 先于 hash 校验)

    def build_other():
        d = DAG("story")

        @d.stage()
        async def s_write(ctx):
            return {"doc": "draft"}

        @d.stage(depends_on=["s_write"])
        async def s_review(ctx):
            r = int(ctx.state.get("round", 0)) + 1
            return {"round": r, "verdict": "pass" if r >= 2 else "fail"}

        @d.stage()
        async def s_revision(ctx):
            return {"doc": "revised"}

        @d.stage()
        async def s_save(ctx):
            return {"saved": True}

        d.add_conditional_edges(
            "s_review",
            lambda s: "ship" if s.get("verdict") == "pass" else "fix",  # 源码不同
            {"ship": "s_save", "fix": "s_revision"},
            max_visits=5,
        )
        d.add_conditional_edges(
            "s_revision", lambda s: "re_review", {"re_review": "s_review"},
            max_visits=5,
        )
        return d

    rt2 = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(tmp_path / "cp"))))
    with pytest.raises(CheckpointMismatchError, match="hash"):
        await rt2.run(build_other(), task_id="t1", resume=True)


# ── fork / run_stage 条件边支持 (ai@home id=272, user 铁律: 单点调试必须能跑) ──


async def test_fork_from_stage_reroutes_and_finishes(tmp_path):
    """fork = 截断到 from_stage 首次完成处 + overrides + 路由续跑.

    pass_round=2 原 run: s_write → s_review(fail) → s_revision → s_review(pass)
    → s_save. fork 回 s_revision 注入 doc marker → 只重跑其后, 折叠不串轮.
    """
    dag = build_story(pass_round=2)
    store = CheckpointStore(FileStorage(str(tmp_path / "cp")))
    events: list[tuple] = []
    rt = Runtime(checkpoint_store=store,
                 on_event=lambda e, d: events.append((e, d.get("stage"))))
    r1 = await rt.run(dag, task_id="t1")
    assert r1.status == "done"
    n0 = len(events)
    r2 = await rt.fork_run(dag, "t1", from_stage="s_revision",
                           overrides={"doc": "fork-marker"})
    assert r2.status == "done"
    # 只重跑了 s_revision 起: s_write 不再执行 (单点调试的成本承诺)
    new_starts = [st for e, st in events[n0:] if e == "stage_start"]
    assert "s_write" not in new_starts
    assert "s_revision" in new_starts
    assert r2.state["doc"] == "revised"   # 重跑的 stage delta 覆盖注入 marker
    assert r2.state["round"] == 2         # visit 尾对齐: 折叠不把第 2 轮串成第 1 轮
    assert store.load_latest("t1").run_id == r2.run_id  # fork is latest
    assert r2.run_id != r1.run_id


async def test_fork_from_judge_loop_full_shape(tmp_path):
    """ai@home R2-B 真实形: route-only judge 回路; fork 从 judge 改判全链重走.

    也是 BUG 1 (producer 冲突) 的实证 dissolution: s_revision 经 2 跳条件边
    覆写 s_write 的 doc — reachable() 已把 router 计入血缘, 不报冲突.
    """
    dag = DAG("story_judge")

    @dag.stage()
    async def s_write(ctx):
        return {"doc": "draft"}

    @dag.stage(depends_on=["s_write"])
    async def s_review(ctx):
        return {"audit": "ok"}

    @dag.stage()
    async def s_judge(ctx):
        return {"route": ctx.state.get("route_override", "ship")}

    @dag.stage()
    async def s_revision(ctx):
        return {"doc": "revised", "route_override": "ship"}  # 改判一次后 ship

    @dag.stage()
    async def s_save(ctx):
        return {"saved": True}

    dag.add_conditional_edges("s_review", lambda s: "go",
                              {"go": "s_judge"}, max_visits=5)
    dag.add_conditional_edges("s_judge", lambda s: s["route"],
                              {"ship": "s_save", "revise": "s_revision"},
                              max_visits=3)
    dag.add_conditional_edges("s_revision", lambda s: "again",
                              {"again": "s_review"}, max_visits=3)

    store = CheckpointStore(FileStorage(str(tmp_path / "cp")))
    rt = Runtime(checkpoint_store=store)
    r1 = await rt.run(dag, task_id="t1")
    assert r1.status == "done"
    assert "s_revision" not in r1.stage_statuses      # 首轮直接 ship
    r2 = await rt.fork_run(dag, "t1", from_stage="s_judge",
                           overrides={"route_override": "revise"})
    assert r2.status == "done"
    assert r2.stage_statuses.get("s_revision") == "done"  # 这次走了 revise 分支
    assert r2.state["saved"] is True
    assert r2.state["doc"] == "revised"


async def test_fork_from_never_executed_stage_rejected(tmp_path):
    """从未执行过的 stage 没有截断点 (路由续跑可能根本不到它) → 明确拒绝."""
    dag = build_story(pass_round=1)
    rt, _store, r1 = await _run(dag, tmp_path)
    assert r1.status == "done"
    with pytest.raises(RuntimeError, match="未在该 task 执行过"):
        await rt.fork_run(dag, "t1", from_stage="s_revision")


async def test_run_stage_replays_single_stage_on_cond_dag(tmp_path):
    """单点隔离测试 (M3): 改 prompt 后只重放该 stage, 不动 checkpoint."""
    dag = build_story(pass_round=2)
    rt, store, r1 = await _run(dag, tmp_path)
    assert r1.status == "done"
    r2 = await rt.run_stage(dag, "t1", "s_revision")
    assert r2.status == "done"
    assert r2.state["doc"] == "revised"
    # replay 不落 cp: latest 指针仍是原 run
    assert store.load_latest("t1").run_id == r1.run_id


# ── viz ──────────────────────────────────────────────────

def test_viz_renders_conditional_edges():
    dag = build_story(pass_round=2)
    m = to_mermaid(dag)
    assert "-.fix.-> s_revision" in m
    assert "-.ship.-> s_save" in m
    assert "-.re_review.-> s_review" in m
    j = to_graph_json(dag)
    cond = [e for e in j["edges"] if e["kind"] == "conditional"]
    assert {e["key"] for e in cond} == {"ship", "fix", "re_review"}
    assert all(e["max_visits"] == 5 for e in cond)
    assert all(e["kind"] == "static"
               for e in j["edges"] if e["kind"] != "conditional")


# ── diff_runs 兼容 (循环图) ─────────────────────────────


async def test_diff_runs_on_looped_runs(tmp_path):
    dag = build_story(pass_round=2)
    store = CheckpointStore(FileStorage(str(tmp_path / "cp")))
    rt = Runtime(checkpoint_store=store)
    r1 = await rt.run(dag, task_id="t1")
    d = store.diff_runs("t1", r1.run_id)
    assert d["workflow_hash_changed"] is False
    assert d["stages"]["s_review"]["input_hash_changed"] in (True, False, None)


# ── BUG 2 回归 (ai@home id=270): 孤儿 stage 静默吞 → 必须 fail-loud ──

def _orphan_dag():
    dag = DAG("story_v1")

    @dag.stage()
    async def s_write(ctx):
        return {"doc": "draft"}

    @dag.stage(depends_on=["s_write"])
    async def s_review(ctx):
        return {"audit_report": "r1", "round": 1}

    @dag.stage()
    async def s_route_judge(ctx):
        return {"route": "revise"}

    @dag.stage()
    async def s_revision(ctx):
        return {"article_md": "v2"}

    @dag.stage()
    async def s_save(ctx):
        return {"article_path": "/x.md"}

    # 只有 judge 持边, s_review 无出边 — 回路没闭合 (ai@home v1 误 wiring)
    dag.add_conditional_edges("s_route_judge",
                              lambda s: s["route"],
                              {"ship": "s_save", "revise": "s_revision",
                               "escalate": "s_save"}, max_visits=3)
    return dag


async def test_orphan_stage_fails_run_not_silent_done(tmp_path):
    """声明的 stage 未被任何路由到达 → run failed (列孤儿), 决不允许 done."""
    _rt, _store, r = await _run(_orphan_dag(), tmp_path)
    assert r.status == "failed"
    assert r.error_class == "OrphanStagesError"
    assert "s_save" in (r.error or "")


async def test_closed_loop_runs_everything(tmp_path):
    """回路闭合 (ai@home 修正 wiring) → 全部 stage 执行, done 合法."""
    dag = DAG("story_ok")

    @dag.stage()
    async def s_write(ctx):
        return {"doc": "draft"}

    @dag.stage(depends_on=["s_write"])
    async def s_review(ctx):
        return {"audit_report": "r1", "round": 1}

    @dag.stage()
    async def s_route_judge(ctx):
        return {"route": "ship"}

    @dag.stage()
    async def s_save(ctx):
        return {"article_path": "/x.md"}

    dag.add_conditional_edges("s_review", lambda s: "judge",
                              {"judge": "s_route_judge"}, max_visits=3)
    dag.add_conditional_edges("s_route_judge", lambda s: s["route"],
                              {"ship": "s_save", "revise": "s_route_judge"},
                              max_visits=3)
    _rt, _store, r = await _run(dag, tmp_path)
    assert r.status == "done"
    assert r.stage_statuses["s_save"] == "done"
