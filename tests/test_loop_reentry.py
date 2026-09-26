"""R2 (id=433 评审): 条件环的静态链形态 — 环头无外部父的入口判定 + route 重入重跑.

泛化业务形 (非特定 caller): 任意 "judge → 回跳早期 stage 重做, 环体是长静态链" 的
工作流. 例: 数据管道 fetch→build→check--(不合格)-->fetch 补拉重跑全链; ML 训练
featurize→train→evaluate--(未收敛)-->train; 文档流水线同构.
"""

from __future__ import annotations

from pavoz import (
    CheckpointStore,
    DAG,
    FileStorage,
    Runtime,
)


def build_pipeline(*, always_redo: bool = False, fail_build_round: int | None = None):
    """fetch → build → check --(redo)--> fetch / --(ok)--> done. 环头无静态父."""
    dag = DAG("pipeline")
    calls: dict[str, int] = {"s_fetch": 0, "s_build": 0, "s_check": 0}

    @dag.stage()
    async def s_fetch(ctx):
        calls["s_fetch"] += 1
        return {"fetched": f"r{calls['s_fetch']}"}

    @dag.stage(depends_on=["s_fetch"])
    async def s_build(ctx):
        calls["s_build"] += 1
        if fail_build_round is not None and calls["s_build"] == fail_build_round:
            raise RuntimeError("build boom")
        return {"artifact": f"a{ctx.state['fetched']}"}

    @dag.stage(depends_on=["s_build"])
    async def s_check(ctx):
        calls["s_check"] += 1
        return {"quality": "bad" if (always_redo or calls["s_check"] == 1) else "good"}

    @dag.stage()
    async def s_done(ctx):
        return {"done": True}

    dag.add_conditional_edges(
        "s_check",
        lambda s: "redo" if s["quality"] == "bad" else "ok",
        {"redo": "s_fetch", "ok": "s_done"},
        max_visits=3,
    )
    return dag, calls


async def _run(dag, tmp_path, task_id="t1", **kw):
    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(tmp_path / "cp"))))
    r = await rt.run(dag, task_id=task_id, **kw)
    return rt, r


async def test_loop_head_without_external_parent_enters_topo(tmp_path):
    """洞①: 环头无外部静态父 (环内静态链不算入口) → 豁免为环入口, 第一圈从它起跑."""
    dag, _ = build_pipeline()
    assert dag.route_only_targets() == {"s_done"}  # s_fetch 不再被错标 route-only
    rt, r = await _run(dag, tmp_path)
    assert r.status == "done"
    assert "s_fetch" in r.stage_statuses  # 第一圈真的跑了环头


async def test_route_reentry_reruns_static_downstream_chain(tmp_path):
    """洞②: judge 回跳已执行环头 → 静态链全链重跑 (新 visit), 不再静默提前 done."""
    dag, calls = build_pipeline()
    rt, r = await _run(dag, tmp_path)
    assert r.status == "done"
    assert calls["s_fetch"] == 2 and calls["s_build"] == 2 and calls["s_check"] == 2
    assert r.state["artifact"] == "ar2"       # 第二圈产物覆盖第一圈
    assert r.state["quality"] == "good"
    assert r.stage_statuses["s_done"] == "done"


async def test_router_chain_loop_behavior_unchanged(tmp_path):
    """回归: router 链环 (R1 形, 环头有外部静态父) — reset 闭包空集, 行为不变."""
    from tests.test_conditional_edges import build_story

    rt, r = await _run(build_story(pass_round=2), tmp_path)
    assert r.status == "done"
    assert r.state["round"] == 2
    assert r.state["verdict"] == "pass"


async def test_reentry_still_fused_by_max_visits(tmp_path):
    """重入不豁免熔断: judge 永远 redo → max_visits 超限 → failed (熔断非兜底)."""
    dag, _ = build_pipeline(always_redo=True)
    rt, r = await _run(dag, tmp_path)
    assert r.status == "failed"
    assert r.error_class == "MaxVisitsExceeded"


async def test_reentry_resume_mid_chain(tmp_path):
    """第二圈中途失败 → resume 从断点续跑剩余链 (reset_pending 落 cp)."""
    dag, calls = build_pipeline(fail_build_round=2)
    rt, r1 = await _run(dag, tmp_path)
    assert r1.status == "failed"
    assert calls["s_fetch"] == 2              # 第二圈已开跑
    r2 = await rt.run(dag, task_id="t1", resume=True)
    assert r2.status == "done"
    assert calls["s_build"] == 3              # 失败点重跑成功
    assert calls["s_check"] == 2 and calls["s_fetch"] == 2  # 已完成的不再重跑
    assert r2.state["artifact"] == "ar2"
