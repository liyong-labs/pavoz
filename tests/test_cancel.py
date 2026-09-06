"""v0.9 协作式取消: Runtime.cancel_check 检查点拦截 + Ctx.cancelled() 轮询.

语义: 拦截发生在 stage 之间 / 重试之间 (检查点), 已完成 stage 照常落 cp →
resume 无缝续跑. stage 内长循环用 ctx.cancelled() 自行轮询自行退出
(循环留业务层原则).
"""

from stageflow import (
    CheckpointStore,
    Ctx,
    DAG,
    FileStorage,
    ReadOnlyStateView,
    Runtime,
)


async def test_cancel_between_stages_then_resume():
    """s_a 完成后取消 → cancelled (s_b 未跑); 解除取消 → resume 续跑到 done."""
    tid = "cxl1"
    store = CheckpointStore(FileStorage(f"/tmp/stageflow-cancel-test-{tid}"))
    box = {"cancel": False}
    rt = Runtime(checkpoint_store=store, cancel_check=lambda t: box["cancel"])
    ran: list[str] = []

    dag = DAG("cxl1")

    @dag.stage()
    async def s_a(ctx):
        ran.append("a")
        box["cancel"] = True  # 模拟 run 中途外部取消
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        ran.append("b")
        return {"b": 2}

    r1 = await rt.run(dag, tid)
    assert r1.status == "cancelled"
    assert "取消" in (r1.error or "") or "cancel" in (r1.error or "")
    assert ran == ["a"], f"s_b 应被拦截, 实际 ran={ran}"
    assert r1.stage_statuses.get("s_a") == "done"

    box["cancel"] = False  # 解除取消 (模拟用户撤回)
    r2 = await rt.run(dag, tid, resume=True)
    assert r2.status == "done"
    assert ran == ["a", "b"], "resume 应只补跑 s_b"


async def test_ctx_cancelled_visible_in_stage():
    """stage 内长循环可轮询 ctx.cancelled() 自行退出 (循环留业务层)."""
    calls = {"n": 0}

    def check(task_id):
        calls["n"] += 1
        return calls["n"] >= 4  # 前几次检查 False, 之后 True

    seen: dict = {}
    dag = DAG("cxl2")

    @dag.stage()
    async def s_loop(ctx):
        for _i in range(10):
            if ctx.cancelled():
                seen["stopped"] = True
                return {"stopped": True}
        seen["finished"] = True
        return {"ok": 1}

    r = await Runtime(cancel_check=check).run(dag, "cxl2")
    assert r.status == "done"
    assert seen.get("stopped") is True, "stage 应轮询到取消并提前退出"


async def test_cancel_check_exception_treated_as_not_cancelled():
    """cancel_check 抛异常 → 视为未取消 (fail-open), run 正常完成."""
    dag = DAG("cxl3")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    def bad(task_id):
        raise RuntimeError("checker db down")

    r = await Runtime(cancel_check=bad).run(dag, "cxl3")
    assert r.status == "done"


async def test_ctx_cancelled_exception_treated_as_not_cancelled():
    """stage 内 ctx.cancelled() 轮询遇 checker 异常 → 视为未取消 (fail-open), run 正常 done."""
    calls = {"n": 0}
    seen: dict = {}
    dag = DAG("cxl5")

    @dag.stage()
    async def s_poll(ctx):
        for _i in range(3):
            calls["n"] += 1
            if ctx.cancelled():
                seen["stopped"] = True
                return {"stopped": True}
        seen["finished"] = True
        return {"ok": 1}

    def bad(task_id):
        raise RuntimeError("checker db down")

    r = await Runtime(cancel_check=bad).run(dag, "cxl5")
    assert r.status == "done"
    assert seen.get("finished") is True, "checker 异常应视为未取消, 循环正常跑完"
    assert calls["n"] == 3, "每次轮询异常都不应终止 stage"
    assert "stopped" not in seen


def test_ctx_cancelled_direct_raising_checker_fail_open():
    """直接构造 Ctx (公开 API) + raising checker → cancelled() 返 False 不抛 (fail-open)."""
    def bad():
        raise RuntimeError("checker db down")

    ctx = Ctx(
        task_id="cxl6",
        run_id="run-cxl6",
        attempt=1,
        dag=DAG("cxl6"),
        stage_name="s_x",
        state=ReadOnlyStateView({}),
        cancel_check=bad,
    )
    assert ctx.cancelled() is False


async def test_replay_emits_no_events():
    """run_stage (单 stage 重放) 全程 0 事件 — replay 不是正式 run (Task 2 契约回归)."""
    import tempfile

    from stageflow import CheckpointStore, FileStorage

    events: list[tuple[str, dict]] = []
    tid = "cxl4"
    with tempfile.TemporaryDirectory() as td:
        store = CheckpointStore(FileStorage(td))
        dag = DAG("cxl4")

        @dag.stage()
        async def s_a(ctx):
            return {"a": 1}

        rt = Runtime(checkpoint_store=store,
                     on_event=lambda ev, d: events.append((ev, d)))
        r1 = await rt.run(dag, tid)
        assert r1.status == "done"
        events.clear()
        r2 = await rt.run_stage(dag, tid, "s_a")
        assert r2.status == "done"
    assert events == [], f"replay 不应发事件, 实际 {events}"
