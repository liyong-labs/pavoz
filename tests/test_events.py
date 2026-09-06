"""v0.9 生命周期事件钩子 (on_event) + RunResult.stage_timings.

观察者模式最小实现: 同步回调, observer 异常隔离 (fail-open), 不引依赖.
"""

from stageflow import DAG, RetryableError, Runtime


async def test_events_lifecycle_order():
    """run_start → stage_start/stage_end 交替 → run_end; payload 通用字段齐."""
    events: list[tuple[str, dict]] = []
    dag = DAG("ev1")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 2}

    rt = Runtime(on_event=lambda ev, d: events.append((ev, d)))
    r = await rt.run(dag, "ev1")
    assert r.status == "done"

    names = [e for e, _ in events]
    assert names[0] == "run_start"
    assert names[-1] == "run_end"
    assert events[-1][1]["status"] == "done"
    assert names.count("stage_start") == 2 and names.count("stage_end") == 2
    starts = [d["stage"] for e, d in events if e == "stage_start"]
    assert starts == ["s_a", "s_b"]
    ends = {d["stage"]: d["status"] for e, d in events if e == "stage_end"}
    assert ends == {"s_a": "done", "s_b": "done"}
    assert all("task_id" in d and "run_id" in d for _, d in events)
    assert events[0][1]["resume"] is False


async def test_stage_retry_event_emitted():
    """RetryableError 重试 → 恰一条 stage_retry (attempt=1, backoff>=0)."""
    events: list[tuple[str, dict]] = []
    dag = DAG("ev2")

    @dag.stage(retries=2)
    async def s_flaky(ctx):
        if ctx.attempt == 1:
            raise RetryableError("boom")
        return {"ok": 1}

    rt = Runtime(on_event=lambda ev, d: events.append((ev, d)))
    r = await rt.run(dag, "ev2")
    assert r.status == "done"
    retries = [d for e, d in events if e == "stage_retry"]
    assert len(retries) == 1
    assert retries[0]["attempt"] == 1 and retries[0]["backoff"] >= 0
    assert retries[0]["error"] == "boom"


async def test_stage_timings_populated():
    """每个执行的 stage 有墙钟耗时 (>=0)."""
    dag = DAG("ev3")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 2}

    r = await Runtime().run(dag, "ev3")
    assert set(r.stage_timings) == {"s_a", "s_b"}
    assert all(t >= 0 for t in r.stage_timings.values())


async def test_on_event_exception_swallowed():
    """observer 抛异常 → 不影响 run (fail-open)."""
    dag = DAG("ev4")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    def bad(ev, d):
        raise RuntimeError("observer bug")

    r = await Runtime(on_event=bad).run(dag, "ev4")
    assert r.status == "done"
