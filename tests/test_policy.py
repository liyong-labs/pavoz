"""W3: EnginePolicy — run 级策略外置 (additive)."""

import asyncio

from pavoz import DAG, EnginePolicy, RetryableError, Runtime


async def test_policy_default_timeout():
    dag = DAG("pol1")

    @dag.stage()
    async def s_slow(ctx):
        await asyncio.sleep(5)

    r = await Runtime(policy=EnginePolicy(default_timeout=0.2)).run(dag, "pol1")
    assert r.status == "failed" and r.retryable is True


async def test_explicit_runtime_timeout_wins():
    dag = DAG("pol2")

    @dag.stage()
    async def s_ok(ctx):
        return {"a": 1}

    rt = Runtime(default_timeout=10, policy=EnginePolicy(default_timeout=0.01))
    r = await rt.run(dag, "pol2")
    assert r.status == "done"  # 显式 Runtime.default_timeout 优先


async def test_max_steps_stops_run():
    dag = DAG("pol3")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 1}

    r = await Runtime(policy=EnginePolicy(max_steps=1)).run(dag, "pol3")
    assert r.status == "failed"
    assert "max_steps" in (r.error or "")
    assert r.stage_statuses == {"s_a": "done", "s_b": "failed"}


async def test_backoff_max_caps_sleep(monkeypatch):
    sleeps: list[float] = []

    async def _fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    dag = DAG("pol4")

    @dag.stage(retries=3)
    async def s_flaky(ctx):
        raise RetryableError("503")

    await Runtime(policy=EnginePolicy(backoff_max=0.5)).run(dag, "pol4")
    assert sleeps and all(s <= 0.5 for s in sleeps)


async def test_max_concurrent_runs_serializes():
    dag = DAG("pol5")
    active = {"n": 0, "peak": 0}

    @dag.stage()
    async def s_work(ctx):
        active["n"] += 1
        active["peak"] = max(active["peak"], active["n"])
        await asyncio.sleep(0.05)
        active["n"] -= 1
        return {}

    rt = Runtime(policy=EnginePolicy(max_concurrent_runs=1))
    await asyncio.gather(*(rt.run(dag, f"pol5-{i}") for i in range(3)))
    assert active["peak"] == 1
