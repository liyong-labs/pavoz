"""R1a: CancelRegistry — graceful 边界取消 (复用 v0.9) + hard 硬杀 (id=137)."""

import asyncio

import pytest

from pavoz import (
    DAG,
    CancelRegistry,
    NotKillable,
    Runtime,
)


async def test_graceful_cancel_via_registry():
    reg = CancelRegistry()
    ran: list[str] = []
    dag = DAG("cr1")

    @dag.stage()
    async def s_a(ctx):
        ran.append("a")
        reg.cancel("cr1")  # 模拟外部取消
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        ran.append("b")
        return {"b": 2}

    r = await Runtime(cancel_registry=reg).run(dag, "cr1")
    assert r.status == "cancelled" and ran == ["a"]


async def test_hard_kill_mid_stage_returns_runresult():
    reg = CancelRegistry()
    dag = DAG("cr2")

    @dag.stage()
    async def s_long(ctx):
        await asyncio.sleep(30)  # 会被硬杀打断
        return {"x": 1}

    rt = Runtime(cancel_registry=reg)
    task = asyncio.create_task(rt.run(dag, "cr2"))
    await asyncio.sleep(0.1)          # 让 run 进入 s_long
    reg.cancel("cr2", mode="hard")    # 硬杀
    r = await asyncio.wait_for(task, timeout=5)
    assert r.status == "cancelled"
    assert "kill" in (r.error or "").lower()
    assert r.failed_stage == "s_long"


async def test_hard_kill_not_killable_raises():
    reg = CancelRegistry()
    dag = DAG("cr3")

    @dag.stage(killable=False)
    async def s_critical(ctx):
        await asyncio.sleep(30)
        return {}

    rt = Runtime(cancel_registry=reg)
    task = asyncio.create_task(rt.run(dag, "cr3"))
    await asyncio.sleep(0.1)
    with pytest.raises(NotKillable):
        reg.cancel("cr3", mode="hard")
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_external_cancel_still_reraises():
    """非 registry 来源的 task.cancel() → CancelledError 照旧上抛 (行为保持)."""
    dag = DAG("cr4")

    @dag.stage()
    async def s_long(ctx):
        await asyncio.sleep(30)
        return {}

    task = asyncio.create_task(Runtime().run(dag, "cr4"))
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
