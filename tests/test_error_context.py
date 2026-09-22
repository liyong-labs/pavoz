"""W1: RunResult 错误上下文 — failed_stage / retryable / state_summary."""

import asyncio

from pavoz import DAG, RetryableError, Runtime, StageError


async def test_stage_error_carries_context():
    dag = DAG("ectx1")

    @dag.stage()
    async def s_bad(ctx):
        return 1 / 0  # 未知异常 → FatalError 语义, retryable=False

    rt = Runtime()
    r = await rt.run(dag, "ectx1")
    assert r.status == "failed"
    assert r.failed_stage == "s_bad"
    assert r.retryable is False
    assert r.error and r.error_class


async def test_retryable_error_exhausted_is_retryable():
    dag = DAG("ectx2")

    @dag.stage(retries=1)
    async def s_flaky(ctx):
        raise RetryableError("503")

    r = await Runtime().run(dag, "ectx2")
    assert r.status == "failed"
    assert r.retryable is True  # 重试耗尽, 但错误本质可重试


async def test_timeout_is_retryable():
    """W4: per-stage timeout 已存在; 超时 = RetryableError 语义 → retryable True."""
    dag = DAG("ectx3")

    @dag.stage(timeout=0.1)
    async def s_slow(ctx):
        await asyncio.sleep(5)

    r = await Runtime().run(dag, "ectx3")
    assert r.status == "failed"
    assert r.failed_stage == "s_slow"
    assert r.retryable is True


async def test_state_summary_present_and_truncated():
    dag = DAG("ectx4")

    @dag.stage()
    async def s_big(ctx):
        return {"blob": "x" * 5000}

    @dag.stage(depends_on=["s_big"])
    async def s_fail(ctx):
        raise StageError("boom")

    r = await Runtime().run(dag, "ectx4")
    assert r.state_summary is not None
    assert len(r.state_summary) <= 2048
    assert "blob" in r.state_summary


async def test_done_run_has_none_context():
    dag = DAG("ectx5")

    @dag.stage()
    async def s_ok(ctx):
        return {"a": 1}

    r = await Runtime().run(dag, "ectx5")
    assert r.status == "done"
    assert r.failed_stage is None and r.retryable is None and r.state_summary is None
