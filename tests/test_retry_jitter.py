"""v0.9 重试退避 full-jitter: uniform(0, cap), cap = min(2^(attempt-1), 30).

雷群防护 (AWS 惯例): 多 task 同时失败时退避随机化, 不再整秒对齐.
"""

from stageflow import DAG, RetryableError, Runtime


async def test_backoff_full_jitter(monkeypatch):
    """第一次重试 (attempt=1) 的退避 = uniform(0, 2^0=1)."""
    import stageflow.runtime as rt_mod

    captured: dict = {}
    monkeypatch.setattr(
        rt_mod.random, "uniform",
        lambda lo, hi: (captured.update(args=(lo, hi)), lo)[1],
    )

    dag = DAG("jitter1")

    @dag.stage(retries=2)
    async def s_flaky(ctx):
        if ctx.attempt == 1:
            raise RetryableError("boom")
        return {"ok": 1}

    rt = Runtime()
    r = await rt.run(dag, "j1")
    assert r.status == "done"
    assert captured["args"] == (0, 1), f"期望 uniform(0, 1), 实际 {captured.get('args')}"
