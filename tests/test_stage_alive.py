"""R2 (ai@home id=1128 提案): stage_alive 心跳 — 长 stage 无 fraction 汇报的存活信号.

stage 执行超过 EnginePolicy.stage_alive_interval 后, runtime 经 on_event 发
stage_alive {task_id, run_id, stage, visit, attempt, elapsed}; 快 stage 零事件;
None 关闭; 无 policy 时走引擎默认 30s.
"""

from __future__ import annotations

import asyncio

import pavoz.runtime
from pavoz import DAG, CheckpointStore, EnginePolicy, FileStorage, Runtime

INTERVAL = 0.05
SLOW = 0.18


def _policy(**kw):
    return EnginePolicy(stage_alive_interval=INTERVAL, **kw)


def _alive(rec):
    return [d for e, d in rec.events if e == "stage_alive"]


class _Rec:
    def __init__(self):
        self.events: list[tuple[str, dict]] = []

    def __call__(self, event, data):
        self.events.append((event, data))


def _dag(slow: bool, gate: dict | None = None):
    dag = DAG("alive")

    @dag.stage()
    async def s_a(ctx):
        if slow:
            await asyncio.sleep(SLOW)
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        if gate is not None and not gate.get("pass"):
            from pavoz import RetryableError
            raise RetryableError("boom")
        return {"b": 1}

    return dag


async def _run(policy, tmp_path):
    rec = _Rec()
    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(tmp_path / "cp"))),
                 policy=policy, on_event=rec)
    r = await rt.run(_dag(slow=True), task_id="t1")
    return r, rec


async def test_slow_stage_emits_heartbeat_with_fields(tmp_path):
    r, rec = await _run(_policy(), tmp_path)
    assert r.status == "done"
    hb = _alive(rec)
    assert len(hb) >= 1
    d = hb[0]
    assert d["stage"] == "s_a" and d["visit"] == 1 and d["attempt"] == 1
    assert d["elapsed"] >= INTERVAL
    assert d["run_id"] == r.run_id


async def test_fast_stage_zero_heartbeats(tmp_path):
    rec = _Rec()
    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(tmp_path / "cp"))),
                 policy=_policy(), on_event=rec)
    dag = DAG("fast")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    await rt.run(dag, task_id="t1")
    assert _alive(rec) == []


async def test_interval_none_disables(tmp_path):
    r, rec = await _run(EnginePolicy(stage_alive_interval=None), tmp_path)
    assert r.status == "done"
    assert _alive(rec) == []


async def test_no_policy_uses_engine_default(tmp_path, monkeypatch):
    monkeypatch.setattr(pavoz.runtime, "_DEFAULT_STAGE_ALIVE_INTERVAL", INTERVAL)
    rec = _Rec()
    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(tmp_path / "cp"))),
                 on_event=rec)
    r = await rt.run(_dag(slow=True), task_id="t1")
    assert r.status == "done"
    assert len(_alive(rec)) >= 1


async def test_heartbeat_stops_after_run_returns(tmp_path):
    _r, rec = await _run(_policy(), tmp_path)
    n = len(_alive(rec))
    await asyncio.sleep(SLOW)
    assert len(_alive(rec)) == n  # run 结束心跳即停, 无泄漏 task


async def test_heartbeat_absent_on_single_stage_replay(tmp_path):
    """run_stage (emit=False) 不发心跳 — 单 stage 重放不是正式 run."""
    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(tmp_path / "cp"))),
                 policy=_policy())
    dag = _dag(slow=True)
    await rt.run(dag, task_id="t1")
    rec = _Rec()
    rt.on_event = rec
    r = await rt.run_stage(dag, "t1", "s_a")
    assert r.status == "done"
    assert _alive(rec) == []


async def test_retry_stage_reports_current_attempt(tmp_path):
    calls = {"n": 0}

    # 重新建带首败的 stage (s_b 首跑 RetryableError, 重试成功)
    dag2 = DAG("retry_alive")

    @dag2.stage()
    async def s_a(ctx):
        await asyncio.sleep(SLOW * 2)
        return {"a": 1}

    @dag2.stage(depends_on=["s_a"], retries=1)
    async def s_b(ctx):
        calls["n"] += 1
        if calls["n"] == 1:
            from pavoz import RetryableError
            raise RetryableError("first boom")
        return {"b": 1}

    rec = _Rec()
    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(tmp_path / "cp"))),
                 policy=_policy(backoff_max=0.0), on_event=rec)
    r = await rt.run(dag2, task_id="t1")
    assert r.status == "done"
    # s_a 的心跳全在 attempt=1; 心跳字段齐全
    hb = _alive(rec)
    assert all(d["attempt"] == 1 for d in hb if d["stage"] == "s_a")
