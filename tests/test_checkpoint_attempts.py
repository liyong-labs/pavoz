"""checkpoint 的 per-attempt 审计轨迹 (0.5.9, 2026-10-18 竞品调研后引入).

调研结论 (notes/2026-10-18-naobao-competitor-survey.md §1.1): zeroflow 把每次
step 的 `status / ts / error / duration_ms` 随 snapshot 落盘, 于是"哪个 stage 慢、
上次为什么失败"在 run 结束后仍可回答。pavoz 此前只有 `stage_statuses` (每 stage
一个终态) + `stage_ts` (完成时刻) — 每次 attempt 的耗时与错误只存在于:

  · 事件流 (回调消费完即弃, 不持久化), 与
  · `RunResult.stage_timings` (只在内存结果里, resume 后拿不到上一轮).

⚠️ 判据必须从**落盘的 checkpoint 读回来**核对。从 `RunResult` 或事件流断言会让
"加了字段但 `_save_cp` 没带上" 这种最可能的实现错误全绿 —— 而那正是本特性要
解决的"run 结束后没有证据"。
"""

from __future__ import annotations

import asyncio
import json

import pavoz.cli as cli
from pavoz import (
    DAG,
    Checkpoint,
    CheckpointStore,
    EnginePolicy,
    FileStorage,
    RetryableError,
    Runtime,
    StageError,
)
from pavoz.checkpoint import workflow_hash


def _store(tmp_path) -> CheckpointStore:
    return CheckpointStore(FileStorage(str(tmp_path / "store")))


def _fast_retry_runtime(store: CheckpointStore) -> Runtime:
    """backoff_max=0: 记录的是 attempt 耗时, 测试不该真的等 jitter 退避."""
    return Runtime(checkpoint_store=store, policy=EnginePolicy(backoff_max=0))


async def test_retry_then_success_records_every_attempt(tmp_path):
    """★ 一次成功的 run 里, 前两次失败 attempt 的记录也必须留在 cp 里."""
    n = {"i": 0}
    dag = DAG("attempts")

    @dag.stage(retries=2)
    async def s(ctx):
        n["i"] += 1
        if n["i"] < 3:
            raise RetryableError(f"boom-{n['i']}")
        return {"ok": True}

    store = _store(tmp_path)
    result = await _fast_retry_runtime(store).run(dag, "T")
    assert result.status == "done"

    recs = store.load_latest("T").stage_attempts["s"]
    assert [r["status"] for r in recs] == ["retry", "retry", "done"]
    assert [r["attempt"] for r in recs] == [1, 2, 3]
    assert [r["visit"] for r in recs] == [1, 1, 1]
    assert [r["error"] for r in recs] == ["boom-1", "boom-2", None]
    assert all(isinstance(r["duration_ms"], int) and r["duration_ms"] >= 0
               for r in recs)
    assert recs[0]["ts"] <= recs[1]["ts"] <= recs[2]["ts"]
    # 只存 stage_statuses + stage_ts 时, 下面这两条信息在 run 结束后就没了:
    assert "boom-1" in recs[0]["error"]
    assert recs[0]["duration_ms"] >= 0


async def test_failed_attempt_error_is_truncated_not_dropped(tmp_path):
    """超长异常消息截断到上限 —— 截断 (保留开头) 而不是整条丢掉."""
    dag = DAG("trunc")

    @dag.stage()
    async def s(ctx):
        raise StageError("x" * 2000)

    store = _store(tmp_path)
    result = await Runtime(checkpoint_store=store).run(dag, "T")
    assert result.status == "failed"

    rec = store.load_latest("T").stage_attempts["s"][0]
    assert rec["status"] == "failed"
    assert rec["error"] == "x" * 500
    # 完整消息仍在调用方可见的 RunResult 里 (审计字段的截断不影响诊断入口).
    assert result.error == "x" * 2000


async def test_resume_appends_to_prior_attempts(tmp_path):
    """★ resume 续跑: 上一轮失败的 attempt 不能被新一轮覆盖."""
    flag = {"fail": True}
    dag = DAG("resume-attempts")

    @dag.stage()
    async def s(ctx):
        if flag["fail"]:
            raise StageError("first run fails")
        return {"ok": True}

    store = _store(tmp_path)
    rt = Runtime(checkpoint_store=store)
    first = await rt.run(dag, "T")
    assert first.status == "failed"

    flag["fail"] = False
    second = await rt.run(dag, "T", resume=True)
    assert second.status == "done"

    recs = store.load_latest("T").stage_attempts["s"]
    assert [r["status"] for r in recs] == ["failed", "done"]
    assert recs[0]["error"] == "first run fails"


async def test_loop_visits_are_distinguished(tmp_path):
    """循环图里同一 stage 多轮执行: 记录要能按 visit 分开 (不能混成一条)."""
    from tests.test_loop_reentry import build_pipeline

    dag, _calls = build_pipeline()          # s_fetch 会被 redo 回跳跑两轮
    store = _store(tmp_path)
    result = await Runtime(checkpoint_store=store).run(dag, "T")
    assert result.status == "done"

    recs = store.load_latest("T").stage_attempts["s_fetch"]
    assert [(r["visit"], r["status"]) for r in recs] == [(1, "done"), (2, "done")]


def test_old_checkpoint_without_attempts_loads_as_empty(tmp_path):
    """0.5.8 及更早的 cp 没有这个字段 — 读它必须是空, 不能 KeyError."""
    dag = DAG("old")

    @dag.stage()
    async def s(ctx):
        return {"x": 1}

    cp = Checkpoint(
        task_id="t", run_id="r", dag_name=dag.name,
        workflow_hash=workflow_hash(dag),
        stage_statuses={"s": "done"}, done_stages=["s"],
        stage_deltas_visits={"s": [{"x": 1}]},
    )
    d = cp.to_dict()
    assert d["stage_attempts"] == {}
    d.pop("stage_attempts")                 # 模拟旧版落盘
    loaded = Checkpoint.from_dict(d)
    assert loaded.stage_attempts == {}
    assert loaded.state == {"x": 1}         # 审计字段缺失不影响 state 重建


def test_attempts_roundtrip_and_are_independent_copies():
    cp = Checkpoint(
        task_id="t", run_id="r", dag_name="d", workflow_hash="h",
        stage_statuses={}, done_stages=[],
        stage_attempts={"s": [{"visit": 1, "attempt": 1, "status": "retry",
                               "ts": 1.5, "duration_ms": 7, "error": "e"}]},
    )
    loaded = Checkpoint.from_dict(cp.to_dict())
    assert loaded.stage_attempts == cp.stage_attempts
    loaded.stage_attempts["s"][0]["status"] = "mutated"
    assert cp.stage_attempts["s"][0]["status"] == "retry"


def test_trace_exposes_stage_attempts(tmp_path, capsys, monkeypatch):
    """CLI 层: 落盘记录要能被 `trace` 直接看到 (否则只有写 Python 的人才拿得到)."""
    root = tmp_path / "store"
    monkeypatch.setenv("PAVOZ_STORAGE_SPEC", "pavoz.storage:FileStorage")
    monkeypatch.setenv("PAVOZ_STORAGE_KWARGS", json.dumps({"root_dir": str(root)}))
    monkeypatch.setenv("PAVOZ_STORAGE", str(root))

    dag = DAG("trace-attempts")

    @dag.stage()
    async def s(ctx):
        return {"a": 1}

    asyncio.run(Runtime(checkpoint_store=CheckpointStore(FileStorage(str(root))))
                .run(dag, "T"))
    rc = cli.main(["trace", "--task-id", "T"])
    out, _err = capsys.readouterr()
    assert rc == 0, out
    data = json.loads(out)
    recs = data["stage_attempts"]["s"]
    assert len(recs) == 1
    rec = recs[0]
    assert (rec["visit"], rec["attempt"], rec["status"], rec["error"]) == \
        (1, 1, "done", None)
    assert isinstance(rec["duration_ms"], int)
    assert isinstance(rec["ts"], (int, float))
