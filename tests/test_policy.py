"""W3: EnginePolicy — run 级策略外置 (additive).

第二组 (2026-10-16 巡检新增): **同一份 policy 必须在三个入口上表现一致** ——
run / run_stage (单 stage 重放) / fork_run。已发现 default_timeout 曾只在
run() 解析 (显式 Runtime 参数 → policy 回退两级), 而 run_stage() 只读
self.default_timeout, 漏掉 policy 回退 → EnginePolicy 对重放**静默失效**:
实测 policy deadline=0.2s 时 run() 超时报 failed, 同一 policy 的 run_stage()
跑满 1s 报 done。下面 `test_*_entry_points` 系列就是钉这条。
"""

import asyncio
import tempfile

from pavoz import DAG, CheckpointStore, EnginePolicy, FileStorage, RetryableError, Runtime


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


# ── default_timeout 在三个入口上的一致性 (2026-10-16 巡检) ────────────────

def _slow_dag(name: str, delay: float) -> DAG:
    """s_slow **不带 stage 级 timeout** — 只有 run 级 deadline 能拦下它.

    (若给 s_slow 挂了 timeout, 就算 run 级 deadline 失效测试也会绿 —— 那样
    就测不到 policy 回退这条路径了。)
    """
    dag = DAG(name)

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_slow(ctx):
        await asyncio.sleep(delay)
        return {"slow": True}

    return dag


async def _seed_cp(delay: float = 0.5):
    """造一个 s_slow 已完成的 task, 供 run_stage 重放 (重放路径要求 cp 存在)."""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    dag = _slow_dag("pol_replay", delay)
    r = await Runtime(checkpoint_store=store).run(dag, "t_replay")
    assert r.status == "done", f"造 cp 的那次 run 应成功, 实得 {r.status}: {r.error}"
    return store, dag


async def test_policy_default_timeout_applies_to_run_stage():
    """EnginePolicy.default_timeout 对**重放**同样生效 (曾静默失效)."""
    store, dag = await _seed_cp()

    rt = Runtime(policy=EnginePolicy(default_timeout=0.2), checkpoint_store=store)
    r = await rt.run_stage(dag, "t_replay", "s_slow")

    assert r.status == "failed", (
        "policy 的 run 级 deadline 必须管到 run_stage — 修复前它只读 "
        f"Runtime.default_timeout, policy 被忽略 (实得 {r.status})")
    assert r.retryable is True, "超时是 RetryableError 语义 (与 run() 同)"


async def test_explicit_runtime_timeout_applies_to_run_stage():
    """显式 Runtime(default_timeout=...) 对重放也生效 (反向: 修的是 policy 回退,
    不能把已生效的显式值弄丢)."""
    store, dag = await _seed_cp()

    r = await Runtime(default_timeout=0.2, checkpoint_store=store).run_stage(
        dag, "t_replay", "s_slow")

    assert r.status == "failed", f"显式 run 级 deadline 对重放必须生效 (实得 {r.status})"


async def test_run_stage_without_any_deadline_is_not_bounded():
    """无 policy 无显式值 → 重放**不**被任何 deadline 拦 (防"顺手给了个默认值")."""
    store, dag = await _seed_cp(delay=0.05)

    r = await Runtime(checkpoint_store=store).run_stage(dag, "t_replay", "s_slow")

    assert r.status == "done", f"没设 deadline 时不该超时 (实得 {r.status}: {r.error})"
