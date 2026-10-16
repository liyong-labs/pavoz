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
import time

import pytest

from pavoz import (
    DAG,
    CheckpointStore,
    EnginePolicy,
    EventRecorder,
    FileStorage,
    RetryableError,
    Runtime,
)


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


async def test_max_concurrent_runs_also_bounds_run_stage():
    """并发闸必须管到单 stage 重放 —— 曾只有 run() 走闸门。

    这个策略的用途是护住**共享下游** (限流 / 连接池), 而重放打的是同一批
    stage、同样的外部调用。实测 (修复前): 同一 policy 下 3 个 run 串行
    (peak=1), 3 个重放却全并发 (peak=3) —— 上限静默失效。
    """
    active = {"n": 0, "peak": 0}
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))

    def _dag(name: str) -> DAG:
        dag = DAG(name)

        @dag.stage()
        async def s_a(ctx):
            return {"a": 1}

        @dag.stage(depends_on=["s_a"])
        async def s_work(ctx):
            active["n"] += 1
            active["peak"] = max(active["peak"], active["n"])
            await asyncio.sleep(0.05)
            active["n"] -= 1
            return {"w": 1}

        return dag

    rt = Runtime(policy=EnginePolicy(max_concurrent_runs=1), checkpoint_store=store)
    dags = [_dag(f"sem{i}") for i in range(3)]
    for dag in dags:                       # 各造一个 s_work 已完成的 cp 供重放
        assert (await rt.run(dag, dag.name)).status == "done"

    active["peak"] = 0
    await asyncio.gather(*(
        rt.run_stage(dag, dag.name, "s_work") for dag in dags
    ))

    assert active["peak"] == 1, (
        f"重放绕过了 max_concurrent_runs (实测 peak={active['peak']}, 上限 1) —— "
        "该策略正是为护住共享下游而设, 静默失效等于没有")


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

# ── 构造期字段校验 (2026-10-16 巡检) ───────────────────────────────────────
#
# EnginePolicy 是 frozen dataclass, 此前**不校验任何字段**, 而调用点一律写
# `if policy.x` —— 于是 nonsense 取值不会报错, 只会在运行期变成怪现象:

#   EnginePolicy(stage_alive_interval=-0.001)
#     → asyncio.sleep(负数) 立即返回 → 心跳变忙循环
#     → 实测: 0.2s 的 stage 喷出 58099 条 stage_alive, run 仍 status=done
#       (CPU 空转 + 事件流/日志/内存三重放大, 全程无提示)
#
#   EnginePolicy(max_concurrent_runs=0)  → 0 假值 → "不限", 与字面意思相反
#   EnginePolicy(max_steps=0)            → 第一个 stage 立刻熔断

@pytest.mark.parametrize("kwargs", [
    {"stage_alive_interval": -0.001},
    {"stage_alive_interval": -30},
    {"default_timeout": -1},
    {"backoff_max": -1},
])
def test_negative_interval_like_fields_rejected_at_construction(kwargs):
    with pytest.raises(ValueError, match="不能为负"):
        EnginePolicy(**kwargs)


@pytest.mark.parametrize("field", ["max_steps", "max_concurrent_runs"])
def test_zero_limit_is_rejected_and_none_is_the_way_to_unlimited(field):
    """0 是假值 → 调用点 `if policy.x` 当成"没设"。'不限'必须显式写 None。"""
    with pytest.raises(ValueError, match="必须 >= 1"):
        EnginePolicy(**{field: 0})
    assert getattr(EnginePolicy(**{field: None}), field) is None


@pytest.mark.parametrize("kwargs", [
    {},                                            # 全默认
    {"default_timeout": 0.2},
    {"max_steps": 1},
    {"backoff_max": 0},                             # 不退避是合法用法
    {"stage_alive_interval": None},                 # 关心跳是合法用法
    {"skip_unchanged": True},
])
def test_sane_values_still_construct(kwargs):
    """反向约束: 校验不许误伤任何合法取值。"""
    EnginePolicy(**kwargs)


async def test_negative_interval_busy_loop_is_now_unreachable(tmp_path):
    """钉住实测过的后果本身, 不只是构造器的报错文案。

    正间隔下心跳条数必须是"约等于 stage 时长 / interval"量级 —— 曾经的忙循环
    (负间隔) 会让这个数字爆炸到五位数。
    """
    dag = DAG("hb_bound")

    @dag.stage()
    async def s_slow(ctx):
        await asyncio.sleep(0.2)
        return {"a": 1}

    rec = EventRecorder()
    rt = Runtime(policy=EnginePolicy(stage_alive_interval=0.05), on_event=rec,
                 checkpoint_store=CheckpointStore(FileStorage(tmp_path / "cp")))
    r = await rt.run(dag, "hb")

    assert r.status == "done"
    alive = [1 for name, _ in rec.events if name == "stage_alive"]
    assert len(alive) <= 10, f"心跳条数失控 ({len(alive)} 条) — 又变忙循环了?"


# ── default_timeout=0: 假值陷阱 (2026-10-16) ────────────────────────────────
#
# 此前 default_timeout 只拒**负数**, 于是 `0` 被放行 —— 而调用点是
# `run_deadline = time.time() + _to if _to else None` (真值判断), 0 被当成
# "没设" ⇒ **run 级超时护栏静默消失**。实测: 一个跑 0.3s 的 stage,
# default_timeout=0 跑满 0.30s 报 done, 而 0.1 报 TimeoutError。
#
# 而 default_timeout 的**全部意义**就是给 LLM 调用封顶。"0 = 完全不管" 是最坏的
# 失效方向: 用户以为护栏在, 实际 LLM 可以无限烧 —— 比报错危险得多。
#
# 两条入口都要堵: `EnginePolicy(default_timeout=0)` (policy 侧) 与
# `Runtime(default_timeout=0)` (显式侧, `_effective_timeout` 里它优先)。

def test_default_timeout_zero_is_rejected():
    """policy 侧: 0 必须报, 而不是静默变成"不限"。"""
    with pytest.raises(ValueError, match="default_timeout 不能为 0"):
        EnginePolicy(default_timeout=0)


def test_runtime_default_timeout_zero_is_rejected():
    """显式侧同样堵 —— 它不走 policy, 却是 policy 的优先级更高的那个来源。"""
    with pytest.raises(ValueError, match="default_timeout 不能为 0"):
        Runtime(default_timeout=0)


@pytest.mark.parametrize("t", [0.001, 0.2, 0.5, 5, 30])
def test_subsecond_and_normal_timeouts_still_legal(t):
    """反向约束: **只拒 0**, 亚秒超时完全合法。

    这条挡的是一个真实踩过的坑: 我第一版把 `default_timeout` 并进
    `max_steps`/`max_concurrent_runs` 那一组判 `< 1`, 结果 `0.2` 也被拒 ——
    而本文件里就有 `EnginePolicy(default_timeout=0.2)` 的既有用例, 亚秒超时是
    常见的正当需求 (LLM 首 token 延迟就是这么卡的)。
    """
    assert EnginePolicy(default_timeout=t).default_timeout == t
    assert Runtime(default_timeout=t).default_timeout == t


def test_none_still_means_unlimited():
    assert EnginePolicy(default_timeout=None).default_timeout is None
    assert Runtime(default_timeout=None).default_timeout is None


async def test_stage_timeout_zero_is_a_real_value_not_silence():
    """★ 覆盖边界: `@dag.stage(timeout=0)` 与 run 级的 0 **行为相反, 这是对的**。

    两个入口读 0 的方式不同:
      · stage 级 `if stage.timeout is not None` → 0 是**真值设置**, 于是超时生效
      · run 级 `run_deadline = now + eff_timeout if eff_timeout else None` → 0 是
        **没设**, 于是 run 级护栏整个消失

    这次修的是 run 级那个 (它才是护栏, 且 `default_timeout` 的全部意义就是给
    LLM 调用封顶)。stage 级不需要动 —— 它的 0 已经按"真值设置"处理了。

    ⚠️ stage 级的 0 并**不是**"立即超时": `_with_timeout` 里
    `timeout = max(0.1, deadline - now)` 有 0.1s 下限, 所以 fn 会真的启动、
    只是被 0.1s 打断。我最初按"立即"写断言 (fn 不该被执行), 实测失败 —— 读代码
    才发现是这个下限。**这正是本仓反复交的学费: 先读代码确认机制, 再造形状。**

    这条用例防的是下一个人"顺手统一": 统一到真值判断会让
    `@dag.stage(timeout=0)` 静默变成不限 (本用例的 status 断言直接红);
    统一到 `is not None` 会让 run 级 0 变成"整个 run 立刻 failed"。
    **两个方向都改变既有可用语义。**
    """
    dag = DAG("stage_zero")

    @dag.stage(timeout=0)
    async def s_a(ctx):
        await asyncio.sleep(30)          # 若 0 被当成"没设", 这里会真跑 30s
        return {"a": 1}

    t0 = time.time()
    r = await Runtime().run(dag, "z0")
    elapsed = time.time() - t0

    assert r.status == "failed", (
        f"stage 级 timeout=0 应当'生效' (failed), 实得 {r.status} —— 若哪天变成 "
        f"done, 说明 `is not None` 被改成了真值判断, 那会让 0 静默变成'不限'")
    assert r.error_class == "TimeoutError"
    # 有界: 0.1s 下限之内。写 30s 的 sleep 就是为了让"变成不限"这件事**慢到能测出来**。
    assert elapsed < 5, f"timeout=0 应在 0.1s 内被打断, 实耗 {elapsed:.2f}s"
