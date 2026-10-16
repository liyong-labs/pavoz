"""事件流必须**成对**: 每个 `stage_start` 都有配对的 `stage_end`。

事件流是监控的输入。消费方按 `start → end` 配对算 stage 耗时/成功率/in-flight,
所以"孤儿事件"不是脏数据, 是**错的统计结果**:

- 孤儿 **end** (有 end 无 start) → 凭空多出一次"瞬间完成", 而那个 stage 根本没跑
- 孤儿 **start** (有 start 无 end) → 那个 stage 永远停在 in-flight, 超时告警反复
  响, 而 runtime 侧其实早已把它标成 cancelled 并落了 cp

2026-10-16 实测: 正常 / stage 失败 / max_steps 熔断 / run_stage 四条路径都配对,
但**两条取消路径各漏一半, 方向正好相反**:

| 路径 | 原状 |
|------|------|
| graceful (stage 边界拦截) | ❌ 孤儿 end —— 只发了 `stage_end`, 没发 `stage_start` |
| hard (中途注入 CancelledError) | ❌ 孤儿 start —— `stage_start` 早发出去了, 取消时直接跳到 `save_cp`/`run_end` |

这两处都在 cancel 相关代码附近, 而本会话已反复改动那一段 —— 改控制流时最容易
漏掉的就是"事件流是否还自洽", 而它没有任何测试在守。
"""

from __future__ import annotations

import asyncio
import tempfile

import pytest

from pavoz import (
    DAG,
    CancelRegistry,
    CheckpointStore,
    EventRecorder,
    FileStorage,
    Runtime,
    StageError,
)


def _rt(rec: EventRecorder, *, reg: CancelRegistry | None = None) -> Runtime:
    return Runtime(checkpoint_store=CheckpointStore(
        FileStorage(tempfile.mkdtemp())), on_event=rec, cancel_registry=reg)


def _pairs(rec: EventRecorder) -> tuple[list, list]:
    """(starts, ends) 按 (stage, visit, attempt) 三元组收集, 顺序保持。"""
    starts, ends = [], []
    for ev, d in rec.events:
        if ev == "stage_start":
            starts.append((d["stage"], d["visit"], d["attempt"]))
        elif ev == "stage_end":
            ends.append((d["stage"], d["visit"], d["attempt"]))
    return starts, ends


def _assert_paired(rec: EventRecorder, label: str) -> None:
    starts, ends = _pairs(rec)
    assert starts == ends, (
        f"{label}: 事件流不成对\n"
        f"       starts={starts}\n"
        f"       ends  ={ends}\n"
        f"       有 start 无 end = {[x for x in starts if starts.count(x) > ends.count(x)]}\n"
        f"       有 end 无 start = {[x for x in ends if ends.count(x) > starts.count(x)]}")


def _slow_dag(name: str) -> DAG:
    dag = DAG(name)

    @dag.stage()
    async def s_a(ctx):
        await asyncio.sleep(0.3)
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 2}

    return dag


# ── 正常路径 (基线: 这些本来就是对的, 一起钉住防止回归) ───────────────────

async def test_normal_run_pairs_events():
    rec = EventRecorder()
    r = await _rt(rec).run(_slow_dag("ev_ok"), "T")
    assert r.status == "done"
    _assert_paired(rec, "正常 done")


async def test_failed_stage_pairs_events():
    dag = DAG("ev_fail")

    @dag.stage()
    async def s_a(ctx):
        raise StageError("炸", category="SEARCH_NO_RESULT")

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 2}

    rec = EventRecorder()
    r = await _rt(rec).run(dag, "T")
    assert r.status == "failed"
    _assert_paired(rec, "stage 抛异常")


# ── 两条取消路径 (本文件的主角) ─────────────────────────────────────────────

@pytest.mark.parametrize("mode", ["graceful", "hard"])
async def test_cancelled_run_pairs_events(mode):
    """★ 两条取消路径的 start/end 都必须配对。

    graceful: 拦截发生在 attempt **之前**, 那个 stage 根本没开始 —— 但事件流里既
    发了 `stage_end` 就不该缺 `stage_start` (消费方是按配对算指标的, 不是按"有没有
    真的跑过")。

    hard: `stage_start` 在 stage 真正开始执行时就发了, 取消时必须补上 `stage_end`,
    否则它永远停在 in-flight。
    """
    rec = EventRecorder()
    reg = CancelRegistry()
    rt = _rt(rec, reg=reg)

    t = asyncio.create_task(rt.run(_slow_dag(f"ev_{mode}"), "T"))
    await asyncio.sleep(0.02)          # 确保 s_a 已在执行
    reg.cancel("T", mode)
    r = await t

    assert r.status == "cancelled", f"前置条件: [{mode}] 应取消, 实得 {r.status}"
    _assert_paired(rec, f"{mode} 取消")

    # 至少要有一个 end 的 status 是 cancelled —— 取消确实发生了, 事件流得说出来。
    # ⚠️ 不能断言"每个 end 都是 cancelled": graceful 取消时 s_a 已经跑完了, 它那条
    # end 的 status 当然是 done (我第一版写得太宽, 自己先红了)。
    cancelled_ends = [
        d for ev, d in rec.events
        if ev == "stage_end" and d["status"] == "cancelled"
    ]
    assert cancelled_ends, (
        f"[{mode}] 事件流里没有任何 status=cancelled 的 stage_end —— "
        f"消费方从事件流看不出哪个 stage 被取消了:\n"
        + "\n".join(f"  {ev} {d.get('stage')} {d.get('status')}"
                    for ev, d in rec.events if ev.startswith('stage_')))

    # 每个 end 都带 status + duration + error —— 字段集与正常路径一致
    for ev, d in rec.events:
        if ev == "stage_end":
            assert {"status", "duration", "error"} <= set(d), (
                f"[{mode}] stage_end 缺字段, 与正常路径不一致: {sorted(d)}")


async def test_hard_kill_does_not_leave_stage_in_flight():
    """★ hard 取消的语义钉法: 事件流里那个 stage **已经结束**了。

    这条单独写是因为它是修复前真正坏掉的东西: 监控按配对算 in-flight 时, 修复前
    这个 stage 会**永远**挂着, 超时告警反复响, 而 runtime 早已把它标成 cancelled
    并落了 cp —— 两边说法不一致。
    """
    rec = EventRecorder()
    reg = CancelRegistry()
    rt = _rt(rec, reg=reg)

    t = asyncio.create_task(rt.run(_slow_dag("ev_inflight"), "T"))
    await asyncio.sleep(0.02)
    reg.cancel("T", "hard")
    r = await t

    ends = [d for ev, d in rec.events if ev == "stage_end" and d["stage"] == "s_a"]
    assert ends, "hard 取消必须为被中断的 stage 补 stage_end"
    assert ends[0]["status"] == "cancelled"
    # 与 runtime 自己的说法一致 (两端口径不能分叉)
    assert r.stage_statuses.get("s_a") == "cancelled", (
        f"事件说 cancelled 而 stage_statuses 说 {r.stage_statuses} —— 两边分叉")


async def test_run_end_status_matches_run_result():
    """`run_end` 恰好一条, 且 status 与 `RunResult.status` 一致 (全路径)。

    监控多半按事件流建告警, 而 run 级结论通常读返回值 —— 两端口径分叉时, 告警
    与实际结果会长期背离。
    """
    for label, mk, kwargs, expect in (
        ("done", lambda: _slow_dag("m1"), {}, "done"),
        ("cancelled", lambda: _slow_dag("m2"), {"cancel": "hard"}, "cancelled"),
    ):
        rec = EventRecorder()
        reg = CancelRegistry()
        rt = _rt(rec, reg=reg)
        if "cancel" in kwargs:
            t = asyncio.create_task(rt.run(mk(), "T"))
            await asyncio.sleep(0.02)
            reg.cancel("T", kwargs["cancel"])
            r = await t
        else:
            r = await rt.run(mk(), "T")

        run_ends = [d for ev, d in rec.events if ev == "run_end"]
        assert len(run_ends) == 1, f"[{label}] run_end 应恰好一条, 实得 {len(run_ends)}"
        assert run_ends[0]["status"] == r.status == expect, (
            f"[{label}] run_end.status={run_ends[0]['status']} "
            f"RunResult.status={r.status}")
        _assert_paired(rec, label)
