"""超时消息里的秒数必须是**真的那个数**。

起因 (2026-10-17 实测): 消息用的是 `f"{timeout:.0f}"` —— 于是 `timeout=0.3` 的
stage 超时被报成 `stage 's' 超时 (0s)`。实测: stage 睡 1.0s + `timeout=0.3` ⇒ 0.30s
就失败, 消息写 "(0s)"。

危害不在于不好看, 而在于**它把用户指向错误的方向**: 读到"0s"的人会去找"是不是超时
配成了 0"这种根本不存在的问题 (顺带一提: `timeout=0` 其实真的会立刻超时, 于是这个
误读还会"看起来对得上"), 而真实原因是**预算只有 0.3 秒、不够这个 stage 跑**。

判据分两层: 格式函数直接测 (含 >= 1 秒那档, 不必真跑长 sleep), 再加一条端到端 ——
真的让一个 0.3s 预算的 stage 超时, 断言消息里有 "0.3" 且**没有** "(0s)"。
"""
from __future__ import annotations

import asyncio

import pytest

from pavoz import DAG, Runtime
from pavoz.runtime import _fmt_secs


@pytest.mark.parametrize("given, expected", [
    (0.3, "0.3"),      # ★ 这档就是原来报成 "0" 的
    (0.1, "0.1"),      # 0.1 是 timeout 的地板值
    (0.05, "0.1"),     # 四舍五入到 1 位; 不能变成 "0"
    (0.999, "1.0"),
    (1.0, "1"),
    (5.0, "5"),
    (30.0, "30"),
    (300.0, "300"),
])
def test_fmt_secs_keeps_sub_second_precision(given, expected):
    """不足 1 秒必须带小数 —— 够 1 秒才取整, 免得长 run 日志拖一堆 `.0`。"""
    assert _fmt_secs(given) == expected


def test_fmt_secs_never_reports_zero_for_a_positive_budget():
    """★ 正面判据: 任何**正数**预算都不许显示成 0 —— 那正是原来那条消息的形状。"""
    for t in (0.05, 0.1, 0.25, 0.5, 0.75, 0.9):
        shown = _fmt_secs(t)
        assert float(shown) > 0, f"{t}s 的预算被显示成 {shown!r} —— 用户会以为配错了"


async def test_timeout_message_reports_the_real_budget():
    """端到端: 0.3s 预算的 stage 超时, 消息里必须有 0.3, 不能是 (0s)。"""
    dag = DAG("tmsg")

    @dag.stage(timeout=0.3)
    async def s_slow(ctx):
        await asyncio.sleep(5)
        return {"v": 1}

    r = await Runtime().run(dag, "tmsg")

    assert r.status == "failed"
    msg = str(r.error)
    assert "超时" in msg, f"没走到超时分支: {msg}"
    assert "0.3" in msg, f"消息没写出真实预算 0.3: {msg}"
    assert "(0s)" not in msg, f"消息又报成 0s: {msg}"


async def test_timeout_message_for_a_long_budget_stays_readable():
    """对照: >= 1 秒那档取整显示 (不拖 `.0`), 且不含多余的 `.0`。"""
    dag = DAG("tmsg2")

    @dag.stage(timeout=0.3)
    async def s_slow(ctx):
        await asyncio.sleep(5)
        return {"v": 1}

    r = await Runtime(default_timeout=5.0).run(dag, "tmsg2")
    # run 级 deadline 5s 先到 → 消息里的数是 run 级预算 (取整那档)
    assert r.status == "failed"
    assert ".0s" not in str(r.error), f"长预算不该拖 .0: {r.error}"
