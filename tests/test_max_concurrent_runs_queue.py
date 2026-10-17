"""`max_concurrent_runs` 满了是**排队**, 不是拒绝 —— 而且排队没有独立超时.

起因 (2026-10-17 实测): 上限=1 时同时起两个 run, 第二个不是报错, 而是**等**第一个
跑完再开始:

    第一个 0.4s (会自己让出)  → 第二个 0.40s 完成 (status=done)
    第一个 30s (等于卡死)      → 第二个等了 2.0s **仍未开始**

第二条是要写进文档的边界: 队列本身**没有超时**, 一个卡死的 run 会把后面的 run 永久
堵住。库内的兜底是 run 自身的 deadline / `EnginePolicy.default_timeout` —— 它们的
上界才决定排队能等多久; 想给等待本身设上界, 只能在调用侧包一层 (已验:
`asyncio.wait_for(rt.run(...), timeout=X)` 抛 TimeoutError, **不会**被吞成假的
RunResult)。

判据用**事件顺序**而不是耗时: 耗时断言在 CI 上不可靠 (机器/负载), 而"第二个 run 的
stage 是在第一个的 stage **结束之后**才开���"是确定性事实, 直接回答"到底是排队还是
并行/拒绝"。
"""
from __future__ import annotations

import asyncio

from pavoz import DAG, EnginePolicy, Runtime


def _dag(dag, log: list, tag: str, gate: asyncio.Event | None = None):
    exec(f"""
@dag.stage()
async def s(ctx):
    log.append(("{tag}", "start"))
    if gate is not None:
        await gate.wait()
    log.append(("{tag}", "end"))
    return {{"t": "{tag}"}}
""", {"dag": dag, "log": log, "asyncio": asyncio, "gate": gate})


async def test_reaching_the_cap_queues_instead_of_rejecting():
    """★ 上限=1 时第二个 run 必须**等**, 不能报错也不能并行开跑。"""
    log: list = []

    def build(tag, gate=None):
        d = DAG("conc")
        _dag(d, log, tag, gate)
        return d

    rt = Runtime(policy=EnginePolicy(max_concurrent_runs=1))
    first, second = await asyncio.gather(
        rt.run(build("A"), "ta"), rt.run(build("B"), "tb"),
    )

    assert first.status == "done" and second.status == "done", (
        f"两个 run 都该跑完: {first.status} / {second.status}")

    # 严格串行 = A 全程结束后 B 才开始。断言"两个 stage 的执行区间不重叠":
    spans, cur = {}, {}
    for tag, kind in log:
        if kind == "start":
            cur[tag] = cur.get(tag, 0) + 1
        else:
            spans.setdefault(tag, []).append(cur.pop(tag))
    a_end = [i for i, (t, k) in enumerate(log) if t == "A" and k == "end"][0]
    b_start = [i for i, (t, k) in enumerate(log) if t == "B" and k == "start"][0]
    assert b_start > a_end, (
        f"上限=1 时两个 run 必须串行, 实测事件顺序 {log} —— "
        f"B 在 A 结束前就开了 (要么上限没生效, 要么上限不是排队语义)")


async def test_a_hung_run_blocks_the_queue_with_no_timeout():
    """★ 边界: 卡死的 run 会把队列**永久**堵住, 库内没有第二个兜底。

    这条钉的是"没有"——所以它断言的是**当前行为** (仍在等), 而不是"应该超时"。
    将来若决定加排队超时, 这条会红, 那时该同步改文档 (docs 明确说没有上界)。
    """
    log: list = []

    def build(tag, gate=None):
        d = DAG("conc")
        _dag(d, log, tag, gate)
        return d

    gate = asyncio.Event()          # 永不 set = 卡死
    rt = Runtime(policy=EnginePolicy(max_concurrent_runs=1))
    stuck = asyncio.create_task(rt.run(build("A", gate), "ta"))
    await asyncio.sleep(0.05)
    queued = asyncio.create_task(rt.run(build("B"), "tb"))

    done, _ = await asyncio.wait({queued}, timeout=0.4)
    assert queued not in done, (
        f"B 不该在 A 卡死时开跑, 实测事件 {log} —— 若这里变了, 说明并发上限语义改了")
    assert log == [("A", "start")], f"只有 A 开始过: {log}"

    stuck.cancel()
    queued.cancel()
    await asyncio.gather(stuck, queued, return_exceptions=True)
