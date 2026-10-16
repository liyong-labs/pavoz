"""取消标志的生命周期 —— 它污染的是**下一个** run, 不是自己那个。

两处修动的回归防线, 都在 2026-10-16 实测复现过:

1. **run 正常收场时清掉竞态残留的标志**。取消请求在**最后一个** stage 执行期间
   到达时, 那个 stage 之后没有下一次 attempt 的边界检查 ⇒ run 正常 `done`, 而
   标志留下了。下一个用同一 task_id 的 run **一个 stage 都没跑**就被判 cancelled。
   而上一轮的日志还写着"已落 cp, resume 可续跑"。

2. **`CancelledError` 只有本进程真的发过 `t.cancel()` 才转 RunResult**。
   registry 硬杀和**外部**取消 (`asyncio.wait_for` 超时 / 调用方 `t.cancel()` /
   loop 关闭) 走的是同一个异常, `CancelledError` 本身不带"谁发起的"信息。判据
   用"有没有取消标志"会被**遗留标志**骗过 —— 上一轮 graceful cancel 设的标志还
   在, 于是本轮的外部超时被吞成 `RunResult(status="cancelled")`,
   **`TimeoutError` 不抛**。超时保护通常是防止 run 无限挂着的最后一道防线。

⚠️ 两个形状都**只能靠精心排时序**造出来, 而且第一版都造错了 —— 记在这儿免得
重走:
- 第 1 条第一版让 cancel 打在 run 收尾**之后** (status=done), 但那个残留是
  **caller** 的竞态造成的, run 的 finally 已经跑过, 清不到 —— 不是这条要修的。
  正确的窗口是 cancel 在**最后一个 stage 执行期间**到达。
- 第 2 条第一版用"遗留标志 + 直接 wait_for", 以为复现了"异常被吞", 实际上
  那条 run 在**边界检查**处就 graceful 返回了, 根本没到 `CancelledError`。
  机制确认后才造出真正可达的形状: graceful cancel 在 run **已进入 stage 之后**
  到达 (这是 graceful cancel 的正常工作方式), 随后外部再取消。
"""

from __future__ import annotations

import asyncio

import pytest

from pavoz import DAG, CheckpointStore, FileStorage, Runtime
from pavoz.cancel import CancelRegistry, NotKillable


def _one_stage(delay: float, name: str = "cx") -> DAG:
    """只有一个 stage —— 于是 cancel 在它执行期间到达时, 之后没有下一次边界检查."""
    dag = DAG(name)

    @dag.stage()
    async def s_a(ctx):
        await asyncio.sleep(delay)
        return {"a": 1}

    return dag


def _two_stage(delay: float, name: str = "cx2") -> DAG:
    dag = DAG(name)

    @dag.stage()
    async def s_a(ctx):
        await asyncio.sleep(delay)
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 2}

    return dag


def _rt(tmp_path, reg):
    return Runtime(checkpoint_store=CheckpointStore(
        FileStorage(tmp_path / "cp")), cancel_registry=reg)


# ── 1. 正常收场不留残留 ─────────────────────────────────────────────────────

async def test_normal_finish_does_not_leak_flag_to_next_run(tmp_path):
    """★ cancel 在**最后一个** stage 执行期间到达 → run 正常 done, 标志须清。

    旧行为: 标志留下, 下一个同 task_id 的 run 一个 stage 都没跑就 cancelled。
    """
    reg = CancelRegistry()
    rt = _rt(tmp_path, reg)

    task = asyncio.create_task(rt.run(_one_stage(0.05, "leak"), "leak"))
    await asyncio.sleep(0.01)          # 确认 s_a 正在执行
    reg.cancel("leak", "graceful")     # 取消请求在 stage 期间到达
    r1 = await task

    assert r1.status == "done", (
        f"前置条件: 该 stage 之后没有下一次边界检查, run 应正常完成, "
        f"实得 {r1.status}")
    assert not reg.is_cancelled("leak"), (
        "run 正常收场却带着取消标志 —— 纯竞态残留, 会污染下一个 run")

    r2 = await rt.run(_one_stage(0.0, "leak"), "leak")
    assert r2.status == "done", (
        f"上一个 run 正常结束, 不该影响新 run, 实得 {r2.status}")
    assert r2.state == {"a": 1}


async def test_cancelled_run_still_keeps_its_flag(tmp_path):
    """反向约束: 被取消的 run **保留**标志 —— 那是 caller 明确表达过的意图。

    修复只能清"自己没被取消却带着标志"的残留。若这里也清, "取消这个 task"就
    变成了"取消当前这一次 run", 语义缩水。
    """
    reg = CancelRegistry()
    rt = _rt(tmp_path, reg)

    t = asyncio.create_task(rt.run(_two_stage(0.2, "keep"), "keep"))
    await asyncio.sleep(0.02)
    reg.cancel("keep", "hard")
    r1 = await t

    assert r1.status == "cancelled"
    assert reg.is_cancelled("keep"), "被取消的 run 必须保留标志 (caller 的意图)"

    # 不 reset 直接复用 → 仍然被取消, 这是既有契约
    r2 = await rt.run(_one_stage(0.0, "keep"), "keep")
    assert r2.status == "cancelled", "未 reset 的复用应仍被取消"

    reg.reset("keep")
    r3 = await rt.run(_one_stage(0.0, "keep"), "keep")
    assert r3.status == "done", "reset 之后应能正常跑"


@pytest.mark.parametrize("mode", ["graceful", "hard"])
async def test_cancel_log_tells_caller_to_reset(tmp_path, caplog, mode):
    """★ 日志不能把 caller 引导到一条走不通的路上。

    修复前 graceful 那条的文案是 "已落 cp, resume 可续跑" —— 只字不提取消标志
    仍在, 而不 reset 的新 run 会立刻再次 cancelled。这是**主动误导**, 不是遗漏。

    两种 mode 都要覆盖: 它们走的是 runtime 里**两条不同的日志**, 而两种情况下
    标志都保留 —— 我第一版只改了 graceful 那条, hard 那条照旧不提 reset, 测试
    (按 graceful 的输出断言) 就漏过去了。
    """
    reg = CancelRegistry()
    rt = _rt(tmp_path, reg)

    with caplog.at_level("WARNING"):
        t = asyncio.create_task(rt.run(_two_stage(0.2, "logmsg"), "logmsg"))
        await asyncio.sleep(0.02)
        reg.cancel("logmsg", mode)
        await t

    text = caplog.text
    assert mode in text or "cancelled" in text, f"应打出取消日志, 实得:\n{text}"
    # 断言查的是**具体的调用形式**而不是 "reset" 这个词 —— 文案里"不 reset 的新
    # run"也含该词, 查词的话把关键那处删掉照样绿 (实测踩过: 变异验证第一次就
    # 因为这条弱断言拿到假绿)。caller 要照着做的是 `registry.reset(task_id)`。
    assert "registry.reset(task_id)" in text, (
        f"[{mode}] 取消日志必须点名 reset(task_id) 这个调用 —— 否则 caller 照着"
        f"'可续跑'去 resume, 结果新 run 一个 stage 都没跑就再次 cancelled。"
        f"实得日志:\n{text}")
    assert "resume 可续跑" not in text, (
        "旧文案主动暗示可以直接续跑, 与实际行为矛盾, 应一并去掉")


# ── 2. 外部取消必须上抛 ─────────────────────────────────────────────────────

async def test_external_cancel_is_not_swallowed_by_a_legacy_flag(tmp_path):
    """★ 遗留标志 + 外部超时 → 必须抛 TimeoutError, 不能转成假 RunResult。

    造法 (第一版造错过, 见模块 docstring): graceful cancel 在 run **已进入
    stage 之后**到达 —— 这是 graceful cancel 的正常工作方式; 随后外部
    `wait_for` 超时。旧判据 (只看"有没有标志") 会把外部的 CancelledError 当成
    registry 硬杀吞掉。
    """
    reg = CancelRegistry()
    rt = _rt(tmp_path, reg)

    task = asyncio.create_task(rt.run(_one_stage(0.5, "ext"), "ext"))
    await asyncio.sleep(0.02)          # 确认已进入 stage (正 await sleep)
    reg.cancel("ext", "graceful")       # stage 执行期间到达 → 标志设立
    await asyncio.sleep(0.01)
    assert reg.is_cancelled("ext"), "前置条件: 遗留标志须存在"
    assert "ext" not in reg._killing, "前置条件: 本进程没发过 t.cancel, _killing 应为空"

    with pytest.raises((asyncio.TimeoutError, asyncio.CancelledError)):
        await asyncio.wait_for(task, timeout=0.05)


async def test_registry_hard_kill_still_returns_run_result(tmp_path):
    """反向约束: 真·registry 硬杀仍必须转成 RunResult, 不能上抛。

    新判据 (查 `_killing`) 若写错成"一律上抛", 硬杀路径就会炸给调用方一个
    异常, 而调用方要的是 `status="cancelled"` + 已落盘的 cp。
    """
    reg = CancelRegistry()
    rt = _rt(tmp_path, reg)

    t = asyncio.create_task(rt.run(_two_stage(0.2, "hard"), "hard"))
    await asyncio.sleep(0.02)

    reg.cancel("hard", "hard")
    assert "hard" in reg._killing, "前置条件: hard cancel 确实发了 t.cancel"
    r = await t

    assert r.status == "cancelled"
    assert r.error == "killed (hard kill by CancelRegistry)"


async def test_killing_marker_is_cleared_when_run_unregisters(tmp_path):
    """`_killing` 与句柄同生命周期 —— 不留到下一个 run。

    若残留, 下一个 run 收到外部取消时会被误判成"registry 硬杀"而吞掉, 也就是
    第 2 条那个缺陷换个入口再来一次。
    """
    reg = CancelRegistry()
    rt = _rt(tmp_path, reg)

    t = asyncio.create_task(rt.run(_two_stage(0.2, "kl"), "kl"))
    await asyncio.sleep(0.02)
    reg.cancel("kl", "hard")
    assert "kl" in reg._killing, "前置条件: hard 期间确有标记 (此刻 run 还没收尾)"
    await t
    # run 收尾后立刻就该没了 —— 这正是本用例要钉的东西
    assert "kl" not in reg._killing, (
        "run 收尾时 `_killing` 必须被清 (与 `_tasks`/`_current` 同生命周期), "
        f"实得仍含 'kl'")

    await rt.run(_one_stage(0.0, "kl"), "kl")
    reg.reset("kl")
    await rt.run(_one_stage(0.0, "kl"), "kl")




async def test_graceful_cancel_does_not_set_killing(tmp_path):
    """graceful 不注入 CancelledError ⇒ 不该设 `_killing`。

    设了的话, 之后**任何**外部取消都会被误判成硬杀 —— 判据的精度就垮了。
    """
    reg = CancelRegistry()
    rt = _rt(tmp_path, reg)

    task = asyncio.create_task(rt.run(_one_stage(5.0, "g"), "g"))
    await asyncio.sleep(0.02)
    reg.cancel("g", "graceful")
    await asyncio.sleep(0.01)
    assert "g" not in reg._killing, "graceful 不该设 _killing"
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def test_hard_cancel_on_unreachable_task_sets_no_killing(tmp_path):
    """硬杀一个**不在跑**的 task: 标志要设 (task 级语义), `_killing` 不设。

    没句柄就没发过 `t.cancel()`, 设 `_killing` 就是在撒谎 —— 而这个谎会让下一个
    run 的外部取消被吞。
    """
    reg = CancelRegistry()
    reg.cancel("nobody", "hard")
    assert reg.is_cancelled("nobody"), "硬杀请求的取消标志仍应留下 (task 级语义)"
    assert "nobody" not in reg._killing, "没有句柄 = 没发过 t.cancel, 不该设 _killing"


async def test_not_killable_stage_blocks_hard_kill(tmp_path):
    """反向约束: killable=False 时硬杀抛 NotKillable, 且**不发** t.cancel。

    抛异常前就已经设标志的话, 那就是"拒绝了但还是拒了" —— 状态不一致。
    """
    reg = CancelRegistry()
    dag = DAG("nk")
    reached = asyncio.Event()

    @dag.stage(killable=False)
    async def s_protected(ctx):
        reached.set()
        await asyncio.sleep(0.2)
        return {"a": 1}

    rt = _rt(tmp_path, reg)
    t = asyncio.create_task(rt.run(dag, "nk"))
    await reached.wait()
    await asyncio.sleep(0.01)

    with pytest.raises(NotKillable):
        reg.cancel("nk", "hard")

    assert "nk" not in reg._killing, "拒绝硬杀就不该设 _killing"
    r = await t
    assert r.status == "done"
