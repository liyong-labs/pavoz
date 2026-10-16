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
from pavoz.cancel import CancelRegistry, NotKillable, TaskIdInUse


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
        f"实得仍含 {sorted(reg._killing)}")

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


# ── 3. 一个 task_id 只能有一个活跃 run ─────────────────────────────────────
#
# registry 按 task_id 寻址, 所以"两个并发 run 共用一个 task_id"在语义上**无法
# 定义** —— `cancel(task_id)` 不知道该取消哪一个 (checkpoint 也按 task_id 存,
# 两者还会抢 latest 指针)。修复前是静默撞车, 三层后果一层比一层糟。

async def test_second_run_on_same_task_id_is_rejected(tmp_path):
    """★ 第二个 run 必须**明确拒绝**, 而不是静默抢走句柄。

    修复前 run#1 收尾时的 `_unregister("SAME")` 会把还在跑的 run#2 的句柄一起
    摘掉, 于是: `cancel(hard)` 拿不到句柄 ⇒ `Task.cancel()` 根本没执行却返回
    成功 (硬杀静默失效); `_current` 同时被清空 ⇒ `killable=False` 读不到 ⇒ 不抛
    `NotKillable`; 但标志照样设上 ⇒ run#2 在 stage 边界被截停 ⇒ **误伤**。
    """
    reg = CancelRegistry()
    rt = _rt(tmp_path, reg)

    first = asyncio.create_task(rt.run(_two_stage(0.2, "shared"), "shared"))
    await asyncio.sleep(0.02)

    with pytest.raises(TaskIdInUse):
        await rt.run(_two_stage(0.05, "other"), "shared")

    r = await first
    assert r.status == "done", f"拒绝第二个 run 不该影响第一个, 实得 {r.status}"


async def test_rejection_leaves_the_first_run_cancelable(tmp_path):
    """★ 拒绝之后, 第一个 run 的取消能力**完好** —— 三个层面都要在。

    这条是"修好撞车"与"只是让它报错"的区别: 只报错但把 registry 状态搅坏的
    实现, 这里会红。
    """
    reg = CancelRegistry()
    rt = _rt(tmp_path, reg)

    first = asyncio.create_task(rt.run(_two_stage(0.5, "kc"), "kc"))
    await asyncio.sleep(0.05)

    with pytest.raises(TaskIdInUse):
        await rt.run(_two_stage(0.05, "x"), "kc")

    # (1) 句柄还在: cancel 能真的杀掉它
    assert reg._tasks.get("kc") is not None, "拒绝第二个 run 不该动第一个 run 的句柄"
    reg.cancel("kc", "hard")
    r = await first
    assert r.status == "cancelled", f"硬杀应生效, 实得 {r.status}"


async def test_killable_false_guard_survives_a_rejected_competing_run(tmp_path):
    """★ `killable=False` 的保护不能被"撞车"绕过去。

    这是原缺陷的第 2 层: `_current` 被别人清掉 ⇒ 读不到 killable ⇒ 不抛
    `NotKillable`。修复后即使有第二个 run 被拒绝, 第一个 run 的 killable 信息
    仍在, 硬杀照旧被拒。
    """
    reg = CancelRegistry()
    dag = DAG("protected")
    reached = asyncio.Event()

    @dag.stage()
    async def s_a(ctx):
        await asyncio.sleep(0.02)
        return {"a": 1}

    @dag.stage(depends_on=["s_a"], killable=False)
    async def s_protected(ctx):
        reached.set()
        await asyncio.sleep(0.3)
        return {"p": 1}

    rt = _rt(tmp_path, reg)
    first = asyncio.create_task(rt.run(dag, "prot"))
    await reached.wait()

    with pytest.raises(TaskIdInUse):
        await rt.run(_two_stage(0.05, "y"), "prot")

    with pytest.raises(NotKillable):
        reg.cancel("prot", "hard")

    assert first.done() is False, "被拒的硬杀不该动得了这个 run"
    r = await first
    assert r.status == "done"


async def test_unregister_only_removes_its_own_handle(tmp_path):
    """`_unregister` 带句柄时只摘自己那个 —— 防御性那一层。

    `_register` 已拒绝重复占用, 所以这条正常走不到; 但万一将来新增了别的调用
    路径, 收尾的 run 也不该把别人还在跑的句柄摘掉。
    """
    reg = CancelRegistry()

    async def _a():
        return None

    async def _b():
        return None

    ta, tb = asyncio.create_task(_a()), asyncio.create_task(_b())
    reg._tasks["k"] = ta                      # 绕过 _register, 模拟历史/竞态残留
    reg._current["k"] = ("s_x", False)
    reg._killing.add("k")

    reg._unregister("k", tb)                  # tb 不是注册的那个
    assert reg._tasks.get("k") is ta, "不该摘掉别人的句柄"
    assert reg._current.get("k") == ("s_x", False)
    assert "k" in reg._killing

    reg._unregister("k", ta)                  # 才是自己的
    assert "k" not in reg._tasks
    assert "k" not in reg._current
    assert "k" not in reg._killing
    await asyncio.gather(ta, tb)


async def test_sequential_runs_on_same_task_id_are_fine(tmp_path):
    """反向约束: **串行**复用同一 task_id 必须照旧 —— 拒绝只针对并发。

    第一个 run 已 `_unregister`, 第二个自然会注册成功。这是绝大多数正常用法
    (run → resume → fork), 撞上了就是灾难性误伤。
    """
    reg = CancelRegistry()
    rt = _rt(tmp_path, reg)

    r1 = await rt.run(_two_stage(0.0, "seq"), "seq")
    assert r1.status == "done"
    r2 = await rt.run(_two_stage(0.0, "seq"), "seq")
    assert r2.status == "done"
    r3 = await rt.run(_one_stage(0.0, "seq"), "seq")
    assert r3.status == "done"


async def test_no_registry_means_shared_task_id_is_not_rejected(tmp_path):
    """没传 registry 时不拦 —— 撞车是 registry 的寻址问题, 与 run 本身无关。

    不传 registry 就没有 `cancel()` 可误伤, 而 checkpoint 抢占本来就是 run
    层的既有语义, 不该在这次改动里被顺带禁止。
    """
    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(tmp_path / "cp")))

    first = asyncio.create_task(rt.run(_two_stage(0.2, "noreg"), "noreg"))
    await asyncio.sleep(0.02)
    second = asyncio.create_task(rt.run(_two_stage(0.05, "noreg"), "noreg"))

    await asyncio.gather(first, second)


async def test_finished_handle_does_not_count_as_occupied(tmp_path):
    """★ "已结束但尚未 `_unregister`" 的句柄**不算占用**。

    `run()` 的 finally 紧跟 `_run_impl` 返回, 所以正常路径下这个窗口极小 ——
    但它存在 (await 边界会让出)。判据若写成 `cur is not None`, 撞上这个窗口的
    串行复用就会被误拒, 而那是最不该误伤的用法 (run → resume → fork)。

    变异验证时正是这一条缺失: 把判据放宽成"已完成的也算占用", 上面那批用例
    **全绿** —— 因为它们都在第一个 run 完全收尾之后才复用。
    """
    reg = CancelRegistry()
    rt = _rt(tmp_path, reg)

    async def _noop():
        return None

    finished = asyncio.create_task(_noop())
    await finished
    assert finished.done()

    # 模拟"上一个 run 已结束, 但 finally 还没跑到"的那一瞬间
    reg._tasks["win"] = finished
    reg._current["win"] = ("s_x", True)

    r = await rt.run(_one_stage(0.0, "win"), "win")   # 不该抛 TaskIdInUse
    assert r.status == "done"
    assert "win" not in reg._tasks, "新 run 应正常接管并清理该键"


# ── TaskIdInUse 的**前提**: 只有配了 cancel_registry 才有 (2026-10-17) ──────
#
# docs/{cn,en}/api.md 与 CHANGELOG 升级注意都写着"并发 run 共用一个 task_id →
# 第二个 run() 抛 TaskIdInUse"。这句话**漏了前提** —— `Runtime.cancel_registry`
# 默认是 `None`, 而占用登记 (`CancelRegistry._register`) 就发生在那条路径上。
#
# 实测 (默认 Runtime, 共用一个 FileStorage):
#     gather(Runtime().run(dag,"same"), Runtime().run(dag,"same"))
#     → 两个都 status=done, 一个 run_id, latest 指针指向最后写完的那个
#
# 也就是说**默认配置下完全没有这层保护**, 而文档读起来像是有的。危害不是数据
# 损坏 (各 run 的 cp 文件独立), 是 latest 指针与 resume/trace 的指向变得不确定。
#
# 这里把三种配置的实际行为**钉成判据**, 让文档那句话不再是一句没人验证的承诺:
#   (1) 默认 (无 registry)        → 两个都跑完, 不报错
#   (2) 共用同一个 registry       → 第二个 TaskIdInUse   ← 文档现在推荐这个
#   (3) 各自各传各的 registry    → 两个都跑完 (等于没传)


async def test_default_runtime_gives_no_in_use_protection(tmp_path):
    """(1) 默认 Runtime: 并发同 task_id 不报错 —— 这是**当前契约**, 不是 bug。

    写这条不是承认现状合理, 而是把"文档那句话的前提"变成机器可验证的事实。
    若将来决定让 `Runtime()` 默认带 registry (或把守卫提到进程级), 这条会红 ——
    那时该同步改 docs 与 CHANGELOG, 而不是让文档继续说一个不存在的默认行为。
    """
    store = CheckpointStore(FileStorage(tmp_path / "cp"))
    a = Runtime(checkpoint_store=store)
    b = Runtime(checkpoint_store=store)
    assert a.cancel_registry is None, "前提: 默认没有 registry"

    ra, rb = await asyncio.gather(
        a.run(_two_stage(0.1, "d1"), "same"),
        b.run(_two_stage(0.1, "d2"), "same"),
    )
    assert ra.status == "done" and rb.status == "done"
    assert ra.run_id != rb.run_id, "两个 run 是独立的 (只是 latest 指针会指后者)"
    assert store.load_latest("same") is not None


async def test_shared_registry_guards_across_runtime_instances(tmp_path):
    """(2) 共用同一个 registry: 跨 Runtime 实例也拦得住 —— 这是文档推荐的配置。

    既有那条并发用例只覆盖了"一个 Runtime + 一个 registry"。而实际用法往往是
    一堆 Runtime 实例, 守卫**只**在它们共享 registry 时成立 —— 这条把那个前提
    本身钉住。
    """
    reg = CancelRegistry()
    a = _rt(tmp_path, reg)
    b = _rt(tmp_path, reg)          # 同一个 reg, 不同 Runtime

    first = asyncio.create_task(a.run(_two_stage(0.2, "s1"), "shared"))
    await asyncio.sleep(0.02)
    with pytest.raises(TaskIdInUse):
        await b.run(_two_stage(0.05, "s2"), "shared")
    assert (await first).status == "done"


async def test_separate_registries_are_equivalent_to_none(tmp_path):
    """(3) 各自各传各的 registry = 没有保护 —— 文档现在明说了, 这里也钉住。"""
    a = _rt(tmp_path, CancelRegistry())
    b = _rt(tmp_path, CancelRegistry())
    ra, rb = await asyncio.gather(
        a.run(_two_stage(0.1, "x1"), "same"),
        b.run(_two_stage(0.1, "x2"), "same"),
    )
    assert ra.status == "done" and rb.status == "done"
