"""D1.5: StageError.category — caller 实战按类别分流 (LLM_TIMEOUT / SEARCH_NO_RESULT / CODE_BUG / NETWORK / AUTH / INFRA / CHECKPOINT_LOAD_FAIL / STATE_VALIDATION / PROTOCOL_BREACH).

要求 (ai@home id=188, user 拍板 ack):
- 9 类枚举 (留扩展位, caller 自由 string 不强 enum)
- caller 通过 raise StageError("msg", category="...") 设置
- RunResult.error_category 暴露
- 不向后兼容 (原则 #2): default None 不破坏现有 caller
"""


import pytest

from pavoz import DAG, FatalError, RetryableError, Runtime, StageError


async def test_stage_error_category_passed_to_runresult():
    """caller raise StageError(category='LLM_TIMEOUT') → r.error_category 暴露."""
    dag = DAG("cat1")

    @dag.stage()
    async def s_bad(ctx):
        raise StageError("LLM API 超时 30s", category="LLM_TIMEOUT")

    r = await Runtime().run(dag, "cat1")
    assert r.status == "failed"
    assert r.failed_stage == "s_bad"
    assert r.error_category == "LLM_TIMEOUT"


async def test_retryable_error_category_passed_to_runresult():
    """caller raise RetryableError(category='NETWORK') → 跑通 (重试耗尽后 fail 但 category 仍记录)."""
    dag = DAG("cat2")

    @dag.stage(retries=0)
    async def s_net(ctx):
        raise RetryableError("connection reset", category="NETWORK")

    r = await Runtime().run(dag, "cat2")
    assert r.status == "failed"
    assert r.error_category == "NETWORK"


async def test_stage_error_without_category_is_none():
    """现有 caller raise StageError("msg") 不传 category → r.error_category is None (不破旧 caller)."""
    dag = DAG("cat3")

    @dag.stage()
    async def s_old(ctx):
        raise StageError("legacy 错误, 没 category")

    r = await Runtime().run(dag, "cat3")
    assert r.status == "failed"
    assert r.error_category is None


async def test_unknown_exception_no_category():
    """caller raise ValueError (非 pavoz 异常类) → r.error_category is None."""
    dag = DAG("cat4")

    @dag.stage()
    async def s_pure(ctx):
        raise ValueError("这不是 pavoz 异常类")

    r = await Runtime().run(dag, "cat4")
    assert r.status == "failed"
    assert r.error_category is None


async def test_done_run_has_none_category():
    """done run → r.error_category is None."""
    dag = DAG("cat5")

    @dag.stage()
    async def s_ok(ctx):
        return {"a": 1}

    r = await Runtime().run(dag, "cat5")
    assert r.status == "done"
    assert r.error_category is None


async def test_cancelled_run_has_none_category():
    """cancelled (graceful) → r.error_category is None."""
    from pavoz import CancelRegistry

    reg = CancelRegistry()
    dag = DAG("cat6")

    @dag.stage()
    async def s_a(ctx):
        reg.cancel("cat6")
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 2}

    r = await Runtime(cancel_registry=reg).run(dag, "cat6")
    assert r.status == "cancelled"
    assert r.error_category is None


async def test_fatal_error_category():
    """FatalError(category='CODE_BUG') → r.error_category == 'CODE_BUG'."""
    dag = DAG("cat7")

    @dag.stage()
    async def s_fatal(ctx):
        raise FatalError("state 校验崩", category="CODE_BUG")

    r = await Runtime().run(dag, "cat7")
    assert r.status == "failed"
    assert r.error_category == "CODE_BUG"


async def test_state_summary_still_truncated_at_2048_with_category():
    """加 category 不破 W1 的 state_summary 长度契约 (≤2048 字符)."""
    dag = DAG("cat8")

    @dag.stage()
    async def s_big(ctx):
        return {"blob": "x" * 5000}

    @dag.stage(depends_on=["s_big"])
    async def s_fail(ctx):
        raise StageError("boom", category="STATE_VALIDATION")

    r = await Runtime().run(dag, "cat8")
    assert r.state_summary is not None and len(r.state_summary) <= 2048
    assert r.error_category == "STATE_VALIDATION"


async def test_category_in_9_enum_set():
    """ai@home ack 9 类枚举 — sanity check 全部能作为 string category 跑通."""
    cats = [
        "LLM_TIMEOUT", "SEARCH_NO_RESULT", "CODE_BUG",
        "NETWORK", "AUTH", "INFRA",
        "CHECKPOINT_LOAD_FAIL", "STATE_VALIDATION", "PROTOCOL_BREACH",
    ]
    for c in cats:
        dag = DAG(f"c_{c}")

        @dag.stage()
        async def s_(ctx, _c=c):  # 默认参数绑循环变量 (ruff B023)
            raise StageError(f"sim {_c}", category=_c)

        r = await Runtime().run(dag, f"c_{c}")
        assert r.error_category == c, f"category {c} 没传过"

# ── 那 9 类里, 引擎自己产不出来 3 类 (2026-10-17 实测 + 文档修正) ────────────
#
# 实测矩阵 (拿真实 run 跑出来, 不是读代码猜的):
#
# | 场景                          | 异常? | status | error_class         | error_category |
# |-------------------------------|-------|--------|---------------------|----------------|
# | StageError(category=LLM_TIMEOUT)| 否   | failed | StageError          | LLM_TIMEOUT ✅  |
# | StageError(不带 category)      | 否   | failed | StageError          | None           |
# | RetryableError / FatalError    | 否   | failed | 同名                | None           |
# | 裸 ValueError / TimeoutError   | 否   | failed | 同名                | None           |
# | stage 返回 NaN                 | 否   | failed | ValueError          | **None**       |
# | router 返回未声明 key          | 否   | failed | UnmappedRouteError  | **None**       |
# | resume 时结构变了              | **是** | -     | CheckpointMismatchError | -          |
# | max_steps 熔断                 | 否   | failed | RetryableError      | None           |
#
# 文档(api.md)推荐 9 类里有 **3 类是引擎侧的**: CHECKPOINT_LOAD_FAIL /
# STATE_VALIDATION / PROTOCOL_BREACH。但引擎自己**一个都不产出**:
#   · STATE_VALIDATION 那个失败以 failed/ValueError + category=None 到达
#   · PROTOCOL_BREACH 那个以 failed/UnmappedRouteError + category=None 到达
#   · CHECKPOINT_LOAD_FAIL 那个**直接抛异常**, RunResult 压根不存在
# 而文档接着写"caller 据此分流重试/通知策略" —— 对这 3 类, 字段是结构性拿不到的。
#
# 这不是 bug 而是**契约没写清**: category 是 caller 自带的自由 string, 引擎不替
# caller 猜业务类别 (不猜是对的)。所以修文档而不是加行为: 让引擎自己给引擎失败打
# 类别 = 新功能, 且会改变 `error_category` 从"caller 没分类就是 None"变成"有时引擎
# 会填" —— 对已经在 dispatch None 的 caller 是行为变更。
#
# 下面把矩阵钉住: 透传通着, 且引擎侧三类确实是 None / 抛异常。哪天真给引擎失败补上
# 默认类别, 这条会红 —— 那时该同步改文档 (而不是让文档继续说一个不存在的现状)。


async def test_engine_side_failures_carry_no_category():
    """★ 引擎自己引发的三类失败: 要么 category=None, 要么压根是异常。"""
    import tempfile

    from pavoz import CheckpointStore, FileStorage

    # (a) STATE_VALIDATION 形状: stage 返回非有限值
    dag = DAG("cat_nf")

    @dag.stage()
    async def s_bad(ctx):
        return {"v": float("nan")}

    with tempfile.TemporaryDirectory() as td:
        r = await Runtime(
            checkpoint_store=CheckpointStore(FileStorage(td))).run(dag, "nf")
    assert r.status == "failed"
    assert r.error_class == "ValueError", f"实得 {r.error_class}"
    assert r.error_category is None, (
        f"引擎自己产的 category 实得 {r.error_category!r} —— 若将来补了默认类别, "
        f"请同步改 docs/cn/api.md 那 9 类的说明 (本条就是那个提醒)")

    # (b) PROTOCOL_BREACH 形状: router 返回未声明 key
    dag2 = DAG("cat_um")

    @dag2.stage()
    async def s_gate(ctx):
        return {}

    @dag2.stage()
    async def s_tgt(ctx):
        return {}

    dag2.add_conditional_edges("s_gate", lambda s: "未声明", {"ok": "s_tgt"})
    with tempfile.TemporaryDirectory() as td:
        r2 = await Runtime(
            checkpoint_store=CheckpointStore(FileStorage(td))).run(dag2, "um")
    assert r2.status == "failed"
    assert r2.error_class == "UnmappedRouteError", f"实得 {r2.error_class}"
    assert r2.error_category is None


async def test_checkpoint_load_failure_raises_instead_of_returning_result():
    """★ CHECKPOINT_LOAD_FAIL 形状: **抛异常**, 没有 RunResult 也没有 category。"""
    import tempfile

    from pavoz import CheckpointMismatchError, CheckpointStore, FileStorage

    d1 = DAG("cat_mm_a")

    @d1.stage()
    async def s_a(ctx):
        return {"a": 1}

    with tempfile.TemporaryDirectory() as td:
        store = CheckpointStore(FileStorage(td))
        await Runtime(checkpoint_store=store).run(d1, "mm")

        d2 = DAG("cat_mm_b")            # 结构不同的另一个 dag

        @d2.stage()
        async def s_b(ctx):
            return {"b": 2}

        with pytest.raises(CheckpointMismatchError):
            await Runtime(checkpoint_store=store).run(d2, "mm", resume=True)


async def test_category_pass_through_still_works():
    """正向对照: 透传机制本身没坏 (引擎侧为 None ≠ 透传失效)。"""
    dag = DAG("cat_ok")

    @dag.stage()
    async def s_bad(ctx):
        raise StageError("x", category="LLM_TIMEOUT")

    r = await Runtime().run(dag, "ok")
    assert r.status == "failed"
    assert r.error_category == "LLM_TIMEOUT", (
        f"caller 自带的 category 必须透传, 实得 {r.error_category!r}")
