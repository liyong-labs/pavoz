"""D1.5: StageError.category — caller 实战按类别分流 (LLM_TIMEOUT / SEARCH_NO_RESULT / CODE_BUG / NETWORK / AUTH / INFRA / CHECKPOINT_LOAD_FAIL / STATE_VALIDATION / PROTOCOL_BREACH).

要求 (ai@home id=188, user 拍板 ack):
- 9 类枚举 (留扩展位, caller 自由 string 不强 enum)
- caller 通过 raise StageError("msg", category="...") 设置
- RunResult.error_category 暴露
- 不向后兼容 (原则 #2): default None 不破坏现有 caller
"""


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