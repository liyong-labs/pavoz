"""Runtime: 顺序执行 / retries / 异常分类 / timeout / state merge / 冲突."""


from stageflow import DAG, RetryableError, Runtime, StageError


def _dag3():
    dag = DAG("t")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": ctx.state["a"] + 1}

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        return {"c": ctx.state["b"] * 2}

    return dag


async def test_sequential_run_state_flow():
    result = await Runtime().run(_dag3(), "t1")
    assert result.status == "done"
    assert result.state == {"a": 1, "b": 2, "c": 4}


async def test_initial_state_visible():
    dag = DAG("t")

    @dag.stage()
    async def s_echo(ctx):
        return {"got": ctx.state["seed"]}

    result = await Runtime().run(dag, "t2", initial_state={"seed": 42})
    assert result.status == "done"
    assert result.state["got"] == 42  # initial_state 保留 + stage 增量
    assert result.state["seed"] == 42


async def test_stage_error_no_retry():
    calls = {"n": 0}
    dag = DAG("t")

    @dag.stage(retries=5)
    async def s_fail(ctx):
        calls["n"] += 1
        raise StageError("业务失败")

    result = await Runtime().run(dag, "t3")
    assert result.status == "failed"
    assert calls["n"] == 1  # 不重试
    assert "业务失败" in (result.error or "")


async def test_retryable_error_retries_then_succeeds():
    calls = {"n": 0}
    dag = DAG("t")

    @dag.stage(retries=2)
    async def s_flaky(ctx):
        calls["n"] += 1
        if calls["n"] < 3:
            raise RetryableError("网络抖一下")
        return {"ok": True}

    result = await Runtime().run(dag, "t4")
    assert result.status == "done"
    assert calls["n"] == 3
    assert result.state == {"ok": True}


async def test_retryable_error_exhausted_fails():
    calls = {"n": 0}
    dag = DAG("t")

    @dag.stage(retries=1)
    async def s_fail(ctx):
        calls["n"] += 1
        raise RetryableError("一直网络错")

    result = await Runtime().run(dag, "t5")
    assert result.status == "failed"
    assert calls["n"] == 2  # 1 + retries=1


async def test_state_conflict_fails():
    dag = DAG("t")

    @dag.stage()
    async def s_dup(ctx):
        return {"dup": 1}

    # 冲突 = stage 代码 bug → 按 FatalError 语义 failed, error 含冲突详情
    result = await Runtime().run(dag, "t6", initial_state={"dup": 99})
    assert result.status == "failed"
    assert "dup" in (result.error or "")


async def test_timeout_fails_stage():
    dag = DAG("t")

    @dag.stage(timeout=0.05)
    async def s_slow(ctx):
        import asyncio

        await asyncio.sleep(1)
        return {"x": 1}

    result = await Runtime().run(dag, "t7")
    assert result.status == "failed"
    assert "超时" in (result.error or "")


async def test_retries_0_default_no_retry():
    calls = {"n": 0}
    dag = DAG("t")

    @dag.stage()  # retries 默认 0
    async def s_fail(ctx):
        calls["n"] += 1
        raise RetryableError("不该重试")

    result = await Runtime().run(dag, "t8")
    assert result.status == "failed"
    assert calls["n"] == 1


async def test_fatal_error_no_retry_no_budget():
    calls = {"n": 0}
    dag = DAG("t")

    @dag.stage(retries=5)
    async def s_bug(ctx):
        calls["n"] += 1
        raise RuntimeError("stage 代码 bug")

    result = await Runtime().run(dag, "t9")
    assert result.status == "failed"
    assert calls["n"] == 1
    assert "FatalError" in (result.error or "")


async def test_stage_returning_non_dict_fails():
    dag = DAG("t")

    @dag.stage()
    async def s_bad(ctx):
        return "not a dict"

    result = await Runtime().run(dag, "t10")
    assert result.status == "failed"


async def test_chain_overwrite_allowed_downstream():
    """v0.1.1 链式演进: 下游覆盖传递上游 producer 的 key 合法 (流水线模式)."""
    dag = DAG("chain_evolve")

    @dag.stage()
    async def s_search(ctx):
        return {"unique_sources": ["u1", "u2"]}

    @dag.stage(depends_on=["s_search"])
    async def s_filter(ctx):
        return {"unique_sources": ["u1"]}  # 演进同一产物

    @dag.stage(depends_on=["s_filter"])
    async def s_compress(ctx):
        return {"unique_sources": ["u1"], "outline": "o1"}

    result = await Runtime().run(dag, "t11")
    assert result.status == "done", result.error
    assert result.state["unique_sources"] == ["u1"]
    assert result.state["outline"] == "o1"


async def test_parallel_producer_conflict_still_raises():
    """平行 producer (无依赖链) 覆盖仍 raise — v1 冲突语义保留."""
    dag = DAG("parallel_conflict")

    @dag.stage()
    async def s_a(ctx):
        return {"x": 1}

    @dag.stage()  # 与 s_a 平行, 不依赖
    async def s_b(ctx):
        return {"x": 2}

    result = await Runtime().run(dag, "t12")
    assert result.status == "failed"
    assert "s_b" in (result.error or "")
    assert "x" in (result.error or "")
