"""TestPipe: mock stage / 跑全图 / 调用记录 / 还原."""

from stageflow import DAG, TestPipe


def _dag():
    dag = DAG("tp")

    @dag.stage()
    async def s_fetch(ctx):
        raise AssertionError("真实现不该被跑 (mock 掉了)")

    @dag.stage(depends_on=["s_fetch"])
    async def s_process(ctx):
        return {"processed": len(ctx.state["items"])}

    return dag


async def test_mock_replaces_stage():
    pipe = TestPipe(_dag(), initial_state={"items": [1, 2, 3]})
    pipe.mock("s_fetch", lambda state: {"fetched": True})  # items 已在 initial_state, 不重写
    result = await pipe.run()
    assert result.status == "done"
    assert result.state["fetched"] is True
    assert result.state["processed"] == 3


async def test_calls_recorded_with_order():
    dag = _dag()
    pipe = TestPipe(dag, initial_state={"items": [1]})
    pipe.mock("s_fetch", lambda state: {"items": state["items"]})
    await pipe.run()
    calls = pipe.calls()
    assert [c["stage"] for c in calls] == ["s_fetch"]
    assert calls[0]["mock"] is True


async def test_mock_restored_after_run():
    dag = _dag()
    pipe = TestPipe(dag)
    pipe.mock("s_fetch", lambda state: {"items": []})
    await pipe.run()
    # 还原后真实现还在 (再跑会 raise AssertionError)
    assert dag.stages["s_fetch"].fn.__name__ == "s_fetch"


async def test_mock_unknown_stage_raises():
    pipe = TestPipe(_dag())
    try:
        pipe.mock("s_nope", lambda state: {})
        assert False, "应该 raise"
    except KeyError:
        pass


async def test_mock_can_be_async():
    async def _async_fetch(state):
        return {"items": ["x"]}

    pipe = TestPipe(_dag())
    pipe.mock("s_fetch", _async_fetch)
    result = await pipe.run()
    assert result.status == "done"
    assert result.state["processed"] == 1
