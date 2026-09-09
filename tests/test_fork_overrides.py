"""v0.9: fork_run overrides 深合并语义 (behavior fix — 兄弟键保留)."""
from pavoz import CheckpointStore, DAG, FileStorage, Runtime


def _mk(task_id, tmp_path):
    store = CheckpointStore(FileStorage(str(tmp_path / task_id)))
    return Runtime(checkpoint_store=store)


def _dag_captured(captured: list):
    dag = DAG("dm")

    @dag.stage()
    async def s_a(ctx):
        return {"llm": {"model": "orig", "temperature": 0.5}, "topic": "t"}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        captured.append(dict(ctx.state.items()))
        return {"b": 1}

    return dag


async def test_nested_patch_preserves_siblings(tmp_path):
    """--set llm.model=x 场景: model 覆盖, temperature 保留 (v0.8 浅替换会抹掉)."""
    rt = _mk("dm1", tmp_path)
    captured: list = []
    dag = _dag_captured(captured)
    await rt.run(dag, "dm1")
    captured.clear()
    await rt.fork_run(dag, "dm1", from_stage="s_b", overrides={"llm": {"model": "new"}})
    s = captured[0]
    assert s["llm"]["model"] == "new"
    assert s["llm"]["temperature"] == 0.5  # v0.8 浅替换下这里是 KeyError


async def test_top_level_scalar_unchanged(tmp_path):
    """v0.8 顶层标量覆盖行为不变 (后向兼容)."""
    rt = _mk("dm2", tmp_path)
    captured: list = []
    dag = _dag_captured(captured)
    await rt.run(dag, "dm2")
    captured.clear()
    await rt.fork_run(dag, "dm2", from_stage="s_b", overrides={"topic": "edited"})
    assert captured[0]["topic"] == "edited"


async def test_dot_path_key_dict_equivalent(tmp_path):
    """dot-path key dict 与 nested dict 等价 (apply_overrides 双格式)."""
    rt = _mk("dm3", tmp_path)
    captured: list = []
    dag = _dag_captured(captured)
    await rt.run(dag, "dm3")
    captured.clear()
    await rt.fork_run(dag, "dm3", from_stage="s_b", overrides={"llm.model": "dp"})
    s = captured[0]
    assert s["llm"]["model"] == "dp"
    assert s["llm"]["temperature"] == 0.5


async def test_dict_replaced_by_scalar(tmp_path):
    """patch 用标量覆盖整个 dict key → 整体替换 (patch 赢)."""
    rt = _mk("dm4", tmp_path)
    captured: list = []
    dag = _dag_captured(captured)
    await rt.run(dag, "dm4")
    captured.clear()
    await rt.fork_run(dag, "dm4", from_stage="s_b", overrides={"llm": "flat"})
    assert captured[0]["llm"] == "flat"


async def test_list_value_replaced_wholesale(tmp_path):
    """list 值整体替换, 不做元素级 merge."""
    rt = _mk("dm5", tmp_path)
    captured: list = []
    dag = _dag_captured(captured)
    await rt.run(dag, "dm5")
    captured.clear()
    await rt.fork_run(dag, "dm5", from_stage="s_b",
                      overrides={"llm": {"model": ["a", "b"]}})
    assert captured[0]["llm"]["model"] == ["a", "b"]


async def test_dunder_overrides_rejected(tmp_path):
    """dunder key 直接进 fork_run → ValueError (runtime 边界防御, T4 review 裁决)."""
    import pytest

    rt = _mk("dm6", tmp_path)
    captured: list = []
    dag = _dag_captured(captured)
    await rt.run(dag, "dm6")
    with pytest.raises(ValueError, match="禁词"):
        await rt.fork_run(dag, "dm6", from_stage="s_b",
                          overrides={"__proto__": {"x": 1}})
