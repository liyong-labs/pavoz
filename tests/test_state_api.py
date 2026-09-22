"""R1b: 程序化查询 API — get_state / list_tasks (ai-writing R1)."""

import pytest

from pavoz import DAG, CheckpointStore, FileStorage, Runtime


async def test_get_state_returns_merged_state(tmp_path):
    store = CheckpointStore(FileStorage(str(tmp_path / "gs1")))
    dag = DAG("gs1")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": ctx.state.get("a", 0) + 1}

    await Runtime(checkpoint_store=store).run(dag, "gs1")
    state = Runtime(checkpoint_store=store).get_state("gs1")
    assert state == {"a": 1, "b": 2}


def test_get_state_no_checkpoint_raises(tmp_path):
    store = CheckpointStore(FileStorage(str(tmp_path / "gs2")))
    with pytest.raises(RuntimeError, match="无 checkpoint"):
        Runtime(checkpoint_store=store).get_state("gs2")


def test_get_state_requires_store():
    with pytest.raises(RuntimeError, match="checkpoint_store"):
        Runtime().get_state("gs3")


async def test_list_tasks(tmp_path):
    store = CheckpointStore(FileStorage(str(tmp_path / "gs4")))
    dag = DAG("gs4")

    @dag.stage()
    async def s_a(ctx):
        return {}

    for tid in ("t-b", "t-a"):
        await Runtime(checkpoint_store=store).run(dag, tid)
    assert store.list_tasks() == ["t-a", "t-b"]
