"""R2: stage input hash — 记录 + fork 跳过未变 stage (T5 测记录, T6 测跳过)."""

from pavoz import (
    DAG,
    CheckpointStore,
    FileStorage,
    Runtime,
    stage_input_hash,
)


def test_stage_input_hash_sensitive_to_source_and_state():
    async def fn_a(ctx):
        return {"a": 1}

    async def fn_b(ctx):  # 源码不同 (哪怕行为同)
        return {"a": 1}

    h1 = stage_input_hash(fn_a, {"x": 1})
    assert h1 != stage_input_hash(fn_a, {"x": 2})   # 输入 state 变 → hash 变
    assert h1 != stage_input_hash(fn_b, {"x": 1})   # fn 源码变 → hash 变
    assert h1 == stage_input_hash(fn_a, {"x": 1})   # 稳定可重现


async def test_run_records_input_hashes(tmp_path):
    store = CheckpointStore(FileStorage(str(tmp_path / "s")))
    dag = DAG("sih1")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 2}

    await Runtime(checkpoint_store=store).run(dag, "sih1")
    cp = store.load_latest("sih1")
    assert set(cp.stage_input_hashes) == {"s_a", "s_b"}
    assert cp.stage_input_hashes["s_a"] != cp.stage_input_hashes["s_b"]
