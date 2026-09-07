"""v0.7: stage_ts — stage 完成时刻记录 (恢复侧 TTL gate 用)."""

import time

from pavoz import CheckpointStore, DAG, FileStorage, Runtime


def _mk(tid):
    return CheckpointStore(FileStorage(f"/tmp/pavoz-stagets-{tid}"))


async def test_stage_ts_recorded_and_persisted():
    tid = "ts1"
    store = _mk(tid)
    rt = Runtime(checkpoint_store=store)
    dag = DAG("ts")

    @dag.stage()
    async def s_a(ctx):
        time.sleep(0.05)
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": ctx.state["a"] + 1}

    await rt.run(dag, tid)
    cp = store.load_latest(tid)
    assert set(cp.stage_ts.keys()) == {"s_a", "s_b"}
    assert cp.stage_ts["s_a"] <= cp.stage_ts["s_b"]
    assert abs(cp.stage_ts["s_a"] - time.time()) < 60
    # 序列化 roundtrip
    from pavoz.checkpoint import Checkpoint
    cp3 = Checkpoint.from_dict(cp.to_dict())
    assert cp3.stage_ts == cp.stage_ts


async def test_resume_and_fork_keep_prefix_stage_ts():
    tid = "ts2"
    store = _mk(tid)
    rt = Runtime(checkpoint_store=store)
    trace = []

    dag = DAG("ts2")

    @dag.stage()
    async def s_a(ctx):
        trace.append("a")
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        trace.append("b")
        return {"b": 2}

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        trace.append("c")
        return {"c": 3}

    r1 = await rt.run(dag, tid)
    assert r1.status == "done"
    cp1 = store.load_latest(tid)

    # fork from s_c: 前缀 s_a/s_b 的 ts 保留
    trace.clear()
    r2 = await rt.fork_run(dag, tid, from_stage="s_c", overrides={"a": 9})
    assert r2.status == "done"
    cp2 = store.load_latest(tid)
    assert cp2.stage_ts["s_a"] == cp1.stage_ts["s_a"]
    assert cp2.stage_ts["s_b"] == cp1.stage_ts["s_b"]
    assert "s_c" in cp2.stage_ts  # 重跑的 stage 有新 ts
    assert trace == ["c"]
