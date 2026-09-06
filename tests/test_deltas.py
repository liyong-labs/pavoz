"""M2: stage_deltas 存储 + rebuild_state_before 重建任意 stage 前 state."""
import pytest

from stageflow.checkpoint import Checkpoint, workflow_hash
from stageflow.dag import DAG


def _dag():
    dag = DAG("d")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": ctx.state["a"] + 1}

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        return {"c": ctx.state["b"] + 1, "a": 99}  # chain-overwrite: s_c 覆盖 s_a 的 a

    return dag


def _cp(dag, initial=None, deltas=None, done=None):
    """构造一个已完成全部 3 stage 的 cp (含 deltas + initial)."""
    return Checkpoint(
        task_id="t-1",
        run_id="r-1",
        dag_name=dag.name,
        workflow_hash=workflow_hash(dag),
        stage_statuses={"s_a": "done", "s_b": "done", "s_c": "done"},
        done_stages=done or ["s_a", "s_b", "s_c"],
        producers={"a": "s_c", "b": "s_b", "c": "s_c"},  # a 的 producer 是最后写者 s_c
        initial_state=dict(initial or {}),
        stage_deltas=dict(deltas or {
            "s_a": {"a": 1},
            "s_b": {"b": 2},
            "s_c": {"c": 3, "a": 99},
        }),
    )


def test_checkpoint_round_trip_preserves_deltas_and_initial():
    dag = _dag()
    cp = _cp(dag, initial={"query": "x"})
    d = cp.to_dict()
    cp2 = Checkpoint.from_dict(d)
    assert cp2.initial_state == {"query": "x"}
    assert cp2.stage_deltas == {
        "s_a": {"a": 1},
        "s_b": {"b": 2},
        "s_c": {"c": 3, "a": 99},
    }


def test_from_dict_tolerates_missing_new_fields():
    """旧格式 cp (v0.5.0, 无新字段) → .get 默认 {} (A7 兼容)."""
    dag = _dag()
    d = _cp(dag).to_dict()
    del d["initial_state"]
    del d["stage_deltas"]
    cp = Checkpoint.from_dict(d)
    assert cp.initial_state == {}
    assert cp.stage_deltas == {}


def test_rebuild_state_before_first_stage_returns_initial():
    dag = _dag()
    cp = _cp(dag, initial={"query": "x"})
    rebuilt = cp.rebuild_state_before("s_a")
    assert rebuilt == {"query": "x"}


def test_rebuild_state_before_mid_stage():
    dag = _dag()
    cp = _cp(dag, initial={"query": "x"})
    rebuilt = cp.rebuild_state_before("s_c")
    # s_c 前: initial + delta(s_a) + delta(s_b) — s_c 自己的覆盖还没发生
    assert rebuilt == {"query": "x", "a": 1, "b": 2}


def test_rebuild_state_before_handles_chain_overwrite():
    dag = _dag()
    cp = _cp(dag)
    rebuilt = cp.rebuild_state_before("s_c")
    assert rebuilt == {"a": 1, "b": 2}  # a 还是 s_a 的 1, 未被 s_c 覆盖


def test_rebuild_unknown_stage_raises():
    dag = _dag()
    cp = _cp(dag)
    with pytest.raises(KeyError):
        cp.rebuild_state_before("s_nonexistent")
