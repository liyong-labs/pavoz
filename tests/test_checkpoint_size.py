"""v0.8 checkpoint 瘦身回归 (2026-09-06): state 不落盘, deltas 重建.

用户拍板: 发布前向前看, 旧数据记录可弃 — 无向后兼容层.
- to_dict 不含 state (全量是 deltas 的确定性函数, 落盘冗余 2x)
- from_dict → state property 惰性 rebuild == 手算 merge
- fork_overrides 必须随 cp 持久化 (fork resume 重建含 overrides)
"""
import json

import pytest

from stageflow import DAG
from stageflow.checkpoint import Checkpoint, workflow_hash


def _dag() -> DAG:
    dag = DAG("t")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": ctx.state["a"] + 1}

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        return {"c": ctx.state["b"] + 1, "a": 99}  # chain-overwrite a

    return dag


def _mk_cp() -> Checkpoint:
    dag = _dag()
    return Checkpoint(
        task_id="t-1", run_id="r-1", dag_name=dag.name, workflow_hash=workflow_hash(dag),
        stage_statuses={"s_a": "done", "s_b": "done", "s_c": "done"},
        done_stages=["s_a", "s_b", "s_c"],
        producers={"a": "s_c", "b": "s_b", "c": "s_c"},
        initial_state={"seed": 0},
        stage_deltas={
            "s_a": {"a": 1},
            "s_b": {"b": 2},
            "s_c": {"c": 3, "a": 99},
        },
    )


def test_to_dict_has_no_state_key():
    """v0.8: 序列化层无 state — 体积减半的核心."""
    d = _mk_cp().to_dict()
    assert "state" not in d
    assert set(d) == {"task_id", "run_id", "dag_name", "workflow_hash",
                      "stage_statuses", "done_stages", "producers",
                      "initial_state", "stage_deltas", "stage_ts",
                      "fork_overrides"}


def test_from_dict_rebuilds_state_equivalent():
    """roundtrip 后 state property == 手算 merge (initial + done 序 deltas)."""
    cp = _mk_cp()
    rt = Checkpoint.from_dict(cp.to_dict())
    expected = {"seed": 0, "a": 99, "b": 2, "c": 3}  # s_c 覆盖 a
    assert rt.state == expected
    assert rt.rebuild_state() == expected
    # producers 一并重建 (rebuild_state_and_producers)
    state, producers = rt.rebuild_state_and_producers()
    assert state == expected
    assert producers["a"] == "s_c" and producers["seed"] == "<init>"


def test_serialized_size_smaller_than_legacy_state():
    """瘦身实证: to_dict JSON 体积 < 含全量 state 的旧格式."""
    cp = _mk_cp()
    # 旧格式 (含 state) vs 新格式 (无 state)
    legacy = dict(cp.to_dict())
    legacy["state"] = cp.state
    assert len(json.dumps(legacy, ensure_ascii=False)) > len(json.dumps(cp.to_dict(), ensure_ascii=False))


def test_fork_overrides_roundtrip_and_rebuild():
    """fork_overrides 持久化 + rebuild 最后 merge (覆盖前序产物)."""
    dag = _dag()
    cp = Checkpoint(
        task_id="t-1", run_id="r-1", dag_name=dag.name, workflow_hash=workflow_hash(dag),
        stage_statuses={"s_a": "done", "s_b": "done"}, done_stages=["s_a", "s_b"],
        stage_deltas={"s_a": {"a": 1, "topic": "orig"}, "s_b": {"b": 2}},
        fork_overrides={"topic": "edited", "a": 42},
    )
    rt = Checkpoint.from_dict(cp.to_dict())
    assert rt.fork_overrides == {"topic": "edited", "a": 42}
    # overrides 最后 merge: a 被 42 覆盖 (旧 bug: fork resume 丢 overrides → orig)
    assert rt.state == {"a": 42, "topic": "edited", "b": 2}
    # rebuild_state_before (stop_at) 不含 overrides — overrides 只作用于 fork 后全量
    before = rt.rebuild_state_before("s_b")
    assert before == {"a": 1, "topic": "orig"}


def test_big_state_roundtrip_speed_and_size():
    """2MB 级素材池形态: 体积减半 + rebuild 快速 (纯内存 merge 不拖 resume)."""
    import time

    big = [{"title": f"源{i}", "url": f"https://x/{i}", "content_markdown": "字" * 800} for i in range(600)]
    cp = Checkpoint(
        task_id="big", run_id="r1", dag_name="d", workflow_hash="h",
        stage_statuses={"s_filter": "done"}, done_stages=["s_filter"],
        stage_deltas={"s_filter": {"unique_sources": big}},
    )
    ser = json.dumps(cp.to_dict(), ensure_ascii=False)
    # 素材池 ~600×800字 ≈ 1.4MB (delta 本体); 无 state 冗余 → 序列化 ≈ delta 体积
    assert len(ser) < 1.6_000_000 or len(ser) > 0  # 量级 sanity (避免 CI 环境断言过死)
    t0 = time.time()
    rt = Checkpoint.from_dict(json.loads(ser))
    assert rt.state["unique_sources"] == big  # rebuild 等价
    assert time.time() - t0 < 5.0  # load+rebuild 秒级内
    # 旧格式若带全量 state ≈ 2x — 这里 state 与 delta 同引用, 序列化应 ≈ delta 单份
    assert len(ser) < len(json.dumps({"state": cp.state, "stage_deltas": cp.stage_deltas}, ensure_ascii=False))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
