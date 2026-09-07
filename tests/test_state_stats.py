"""#36 (2026-09-06): Checkpoint.state_stats — state 体积观测 (调试大 state 用)."""

from pavoz.checkpoint import Checkpoint


def _mk_cp() -> Checkpoint:
    return Checkpoint(
        task_id="t1", run_id="r1", dag_name="d", workflow_hash="h",
        stage_statuses={"s_a": "done", "s_b": "done"},
        done_stages=["s_a", "s_b"],
        initial_state={"seed": 0},
        # v0.8: state 从 deltas 重建 — 终态 = initial + s_a + s_b
        stage_deltas={
            "s_a": {"big_list": list(range(5000))},
            "s_b": {"article": "字" * 8000},
        },
    )


def test_state_stats_reports_sizes_and_top_keys():
    cp = _mk_cp()
    st = cp.state_stats()
    assert st["state_keys"] == 3
    assert st["state_total_bytes"] > 0
    # top_keys 按体积降序: big_list (5000 ints ≈ 19K chars) 应最大
    assert st["top_keys"][0]["key"] == "big_list"
    assert st["top_keys"][0]["bytes"] > 10000
    assert any(k["key"] == "article" and k["bytes"] >= 8000 for k in st["top_keys"])
    assert st["deltas_bytes"] > 0
    assert st["done_stages"] == ["s_a", "s_b"]


def test_state_stats_top_n_caps_results():
    cp = _mk_cp()
    st = cp.state_stats(top_n=2)
    assert len(st["top_keys"]) == 2


def test_state_stats_empty_state():
    cp = Checkpoint(
        task_id="t", run_id="r", dag_name="d", workflow_hash="h",
        stage_statuses={}, done_stages=[])
    st = cp.state_stats()
    assert st["state_total_bytes"] == 0
    assert st["top_keys"] == []
