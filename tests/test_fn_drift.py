"""R2 (ai@home id=1132 请求): 源码漂移可见化 — fork/replay 时 log fn 漂移.

fn 体不计 workflow_hash (改 prompt 免全链重跑, 有意设计), 代价是"改了代码
跑的是新是旧"不可见. 漂移时不阻断不拒续, log 一行把"静默"变"可见".
"""

from __future__ import annotations

import logging

from pavoz import DAG, CheckpointStore, FileStorage, Runtime


def build(variant: str):
    """同结构 (workflow_hash 相同) 不同 fn 源码 — 字面量必须写进源码文本,
    否则两个 variant 的源码指纹相同, 无漂移可言."""
    dag = DAG("drift")

    if variant == "a":

        @dag.stage()
        async def s_a(ctx):
            return {"v": "a"}

        @dag.stage(depends_on=["s_a"])
        async def s_b(ctx):
            return {"out": "from-a"}
    else:

        @dag.stage()
        async def s_a(ctx):
            return {"v": "b"}

        @dag.stage(depends_on=["s_a"])
        async def s_b(ctx):
            return {"out": "from-b"}

    return dag


async def _run_once(dag, tmp_path):
    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(tmp_path / "cp"))))
    r = await rt.run(dag, task_id="t1")
    return rt, r


async def test_fork_logs_source_drift_and_runs_new_code(tmp_path, caplog):
    dag_a, dag_b = build("a"), build("b")
    assert dag_a.topo_order() == dag_b.topo_order()  # 结构一致
    rt, _ = await _run_once(dag_a, tmp_path)

    with caplog.at_level(logging.WARNING, logger="pavoz"):
        r = await rt.fork_run(dag_b, "t1", from_stage="s_a")

    assert r.status == "done"
    assert any("源码" in rec.message and "s_a" in rec.message
               for rec in caplog.records), "漂移应有一行 WARNING"
    assert r.state["out"] == "from-b"  # 新代码确实在跑


async def test_fork_without_drift_no_warning(tmp_path, caplog):
    rt, _ = await _run_once(build("a"), tmp_path)
    with caplog.at_level(logging.WARNING, logger="pavoz"):
        r = await rt.fork_run(build("a"), "t1", from_stage="s_a")
    assert r.status == "done"
    assert not any("源码" in rec.message for rec in caplog.records)


async def test_run_stage_logs_source_drift(tmp_path, caplog):
    dag_a, dag_b = build("a"), build("b")
    rt, _ = await _run_once(dag_a, tmp_path)
    with caplog.at_level(logging.WARNING, logger="pavoz"):
        r = await rt.run_stage(dag_b, "t1", "s_b")
    assert r.status == "done"
    assert any("源码" in rec.message and "s_b" in rec.message
               for rec in caplog.records)


async def test_old_cp_without_fn_sources_tolerated():
    """旧 cp 无 stage_fn_sources 字段 → A7 默认空, 不漂移检查不报错."""
    from pavoz import Checkpoint

    cp = Checkpoint.from_dict({
        "task_id": "t", "run_id": "r", "dag_name": "d", "workflow_hash": "x",
        "stage_statuses": {}, "done_stages": [], "producers": {},
        "initial_state": {},
    })
    assert cp.stage_fn_sources == {}
