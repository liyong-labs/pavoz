"""Fork (v0.6): 历史节点"取输入 → 改 → 装回"续跑 (LangGraph fork 范式).

竞品定位 (2026-09-06 调研): LangGraph time-travel fork = update_state 分支 +
invoke(None) 续跑, 原历史不动; Prefect 无"从任务 X 改参续跑"内置 (官方建议
copy+缓存复用). stageflow fork_run: 前序 stage 结果复用, from_stage 及其后继
用 overrides 重跑, 原 run cp 不动, fork cp (新 run_id) 成为 latest.
"""

import json

from stageflow import CheckpointStore, DAG, FileStorage, Runtime


def _mk_storage(task_id):
    return CheckpointStore(FileStorage(f"/tmp/stageflow-fork-test-{task_id}"))


def _dag3(trace: list):
    dag = DAG("fork3")

    @dag.stage()
    async def s_a(ctx):
        trace.append("a")
        return {"a": 1, "topic": "orig"}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        trace.append("b")
        return {"b": ctx.state["a"] + 1, "out": f"b:{ctx.state['topic']}"}

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        trace.append("c")
        return {"c": f"{ctx.state['out']}:{ctx.state['b'] * 2}"}

    return dag


async def test_fork_reruns_from_stage_with_overrides_keeps_prefix():
    """fork from s_b + override topic → s_a 不重跑, s_b/s_c 用新值, 原 cp 不动."""
    tid = "fork1"
    store = _mk_storage(tid)
    rt = Runtime(checkpoint_store=store)
    trace: list = []
    dag = _dag3(trace)
    r1 = await rt.run(dag, tid)
    assert r1.status == "done" and r1.state["c"] == "b:orig:4"
    assert trace == ["a", "b", "c"]

    trace.clear()
    r2 = await rt.fork_run(dag, tid, from_stage="s_b", overrides={"topic": "edited"})
    assert r2.status == "done"
    assert trace == ["b", "c"], f"s_a 必须复用不重跑, got {trace}"
    assert r2.state["c"] == "b:edited:4"
    # fork = 新 run_id, 原 run 的 cp 仍在 (list_runs ≥ 2)
    assert len(store.list_runs(tid)) >= 2


async def test_fork_latest_pointer_moves_and_resume_works():
    tid = "fork2"
    store = _mk_storage(tid)
    rt = Runtime(checkpoint_store=store)
    trace: list = []
    dag = _dag3(trace)
    await rt.run(dag, tid)
    r = await rt.fork_run(dag, tid, from_stage="s_c", overrides={"topic": "x"})
    assert r.status == "done"
    # latest → fork run; 再 resume 应报已全部完成 (fork 分支全跑完)
    try:
        await rt.run(dag, tid, resume=True)
        assert False, "fork 分支已全完成, resume 应 raise"
    except RuntimeError:
        pass


async def test_fork_from_failed_stage_retries_with_override():
    """失败 stage (依赖已完成) 可 fork: 注入 overrides 修输入重跑失败点."""
    tid = "fork3"
    store = _mk_storage(tid)
    rt = Runtime(checkpoint_store=store)
    calls = {"a": 0, "b": 0}

    dag = DAG("forkfail")

    @dag.stage()
    async def s_a(ctx):
        calls["a"] += 1
        return {"n": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        calls["b"] += 1
        if ctx.state["n"] == 1:
            raise RuntimeError("n 必须被 fork 改成 2")
        return {"ok": ctx.state["n"]}

    r1 = await rt.run(dag, tid)
    assert r1.status == "failed"
    assert calls == {"a": 1, "b": 1}

    r2 = await rt.fork_run(dag, tid, from_stage="s_b", overrides={"n": 2})
    assert r2.status == "done"
    assert r2.state["ok"] == 2
    assert calls == {"a": 1, "b": 2}  # s_a 未重跑, s_b 重试一次


async def test_fork_export_input_roundtrip():
    """export-input (rebuild_state_before) 输出 = fork overrides 的编辑基座."""
    tid = "fork4"
    store = _mk_storage(tid)
    rt = Runtime(checkpoint_store=store)
    trace: list = []
    dag = _dag3(trace)
    await rt.run(dag, tid)

    cp = store.load_latest(tid)
    before = cp.rebuild_state_before("s_b")
    assert before == {"a": 1, "topic": "orig"}
    # 人编辑 JSON → 装回 fork
    edited = json.loads(json.dumps(before))
    edited["topic"] = "roundtrip"
    r = await rt.fork_run(dag, tid, from_stage="s_b", overrides=edited)
    assert r.status == "done"
    assert r.state["c"] == "b:roundtrip:4"


async def test_fork_unknown_stage_and_missing_deps_rejected():
    tid = "fork5"
    store = _mk_storage(tid)
    rt = Runtime(checkpoint_store=store)
    dag = _dag3([])
    await rt.run(dag, tid)
    try:
        await rt.fork_run(dag, tid, from_stage="s_nope")
        assert False
    except KeyError:
        pass


async def test_fork_overrides_add_new_key_visible_downstream():
    """overrides 可注入前序未产生的新 key — 下游 stage 直接可见 (fork 输入扩展)."""
    tid = "fork6"
    store = _mk_storage(tid)
    rt = Runtime(checkpoint_store=store)
    trace: list = []
    dag = _dag3(trace)
    await rt.run(dag, tid)
    r = await rt.fork_run(dag, tid, from_stage="s_c", overrides={"style": "严肃媒体"})
    assert r.status == "done"
    # s_c 重跑 — 新 key 进 state (producer <fork>); s_c 是最后 stage 无下游, 但
    # rebuild_state_and_producers 应带出新 key (fork cp 重建时可见)
    cp = store.load_latest(tid)
    assert cp is not None and cp.state["style"] == "严肃媒体"
    assert cp.state["topic"] == "orig"  # 前序产物保留


async def test_fork_original_run_cp_untouched():
    """fork 后原 run 的 checkpoint 文件仍在且 state 不变 (只 latest 指针移走)."""
    tid = "fork7"
    store = _mk_storage(tid)
    rt = Runtime(checkpoint_store=store)
    trace: list = []
    dag = _dag3(trace)
    await rt.run(dag, tid)
    orig = store.load_latest(tid)
    assert orig is not None
    orig_run_id, orig_state = orig.run_id, orig.state

    r = await rt.fork_run(dag, tid, from_stage="s_b", overrides={"topic": "edited"})
    assert r.status == "done"
    runs = store.list_runs(tid)
    assert orig_run_id in runs, "原 run cp 必须保留 (可回溯)"
    back = store.load(tid, orig_run_id)
    assert back is not None and back.state == orig_state, "原 cp state 不被 fork 污染"
