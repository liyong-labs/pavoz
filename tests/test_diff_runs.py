"""R4 (0.5.4): diff_runs — 两个 run 的 stage 级 structured diff (id=135).

要求 (ai@home): 给两个 run_id, 输出 stage 级 diff (input hash / output diff /
status), 不做 AI 摘要, 只要 structured diff, 全 JSON-serializable (R5 闭合).
"""


from pavoz import DAG, CheckpointStore, FileStorage, Runtime, diff_runs


def _mk_store(tmp_path, name):
    return CheckpointStore(FileStorage(str(tmp_path / name)))

def _mk_dag(name, multiplier):
    """multiplier 分支写源码字面量 (闭包变量不进指纹 — _build_fu2 同款教训)."""
    dag = DAG(name)

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    if multiplier == 7:
        @dag.stage(depends_on=["s_a"])
        async def s_b(ctx):
            return {"b": ctx.state.get("a", 0) * 7}
    else:
        @dag.stage(depends_on=["s_a"])
        async def s_b(ctx):
            return {"b": ctx.state.get("a", 0) * 14}

    return dag

async def test_identical_runs_empty_diff(tmp_path):
    """同 DAG 同输入跑两次 → 无任何 diff (stage 级全同)."""
    store = _mk_store(tmp_path, "dr1")
    dag = _mk_dag("dr1", multiplier=7)
    rt = Runtime(checkpoint_store=store)
    r1 = await rt.run(dag, "dr1")
    r2 = await rt.run(dag, "dr1")

    cp_a = store.load("dr1", r1.run_id)
    cp_b = store.load("dr1", r2.run_id)
    d = diff_runs(cp_a, cp_b)

    assert d["workflow_hash_changed"] is False
    assert d["state_diff"] == {}
    s_a = d["stages"]["s_a"]
    assert s_a["status"] == ["done", "done"]
    assert s_a["input_hash_changed"] is False
    assert s_a["output_diff"] == {}

async def test_output_change_shown_in_diff(tmp_path):
    """s_b 源码变 (×7→×14) → s_b output_diff = {b: [7, 14]}; s_a 无 diff."""
    store = _mk_store(tmp_path, "dr2")
    rt = Runtime(checkpoint_store=store)
    r1 = await rt.run(_mk_dag("dr2", multiplier=7), "dr2")
    r2 = await rt.run(_mk_dag("dr2", multiplier=14), "dr2")

    d = diff_runs(store.load("dr2", r1.run_id), store.load("dr2", r2.run_id))
    assert d["workflow_hash_changed"] is False  # 结构 (名/依赖) 没变
    assert d["stages"]["s_a"]["output_diff"] == {}
    assert d["stages"]["s_b"]["output_diff"] == {"b": [7, 14]}
    assert d["state_diff"] == {"b": [7, 14]}  # 最终 state 也是 b 变了

async def test_input_hash_change_flagged(tmp_path):
    """s_b 源码变 → input_hash_changed=True; s_a 不变 → False."""
    store = _mk_store(tmp_path, "dr3")
    rt = Runtime(checkpoint_store=store)
    r1 = await rt.run(_mk_dag("dr3", multiplier=7), "dr3")
    r2 = await rt.run(_mk_dag("dr3", multiplier=14), "dr3")

    d = diff_runs(store.load("dr3", r1.run_id), store.load("dr3", r2.run_id))
    assert d["stages"]["s_a"]["input_hash_changed"] is False
    assert d["stages"]["s_b"]["input_hash_changed"] is True

async def test_status_change_shown(tmp_path):
    """一侧 failed 一侧 done → status diff 可见."""
    store = _mk_store(tmp_path, "dr4")

    def _add_s_c(dag):
        @dag.stage(depends_on=["s_b"])
        async def s_c(ctx):
            return 1 / 0 if ctx.state.get("b", 0) > 10 else {"c": 1}

    dag1 = _mk_dag("dr4", multiplier=7)
    _add_s_c(dag1)
    dag2 = _mk_dag("dr4", multiplier=14)
    _add_s_c(dag2)  # 两个 dag 都要有 s_c (否则一侧 done 一侧根本没跑)

    rt = Runtime(checkpoint_store=store)
    r1 = await rt.run(dag1, "dr4")   # b=7 → c done
    assert r1.status == "done"
    r2 = await rt.run(dag2, "dr4")  # b=14 → c 1/0 fail
    assert r2.status == "failed"

    d = diff_runs(store.load("dr4", r1.run_id), store.load("dr4", r2.run_id))
    assert d["stages"]["s_c"]["status"] == ["done", "failed"]
    assert d["stages"]["s_c"]["input_hash_changed"] is True  # 输入 b 也变了

async def test_stage_added_in_b_side(tmp_path):
    """v2 多一个 stage (结构变) → workflow_hash_changed + 新 stage in_a=False."""
    store = _mk_store(tmp_path, "dr5")
    rt = Runtime(checkpoint_store=store)
    r1 = await rt.run(_mk_dag("dr5", multiplier=7), "dr5")

    dag2 = _mk_dag("dr5", multiplier=7)

    @dag2.stage(depends_on=["s_b"])
    async def s_new(ctx):
        return {"n": 1}

    r2 = await rt.run(dag2, "dr5")
    d = diff_runs(store.load("dr5", r1.run_id), store.load("dr5", r2.run_id))
    assert d["workflow_hash_changed"] is True
    assert d["stages"]["s_new"]["status"] == [None, "done"]
    assert d["stages"]["s_a"]["status"] == ["done", "done"]  # 旧 stage 照常比

async def test_store_diff_runs_convenience(tmp_path):
    """CheckpointStore.diff_runs(task, a, b="") — "" = latest 指针 (load_compatible 惯例)."""
    store = _mk_store(tmp_path, "dr6")
    dag = _mk_dag("dr6", multiplier=7)
    rt = Runtime(checkpoint_store=store)
    r1 = await rt.run(dag, "dr6")
    r2 = await rt.run(dag, "dr6")

    d = store.diff_runs("dr6", r1.run_id, r2.run_id)
    assert d["run_id_a"] == r1.run_id and d["run_id_b"] == r2.run_id
    d2 = store.diff_runs("dr6", r1.run_id)  # b 缺省 = latest
    assert d2["run_id_b"] == r2.run_id
