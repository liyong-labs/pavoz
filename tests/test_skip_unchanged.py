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


def _build_fu1(dag: DAG, ran: list, b_variant: bool):
    """fu1 的 DAG 工厂. b_variant=False/True 生成 s_b 源码不同的两个版本;
    s_a/s_c 两版本源码完全一致 (同名同体同缩进 — 指纹才可能命中)."""
    suffix = "2" if b_variant else "1"

    @dag.stage()
    async def s_a(ctx):
        ran.append("a" + suffix)
        return {"a": 1}

    if b_variant:
        @dag.stage(depends_on=["s_a"])
        async def s_b(ctx):
            ran.append("b" + suffix)
            _ = ctx.state.get("a")  # 源码差异点
            return {"b": 10}
    else:
        @dag.stage(depends_on=["s_a"])
        async def s_b(ctx):
            ran.append("b" + suffix)
            return {"b": 10}

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        ran.append("c" + suffix)
        return {"c": 100}

    return dag


async def test_fork_skip_unchanged_reuses_downstream(tmp_path):
    """钱景测试 (ai-writing): 改 s_b 内部逻辑 (输出不变) → b 重跑, c 跳过."""
    store = CheckpointStore(FileStorage(str(tmp_path / "fu1")))
    ran: list[str] = []

    dag_v1 = _build_fu1(DAG("fu1"), ran, b_variant=False)
    await Runtime(checkpoint_store=store).run(dag_v1, "fu1")
    assert ran == ["a1", "b1", "c1"]

    # v2: b 的源码变了 (加一行标记), 但返回值不变 → b 重跑, c 输入没变 → 跳过
    dag_v2 = _build_fu1(DAG("fu1"), ran, b_variant=True)

    ran.clear()
    r = await Runtime(checkpoint_store=store).fork_run(
        dag_v2, "fu1", from_stage="s_b", skip_unchanged=True)
    assert r.status == "done"
    assert ran == ["b2"], f"b 应重跑, c 应跳过, 实际 ran={ran}"
    assert r.stage_statuses["s_c"] == "done"  # c 复用历史


def _build_fu2(dag: DAG, variant: int):
    """fu2 的 DAG 工厂. multiplier 写成源码字面量 (运行时闭包值不进指纹)."""

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    if variant == 1:
        @dag.stage(depends_on=["s_a"])
        async def s_b(ctx):
            return {"b": ctx.state.get("a", 0) * 10}
    else:
        @dag.stage(depends_on=["s_a"])
        async def s_b(ctx):
            return {"b": ctx.state.get("a", 0) * 20}

    return dag


async def test_fork_skip_unchanged_reruns_when_output_changes(tmp_path):
    """上游输出真的变了 → 下游 hash 变 → 连锁重跑."""
    store = CheckpointStore(FileStorage(str(tmp_path / "fu2")))
    dag_v1 = _build_fu2(DAG("fu2"), variant=1)
    await Runtime(checkpoint_store=store).run(dag_v1, "fu2")

    dag_v2 = _build_fu2(DAG("fu2"), variant=2)

    r = await Runtime(checkpoint_store=store).fork_run(
        dag_v2, "fu2", from_stage="s_b", skip_unchanged=True)
    assert r.status == "done"
    assert r.state["b"] == 20  # 重跑后的新值


async def test_fork_overrides_rerun_from_stage_even_with_skip(tmp_path):
    """overrides 改变 from_stage 输入 → 必然重跑 (语义保底)."""
    store = CheckpointStore(FileStorage(str(tmp_path / "fu3")))
    ran: list[str] = []
    dag = DAG("fu3")

    @dag.stage()
    async def s_a(ctx):
        return {"prompt": "v1", "a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        ran.append("b")
        return {"b": ctx.state.get("prompt")}

    await Runtime(checkpoint_store=store).run(dag, "fu3")
    ran.clear()
    r = await Runtime(checkpoint_store=store).fork_run(
        dag, "fu3", from_stage="s_b", overrides={"prompt": "v2"},
        skip_unchanged=True)
    assert ran == ["b"] and r.state["b"] == "v2"


async def test_fork_skip_respects_stage_opt_out(tmp_path):
    """grill Q3: 副作用 stage 标 skip_unchanged=False → 即使 hash 命中也不跳."""
    store = CheckpointStore(FileStorage(str(tmp_path / "fu4")))
    ran: list[str] = []
    dag = DAG("fu4")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"], skip_unchanged=False)
    async def s_side_effect(ctx):
        ran.append("side")
        return {"b": 1}

    await Runtime(checkpoint_store=store).run(dag, "fu4")
    ran.clear()
    r = await Runtime(checkpoint_store=store).fork_run(
        dag, "fu4", from_stage="s_side_effect", skip_unchanged=True)
    assert r.status == "done"
    assert ran == ["side"], "标了 skip_unchanged=False 的 stage 必须重跑"
