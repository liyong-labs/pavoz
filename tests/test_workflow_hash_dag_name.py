"""DAG 名必须计入结构指纹 —— 否则两条流水线可以静默混流.

背景 (2026-10-16 巡检): `workflow_hash` 只覆盖 stage 名 + 依赖 + retries +
timeout + 条件边, **不含 dag 名**。于是结构相同、名字不同的两个 DAG (两条不同
流水线) 拿到同一个 hash; task_id 撞车时 resume 会拿错 cp 且毫无提示。实测:

    ① dag "extract" 首跑, 末 stage 失败 → cp 部分完成 (by=EXTRACT)
    ② dag "classify" resume 同一 task → status=done
       state = {'by': 'EXTRACT', ..., 'final': 'EXTRACT'}
       RunResult.dag_name = 'classify'      ← 报告是 classify, 装的是 extract 的数据

RunResult.dag_name 撒谎, state 跨流水线污染, 而 task_id 是幂等键 —— 拿它续跑的
前提本就是"同一个工作流", 名字是工作流身份的一部分。

本测试钉住三件事: 名字进指纹 (不同名 → 不同 hash)、名字进指纹的**代价** (改名
= 旧 cp 不能续跑, 这是刻意的)、以及跨 DAG resume 真的会被 CheckpointMismatchError
拦住 —— 而不是只测 hash 值。
"""
from __future__ import annotations

import tempfile

import pytest

from pavoz import (
    DAG,
    CheckpointMismatchError,
    CheckpointStore,
    FileStorage,
    Runtime,
    StageError,
    TestPipe,
    workflow_hash,
)


def _build(name: str, label: str, *, fail_last: bool):
    dag = DAG(name)

    @dag.stage()
    async def s_a(ctx):
        return {"by": label}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        if fail_last:
            raise StageError("b 失败", category="SEARCH_NO_RESULT")
        return {"final": ctx.state["by"]}

    return dag


def test_dag_name_is_part_of_the_fingerprint():
    """结构相同、名字不同 → 指纹必须不同 (这是本轮修复的全部内容)。"""
    h_extract = workflow_hash(_build("extract", "X", fail_last=False))
    h_classify = workflow_hash(_build("classify", "Y", fail_last=False))
    assert h_extract != h_classify, "dag 名没进指纹 → 两条流水线指纹相同"


def test_same_dag_name_same_structure_is_still_stable():
    """反向约束: 同一名字同一结构 → 指纹稳定 (不许把名字变成随机盐)。"""
    assert (workflow_hash(_build("extract", "A", fail_last=False))
            == workflow_hash(_build("extract", "B", fail_last=False)))


async def test_renaming_a_dag_invalidates_old_checkpoint():
    """改名的**代价**, 刻意如此: 宁可拒绝, 不可静默混流。"""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    r = await rt.run(_build("extract", "EXTRACT", fail_last=True), "t1")
    assert r.status == "failed", "前提: 先造一个部分完成的 cp"

    with pytest.raises(CheckpointMismatchError, match="≠ 当前 DAG hash"):
        await rt.run(_build("extract_v2", "EXTRACT", fail_last=True), "t1", resume=True)


async def test_cross_dag_resume_is_rejected_instead_of_mixing_state():
    """核心防线: 换一条流水线来 resume → 响, 而不是静默产出混合 state。"""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    await rt.run(_build("extract", "EXTRACT", fail_last=True), "t2")

    with pytest.raises(CheckpointMismatchError):
        await rt.run(_build("classify", "CLASSIFY", fail_last=False), "t2", resume=True)


async def test_cross_dag_fork_run_is_rejected_too():
    """fork_run 走另一条入口, 同样要拦。"""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    await rt.run(_build("extract", "EXTRACT", fail_last=True), "t3")

    with pytest.raises(CheckpointMismatchError):
        await rt.fork_run(_build("classify", "CLASSIFY", fail_last=False),
                          "t3", from_stage="s_a")


def test_replay_from_rejects_cross_dag_checkpoint(tmp_path):
    """TestPipe.replay_from 是第三条入口 —— 同样要拦, 且消息共用同一构造点。"""
    import asyncio

    async def _go():
        store = CheckpointStore(FileStorage(tmp_path / "cp"))
        rt = Runtime(checkpoint_store=store)
        await rt.run(_build("extract", "EXTRACT", fail_last=True), "t4")
        return store.load_latest("t4")

    cp = asyncio.run(_go())
    with pytest.raises(CheckpointMismatchError, match="extract"):
        TestPipe.replay_from(cp, _build("classify", "CLASSIFY", fail_last=False))


async def test_mismatch_message_names_both_dags_when_they_differ():
    """名字不同是最常见的 mismatch 原因, 消息要点名双方 (别让人自己猜)。"""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    rt = Runtime(checkpoint_store=store)
    await rt.run(_build("extract", "EXTRACT", fail_last=True), "t5")

    with pytest.raises(CheckpointMismatchError) as ei:
        await rt.run(_build("classify", "CLASSIFY", fail_last=False), "t5", resume=True)
    msg = str(ei.value)
    assert "extract" in msg and "classify" in msg, f"消息该点名两个 dag 名: {msg}"
    assert "不能 resume" in msg


# ── workflow_hash 的缓存必须能被结构变更打破 ────────────────────────────────
#
# 起因 (2026-10-17 实测, 160 个 stage 的线性 DAG profile):
#
#   _save_cp          0.852s / 共 0.929s   ← 92%
#     └ workflow_hash 0.290s               ← **每个 stage 重算一次**
#
# `workflow_hash(dag)` 是 DAG **结构**的纯函数, 而一次 run 里结构不变 —— 也就是说
# 这 31% 的运行时花在反复重算一个常量上, 且**所有规模都这样** (n=10 时也一样是常量
# 比例), 不是"到几百个 stage 才出现"的规模问题。所以这是纯粹的浪费, 不是提前设计。
#
# 缓存的风险只有一个: **结构变了却用旧指纹** —— 那会让 resume/fork 拿一个已经不
# 对的 hash 去匹配 cp, 正是这个函数存在的理由。所以下面四条钉的是**失效语义**,
# 不是"快了多少" (耗时断言在 CI 上不可靠, 也不该进判据)。


def test_workflow_hash_is_stable_across_calls():
    """同一个未变的 DAG → 同样的 hash (缓存不能改结果)。"""
    dag = DAG("hash_cache_stable")

    @dag.stage()
    async def s_a(ctx):
        return {}

    first = workflow_hash(dag)
    assert workflow_hash(dag) == first, "同结构重复取 hash 必须一致"
    assert workflow_hash(dag) == first, "第三次也一样 —— 缓存不是一次性的"


def _linear_dag(name: str, n: int) -> DAG:
    """n 个 stage 的线性 DAG。

    用 exec 生成而不是手写循环里的 @dag.stage(): 装饰器拿到的是同一个函数对象,
    靠改 __name__ 区分会踩 "duplicate stage name" —— 这个坑本轮踩过一次。
    """
    src = ["from pavoz import DAG", f"d = DAG({name!r})", "prev = None"]
    for i in range(n):
        src += ["@d.stage(depends_on=[prev] if prev else None)",
                "async def f{i}(ctx):".replace("{i}", str(i)),
                "    return {'acc': ctx.state.get('acc', 0) + 1}",   # 无插值, 不必是 f-string
                f"prev = 'f{i}'"]
    ns: dict = {}
    exec("\n".join(src), ns)          # noqa: S102 — 生成测试用的 DAG 而已
    return ns["d"]


def test_workflow_hash_computed_once_per_run_not_per_stage(monkeypatch):
    """★ 结构指纹整次 run 只算一次, 不是每个 stage 一次。

    起因是 profile 而非猜测: 160 stage 的线性 DAG 里 `_save_cp` 占 92% 运行时,
    其中 `workflow_hash` 占 31% —— 而它是 dag 的纯函数, 在一次 run 里是常量。
    修法是把值当参数传下去 (不是"缓存到 dag 对象上")。

    判据用**调用次数**, 不用耗时: 耗时断言在 CI 上不可靠, 而且它只能间接说明。
    数调用次数是直接说明"没有每 stage 重算"。
    """
    import pavoz.runtime as rt
    from pavoz.checkpoint import workflow_hash as real_hash

    calls = {"n": 0}

    def counting(dag):
        calls["n"] += 1
        return real_hash(dag)

    monkeypatch.setattr(rt, "workflow_hash", counting)

    dag = _linear_dag("wf_once", 5)

    import asyncio
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        from pavoz.checkpoint import CheckpointStore
        from pavoz.storage import FileStorage

        store = CheckpointStore(FileStorage(td))
        r = asyncio.run(rt.Runtime(checkpoint_store=store).run(dag, "wf_once"))
    assert r.status == "done", f"前置: run 应完成, 实得 {r.status}"

    # 允许 1 次 (run 开头)。每个 stage 一次的话这里是 6 (5 stage + resume 分支)。
    assert calls["n"] <= 1, (
        f"5 个 stage 的 run 算了 {calls['n']} 次结构指纹 —— 每 stage 重算是个纯常量, "
        f"160 stage 时占 31% 运行时")


def test_checkpoint_hash_is_never_stale(monkeypatch):
    """★ 传下去的值必须是**真值** —— 优化不能变成"陈旧指纹"。

    这才是这个改动唯一真正的风险面: 值算一次是对的, 但如果传下去的东西与
    `workflow_hash(dag)` 不一致, resume/fork 就会拿一个对不上的指纹去匹配 cp ——
    而"指纹对不上要报错"正是这个函数存在的理由。所以每个落盘的 cp 都验一遍。
    """
    import asyncio
    import tempfile

    import pavoz.runtime as rt
    from pavoz.checkpoint import CheckpointStore, workflow_hash
    from pavoz.storage import FileStorage

    dag = DAG("wf_fresh")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 2}

    with tempfile.TemporaryDirectory() as td:
        store = CheckpointStore(FileStorage(td))
        r = asyncio.run(rt.Runtime(checkpoint_store=store).run(dag, "wf_fresh"))
        assert r.status == "done"
        cp = store.load_latest("wf_fresh")
        assert cp is not None, "落盘 cp 不见了 —— 那这个用例验不到任何东西"
        assert cp.workflow_hash == workflow_hash(dag), (
            f"cp 里的指纹与现算的不一致: 落盘 {cp.workflow_hash} vs 现算 "
            f"{workflow_hash(dag)} —— 优化把'每次重算'换成了'陈旧值'")

def test_workflow_hash_cache_invalidated_by_retries_change():
    """★ 改 retries → hash 必须变。retries 计入指纹, 缓存必须跟着失效。"""
    dag = DAG("hash_cache_retry")

    @dag.stage(retries=0)
    async def s_a(ctx):
        return {}

    before = workflow_hash(dag)
    dag.stages["s_a"].retries = 3          # 直接改 (与 TestPipe 换 fn 同类操作)
    assert workflow_hash(dag) != before, (
        "retries 变了但 hash 没变 —— 缓存没被打破, 旧 cp 会被当成同结构 cp")
