"""CheckpointStore.load_compatible — 已文档化的公开 API, 此前零测试零内部调用.

背景: 2026-10 覆盖率调查发现 checkpoint.py 的 load_compatible 整段未覆盖。查证:
它是**文档化的公开 API** (docs/{cn,en}/api.md 均列出, CHANGELOG 记过签名
2→3 参变更), 但 pavoz 内部没有调用方 —— resume / fork 路径各自内联校验
workflow_hash (runtime.py 三处)。也就是说 "加载 + 拒绝陈旧 checkpoint" 这个
契约有两份实现, 只有被调用得多的那份有测试。

本测试把公开方法本身的契约钉住: 匹配则返回、结构变了则拒、"" 是 latest 哨兵、
未知 task 返回 None, 以及最关键的一条 —— **只改 stage 函数体不算结构变更**
(workflow_hash 有意不含函数体, 这是"改 prompt 免全链重跑"的基础)。
"""
from __future__ import annotations

import pytest

from pavoz import (
    CheckpointMismatchError,
    CheckpointStore,
    DAG,
    FileStorage,
    Runtime,
)


def make_dag(variant: str = "base") -> DAG:
    """两 stage 链。variant 改变**函数体源码文本**, 结构保持不变。

    必须用字面量分支而不是闭包变量: 源码指纹走 inspect.getsource, 闭包捕获的
    value 不在源码文本里 —— 用参数的话两个 DAG 的 fn 文本完全相同, "改函数体"
    这个前提根本不成立 (第一版就这么写, 反向验证时才发现: 把 fn 塞进
    workflow_hash 后测试仍然全绿)。
    """
    dag = DAG("cp")

    if variant == "edited":

        @dag.stage()
        async def s_a(ctx):
            return {"a": 999}          # 字面量不同 → 源码文本不同

    else:

        @dag.stage()
        async def s_a(ctx):
            return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": ctx.state["a"] + 1}

    return dag


@pytest.fixture
async def store_and_run(tmp_path):
    store = CheckpointStore(FileStorage(str(tmp_path / "cp")))
    dag = make_dag()
    rt = Runtime(checkpoint_store=store)
    r = await rt.run(dag, task_id="t1")
    assert r.status == "done"
    return store, dag, r


async def test_matching_dag_returns_checkpoint(store_and_run):
    store, dag, r = store_and_run
    cp = store.load_compatible("t1", r.run_id, dag)
    assert cp is not None
    assert cp.run_id == r.run_id


async def test_structural_change_is_refused(store_and_run):
    """加一个 stage → workflow_hash 变 → 必须显式拒绝, 不静默续跑."""
    store, _dag, r = store_and_run
    dag_more = make_dag()
    dag_more.stage()(_extra_stage)
    with pytest.raises(CheckpointMismatchError, match="DAG hash"):
        store.load_compatible("t1", r.run_id, dag_more)


async def test_body_only_change_stays_compatible(store_and_run):
    """只改 stage 函数体 → 结构未变 → 仍可续跑。

    workflow_hash 有意不含函数体 (这是"改 prompt 免全链重跑"的地基), 所以
    这里必须**兼容** —— 与上一条构成一对, 防止有人给 hash 加上函数体。
    """
    store, _dag, r = store_and_run
    dag_edited = make_dag("edited")  # 同结构, 不同函数体
    cp = store.load_compatible("t1", r.run_id, dag_edited)
    assert cp is not None


async def test_empty_run_id_resolves_latest(store_and_run):
    store, dag, r = store_and_run
    cp = store.load_compatible("t1", "", dag)
    assert cp is not None
    assert cp.run_id == r.run_id


async def test_unknown_task_returns_none(store_and_run):
    store, dag, _r = store_and_run
    assert store.load_compatible("no-such-task", "", dag) is None


async def _extra_stage(ctx):
    return {"extra": True}
