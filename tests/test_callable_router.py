"""可调用对象当 route_fn: 每一次 checkpoint save 都静默失败, 而 run 报 done.

背景 (2026-10-16 巡检发现): `_stage_source_id` 只认 plain function 与绑定方法 ——
`class Router: def __call__(self, state)` 这种写法 (带查找表的 router 对象, 很常见)
会连撞两道: `inspect.getsource(实例)` 抛 TypeError, 老 fallback 取 `fn.__code__`
又抛 AttributeError。

而这个异常发生在 `_save_cp` 里, 被那条**"失败不杀 run"** 的兜底 (`except Exception:
logger.exception`) 吞掉。实测后果:

    run status = done            ← 表面成功
    落盘 checkpoint 文件数 = 0    ← 什么都没存
    load_latest() = None         ← resume / replay / trace / fork / diff 全 unavailable

只剩一行没人读的日志。这就是最坏的一类静默失败: 用户以为存下来了, 事发后才发现
整条恢复链路是空的。

本文件钉住: 可调用对象 (以及各类"取不到源码/字节码"的怪对象) 走完 run 之后
checkpoint 必须真的在盘上, 且 `cp.state == 活跑 state`。
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from pavoz import DAG, CheckpointStore, FileStorage, Runtime
from pavoz.checkpoint import _stage_source_id, workflow_hash


class TableRouter:
    """带查找表的 router 对象 —— 最常见的可调用对象写法.

    table 的值是**路由 key** (要出现在 add_conditional_edges 的 mapping 里),
    不是 stage 名 —— 我第一版写成 `{"yes": "s_yes"}`, 结果运行期报
    "route_fn 返回未声明 key 's_yes'", 框架是对的, 试例写错了。
    """

    def __init__(self, table: dict):
        self.table = table

    def __call__(self, state):
        return self.table[state["k"]]


class AsyncTableRouter(TableRouter):
    async def __call__(self, state):      # noqa: D102 — 故意: 测守卫
        return self.table[state["k"]]


class Weird:
    """探测任何属性都会炸 —— getattr 的默认值对它无效."""

    def __getattr__(self, name):
        raise RuntimeError("别碰我")


def _count_cp(root: Path) -> int:
    return len(list(root.rglob("checkpoint.json")))


# ── _stage_source_id 的"绝不能抛"契约 ─────────────────────────────────────

# 用**工厂**而不是实例: pytest 收集 parametrize 参数时会给每个值取 id/做 repr,
# 而 Weird 的 __getattr__ 对任何属性都抛 —— 直接把实例写在参数表里, 收集阶段
# 就炸了 (实测)。工厂把构造推迟到用例体内。
@pytest.mark.parametrize("label,make", [
    ("plain def", lambda: (lambda s: "yes")),
    ("可调用对象", lambda: TableRouter({"a": "yes"})),
    ("怪对象 (__getattr__ 抛)", Weird),
    ("内置函数", lambda: len),
])
def test_stage_source_id_never_raises(label, make):
    """指纹算不出可以退化成类型名, 但**绝不能抛** —— 见模块 docstring 的代价说明."""
    out = _stage_source_id(make())
    assert isinstance(out, str) and out, f"{label}: 应返回非空 str"


def test_callable_object_fingerprint_tracks_its_call_method():
    """两个同结构的 router 对象指纹相同 (结构级指纹, 与实例状态无关)."""
    a = _stage_source_id(TableRouter({"x": "y"}))
    b = _stage_source_id(TableRouter({"z": "w"}))
    assert a == b, "同类型同实现的 router 应得同一指纹"


def test_fingerprint_fallback_is_stable_across_processes():
    """兜底不能用 id() 之类的进程内标识 —— 两个进程算出的 hash 必须一致,
    否则一个进程写的 cp 另一个进程认不出来。"""
    obj = Weird()
    first = _stage_source_id(obj)
    # 同类型的新实例 (不同 id) 必须得到同一指纹
    assert _stage_source_id(Weird()) == first, "兜底指纹与实例身份无关"


# ── 端到端: 存下来了才算数 ────────────────────────────────────────────────

async def test_run_with_a_callable_object_router_persists_its_checkpoint():
    dag = DAG("obj_router")
    router = TableRouter({"yes": "yes"})

    @dag.stage()
    async def s_a(ctx):
        return {"k": "yes"}

    @dag.stage()
    async def s_yes(ctx):
        return {"y": 1}

    dag.add_conditional_edges("s_a", router, {"yes": "s_yes"}, max_visits=2)

    root = Path(tempfile.mkdtemp())
    store = CheckpointStore(FileStorage(root))
    r = await Runtime(checkpoint_store=store).run(dag, "obj")

    assert r.status == "done"
    assert _count_cp(root) == 1, (
        "checkpoint 必须真的落盘 —— 修复前是 0 (save 被'失败不杀 run'吞掉, 而 run "
        "报成功: resume/replay/trace/fork/diff 全 unavailable, 只剩一行没人读的日志)")
    cp = store.load_latest("obj")
    assert cp is not None and cp.state == r.state, "落盘 cp 与活跑结果必须一致"
    # 顺带确认整条恢复链路可用 (它依赖 cp 存在)。注意 dag_name 是 **DAG 的**名字,
    # 不是 task_id —— 我第一版写错了 (把两者混了)。
    assert cp.dag_name == "obj_router"


def test_workflow_hash_works_for_a_callable_object_router():
    """workflow_hash 是这条链路的入口, 它自己也不能因为 router 形态而炸."""
    # 同名: dag 名自 d21aab8 起计入指纹 (拦住跨流水线混流), 名字不同 → hash
    # 本就该不同。我第一版用 obj1/obj2, 于是"同结构 → 同指纹"这条断言是错的。
    a = DAG("same_name")
    b = DAG("same_name")

    def _build(d, tag):
        @d.stage()
        async def s_a(ctx):
            return {"k": tag}

        @d.stage()
        async def s_yes(ctx):
            return {"y": 1}

        d.add_conditional_edges("s_a", TableRouter({"yes": "yes"}),
                                {"yes": "s_yes"}, max_visits=2)

    _build(a, "t")
    _build(b, "t")
    assert isinstance(workflow_hash(a), str)
    assert workflow_hash(a) == workflow_hash(b), "同结构 → 同指纹"


# ── async 藏在 __call__ 里: 声明期就要拒 ─────────────────────────────────

def test_async_call_method_is_rejected_at_declaration():
    """★ 这是我上一轮那个守卫的漏网形态.

    `inspect.iscoroutinefunction(实例)` 是 False —— async 藏在 `__call__` 里。
    漏掉它就重演那条误导性错误: 运行期报 "route_fn 返回未声明 key <coroutine
    object ...>" (说路由返回了未声明的 key, 实际是路由压根没被调用), 外加一条
    "coroutine was never awaited" 警告。
    """
    dag = DAG("async_obj")

    @dag.stage()
    async def s_a(ctx):
        return {}

    @dag.stage()
    async def s_yes(ctx):
        return {}

    with pytest.raises(TypeError, match="同步纯函数"):
        dag.add_conditional_edges("s_a", AsyncTableRouter({"yes": "yes"}),
                                  {"yes": "s_yes"}, max_visits=2)


def test_sync_call_method_is_still_accepted():
    """反向约束: 别把可调用对象一并拒了 —— 那是合法写法。"""
    dag = DAG("sync_obj")

    @dag.stage()
    async def s_a(ctx):
        return {}

    @dag.stage()
    async def s_yes(ctx):
        return {}

    dag.add_conditional_edges("s_a", TableRouter({"yes": "yes"}),
                              {"yes": "s_yes"}, max_visits=2)
    dag.validate()
