"""DAG 解析 + cycle 检测 + 拓扑排序 + 装饰器契约."""

import pytest

from pavoz import DAG, CycleError, UnknownDepError


def _mk(name="t"):
    return DAG(name)


def test_stage_must_be_async():
    dag = _mk()
    with pytest.raises(TypeError, match="async def"):
        dag.stage()(lambda ctx: {"a": 1})


def test_duplicate_stage_name():
    dag = _mk()

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    # 同名注册 (通过 direct 调 fn 模拟, 同 scope 第二次 def 会 shadow)
    async def s_a(ctx):  # noqa: F811
        return {"a": 2}

    with pytest.raises(ValueError, match="重名"):
        dag.stage()(s_a)


def test_unknown_dep():
    dag = _mk()

    @dag.stage(depends_on=["nope"])
    async def s_a(ctx):
        return {"a": 1}

    with pytest.raises(UnknownDepError, match="nope"):
        dag.validate()


def test_cycle_detected():
    dag = _mk()

    @dag.stage(depends_on=["s_b"])
    async def s_a(ctx):
        return {}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {}

    with pytest.raises(CycleError):
        dag.validate()


def test_self_cycle_detected():
    dag = _mk()

    @dag.stage(depends_on=["s_self"])
    async def s_self(ctx):
        return {}

    with pytest.raises(CycleError):
        dag.validate()


def test_topo_order_respects_deps():
    dag = _mk()

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        return {}

    @dag.stage()
    async def s_a(ctx):
        return {}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {}

    @dag.stage(depends_on=["s_a", "s_c"])
    async def s_d(ctx):
        return {}

    order = dag.topo_order()
    assert order.index("s_a") < order.index("s_c")
    assert order.index("s_c") < order.index("s_d")
    assert order.index("s_a") < order.index("s_d")


def test_validate_freezes():
    dag = _mk()

    @dag.stage()
    async def s_a(ctx):
        return {}

    dag.validate()
    with pytest.raises(RuntimeError, match="冻结"):

        @dag.stage()
        async def s_b(ctx):
            return {}


def test_dag_requires_name():
    with pytest.raises(ValueError):
        DAG("  ")
