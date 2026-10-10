"""异常层级: 库自身抛出的异常统一挂在 PavozError 下, 消费方一个 except 兜住.

背景: StageError/RetryableError/FatalError 早已挂 PavozError, 但条件边那三个
(UnmappedRouteError / MaxVisitsExceeded / OrphanStagesError) 直接继承 Exception,
且 PavozError 本身没在顶层导出 — 消费方无法统一捕获, 只能逐个 import。

判据用"真的 except 一次"而非只断言 issubclass: 让 Python 自己验捕获成立。
另钉两条边界, 防将来把不该进树的异常挂进来 / 把该进的挪出去。
"""

from __future__ import annotations

import asyncio
import tempfile

from pavoz import (
    DAG,
    CheckpointMismatchError,
    CheckpointStore,
    FatalError,
    FileStorage,
    MaxVisitsExceeded,
    OrphanStagesError,
    PavozError,
    RetryableError,
    Runtime,
    StageError,
    UnmappedRouteError,
)

ENGINE_ERRORS = (
    StageError,
    RetryableError,
    FatalError,
    UnmappedRouteError,
    MaxVisitsExceeded,
    OrphanStagesError,
)


def test_pavoz_error_exported_at_top_level():
    import pavoz

    assert "PavozError" in pavoz.__all__
    assert pavoz.PavozError is PavozError


def test_all_engine_errors_derive_from_base():
    for exc in ENGINE_ERRORS:
        assert issubclass(exc, PavozError), f"{exc.__name__} 应挂在 PavozError 下"


def test_single_except_clause_catches_each_engine_error():
    """一个 except PavozError 真的能兜住每一个 (不是只查 issubclass)."""
    for exc in ENGINE_ERRORS:
        caught = None
        try:
            raise exc("boom")
        except PavozError as e:  # noqa: PERF203 — 逐个验证正是本测试的目的
            caught = e
        assert isinstance(caught, exc), f"{exc.__name__} 未被 PavozError 捕获"


def test_checkpoint_mismatch_stays_outside_the_tree():
    """调用方用错 (cp 与 DAG 结构不匹配) 不是引擎判的失败, 故意不进这棵树."""
    assert not issubclass(CheckpointMismatchError, PavozError)
    assert issubclass(CheckpointMismatchError, Exception)


def test_run_reports_engine_error_as_failed_not_raised():
    """run() 不抛这些 — 转成 failed + error_class (既有契约, 本测试钉住)."""
    dag = DAG("h")
    calls = {"n": 0}

    @dag.stage()
    async def s_a(ctx):
        calls["n"] += 1
        return {"n": calls["n"]}

    @dag.stage()
    async def s_b(ctx):
        return {"b": 1}

    dag.add_conditional_edges("s_a", lambda s: "nope", {"ok": "s_b"})

    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(tempfile.mkdtemp())))
    r = asyncio.run(rt.run(dag, task_id="t1"))
    assert r.status == "failed"
    assert r.error_class == "UnmappedRouteError"
