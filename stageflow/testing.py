"""TestPipe — 回归测试夹具 (v1 第一天就要有).

stage 可以被 mock 输入/输出, 跑全 DAG 验证 stage 修改不破坏链路.
naobao 调研: justpipe 的 TestPipe 是自研编排器最容易漏的一环 (2026-09-05).

用法:
    pipe = TestPipe(my_dag, initial_state={"query": "x"})
    pipe.mock("s_search", lambda ctx: {"sources": [fake_source]})
    result = await pipe.run()

mock fn 签名 = stage 签名: lambda ctx -> dict (同步也可, 内部 await 兼容)
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from .dag import DAG
from .runtime import Runtime, _default_caller
from .state import ReadOnlyStateView
from .types import RunResult

__all__ = ["TestPipe"]

MockFn = Callable[[ReadOnlyStateView], dict | Awaitable[dict]]


class TestPipe:
    """喂假 stage 输出跑全图. 不依赖真实 LLM/SE/Extract."""

    __test__ = False  # 防 pytest 把 TestPipe 当测试类收集

    def __init__(
        self,
        dag: DAG,
        *,
        initial_state: dict | None = None,
        caller=None,
    ):
        self.dag = dag
        self.initial_state = initial_state or {}
        self._mocks: dict[str, MockFn] = {}
        self._recorded: list[dict] = []  # 每 stage 调用记录 (断言语料)
        self._caller = caller or _default_caller

    def mock(self, stage_name: str, fn: MockFn) -> TestPipe:
        """mock 指定 stage (跳过真实现). fn 收 ctx.state (ReadOnlyStateView)."""
        if stage_name not in self.dag.stages:
            raise KeyError(f"stage '{stage_name}' 不在 DAG {self.dag.name} 里")
        self._mocks[stage_name] = fn
        return self

    def calls(self) -> list[dict]:
        """stage 调用记录: [{stage, mock, args}]. 用于断言调用顺序/次数."""
        return list(self._recorded)

    async def run(self, *, resume: bool = False) -> RunResult:
        """跑全图 (mock 的 stage 用假输出, 其余走真实现)."""

        runtime = Runtime(
            checkpoint_store=None,  # TestPipe 不落盘 (回归用, 不恢复)
            caller=self._caller,
        )
        original: dict[str, object] = {}

        # monkey-patch: 把被 mock 的 stage 换成假 fn
        for name, mock_fn in self._mocks.items():
            original[name] = self.dag.stages[name].fn

            async def _wrapped(ctx, _name=name, _fn=mock_fn):
                out = _fn(ctx.state)
                if asyncio.iscoroutine(out):
                    out = await out
                self._recorded.append({"stage": _name, "mock": True, "state_keys": list(ctx.state.keys())})
                if not isinstance(out, dict):
                    raise TypeError(f"mock '{_name}' 必须返回 dict, got {type(out).__name__}")
                return out

            self.dag.stages[name].fn = _wrapped  # type: ignore[assignment]

        try:
            result = await runtime.run(
                self.dag,
                task_id="test-pipe",
                initial_state=self.initial_state,
                resume=resume,
            )
        finally:
            # 还原 stage (TestPipe 实例可复用)
            for name, fn in original.items():
                self.dag.stages[name].fn = fn  # type: ignore[assignment]
        return result
