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

from .checkpoint import Checkpoint, CheckpointMismatchError, workflow_hash
from .dag import DAG
from .runtime import Runtime, _noop_caller
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
        self._caller = caller or _noop_caller

    def mock(self, stage_name: str, fn: MockFn) -> TestPipe:
        """mock 指定 stage (跳过真实现). fn 收 ctx.state (ReadOnlyStateView)."""
        if stage_name not in self.dag.stages:
            raise KeyError(f"stage '{stage_name}' 不在 DAG {self.dag.name} 里")
        self._mocks[stage_name] = fn
        return self

    def calls(self) -> list[dict]:
        """stage 调用记录: [{stage, mock, args}]. 用于断言调用顺序/次数."""
        return list(self._recorded)

    @classmethod
    def replay_from(cls, cp: Checkpoint, dag: DAG) -> TestPipe:
        """从真实 Checkpoint 构造 TestPipe: 已完成的 stage 用历史 delta 喂 (mock),
        未完成的走真实现 — 图回归夹具 (M2, ROADMAP). 用法: 真跑存 cp → 改 stage
        实现 → replay_from 全 mock 回归对比终态 (result.state == cp.state).

        防护 (同 Runtime.run_stage): cp 与 dag 的 workflow_hash 不一致 (DAG
        结构变了) → CheckpointMismatchError; v0.5.0 旧 cp (无 stage_deltas) →
        RuntimeError 提示重跑一次 — 不静默错配 / 不静默真跑.
        """
        cur_hash = workflow_hash(dag)
        if cp.workflow_hash != cur_hash:
            raise CheckpointMismatchError(
                f"task {cp.task_id} run {cp.run_id[:8]} checkpoint 的 DAG hash "
                f"{cp.workflow_hash} ≠ 当前 DAG hash {cur_hash}. DAG 结构变了, "
                f"不能 replay (只改 stage 函数体不影响 hash; 改依赖/retries/timeout 会)."
            )
        if cp.done_stages and not cp.stage_deltas and not cp.initial_state and cp.state:
            raise RuntimeError(
                "checkpoint 无 stage_deltas — v0.5.0 旧版产物, 请重新 run 一次再重放"
            )
        pipe = cls(dag, initial_state=dict(cp.initial_state))
        for stage_name in cp.done_stages:
            delta = cp.stage_deltas.get(stage_name)
            if delta is not None:
                pipe.mock(stage_name, lambda _state, _d=delta: dict(_d))
        return pipe

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
