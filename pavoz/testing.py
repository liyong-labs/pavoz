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

from .checkpoint import (
    Checkpoint,
    CheckpointMismatchError,
    dag_mismatch_msg,
    workflow_hash,
)
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
        """stage 调用记录, 每项含 `stage` / `mock` / `state_keys`, 按调用序。

        用于断言调用顺序与次数; `state_keys` 是该 stage 被调用时**看到的 state
        顶层 key** (断言"下游确实拿到了上游的产出"时用)。仅 mock 过的 stage 会
        记进这里 —— 未 mock 的 stage 走真实现, 不留调用记录。
        """
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
            raise CheckpointMismatchError(dag_mismatch_msg(
                cp.task_id, cp, dag, action="DAG 结构变了, 不能 replay"))
        # ⚠️ 环图 (条件边回路, R1/0.5.5) 上本夹具**只保证终态自洽, 不保证路径
        # 一致**: 每个已完成 stage 被 mock 成"返回它**最后一次** visit 的 delta"
        # (cp.stage_deltas 是 last-write 视图), 于是回路里每轮都拿终值 —— 真实的
        # 3 轮会被压成 1 轮。多数场景下终态仍与 cp.state 相等 (因为终值本身就是
        # 终点), 但轮次敏感的 stage (计数/累加) 会算错。条件边比本夹具晚出,
        # 这个限制此前没写下来。
        if cp.done_stages and not cp.stage_deltas:
            # v0.8: state 不落盘, 不能依赖 cp.state (property 重建恒有值) — 只看 deltas
            raise RuntimeError(
                "checkpoint 无 stage_deltas — 旧版产物, 请重新 run 一次再重放"
            )
        pipe = cls(dag, initial_state=dict(cp.initial_state))
        for stage_name in cp.done_stages:
            delta = cp.stage_deltas.get(stage_name)
            if delta is not None:
                pipe.mock(stage_name, lambda _state, _d=delta: dict(_d))
        return pipe

    async def run(self, *, resume: bool = False) -> RunResult:
        """跑全图 (mock 的 stage 用假输出, 其余走真实现).

        `resume` 保留但**不能用**: TestPipe 不落盘 (无 checkpoint_store), 续跑
        无从谈起。此前它一路传到 Runtime 才炸, 报的是内部不变量
        "resume=True requires checkpoint_store" —— 与调用方毫无关系的话。现直接
        在这里说清。要在真 cp 上做回归用 `TestPipe.replay_from`; 要真续跑用
        `Runtime(checkpoint_store=...).run(..., resume=True)`。
        """
        if resume:
            raise ValueError(
                "TestPipe.run(resume=True) 不支持: TestPipe 不落盘, 没有可续的 "
                "checkpoint. 要在真 cp 上回归用 TestPipe.replay_from(cp, dag); "
                "要真续跑用 Runtime(checkpoint_store=...).run(..., resume=True)."
            )

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
