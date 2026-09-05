"""Runtime: 执行 DAG. 顺序执行 + per-node retries + absolute deadline + state merge + checkpoint.

流程 (单 task):
    runtime.run(task_id, dag, initial_state, resume=False)
      ├─ 加载 checkpoint (resume=True 且存在) → 校验 workflow hash
      ├─ 按拓扑序跑未完成 stage:
      │    ├─ 每 stage: 构造 ctx → 深拷贝 state 快照 → 调 fn(req, ctx)
      │    ├─ StageError → fail 终态 (不重试)
      │    ├─ RetryableError → 扣 retries, 指数退避重试; 耗尽 → fail
      │    ├─ FatalError → 立即 fail (不消耗 retries)
      │    ├─ timeout → fail (RetryableError 语义, 可重试)
      │    └─ return dict → merge_state (冲突 → StateConflictError → fail)
      └─ 每 stage 后: checkpoint.save
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from .checkpoint import Checkpoint, CheckpointStore
from .dag import DAG
from .state import ReadOnlyStateView, deep_validate_state, merge_state, snapshot
from .types import FatalError, RetryableError, RunResult, StageError

logger = logging.getLogger("stageflow")

__all__ = ["CallResult", "Ctx", "Runtime"]


class CallResult(dict):
    """ctx.call 的返回: 标准 dict, 业务 adapter 决定内容."""


@dataclass
class Ctx:
    """传给 stage 的上下文. read-only state + call/log API.

    stage 拿到的 ctx.state 是 run 级 state 的 deep copy (defensive) —
    改它不影响 run state; 真正的写入靠 return dict.
    """

    task_id: str
    dag: DAG
    stage_name: str
    state: ReadOnlyStateView
    caller: Callable[[str, str, dict], Awaitable[dict]] = field(
        default=lambda kind, op, params: CallResult(kind=kind, op=op, params=params)
    )
    logger: logging.Logger = field(default_factory=lambda: logger)
    deadline: float | None = None  # absolute deadline (time.time()), 无 = 不限制

    async def call(self, kind: str, op: str, params: dict | None = None) -> dict:
        """对外调用唯一入口. kind: 'llm'/'search'/'extract'/'http'.

        caller (业务 adapter) 决定实际执行. 默认 no-op (demo/TestPipe 覆盖).
        """
        return await self.caller(kind, op, params or {})

    def log(self, msg: str, *, level: str = "INFO", **meta) -> None:
        """业务日志 (JSON trace 的一部分). level: DEBUG/INFO/WARN/ERROR."""
        lvl = getattr(logging, level.upper(), logging.INFO)
        self.logger.log(lvl, "task=%s stage=%s %s %s", self.task_id, self.stage_name, msg, meta)

    def remaining_seconds(self) -> float | None:
        """absolute deadline 剩余秒数. 无 deadline 返 None."""
        if self.deadline is None:
            return None
        return max(0.0, self.deadline - time.time())


@dataclass
class Runtime:
    """执行引擎. 每次 run 一个 task (可复用实例跑多个 task)."""

    checkpoint_store: CheckpointStore | None = None
    caller: Callable[[str, str, dict], Awaitable[dict]] | None = None
    default_timeout: float | None = None  # 整个 run 的 absolute deadline (秒)

    # ── 主入口 ──────────────────────────────────────────
    async def run(
        self,
        dag: DAG,
        task_id: str,
        *,
        initial_state: dict | None = None,
        resume: bool = True,
        deadline: float | None = None,
    ) -> RunResult:
        """执行 DAG. task_id 由 caller 提供 (opaque key)."""
        dag.validate()
        result = RunResult(task_id, dag.name)
        state: dict[str, Any] = {}
        done_stages: list[str] = []
        stage_statuses: dict[str, str] = {}

        # ── resume: 加载 checkpoint ──
        if resume and self.checkpoint_store is not None:
            cp = self.checkpoint_store.load_compatible(task_id, dag)
            if cp is not None:
                state = cp.state
                done_stages = list(cp.done_stages)
                stage_statuses = dict(cp.stage_statuses)
                result.state = state
                logger.info("task=%s resume: 已完成 %d stages", task_id, len(done_stages))

        # initial_state 只在无 checkpoint 时注入 (resume 时 initial_state 已含在 cp.state)
        if not done_stages and initial_state:
            state = merge_state({}, initial_state, "<init>")
            result.state = state

        deep_validate_state(state)

        run_deadline = deadline or (time.time() + self.default_timeout if self.default_timeout else None)

        order = dag.topo_order()
        for name in order:
            if name in done_stages:
                continue
            stage = dag.stages[name]

            # per-stage timeout: min(stage.timeout, remaining run deadline)
            stage_deadline: float | None = None
            if stage.timeout is not None:
                stage_deadline = time.time() + stage.timeout
            if run_deadline is not None:
                stage_deadline = (
                    min(stage_deadline, run_deadline) if stage_deadline is not None else run_deadline
                )

            status, state, err = await self._run_stage(
                dag, stage.fn, name, task_id, state, stage.retries, stage_deadline
            )
            stage_statuses[name] = status
            result.stage_statuses = stage_statuses
            result.state = state

            if status == "failed":
                result.status = "failed"
                result.error = err
                self._save_cp(task_id, dag, state, done_stages, stage_statuses)
                return result

            done_stages.append(name)
            self._save_cp(task_id, dag, state, done_stages, stage_statuses)
            logger.info("task=%s stage=%s done (len state=%d)", task_id, name, len(state))

        result.status = "done"
        if self.checkpoint_store is not None:
            self.checkpoint_store.delete(task_id)  # 跑完清 checkpoint
        return result

    # ── 单 stage ────────────────────────────────────────
    async def _run_stage(
        self,
        dag: DAG,
        fn: Callable,
        name: str,
        task_id: str,
        state: dict,
        retries: int,
        stage_deadline: float | None,
    ) -> tuple[str, dict, str | None]:
        """跑一个 stage (含 retry). 返 (status, new_state, error)."""
        attempt = 0
        while True:
            attempt += 1
            try:
                # 每 attempt 深拷贝 state (防 stage 意外 mutate 污染后续重试)
                ctx = Ctx(
                    task_id=task_id,
                    dag=dag,
                    stage_name=name,
                    state=ReadOnlyStateView(snapshot(state)),
                    caller=self.caller or _default_caller,
                    deadline=stage_deadline,
                )
                delta = await self._with_timeout(fn, None, ctx, stage_deadline, name, task_id)
                if delta is None:
                    delta = {}
                if not isinstance(delta, dict):
                    raise FatalError(
                        f"stage '{name}' 必须返回 dict, got {type(delta).__name__}"
                    )
                deep_validate_state(delta)
                new_state = merge_state(state, delta, name)
                return "done", new_state, None

            except (StageError, FatalError) as e:
                # 业务错误 / 程序 bug: 不重试
                logger.warning("task=%s stage=%s %s: %s", task_id, name, type(e).__name__, e)
                return "failed", state, str(e)
            except (RetryableError, TimeoutError) as e:
                if attempt <= retries:
                    backoff = min(2 ** (attempt - 1), 30)
                    logger.warning(
                        "task=%s stage=%s attempt=%d/%d %s, 退避 %ss: %s",
                        task_id, name, attempt, retries + 1, type(e).__name__, backoff, e,
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.warning("task=%s stage=%s retries 耗尽: %s", task_id, name, e)
                return "failed", state, str(e)
            except Exception as e:  # 未知异常 → FatalError 语义
                logger.exception("task=%s stage=%s 未预期异常", task_id, name)
                return "failed", state, f"FatalError: {e}"

    # ── timeout wrapper ─────────────────────────────────
    async def _with_timeout(self, fn, _req_unused, ctx, deadline, name, task_id):
        """包 asyncio.wait_for. stage fn 签名 fn(ctx) — v1 统一 ctx-only."""
        if deadline is None:
            return await fn(ctx)
        timeout = max(0.1, deadline - time.time())
        try:
            return await asyncio.wait_for(fn(ctx), timeout=timeout)
        except TimeoutError:
            raise TimeoutError(f"stage '{name}' 超时 ({timeout:.0f}s)") from None

    # ── checkpoint ──────────────────────────────────────
    def _save_cp(self, task_id: str, dag: DAG, state: dict, done_stages: list, statuses: dict) -> None:
        if self.checkpoint_store is None:
            return
        try:
            from .checkpoint import workflow_hash

            cp = Checkpoint(
                task_id=task_id,
                dag_name=dag.name,
                workflow_hash=workflow_hash(dag),
                stage_statuses=dict(statuses),
                state=state,
                done_stages=list(done_stages),
            )
            self.checkpoint_store.save(cp)
        except Exception:
            logger.exception("task=%s checkpoint 保存失败 (non-fatal)", task_id)


async def _default_caller(kind: str, op: str, params: dict) -> dict:
    """默认 caller: no-op echo. 真调用由业务注入 (ai_writer adapter / TestPipe mock)."""
    return CallResult(kind=kind, op=op, params=params)
