"""Runtime: 执行 DAG. 顺序执行 + per-node retries + absolute deadline + state merge + checkpoint.

流程 (一次 run = 一个 run_id):
    runtime.run(dag, task_id=None, initial_state, resume=False)
      ├─ task_id: 省略 → UUID4; 入口校验 (A3: 非空/≤128/字符集受限)
      ├─ resume=True → 按指针加载该 task 最新 checkpoint (复用其 run_id):
      │    ├─ 无 cp → RuntimeError; 已全部完成 → RuntimeError (done guard)
      │    ├─ DAG hash 变了 → CheckpointMismatchError
      │    └─ 恢复 state/done_stages/producers, 只跑未完成 stage
      ├─ 按拓扑序跑未完成 stage:
      │    ├─ 每 stage: 构造 ctx (含 task_id/run_id/attempt) → 深拷贝 state 快照 → 调 fn(ctx)
      │    ├─ StageError → fail 终态 (不重试)
      │    ├─ RetryableError → 扣 retries, 指数退避重试; 耗尽 → fail
      │    ├─ FatalError → 立即 fail (不消耗 retries)
      │    ├─ timeout → fail (RetryableError 语义, 可重试)
      │    └─ return dict → merge_state (冲突 → StateConflictError → fail)
      └─ 每 stage 后: checkpoint.save (runs/{task_id}/{run_id}/checkpoint + latest 指针)
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from ._id import new_id
from .checkpoint import (
    Checkpoint,
    CheckpointMismatchError,
    CheckpointStore,
    workflow_hash,
)
from .dag import DAG
from .state import ReadOnlyStateView, deep_validate_state, merge_state, snapshot
from .types import FatalError, RetryableError, RunResult, StageError

logger = logging.getLogger("stageflow")

__all__ = ["CallMeta", "CallResult", "Ctx", "Runtime"]


class CallResult(dict):
    """ctx.call 的返回: 标准 dict, 业务 adapter 决定内容."""


@dataclass(frozen=True)
class CallMeta:
    """ctx.call 自动携带的执行上下文 — caller 记录 trace 用.

    caller 落 trace/llm_calls 时用它关联执行现场: task/run/stage/attempt.
    """

    task_id: str
    run_id: str
    stage: str
    attempt: int


@dataclass
class Ctx:
    """传给 stage 的上下文. read-only state + call/log API.

    stage 拿到的 ctx.state 是 run 级 state 的 deep copy (defensive) —
    改它不影响 run state; 真正的写入靠 return dict.
    """

    task_id: str
    run_id: str           # 每次 runtime.run() 一个 UUID4 (resume 复用)
    attempt: int          # 1-based, RetryableError retries 时 +1
    dag: DAG
    stage_name: str
    state: ReadOnlyStateView
    caller: Callable[[str, str, dict, CallMeta], Awaitable[dict]] = field(
        default=lambda kind, op, params, meta: CallResult(kind=kind, op=op, params=params)
    )
    logger: logging.Logger = field(default_factory=lambda: logger)
    deadline: float | None = None  # absolute deadline (time.time()), 无 = 不限制

    async def call(self, kind: str, op: str, params: dict | None = None) -> dict:
        """Outbound call entry point. `kind` is opaque (caller-defined: 'llm', 'search', 'http', ...).

        Caller (via Runtime's caller injection) decides actual execution.
        Default is a no-op (suitable for TestPipe mocks and demos).
        """
        meta = CallMeta(
            task_id=self.task_id,
            run_id=self.run_id,
            stage=self.stage_name,
            attempt=self.attempt,
        )
        return await self.caller(kind, op, params or {}, meta)


@dataclass
class Runtime:
    """执行引擎. 每次 run 一个 task (可复用实例跑多个 task)."""

    checkpoint_store: CheckpointStore | None = None
    caller: Callable[[str, str, dict, CallMeta], Awaitable[dict]] | None = None
    default_timeout: float | None = None  # 整个 run 的 absolute deadline (秒)

    # ── 主入口 ──────────────────────────────────────────
    async def run(
        self,
        dag: DAG,
        task_id: str | None = None,
        *,
        initial_state: dict | None = None,
        resume: bool = False,
    ) -> RunResult:
        """执行 DAG 一次 run.

        Args:
            dag: 要执行的 DAG
            task_id: caller 提供的 task ID (省略 → 自动 UUID4). 跨 retry/resume
                     稳定 — 幂等键. resume 时必填 (自动生成的 ID 不会有 cp).
            initial_state: 起始 state (默认 {}; resume 命中 cp 时忽略, 已含在 cp.state)
            resume: True → 从该 task 最新 checkpoint 续跑 (复用原 run_id).
                     无 cp / 已全部完成 → RuntimeError. 重跑用 resume=False.
        """
        # task_id: 省略 → UUID4; 显式传值 → A3 校验 (直接进 storage key 路径)
        if task_id is None:
            task_id = new_id()
        _validate_task_id(task_id)

        dag.validate()
        result = RunResult(task_id=task_id, dag_name=dag.name)

        # ── run_id: 每次 run 一个 UUID4; resume 复用 cp 的 run_id ──
        cp: Checkpoint | None = None
        if resume:
            if self.checkpoint_store is None:
                raise RuntimeError("resume=True requires checkpoint_store")
            cp = self.checkpoint_store.load_latest(task_id)
            if cp is None:
                raise RuntimeError(
                    f"task_id={task_id} 无 checkpoint, 不能 resume. 首次跑用 resume=False."
                )
            # done guard: topo 全覆盖 + 无 failed = 已完成 → 明确报错防静默 no-op
            topo = set(dag.topo_order())
            if topo <= set(cp.done_stages) and not any(
                s == "failed" for s in cp.stage_statuses.values()
            ):
                raise RuntimeError(
                    f"task_id={task_id} run={cp.run_id[:8]} 已全部完成. "
                    f"重跑请用 resume=False (起新 run_id)."
                )
            run_id = cp.run_id  # Continue-As-New: 跨 worker 重启同 run_id
        else:
            run_id = new_id()
        result.run_id = run_id

        state: dict[str, Any] = {}
        done_stages: list[str] = []
        stage_statuses: dict[str, str] = {}
        # v0.1.1 链式覆盖: state key → producer stage
        producers: dict[str, str] = {}

        # ── resume: 恢复 checkpoint 状态 ──
        if cp is not None:
            cur_hash = workflow_hash(dag)
            if cp.workflow_hash != cur_hash:
                raise CheckpointMismatchError(
                    f"task {task_id} run {cp.run_id[:8]} checkpoint 的 DAG hash "
                    f"{cp.workflow_hash} ≠ 当前 DAG hash {cur_hash}. "
                    f"DAG 结构变了, 不能 resume. "
                    f"如需强制重跑: 删 checkpoint 或 Runtime(..., resume=False)"
                )
            state = dict(cp.state)
            done_stages = list(cp.done_stages)
            stage_statuses = dict(cp.stage_statuses)
            producers = dict(cp.producers)
            result.state = state
            logger.info(
                "task=%s run=%s resume: 已完成 %d stages",
                task_id, run_id[:8], len(done_stages),
            )

        # initial_state 只在无 checkpoint 时注入 (resume 时 initial_state 已含在 cp.state)
        if not done_stages and initial_state:
            state = merge_state({}, initial_state, "<init>")
            for _k in initial_state:
                producers[_k] = "<init>"
            result.state = state

        deep_validate_state(state)

        run_deadline = time.time() + self.default_timeout if self.default_timeout else None

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

            status, new_state, err, producers = await self._run_stage(
                dag, stage.fn, name, task_id, run_id, state, stage.retries,
                stage_deadline, producers,
            )
            state = new_state
            stage_statuses[name] = status
            result.stage_statuses = stage_statuses
            result.state = state

            if status == "failed":
                result.status = "failed"
                result.error = err
                self._save_cp(task_id, run_id, dag, state, done_stages, stage_statuses, producers)
                return result

            done_stages.append(name)
            self._save_cp(task_id, run_id, dag, state, done_stages, stage_statuses, producers)
            logger.info(
                "task=%s run=%s stage=%s done (len state=%d)",
                task_id, run_id[:8], name, len(state),
            )

        result.status = "done"
        return result

    # ── 单 stage ────────────────────────────────────────
    async def _run_stage(
        self,
        dag: DAG,
        fn: Callable,
        name: str,
        task_id: str,
        run_id: str,
        state: dict,
        retries: int,
        stage_deadline: float | None,
        producers: dict[str, str],
    ) -> tuple[str, dict, str | None, dict]:
        """跑一个 stage (含 retry). 返 (status, new_state, error, producers).

        ctx.attempt = 1-based 当前尝试次数, RetryableError 重试时 +1.
        """
        attempt = 0
        while True:
            attempt += 1
            try:
                # 每 attempt 深拷贝 state (防 stage 意外 mutate 污染后续重试)
                ctx = Ctx(
                    task_id=task_id,
                    run_id=run_id,
                    attempt=attempt,
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
                # v0.1.1 链式覆盖: producer 是当前 stage 传递上游 → 允许覆盖 (流水线演进)
                _ow = {
                    k for k in delta
                    if k in state and producers.get(k)
                    and dag.reachable(producers[k], name)
                }
                new_state = merge_state(state, delta, name, overwrite_keys=_ow)
                for _k in delta:
                    producers[_k] = name
                return "done", new_state, None, producers

            except (StageError, FatalError) as e:
                # 业务错误 / 程序 bug: 不重试
                logger.warning(
                    "task=%s run=%s stage=%s %s: %s",
                    task_id, run_id[:8], name, type(e).__name__, e,
                )
                return "failed", state, str(e), producers
            except (RetryableError, TimeoutError) as e:
                if attempt <= retries:
                    backoff = min(2 ** (attempt - 1), 30)
                    logger.warning(
                        "task=%s run=%s stage=%s attempt=%d/%d %s, 退避 %ss: %s",
                        task_id, run_id[:8], name, attempt, retries + 1,
                        type(e).__name__, backoff, e,
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.warning(
                    "task=%s run=%s stage=%s retries 耗尽: %s",
                    task_id, run_id[:8], name, e,
                )
                return "failed", state, str(e), producers
            except Exception as e:  # 未知异常 → FatalError 语义
                logger.exception("task=%s run=%s stage=%s 未预期异常", task_id, run_id[:8], name)
                return "failed", state, f"FatalError: {e}", producers

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
    def _save_cp(self, task_id: str, run_id: str, dag: DAG, state: dict,
                 done_stages: list, statuses: dict, producers: dict | None = None) -> None:
        if self.checkpoint_store is None:
            return
        try:
            cp = Checkpoint(
                task_id=task_id,
                run_id=run_id,
                dag_name=dag.name,
                workflow_hash=workflow_hash(dag),
                stage_statuses=dict(statuses),
                state=state,
                done_stages=list(done_stages),
                producers=dict(producers or {}),
            )
            self.checkpoint_store.save(cp)
        except Exception:
            logger.exception(
                "task=%s run=%s checkpoint 保存失败 (non-fatal)", task_id, run_id[:8]
            )


def _validate_task_id(task_id: str) -> None:
    """task_id 直接进 storage key 路径 — 必须无 '/' 且字符集受限."""
    if not task_id:
        raise ValueError("task_id 不能为空")
    if len(task_id) > 128:
        raise ValueError(f"task_id 过长: {len(task_id)} > 128")
    if not all(c.isalnum() or c in "_.-" for c in task_id):
        raise ValueError(
            f"task_id 含非法字符: {task_id!r}. 只允许 [A-Za-z0-9_.-] "
            f"(不含 '/', 防止 storage key 路径注入)"
        )


async def _default_caller(kind: str, op: str, params: dict, meta: CallMeta) -> dict:
    """默认 caller: no-op echo. 真调用由业务注入 (ai_writer adapter / TestPipe mock)."""
    return CallResult(kind=kind, op=op, params=params)
