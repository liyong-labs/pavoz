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
import json
import logging
import os
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from ._id import new_id
from .cancel import CancelRegistry
from .checkpoint import (
    Checkpoint,
    CheckpointMismatchError,
    CheckpointStore,
    stage_input_hash,
    workflow_hash,
)
from .dag import DAG
from .policy import EnginePolicy
from .state import (
    ReadOnlyStateView,
    _validate_dict_keys,
    apply_overrides,
    deep_validate_state,
    merge_state,
    snapshot,
)
from .types import FatalError, RetryableError, RunResult, StageError

logger = logging.getLogger("pavoz")

__all__ = ["CallMeta", "CallResult", "Ctx", "Runtime"]


async def _noop_caller(kind: str, op: str, params: dict, meta: CallMeta) -> dict:
    """默认 caller: no-op echo. 真调用由业务注入 (业务 adapter / TestPipe mock).

    必须是 async — Ctx.call 恒 await caller (sync fn 会被 await 崩 TypeError).
    """
    return CallResult(kind=kind, op=op, params=params)


def _fill_error_context(result: RunResult, stage_name: str,
                        err_class: str | None, state: dict) -> None:
    """W1 (0.5.3): caller-friendly 错误上下文.

    失败/取消时填 failed_stage / retryable / state_summary — caller 接到
    RunResult 即有"哪个 stage 错、能否重试、state 现场", 不必重跑诊断.
    """
    result.failed_stage = stage_name
    result.retryable = None if err_class is None else err_class in (
        "RetryableError", "TimeoutError")
    try:
        result.state_summary = json.dumps(
            state, ensure_ascii=False, default=str)[:2048]
    except (TypeError, ValueError):
        result.state_summary = None


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
        default=_noop_caller
    )
    logger: logging.Logger = field(default_factory=lambda: logger)
    deadline: float | None = None  # absolute deadline (time.time()), 无 = 不限制
    cancel_check: Callable[[], bool] | None = None  # v0.9: bound 到本 task 的取消检查
    on_event: Callable[[str, dict], None] | None = None  # R3: 进度心跳出口 (Runtime._emit)
    _last_progress: float | None = field(
        default=None, repr=False, compare=False)  # R3: 同 fraction 去抖

    def set_progress(self, fraction: float, note: str | None = None) -> None:
        """Stage 内可选进度心跳 (R3, 2026-09-10 ai-research PR).

        长 stage (分钟级 LLM 调用) 期间 "在算还是卡死" 的观测信号. 语义:
        - **best-effort 瞬态信号**: 只进 on_event 事件流 (stage_progress),
          **不写 checkpoint**, 不影响 deadline / 重试 / 取消语义, 无顺序送达保证.
        - 同 fraction 去抖 (引擎内, 一次比较); 未启用事件 (on_event=None) 时 no-op.
        - fraction 约定 0.0~1.0; note 自由文本 (如 "batch 3/10").
        """
        if self.on_event is None:
            return
        if self._last_progress is not None and fraction == self._last_progress:
            return
        self._last_progress = fraction
        self.on_event("stage_progress", {
            "task_id": self.task_id, "run_id": self.run_id,
            "stage": self.stage_name, "attempt": self.attempt,
            "fraction": fraction, "note": note,
        })

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

    def cancelled(self) -> bool:
        """协作式取消轮询: True = caller 要求取消本 task.

        stage 内长循环 (批量 LLM 调用等) 自行决定轮询频率与退出方式 —
        runtime 只在 stage 边界强制拦截.
        checker 异常 → 视为未取消 (fail-open, 与 Runtime._is_cancelled 同语义).
        """
        if self.cancel_check is None:
            return False
        try:
            return bool(self.cancel_check())
        except Exception:
            self.logger.exception(
                "cancel_check 异常 (视为未取消) task=%s stage=%s",
                self.task_id, self.stage_name,
            )
            return False


@dataclass
class Runtime:
    """执行引擎. 每次 run 一个 task (可复用实例跑多个 task)."""

    checkpoint_store: CheckpointStore | None = None
    caller: Callable[[str, str, dict, CallMeta], Awaitable[dict]] | None = None
    default_timeout: float | None = None  # 整个 run 的 absolute deadline (秒)
    on_event: Callable[[str, dict], None] | None = None  # v0.9: 生命周期事件钩子
    cancel_check: Callable[[str], bool] | None = None  # v0.9: task_id → 已取消?
    debug_dir: str | None = None  # W2: 每 stage 后落 state 快照 JSON (调试)
    cancel_registry: CancelRegistry | None = None  # R1a: graceful|hard 取消登记处
    policy: EnginePolicy | None = None  # W3: run 级策略 (additive, 显式参数优先)
    _run_sem: asyncio.Semaphore | None = field(
        default=None, init=False, repr=False, compare=False)  # W3: 并发闸 (懒建)

    # ── 事件 ────────────────────────────────────────────
    def _emit(self, event: str, data: dict) -> None:
        """生命周期事件 → on_event 回调. observer 异常隔离 (log + 忽略), 不影响 run."""
        if self.on_event is None:
            return
        try:
            self.on_event(event, data)
        except Exception:
            logger.exception("on_event 回调异常 (忽略) event=%s", event)

    def _is_cancelled(self, task_id: str) -> bool:
        """查取消状态. checker 异常 → 视为未取消 (fail-open, 检查器 bug 不杀业务 run)."""
        if self.cancel_check is not None:
            try:
                if bool(self.cancel_check(task_id)):
                    return True
            except Exception:
                logger.exception("cancel_check 异常 (视为未取消) task=%s", task_id)
        if self.cancel_registry is not None:
            return self.cancel_registry.is_cancelled(task_id)
        return False

    # ── 主入口 ──────────────────────────────────────────
    async def run(
        self,
        dag: DAG,
        task_id: str | None = None,
        *,
        initial_state: dict | None = None,
        resume: bool = False,
        skip_unchanged: bool | None = None,
    ) -> RunResult:
        """执行 DAG 一次 run.

        Args:
            dag: 要执行的 DAG
            task_id: caller 提供的 task ID (省略 → 自动 UUID4). 跨 retry/resume
                     稳定 — 幂等键. resume 时必填 (自动生成的 ID 不会有 cp).
            initial_state: 起始 state (默认 {}; 仅在无已完成 stage 时注入 — resume
                     命中 cp 且 cp 已有完成 stage 时忽略, 初始值已含在 cp.state)
            resume: True → 从该 task 最新 checkpoint 续跑 (复用原 run_id).
                     无 cp / 已全部完成 → RuntimeError. 重跑用 resume=False.
        """
        # task_id: 省略 → UUID4; 显式传值 → A3 校验 (直接进 storage key 路径)
        if task_id is None:
            task_id = new_id()
        _validate_task_id(task_id)
        # R1a: 登记运行句柄 (registry 硬杀靠它 cancel 本 asyncio Task)
        if self.cancel_registry is not None:
            self.cancel_registry._register(task_id, asyncio.current_task())
        try:
            return await self._run_impl(
                dag, task_id, initial_state, resume, skip_unchanged)
        finally:
            if self.cancel_registry is not None:
                self.cancel_registry._unregister(task_id)

    async def _run_impl(
        self,
        dag: DAG,
        task_id: str,
        initial_state: dict | None,
        resume: bool,
        skip_unchanged: bool | None,
    ) -> RunResult:
        """run() 的闸门层 (W3): task_id 已解析, registry 已登记."""
        # W3: 有效 deadline = 显式参数优先, 缺省看 policy; 绝对时刻在 _run_inner
        # 获得并发闸门后才起算 (排队不烧 deadline).
        _pto = self.policy.default_timeout if self.policy else None
        eff_timeout = (
            self.default_timeout if self.default_timeout is not None else _pto
        )
        if self.policy and self.policy.max_concurrent_runs:
            if self._run_sem is None:
                self._run_sem = asyncio.Semaphore(self.policy.max_concurrent_runs)
            async with self._run_sem:
                return await self._run_inner(
                    dag, task_id, initial_state, resume, eff_timeout, skip_unchanged)
        return await self._run_inner(
            dag, task_id, initial_state, resume, eff_timeout, skip_unchanged)

    async def _run_inner(
        self,
        dag: DAG,
        task_id: str | None,
        initial_state: dict | None,
        resume: bool,
        eff_timeout: float | None,
        skip_unchanged: bool | None,
    ) -> RunResult:
        """run() 主体 (W3 拆出): 并发闸门之内. 语义与拆分前完全一致."""

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
        # v0.5.1 (M2): 每 stage return delta (按完成序) + run 初始 state — M3 replay 重建用
        stage_deltas: dict[str, dict] = {}
        # v0.7: stage 完成 epoch ts — 恢复侧 TTL 判定 (内容过期 gate) 用
        stage_ts: dict[str, float] = {}
        initial_state_saved: dict = {}
        # v0.8: fork overrides 随每次 save 携带 — fork_cp 首写外, resume 续跑
        # 的每轮 _save_cp 都必须带上, 否则 overrides 只活到第一次 save 就被丢
        _fork_overrides: dict = {}
        # R2 (0.5.3): stage 名 → 执行输入 hash, 随 cp 持久化 (fork skip_unchanged 用)
        input_hashes: dict[str, str] = {}

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
            state = cp.state  # property: 从 deltas 重建 (v0.8 不落盘)
            done_stages = list(cp.done_stages)
            stage_statuses = dict(cp.stage_statuses)
            producers = dict(cp.producers)
            stage_deltas = dict(cp.stage_deltas)      # resume 续收集
            stage_ts = dict(cp.stage_ts)              # v0.7: resume 续记
            initial_state_saved = dict(cp.initial_state)
            _fork_overrides = dict(cp.fork_overrides)  # fork 分支续保
            input_hashes = dict(cp.stage_input_hashes)  # R2: resume 续记
            result.state = state
            logger.info(
                "task=%s run=%s resume: 已完成 %d stages",
                task_id, run_id[:8], len(done_stages),
            )

        # initial_state 仅在无已完成 stage 时注入 (resume 且 cp 已有完成 stage → 已含在 cp.state)
        if not done_stages and initial_state:
            state = merge_state({}, initial_state, "<init>")
            initial_state_saved = dict(initial_state)  # 存初始 (rebuild 需要)
            for _k in initial_state:
                producers[_k] = "<init>"
            result.state = state

        deep_validate_state(state)

        run_deadline = time.time() + eff_timeout if eff_timeout else None

        self._emit("run_start", {"task_id": task_id, "run_id": run_id,
                                 "dag": dag.name, "resume": cp is not None})

        order = dag.topo_order()
        steps_used = 0  # W3: stage attempt 总数 (max_steps 熔断计数)
        # R2: skip_replay — 非 done 的 stage 若历史 status=done 且输入 hash
        # 命中 (fn 源码 + 执行前 state 均未变) → 重放历史 delta, 不重跑.
        # 只信历史 status="done" (失败 stage 永远重跑); 副作用 stage 用
        # Stage.skip_unchanged=False 拒跳.
        skip_replay = skip_unchanged if skip_unchanged is not None else (
            self.policy.skip_unchanged if self.policy else False)
        for name in order:
            if name in done_stages:
                continue
            stage = dag.stages[name]

            # R2: 重放命中 (在 pre-exec 记录覆写 input_hashes 之前比对)
            if skip_replay and stage.skip_unchanged \
                    and stage_statuses.get(name) == "done" \
                    and input_hashes.get(name) == stage_input_hash(stage.fn, state):
                _dl = stage_deltas.get(name) or {}
                state.update(_dl)
                for _k in _dl:
                    producers[_k] = name
                done_stages.append(name)
                result.stage_statuses = stage_statuses
                result.state = state
                self._save_cp(task_id, run_id, dag, done_stages, stage_statuses,
                              producers, stage_deltas, initial_state_saved, stage_ts,
                              _fork_overrides, input_hashes)
                self._emit("stage_end", {"task_id": task_id, "run_id": run_id,
                                         "stage": name, "attempt": 0,
                                         "status": "skipped", "duration": 0.0,
                                         "error": None})
                logger.info(
                    "task=%s run=%s stage=%s 输入 hash 命中, 重放历史结果 (skip_unchanged)",
                    task_id, run_id[:8], name,
                )
                continue

            # W3: max_steps 熔断 — attempt 预算耗尽, 当前 stage 记 failed 终止
            if self.policy and self.policy.max_steps is not None \
                    and steps_used >= self.policy.max_steps:
                stage_statuses[name] = "failed"
                result.stage_statuses = stage_statuses
                result.state = state
                result.status = "failed"
                result.error = (
                    f"max_steps ({self.policy.max_steps}) exceeded at stage '{name}'"
                )
                result.error_class = "MaxStepsExceeded"
                _fill_error_context(result, name, "MaxStepsExceeded", state)
                self._save_cp(task_id, run_id, dag, done_stages, stage_statuses,
                              producers, stage_deltas, initial_state_saved, stage_ts,
                              _fork_overrides, input_hashes)
                self._emit("run_end", {"task_id": task_id, "run_id": run_id,
                                       "dag": dag.name, "status": "failed"})
                logger.warning(
                    "task=%s run=%s max_steps=%d 熔断 at stage=%s",
                    task_id, run_id[:8], self.policy.max_steps, name,
                )
                return result

            # per-stage timeout: min(stage.timeout, remaining run deadline)
            stage_deadline: float | None = None
            if stage.timeout is not None:
                stage_deadline = time.time() + stage.timeout
            if run_deadline is not None:
                stage_deadline = (
                    min(stage_deadline, run_deadline) if stage_deadline is not None else run_deadline
                )

            _st0 = time.time()
            # R1a: 记当前 stage (含 killable) — registry 硬杀时判定可否杀
            if self.cancel_registry is not None:
                self.cancel_registry._set_stage(task_id, name, stage.killable)
            input_hashes[name] = stage_input_hash(stage.fn, state)  # R2: 执行前记
            try:
                status, new_state, err, err_class, producers, delta, attempts = \
                    await self._run_stage(
                        dag, stage.fn, name, task_id, run_id, state, stage.retries,
                        stage_deadline, producers,
                    )
            except asyncio.CancelledError:
                # R1a: registry 来源的硬杀 → 落 cp 返 cancelled RunResult;
                # 非 registry 来源 (外部 task.cancel) → 照旧上抛, 行为保持.
                if self.cancel_registry is None \
                        or not self.cancel_registry.is_cancelled(task_id):
                    raise
                stage_statuses[name] = "cancelled"
                result.stage_statuses = stage_statuses
                result.state = state
                result.status = "cancelled"
                result.error = "killed (hard kill by CancelRegistry)"
                result.error_class = None
                _fill_error_context(result, name, None, state)
                self._save_cp(task_id, run_id, dag, done_stages, stage_statuses,
                              producers, stage_deltas, initial_state_saved, stage_ts,
                              _fork_overrides, input_hashes)
                self._emit("run_end", {"task_id": task_id, "run_id": run_id,
                                       "dag": dag.name, "status": "cancelled"})
                logger.warning(
                    "task=%s run=%s hard-killed at stage=%s (已完成 %d stages 已落 cp)",
                    task_id, run_id[:8], name, len(done_stages),
                )
                return result
            steps_used += attempts
            result.stage_timings[name] = time.time() - _st0
            state = new_state
            stage_statuses[name] = status
            result.stage_statuses = stage_statuses
            result.state = state
            self._dump_debug(task_id, run_id, name, status, state,
                             time.time() - _st0)

            if status == "cancelled":
                result.status = "cancelled"
                result.error = err
                result.error_class = err_class
                _fill_error_context(result, name, err_class, state)
                result.stage_statuses[name] = "cancelled"
                self._emit("run_end", {"task_id": task_id, "run_id": run_id,
                                       "dag": dag.name, "status": "cancelled"})
                logger.warning(
                    "task=%s run=%s cancelled at stage=%s (已完成 %d stages 已落 cp, resume 可续跑)",
                    task_id, run_id[:8], name, len(done_stages),
                )
                return result

            if status == "failed":
                result.status = "failed"
                result.error = err
                result.error_class = err_class
                _fill_error_context(result, name, err_class, state)
                self._save_cp(task_id, run_id, dag, done_stages, stage_statuses,
                              producers, stage_deltas, initial_state_saved, stage_ts,
                              _fork_overrides, input_hashes)
                self._emit("run_end", {"task_id": task_id, "run_id": run_id,
                                       "dag": dag.name, "status": result.status})
                return result

            if delta:
                stage_deltas[name] = delta  # 收集 (完成序)
            stage_ts[name] = time.time()   # v0.7: 完成时刻 (内容过期 TTL 判定用)
            done_stages.append(name)
            self._save_cp(task_id, run_id, dag, done_stages, stage_statuses,
                          producers, stage_deltas, initial_state_saved, stage_ts,
                          _fork_overrides, input_hashes)
            logger.info(
                "task=%s run=%s stage=%s done (len state=%d)",
                task_id, run_id[:8], name, len(state),
            )

        result.status = "done"
        self._emit("run_end", {"task_id": task_id, "run_id": run_id,
                               "dag": dag.name, "status": result.status})
        return result

    # ── 单 stage 重放 (M3 调试) ─────────────────────────
    async def run_stage(
        self,
        dag: DAG,
        task_id: str,
        stage_name: str,
    ) -> RunResult:
        """重放单 stage: 用已保存 cp 重建其执行前 state, 只跑该 stage.

        调试用 (改 prompt/参数后秒级看效果, 不重跑前序 stage).
        - 不落 checkpoint / 不动 latest pointer (A6/A7: 防 replay 污染 resume 目标)
        - ctx.run_id = 新 UUID4 (临时重放 run 标识, 只活在本次调用)
        - stage 已完成过 → rebuild_state_before; 未完成但依赖已全完成
          (如原始 run 里失败的 stage) → rebuild_state; 依赖未完成 → 明确报错
        - 覆盖判定用"执行时点 producers" (随 state 重建, 不用 cp 终态
          producers) — chain-overwrite 下重放中间 stage 改写上游 key 不误报冲突

        Args:
            dag: DAG (须与 cp 的 workflow_hash 一致, 否则 CheckpointMismatchError)
            task_id: 已有 checkpoint 的 task
            stage_name: 要重放的 stage (已完成过, 或依赖已完成 — 才有 delta 可重建)
        """
        if self.checkpoint_store is None:
            raise RuntimeError("run_stage requires checkpoint_store")
        _validate_task_id(task_id)
        dag.validate()

        cp = self.checkpoint_store.load_latest(task_id)
        if cp is None:
            raise RuntimeError(
                f"task_id={task_id} 无 checkpoint, 不能重放. 先 run 一次."
            )
        cur_hash = workflow_hash(dag)
        if cp.workflow_hash != cur_hash:
            raise CheckpointMismatchError(
                f"task {task_id} run {cp.run_id[:8]} checkpoint 的 DAG hash "
                f"{cp.workflow_hash} ≠ 当前 DAG hash {cur_hash}. DAG 结构变了, "
                f"不能重放 (改依赖/retries/timeout 会变 hash)."
            )
        stage = dag.stages.get(stage_name)
        if stage is None:
            raise KeyError(f"stage '{stage_name}' 不在 DAG {dag.name} 里")

        # 无 stage_deltas 的 cp (旧版产物 / 手工残缺) → rebuild 静默给出空输入 → 明确报错
        # v0.8: state 不落盘, 检查不能依赖 cp.state (property 重建恒有值) — 只看 deltas
        if cp.done_stages and not cp.stage_deltas:
            raise RuntimeError(
                "checkpoint 无 stage_deltas — 旧版产物, 请重新 run 一次再重放"
            )

        # 重建 stage 执行前 state + 执行时点 producers (覆盖判定用 — 不是终态
        # cp.producers: chain-overwrite 下终态 producer 是后写者, 会误拒中间重放):
        # 已完成 → before; 未完成但依赖已全完成 (原始 run 失败/中断的 stage) →
        # 全量 merge done deltas (F2)
        if stage_name in cp.done_stages:
            state, producers = cp.rebuild_state_and_producers_before(stage_name)
        else:
            missing = [d for d in stage.depends_on if d not in cp.done_stages]
            if missing:
                raise RuntimeError(
                    f"stage '{stage_name}' 的依赖未完成 ({missing}), 无法重建其执行前 "
                    f"state. 先跑完整 run 或 resume."
                )
            state, producers = cp.rebuild_state_and_producers()

        run_id = new_id()  # 临时重放 run; 不落 cp
        result = RunResult(task_id=task_id, dag_name=dag.name, run_id=run_id)
        result.state = state

        run_deadline = time.time() + self.default_timeout if self.default_timeout else None
        stage_deadline: float | None = None
        if stage.timeout is not None:
            stage_deadline = time.time() + stage.timeout
        if run_deadline is not None:
            stage_deadline = (
                min(stage_deadline, run_deadline) if stage_deadline is not None
                else run_deadline
            )

        status, new_state, err, err_class, _producers, _delta, _attempts = \
            await self._run_stage(
                dag, stage.fn, stage_name, task_id, run_id, state, stage.retries,
                stage_deadline, producers, emit=False,
            )
        result.state = new_state
        result.stage_statuses = {stage_name: status}
        if status == "failed":
            result.status = "failed"
            result.error = err
            result.error_class = err_class
            _fill_error_context(result, stage_name, err_class, new_state)
        elif status == "cancelled":
            result.status = "cancelled"
            result.error = err
            result.error_class = err_class
            _fill_error_context(result, stage_name, err_class, new_state)
        else:
            result.status = "done"
        return result

    # ── Fork (v0.6, LangGraph fork 范式): 取历史节点输入 → 改 → 装回续跑 ──────
    async def fork_run(
        self,
        dag: DAG,
        task_id: str,
        *,
        from_stage: str,
        overrides: dict | None = None,
        run_id: str | None = None,
        skip_unchanged: bool | None = None,
    ) -> RunResult:
        """从历史 run 的 from_stage 分支续跑: 前序 stage 结果复用 (不重跑),
        from_stage 及其后继用注入的 overrides 重跑. 原 run 的 checkpoint 不动,
        fork 的新 checkpoint (新 run_id) 成为 latest (LangGraph: fork is latest).

        overrides: 顶层 state key 覆盖, 注入到 from_stage 的执行前 state
        (与 checkpoint state 同为 json-serializable dict).

        调试闭环 (user 2026-09-06 拍板 "任意过去节点取输入改一改装回去"):
            pavoz export-input --task-id X --from-stage s_b > in.json
            # 人编辑 in.json (改几个 key)
            pavoz fork-run --task-id X --from-stage s_b --input in.json
        from_stage 可以是: ① 已完成的 stage (截断重跑) ② 失败/未完成的 stage
        (其依赖已完成 — 修输入重跑失败点, 等价注入式 resume).

        skip_unchanged (R2, 0.5.3): True 时从 from_stage 起沿 topo 序逐 stage
        比对输入 hash (stage_input_hash = fn 源码 + 执行前 state) — 命中且该
        stage 未标 skip_unchanged=False → 复用历史结果不重跑; 首个未命中处
        截断, 其后全部重跑. from_stage 退化为"起点提示" (自身 hash 命中也被
        跳过); overrides 注入会改变输入 hash → 必然重跑. None (缺省) 取
        EnginePolicy.skip_unchanged. 副作用 stage 请标 skip_unchanged=False,
        或自行保证幂等 — 跳过 = 副作用不发生.

        Returns: RunResult (fork run 的最终状态). 调用方可用 checkpoint
        state_stats / CLI state 查看 fork 分支.
        """
        if self.checkpoint_store is None:
            raise RuntimeError("fork_run requires checkpoint_store")
        _validate_task_id(task_id)
        cp = self.checkpoint_store.load_latest(task_id)
        if cp is None:
            raise RuntimeError(f"task {task_id} 无 checkpoint, 不能 fork")
        cur_hash = workflow_hash(dag)
        if cp.workflow_hash != cur_hash:
            raise CheckpointMismatchError(
                f"task {task_id} run {cp.run_id[:8]} checkpoint 的 DAG hash "
                f"{cp.workflow_hash} ≠ 当前 DAG hash {cur_hash}. DAG 结构变了, 不能 fork."
            )
        if from_stage not in dag.stages:
            raise KeyError(
                f"stage '{from_stage}' 不在 DAG {dag.name}: {list(dag.stages)}"
            )
        missing = [d for d in dag.stages[from_stage].depends_on if d not in cp.done_stages]
        if missing:
            raise RuntimeError(
                f"stage '{from_stage}' 的依赖未完成 ({missing}), 无法重建其执行前 state."
            )

        # truncate: 保留 from_stage 之前 (run 为线性 topo 序 → done_stages 即执行序)
        keep = []
        for _d in cp.done_stages:
            if _d == from_stage:
                break
            keep.append(_d)

        # state 重建 = initial + 保留 deltas (按序) + overrides (用户注入, 最后生效)
        state = dict(cp.initial_state)
        producers = {k: "<init>" for k in cp.initial_state}
        kept_deltas: dict[str, dict[str, Any]] = {}
        for _d in keep:
            _dl = cp.stage_deltas.get(_d)
            if _dl:
                state.update(_dl)
                kept_deltas[_d] = _dl
                for _k in _dl:
                    producers[_k] = _d

        # R2: fork_cp 携带全量历史 (deltas/statuses/ts/hashes 含 from_stage 之后的)
        # — run 循环在 skip_unchanged 开启时逐 stage 比对, 命中即重放. done_stages
        # 仍截到 keep: 历史 status="done" 的条目由 run 循环重放判定, 失败/未跑的重跑.
        _skip = skip_unchanged if skip_unchanged is not None else (
            self.policy.skip_unchanged if self.policy else False)
        if overrides:
            # v0.9 深合并: v0.8 的 state.update 顶层浅替换会把嵌套 dict 的兄弟键抹掉
            # (--set llm.model=x → temperature/chain 全丢). apply_overrides 只覆盖
            # patch leaf, 兄弟键保留; nested dict 与 dot-path key dict 双格式等价.
            _validate_dict_keys(overrides)  # dunder/非法 key 在 runtime 边界拒绝
            state = apply_overrides(state, overrides)
            for _k in overrides:
                producers[_k] = "<fork>"  # 仍按顶层 override key 记 producer
        deep_validate_state(state)

        fork_cp = Checkpoint(
            task_id=task_id,
            run_id=run_id or new_id(),
            dag_name=cp.dag_name,
            workflow_hash=cur_hash,
            stage_statuses=dict(cp.stage_statuses),
            done_stages=keep,
            producers=producers,
            initial_state=dict(cp.initial_state),
            stage_deltas=dict(cp.stage_deltas),
            stage_ts=dict(cp.stage_ts),
            fork_overrides=dict(overrides or {}),
            stage_input_hashes=dict(cp.stage_input_hashes),
        )
        self.checkpoint_store.save(fork_cp)  # save 同时把 latest 指针移到 fork
        logger.info(
            "task=%s fork from_stage=%s overrides_keys=%s run=%s (原 run %s, 复用 %d 个前序 stage)",
            task_id, from_stage, list(overrides or {})[:8],
            fork_cp.run_id[:8], cp.run_id[:8], len(keep),
        )
        # 续跑: resume=True 读 latest (= fork cp), 跳过 keep, 从 from_stage 顺序执行
        return await self.run(dag, task_id, resume=True, skip_unchanged=_skip)

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
        emit: bool = True,
    ) -> tuple[str, dict, str | None, str | None, dict, dict | None, int]:
        """跑一个 stage (含 retry). 返 (status, new_state, error, error_class, producers, delta, attempts).

        error_class = 原始异常类名 (R2): 未知异常保真 (业务 PipelineError ≠ 引擎
        FatalError, 消费者分诊用); StageError/FatalError → 自身类名; cancelled → None.

        delta = stage 的 return dict (成功, 可能 {}), 失败 = None.
        ctx.attempt = 1-based 当前尝试次数, RetryableError 重试时 +1.
        attempts = 实际执行 attempt 总数 (W3 max_steps 熔断计数用).
        emit=False → 不发生命周期事件 (单 stage 重放不是正式 run).
        """
        attempt = 0
        _t0 = time.time()
        while True:
            if self._is_cancelled(task_id):
                logger.warning("task=%s run=%s stage=%s 取消拦截 (attempt 前)",
                               task_id, run_id[:8], name)
                if emit:
                    self._emit("stage_end", {"task_id": task_id, "run_id": run_id,
                                             "stage": name, "attempt": attempt,
                                             "status": "cancelled",
                                             "duration": time.time() - _t0,
                                             "error": "cancelled by caller"})
                return "cancelled", state, "stage 已取消 (cancelled by caller)", None, producers, None, attempt
            attempt += 1
            if emit:
                self._emit("stage_start", {"task_id": task_id, "run_id": run_id,
                                           "stage": name, "attempt": attempt})
            try:
                # 每 attempt 深拷贝 state (防 stage 意外 mutate 污染后续重试)
                ctx = Ctx(
                    task_id=task_id,
                    run_id=run_id,
                    attempt=attempt,
                    dag=dag,
                    stage_name=name,
                    state=ReadOnlyStateView(snapshot(state)),
                    caller=self.caller or _noop_caller,
                    deadline=stage_deadline,
                    cancel_check=(lambda: self._is_cancelled(task_id)) if self.cancel_check else None,
                    on_event=(self._emit if emit else None),
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
                if emit:
                    self._emit("stage_end", {"task_id": task_id, "run_id": run_id, "stage": name,
                                             "attempt": attempt, "status": "done",
                                             "duration": time.time() - _t0, "error": None})
                return "done", new_state, None, None, producers, delta, attempt

            except (StageError, FatalError) as e:
                # 业务错误 / 程序 bug: 不重试
                logger.warning(
                    "task=%s run=%s stage=%s %s: %s",
                    task_id, run_id[:8], name, type(e).__name__, e,
                )
                if emit:
                    self._emit("stage_end", {"task_id": task_id, "run_id": run_id, "stage": name,
                                             "attempt": attempt, "status": "failed",
                                             "duration": time.time() - _t0, "error": str(e)})
                return "failed", state, str(e), type(e).__name__, producers, None, attempt
            except (RetryableError, TimeoutError) as e:
                if attempt <= retries:
                    # v0.9: full jitter (AWS 惯例) — 多 task 同步重试防雷群
                    # W3: 退避上限走 EnginePolicy.backoff_max (缺省 30, 原硬编码)
                    _bmax = self.policy.backoff_max if self.policy else 30.0
                    backoff = random.uniform(0, min(2 ** (attempt - 1), _bmax))
                    logger.warning(
                        "task=%s run=%s stage=%s attempt=%d/%d %s, 退避 %ss: %s",
                        task_id, run_id[:8], name, attempt, retries + 1,
                        type(e).__name__, backoff, e,
                    )
                    if emit:
                        self._emit("stage_retry", {"task_id": task_id, "run_id": run_id,
                                                   "stage": name, "attempt": attempt,
                                                   "backoff": backoff, "error": str(e)})
                    await asyncio.sleep(backoff)
                    continue
                logger.warning(
                    "task=%s run=%s stage=%s retries 耗尽: %s",
                    task_id, run_id[:8], name, e,
                )
                if emit:
                    self._emit("stage_end", {"task_id": task_id, "run_id": run_id, "stage": name,
                                             "attempt": attempt, "status": "failed",
                                             "duration": time.time() - _t0, "error": str(e)})
                return "failed", state, str(e), type(e).__name__, producers, None, attempt
            except Exception as e:  # 未知异常 → FatalError 语义
                logger.exception("task=%s run=%s stage=%s 未预期异常", task_id, run_id[:8], name)
                if emit:
                    self._emit("stage_end", {"task_id": task_id, "run_id": run_id, "stage": name,
                                             "attempt": attempt, "status": "failed",
                                             "duration": time.time() - _t0, "error": str(e)})
                return "failed", state, f"FatalError: {e}", type(e).__name__, producers, None, attempt

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
    def _dump_debug(self, task_id: str, run_id: str, stage: str,
                    status: str, state: dict, duration: float) -> None:
        """W2 (0.5.3): debug_dir 每 stage 快照. 失败不杀 run (与 _save_cp 同语义)."""
        if self.debug_dir is None:
            return
        try:
            d = os.path.join(self.debug_dir, task_id)
            os.makedirs(d, exist_ok=True)
            seq = len([f for f in os.listdir(d) if f.endswith(".json")])
            path = os.path.join(d, f"{seq:02d}_{stage}_{status}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"task_id": task_id, "run_id": run_id, "stage": stage,
                           "status": status, "duration": duration, "state": state},
                          f, ensure_ascii=False, default=str)
        except Exception:
            logger.exception("debug_dir 落盘失败 (non-fatal) task=%s stage=%s",
                             task_id, stage)

    def _save_cp(self, task_id: str, run_id: str, dag: DAG,
                 done_stages: list, statuses: dict, producers: dict | None = None,
                 stage_deltas: dict | None = None,
                 initial_state: dict | None = None,
                 stage_ts: dict | None = None,
                 fork_overrides: dict | None = None,
                 input_hashes: dict | None = None) -> None:
        if self.checkpoint_store is None:
            return
        try:
            cp = Checkpoint(
                task_id=task_id,
                run_id=run_id,
                dag_name=dag.name,
                workflow_hash=workflow_hash(dag),
                stage_statuses=dict(statuses),
                done_stages=list(done_stages),
                producers=dict(producers or {}),
                initial_state=dict(initial_state or {}),
                stage_deltas=dict(stage_deltas or {}),
                stage_ts=dict(stage_ts or {}),
                fork_overrides=dict(fork_overrides or {}),
                stage_input_hashes=dict(input_hashes or {}),
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
    # str.isalnum() 认 Unicode 字母 (如 "任务" isalpha=True) — 必须显式 ASCII-only
    if not all(c.isascii() and (c.isalnum() or c in "_.-") for c in task_id):
        raise ValueError(
            f"task_id 含非法字符: {task_id!r}. 只允许 [A-Za-z0-9_.-] "
            f"(不含 '/', 防止 storage key 路径注入)"
        )

