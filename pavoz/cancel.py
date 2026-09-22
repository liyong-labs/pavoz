"""CancelRegistry: task 级取消登记处 (R1a, 0.5.3; id=137 user 拍板"做好幂等, 硬杀问题不大").

graceful = 边界拦截 (复用 v0.9 cancel_check 语义, stage 间/重试间).
hard     = 对正在执行的 stage 注入 CancelledError (asyncio Task.cancel),
           run() 捕获后落 cp 并返回 RunResult(status="cancelled") —
           caller 须保证 stage 幂等 (写入带 idempotency_key); 标了
           killable=False 的 stage 硬杀 → NotKillable (cancel 调用处抛).
in-process 边界: registry 只能杀同进程、由本进程 Runtime 启动的 run.
"""

from __future__ import annotations

import asyncio

__all__ = ["CancelRegistry", "NotKillable"]


class NotKillable(Exception):
    """目标 run 当前 stage 标了 killable=False, 拒绝硬杀."""


class CancelRegistry:
    """task_id → 取消标志 + 运行句柄. 线程模型: 同一 event loop 内使用."""

    def __init__(self):
        self._flags: set[str] = set()
        self._tasks: dict[str, asyncio.Task] = {}
        self._current: dict[str, tuple[str, bool]] = {}  # task_id → (stage, killable)

    def cancel(self, task_id: str, mode: str = "graceful") -> None:
        """请求取消. graceful = stage 边界拦截; hard = 立即注入 CancelledError.

        hard 时若当前 stage 标了 killable=False → 抛 NotKillable (不杀).
        """
        if mode not in ("graceful", "hard"):
            raise ValueError(f"mode 须为 graceful|hard, got {mode!r}")
        if mode == "hard":
            cur = self._current.get(task_id)
            if cur and not cur[1]:
                raise NotKillable(
                    f"task={task_id} 当前 stage '{cur[0]}' 标了 killable=False, "
                    f"拒绝硬杀 (等它跑完或用 graceful)"
                )
            self._flags.add(task_id)
            t = self._tasks.get(task_id)
            if t is not None and not t.done():
                t.cancel()
        else:
            self._flags.add(task_id)

    def is_cancelled(self, task_id: str) -> bool:
        return task_id in self._flags

    def reset(self, task_id: str) -> None:
        """清除取消标志 (run 结束后复用同一 task_id 前 caller 自行决定)."""
        self._flags.discard(task_id)

    # ── Runtime 内部挂钩 (下划线 = 非 caller API) ──────────
    def _register(self, task_id: str, task: asyncio.Task) -> None:
        self._tasks[task_id] = task

    def _unregister(self, task_id: str) -> None:
        self._tasks.pop(task_id, None)
        self._current.pop(task_id, None)

    def _set_stage(self, task_id: str, stage: str, killable: bool) -> None:
        self._current[task_id] = (stage, killable)
