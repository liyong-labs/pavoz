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
        # 本进程**确实向该 task 发过 `Task.cancel()`** 的 task_id.
        #
        # 为什么需要它 (2026-10-16): `CancelledError` 本身不带"谁发起的"信息,
        # 而 registry 硬杀和**外部**取消 (asyncio.wait_for 超时 / 调用方
        # t.cancel() / loop 关闭) 走的是同一个异常。runtime 此前只看"有没有取消
        # 标志"就把 `CancelledError` 转成 RunResult(status="cancelled") —— 于是
        # 外部取消被**吞掉**, 调用方拿到一个假的 RunResult 而不是异常:
        # `asyncio.wait_for(rt.run(...), timeout=0.1)` 不抛 TimeoutError。
        # 后果是调用方的超时保护静默失效, 而超时保护通常是防止 run 无限挂着的
        # 最后一道防线。
        #
        # 生命周期: 只在**真的** t.cancel() 时加 (拿不到句柄/已结束则不加),
        # `_unregister` (run 收尾) 时清 —— 与 `_tasks`/`_current` 同生命周期。
        self._killing: set[str] = set()

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
                self._killing.add(task_id)   # 只有真发了 cancel 才算数
                t.cancel()
        else:
            self._flags.add(task_id)

    def is_cancelled(self, task_id: str) -> bool:
        return task_id in self._flags

    def reset(self, task_id: str) -> None:
        """清除取消标志。

        何时**必须**调: 一个 run 以 `status="cancelled"` 收场后, 标志按设计保留
        (caller 明确表达过"取消这个 task")。此时复用同一 task_id 启动新 run /
        `resume`, 会**一个 stage 都没跑**就被判 cancelled —— 所以想续跑就得先
        reset。run 自己的日志也会这么提示。

        何时**不用**调: run **正常**结束 (done/failed) 时 `Runtime.run()` 会自己
        清掉残留标志 (2026-10-16) —— "正常结束却带着取消标志"只可能是取消请求
        比收尾还晚的竞态残留, 没有语义要保留。
        """
        self._flags.discard(task_id)

    # ── Runtime 内部挂钩 (下划线 = 非 caller API) ──────────
    def _register(self, task_id: str, task: asyncio.Task) -> None:
        self._tasks[task_id] = task

    def _unregister(self, task_id: str) -> None:
        self._tasks.pop(task_id, None)
        self._current.pop(task_id, None)
        self._killing.discard(task_id)      # 与句柄同生命周期, 不留到下一个 run

    def _set_stage(self, task_id: str, stage: str, killable: bool) -> None:
        self._current[task_id] = (stage, killable)
