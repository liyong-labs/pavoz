"""EnginePolicy: run 级策略外置 (W3, 0.5.3, additive 非 BREAKING).

zeroflow 同款思路: 策略对象化. Runtime 旧参数 (default_timeout 等) 全部
保留 — policy 只在显式参数缺省时生效, caller 0 迁移.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["EnginePolicy"]


@dataclass(frozen=True)
class EnginePolicy:
    """run 级引擎策略. 全部字段可选, None/False = 引擎默认行为.

    default_timeout: 整 run absolute deadline (秒); Runtime.default_timeout
        显式给值时优先生效 (后者省略才看这里).
    max_steps: 单 run stage attempt 总数上限 (含重试) — 防死循环熔断,
        超限时当前 stage 记 failed (error_class=MaxStepsExceeded).
    backoff_max: RetryableError 重试退避上限 (秒), 替代原硬编码 30.
    max_concurrent_runs: 同一 Runtime 实例进程内并发 run 上限
        (asyncio.Semaphore, 排队等闸; deadline 在获得闸门后才起算).
    skip_unchanged: R2 — fork_run 未显式传 skip_unchanged 时用它.
    """

    default_timeout: float | None = None
    max_steps: int | None = None
    backoff_max: float = 30.0
    max_concurrent_runs: int | None = None
    skip_unchanged: bool = False
