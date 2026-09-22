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

    何时传: 要给 run 加护栏 (超时/熔断/并发闸) 或开 fork 重放优化时;
    不传 = 引擎默认行为, 与 0.5.2 完全一致. Runtime 同名显式参数永远优先.

    default_timeout: 整 run absolute deadline (秒). 默认 None = 不限时.
        何时覆写: run 挂死会白占资源时 (LLM 长任务常见 300~3600);
        Runtime(default_timeout=...) 显式给值时优先生效 (后者省略才看这里).
    max_steps: 单 run stage attempt 总数上限 (含重试), 防死循环熔断.
        默认 None = 不熔断. 何时覆写: 推荐 ≥ stage 数 × (1 + retries),
        如 8 stage × retry 2 取 50 量级; 超限时当前 stage 记 failed
        (error_class=MaxStepsExceeded).
    backoff_max: RetryableError 重试退避上限 (秒). 默认 30.0 (替代旧硬编码 30).
        何时覆写: 重试密集想快速失败 → 调低 (如 5.0); 极少调高.
    max_concurrent_runs: 同一 Runtime 实例进程内并发 run 上限. 默认 None = 不限.
        何时覆写: 下游 (LLM API / DB) 有并发配额时取配额值; 排队等闸
        (asyncio.Semaphore), deadline 在获得闸门后才起算.
    skip_unchanged: R2 — fork_run 未显式传 skip_unchanged 时用它. 默认 False.
        何时覆写: prompt 迭代期频繁 fork_run 重放 → True 省重跑;
        副作用 stage 必须标 Stage.skip_unchanged=False 拒跳.
    """

    default_timeout: float | None = None
    max_steps: int | None = None
    backoff_max: float = 30.0
    max_concurrent_runs: int | None = None
    skip_unchanged: bool = False
