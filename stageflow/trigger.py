"""TaskTrigger Protocol — 编排器与业务 task 表的边界.

编排器不直接查业务表 (research_tasks / 未来其他系统的表).
业务系统实现这些方法 (adapter), 编排器只通过接口拿/放任务.

两种用法:
1. 业务 worker 主动 poll: list_pending() → claim() → runtime.run() → mark_done/failed
2. 纯库模式: 不用 TaskTrigger, 业务直接调 runtime.run (task 来源业务自己管)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

__all__ = ["TaskRef", "TaskTrigger"]


@dataclass
class TaskRef:
    """业务侧给编排器的最小任务描述."""

    id: str
    dag: str                      # 要跑的 DAG 名
    payload: dict[str, Any] = field(default_factory=dict)  # 业务初始 state (可选)
    meta: dict[str, Any] = field(default_factory=dict)     # 业务附加信息


class TaskTrigger(ABC):
    """业务侧实现的 adapter. 编排器不感知具体 DB schema."""

    @abstractmethod
    def list_pending(self, max_concurrent: int) -> list[TaskRef]:
        """返回可启动的任务 (≤ max_concurrent 余量).

        业务 SQL 自己过滤 (status='pending' + 当前 in_progress 数 < max_concurrent).
        """

    @abstractmethod
    def claim(self, task_id: str) -> bool:
        """抢占任务. 成功返 True (task 进 running). 失败 (被抢走) 返 False.

        实现建议: 原子 UPDATE ... WHERE status='pending' RETURNING — 天然防双 worker.
        """

    @abstractmethod
    def mark_done(self, task_id: str, result: dict) -> None:
        """任务全部 stage 跑完. 业务写自己的终态."""

    @abstractmethod
    def mark_failed(self, task_id: str, error: str) -> None:
        """任务失败. 业务写自己的终态."""
