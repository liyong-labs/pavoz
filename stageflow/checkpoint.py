"""Checkpoint: 每 node 后落盘 + workflow hash + mismatch 拒 resume.

- checkpoint 存: {workflow_hash, task_id, dag_name, stage_statuses, state, done_stages}
- resume 时 workflow_hash 不匹配 (DAG 结构变了) → CheckpointMismatchError, 除非显式 force
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from .dag import DAG
from .storage import StorageBackend

__all__ = ["CheckpointMismatchError", "CheckpointStore", "workflow_hash"]


def workflow_hash(dag: DAG) -> str:
    """DAG 结构的稳定指纹: stage 名 + 依赖 + retries + timeout.

    任何影响执行语义的结构改动 → hash 变 → 旧 checkpoint 拒 resume.
    只改 stage 函数体 (fn) 不改结构 → hash 不变 (resume 兼容, fn 重新 import 即新代码).
    """
    rows = []
    for name in dag.topo_order():
        s = dag.stages[name]
        rows.append(f"{name}:dep={','.join(s.depends_on)}:retry={s.retries}:to={s.timeout}")
    raw = "\n".join(rows)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


class CheckpointMismatchError(Exception):
    """checkpoint 的 DAG hash ≠ 当前 DAG hash. 结构变了, 不能续跑."""


@dataclass
class Checkpoint:
    task_id: str
    dag_name: str
    workflow_hash: str
    stage_statuses: dict[str, str]
    state: dict[str, Any]
    done_stages: list[str]  # 按完成顺序
    # v0.1.1: state key → producer stage. 链式覆盖判定用.
    # 旧 checkpoint 无此字段 → {} → resume 走 legacy 宽松模式 (视同单链).
    producers: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "dag_name": self.dag_name,
            "workflow_hash": self.workflow_hash,
            "stage_statuses": self.stage_statuses,
            "state": self.state,
            "done_stages": self.done_stages,
            "producers": self.producers,
        }

    @classmethod
    def from_dict(cls, d: dict) -> Checkpoint:
        return cls(
            task_id=d["task_id"],
            dag_name=d["dag_name"],
            workflow_hash=d["workflow_hash"],
            stage_statuses=d["stage_statuses"],
            state=d["state"],
            done_stages=d["done_stages"],
            producers=dict(d.get("producers") or {}),
        )


class CheckpointStore:
    """读写 Checkpoint. key = runs/{task_id}/checkpoint"""

    def __init__(self, storage: StorageBackend):
        self.storage = storage

    @staticmethod
    def _key(task_id: str) -> str:
        return f"runs/{task_id}/checkpoint"

    def save(self, cp: Checkpoint) -> None:
        self.storage.put(self._key(cp.task_id), cp.to_dict())

    def load(self, task_id: str) -> Checkpoint | None:
        d = self.storage.get(self._key(task_id))
        return Checkpoint.from_dict(d) if d else None

    def delete(self, task_id: str) -> None:
        self.storage.delete(self._key(task_id))

    def load_compatible(self, task_id: str, dag: DAG) -> Checkpoint:
        """加载 + 校验 workflow hash. mismatch → CheckpointMismatchError."""
        cp = self.load(task_id)
        if cp is None:
            return cp  # type: ignore[return-value]  # None = 无 checkpoint
        cur_hash = workflow_hash(dag)
        if cp.workflow_hash != cur_hash:
            raise CheckpointMismatchError(
                f"task {task_id} checkpoint 的 DAG hash {cp.workflow_hash} "
                f"≠ 当前 DAG hash {cur_hash}. DAG 结构变了, 不能 resume."
                f"(只改 stage 函数体不影响 hash; 改依赖/retries/timeout 会). "
                f"如需强制重跑: 删 checkpoint 或 Runtime(..., resume=False)"
            )
        return cp
