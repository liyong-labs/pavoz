"""Checkpoint: per-run 持久化 + workflow hash + mismatch 拒 resume.

- 每次 runtime.run() 一个 run_id (UUID4); checkpoint 存 runs/{task_id}/{run_id}/checkpoint.
- 指针文件 runs/{task_id}/latest 记 {"run_id": ...} — load_latest 靠它
  (uuid4 字典序 ≠ 时间序, 不能拿 list_keys 末位当 "最新").
- 同 task_id 可有多个 run_id 的 checkpoint (original / resume / replay),
  resume 复用同一 run_id.
- run_id 必填 — 无 legacy task 级单 cp lane (旧 key runs/{task_id}/checkpoint
  已 orphan). run_id="" 仅在 load_compatible 作 "加载最新" 哨兵.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from .dag import DAG
from .storage import StorageBackend

__all__ = ["Checkpoint", "CheckpointMismatchError", "CheckpointStore", "workflow_hash"]


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
    """一次 runtime.run() (一个 run_id) 的持久化状态.

    stage_deltas + initial_state 合起来可重建任意 stage 前的 state
    (M3 replay 用). 旧 cp (v0.5.0) 无这两个字段 → from_dict .get 默认 {}.
    """

    task_id: str
    run_id: str
    dag_name: str
    workflow_hash: str
    stage_statuses: dict[str, str]
    state: dict[str, Any]
    done_stages: list[str]  # 按完成顺序
    # v0.1.1: state key → producer stage. 链式覆盖判定用.
    producers: dict[str, str] = field(default_factory=dict)
    # v0.5.1 (M2): 每 stage 的原始 return delta (按完成序) + run 的初始 state.
    initial_state: dict[str, Any] = field(default_factory=dict)
    stage_deltas: dict[str, dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "run_id": self.run_id,
            "dag_name": self.dag_name,
            "workflow_hash": self.workflow_hash,
            "stage_statuses": self.stage_statuses,
            "state": self.state,
            "done_stages": self.done_stages,
            "producers": self.producers,
            "initial_state": self.initial_state,
            "stage_deltas": self.stage_deltas,
        }

    @classmethod
    def from_dict(cls, d: dict) -> Checkpoint:
        # run_id 必填 (旧无 run_id 数据已 hard cut orphan); 以后加新字段一律 .get 默认 (A7)
        return cls(
            task_id=d["task_id"],
            run_id=d["run_id"],
            dag_name=d["dag_name"],
            workflow_hash=d["workflow_hash"],
            stage_statuses=d["stage_statuses"],
            state=d["state"],
            done_stages=d["done_stages"],
            producers=dict(d.get("producers") or {}),
            initial_state=dict(d.get("initial_state") or {}),
            stage_deltas=dict(d.get("stage_deltas") or {}),
        )

    def rebuild_state(self) -> dict[str, Any]:
        """重建所有已完成 stage merge 后的 state (给未完成的 stage 当执行前 state).

        重放"原始 run 失败/中断的 stage"用 — 顺序执行 + 首败即停下,
        未完成 stage 的依赖必已全部完成, 全量 merge done deltas 即其执行前 state.

        Returns: 新 dict (不 mutate).
        """
        return self._rebuild_state()

    def rebuild_state_before(self, stage_name: str) -> dict[str, Any]:
        """重建 stage 执行前的 state = initial + 已完成且在该 stage 前的 deltas.

        顺序按 done_stages 完成序 merge (dict.update) — chain-overwrite 由
        完成顺序天然处理 (后完成者覆盖先完成者). 要求 stage_name 的依赖已全完成
        (即 stage_name in done_stages 或它的 depends_on ⊆ done_stages) —
        否则它之前的 delta 不齐, raise KeyError.

        Returns: 新 dict (不 mutate).
        """
        if stage_name not in self.done_stages and stage_name not in self.stage_deltas:
            raise KeyError(
                f"stage '{stage_name}' 不在 cp 的完成记录里 "
                f"(done_stages={self.done_stages}). 无法重建其执行前 state."
            )
        return self._rebuild_state(stop_at=stage_name)

    def _rebuild_state(self, stop_at: str | None = None) -> dict[str, Any]:
        """initial + done_stages 完成序 deltas 的 merge. stop_at 命中断 (不含)."""
        rebuilt = dict(self.initial_state)
        for done in self.done_stages:
            if done == stop_at:
                break
            delta = self.stage_deltas.get(done)
            if delta:
                rebuilt.update(delta)
        return rebuilt


class CheckpointStore:
    """Per-task, per-run checkpoint 持久化.

    Key format: runs/{task_id}/{run_id}/checkpoint (run_id 必填)
    + 指针 runs/{task_id}/latest = {"run_id": ...}.
    """

    def __init__(self, storage: StorageBackend):
        self.storage = storage

    @staticmethod
    def _key(task_id: str, run_id: str) -> str:
        """run 级 key: runs/{task_id}/{run_id}/checkpoint."""
        return f"runs/{task_id}/{run_id}/checkpoint"

    @staticmethod
    def _latest_key(task_id: str) -> str:
        """最新 run 指针文件 key. 内容 = {"run_id": "<latest>"}."""
        return f"runs/{task_id}/latest"

    def save(self, cp: Checkpoint) -> None:
        self.storage.put(self._key(cp.task_id, cp.run_id), cp.to_dict())
        # 指针: 永远指向最后保存的 run (uuid4 字典序 ≠ 时间序, 不能靠排序).
        self.storage.put(self._latest_key(cp.task_id), {"run_id": cp.run_id})

    def load(self, task_id: str, run_id: str) -> Checkpoint | None:
        """读指定 run 的 checkpoint."""
        d = self.storage.get(self._key(task_id, run_id))
        return Checkpoint.from_dict(d) if d else None

    def list_runs(self, task_id: str) -> list[str]:
        """列 task 的所有 run_id (字典序, UUID4 下 ≠ 时间序).

        调试/清理用. 不含指针文件 — resume 一律走 load_latest (指针),
        不要拿排序当 "最新".
        """
        prefix = f"runs/{task_id}/"
        keys = self.storage.list_keys(prefix)
        run_ids: list[str] = []
        for k in keys:
            tail = k[len(prefix):]
            if tail.endswith("/checkpoint"):
                run_ids.append(tail[: -len("/checkpoint")])
        return sorted(run_ids)

    def load_latest(self, task_id: str) -> Checkpoint | None:
        """按指针文件加载该 task 最新 run 的 checkpoint.

        指针指向的 run 已被删 → load 返 None (边界: caller 收到 "无 checkpoint").
        """
        d = self.storage.get(self._latest_key(task_id))
        if d is None:
            return None
        return self.load(task_id, d["run_id"])

    def delete(self, task_id: str, run_id: str) -> None:
        """删指定 run 的 checkpoint.

        指针不随删除更新 (A2): 删的恰是指针指向的 run → 后续 load_latest
        读到已删 run → 返 None → caller 收到明确 "无 checkpoint".
        """
        self.storage.delete(self._key(task_id, run_id))

    def load_compatible(self, task_id: str, run_id: str, dag: DAG) -> Checkpoint | None:
        """加载 + 校验 workflow hash. mismatch → CheckpointMismatchError.

        run_id="" → 加载该 task 最新 run (load_latest 哨兵).
        """
        if run_id:
            cp = self.load(task_id, run_id)
        else:
            cp = self.load_latest(task_id)
        if cp is None:
            return None
        cur_hash = workflow_hash(dag)
        if cp.workflow_hash != cur_hash:
            raise CheckpointMismatchError(
                f"task {task_id} run {cp.run_id[:8]} checkpoint 的 DAG hash {cp.workflow_hash} "
                f"≠ 当前 DAG hash {cur_hash}. DAG 结构变了, 不能 resume."
                f"(只改 stage 函数体不影响 hash; 改依赖/retries/timeout 会). "
                f"如需强制重跑: 删 checkpoint 或 Runtime(..., resume=False)"
            )
        return cp
