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
import json
from dataclasses import dataclass, field
from typing import Any

from .dag import DAG
from .state import apply_overrides
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

    v0.8 (2026-09-06, user: 发布前向前看, 旧数据记录可弃): **state 不落盘** —
    全量 state 是 stage_deltas + initial_state 的确定性函数 (_rebuild_state),
    双份落盘 = 2x 体积 (ai_writer 真实 cp 2.8MB, state 冗余 ~1.4MB). 序列化层
    只存增量 + 元数据; state 变 property (load 后惰性重建).
    """

    task_id: str
    run_id: str
    dag_name: str
    workflow_hash: str
    stage_statuses: dict[str, str]
    done_stages: list[str]  # 按完成顺序
    # v0.1.1: state key → producer stage. 链式覆盖判定用.
    producers: dict[str, str] = field(default_factory=dict)
    # v0.5.1 (M2): 每 stage 的原始 return delta (按完成序) + run 的初始 state.
    initial_state: dict[str, Any] = field(default_factory=dict)
    stage_deltas: dict[str, dict[str, Any]] = field(default_factory=dict)
    # v0.7: stage 完成 epoch ts — 恢复侧内容过期 (TTL) gate 判定用.
    stage_ts: dict[str, float] = field(default_factory=dict)
    # v0.8: fork_run 的 overrides — fork cp 专用 (前序 keep 之上最后 merge).
    # 曾靠全量 state 落盘持久化; state 不落盘后 overrides 必须显式存, 否则
    # fork resume 时 rebuild 丢 overrides → 从 from_stage 重跑用旧输入 (bug).
    fork_overrides: dict[str, Any] = field(default_factory=dict)

    @property
    def state(self) -> dict[str, Any]:
        """全量 merge 后 state — 从 initial_state + stage_deltas 惰性重建.

        不落盘 (v0.8 瘦身), 每次访问现算 (纯内存 dict merge, 2MB 级 ~10ms).
        """
        return self._rebuild_state()[0]

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "run_id": self.run_id,
            "dag_name": self.dag_name,
            "workflow_hash": self.workflow_hash,
            "stage_statuses": self.stage_statuses,
            "done_stages": self.done_stages,
            "producers": self.producers,
            "initial_state": self.initial_state,
            "stage_deltas": self.stage_deltas,
            "stage_ts": self.stage_ts,
            "fork_overrides": self.fork_overrides,
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
            done_stages=d["done_stages"],
            producers=dict(d.get("producers") or {}),
            initial_state=dict(d.get("initial_state") or {}),
            stage_deltas=dict(d.get("stage_deltas") or {}),
            stage_ts=dict(d.get("stage_ts") or {}),
            fork_overrides=dict(d.get("fork_overrides") or {}),
        )

    def rebuild_state(self) -> dict[str, Any]:
        """重建所有已完成 stage merge 后的 state (给未完成的 stage 当执行前 state).

        重放"原始 run 失败/中断的 stage"用 — 顺序执行 + 首败即停下,
        未完成 stage 的依赖必已全部完成, 全量 merge done deltas 即其执行前 state.

        Returns: 新 dict (不 mutate).
        """
        return self.rebuild_state_and_producers()[0]

    def rebuild_state_and_producers(self) -> tuple[dict[str, Any], dict[str, str]]:
        """重建全量 merge 后的 (state, producers) — producers 为执行时点值.

        未完成但依赖已完成的 stage 重放用 (run_stage): 与 rebuild_state 同一次
        遍历收集 producers (initial keys → "<init>"; merged delta 的 keys → 其 stage).
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
        return self.rebuild_state_and_producers_before(stage_name)[0]

    def rebuild_state_and_producers_before(
        self, stage_name: str
    ) -> tuple[dict[str, Any], dict[str, str]]:
        """重建 stage 执行前的 (state, producers) — producers 按执行时点重建.

        cp.producers 是终态 (chain-overwrite 后指向最后写者); 重放中间 stage 的
        覆盖判定必须用"该 stage 执行时点"的 producer — 这里随 state 重建同步
        收集: initial keys → "<init>", 已 merge delta (stage X) 的 keys → X.
        """
        if stage_name not in self.done_stages and stage_name not in self.stage_deltas:
            raise KeyError(
                f"stage '{stage_name}' 不在 cp 的完成记录里 "
                f"(done_stages={self.done_stages}). 无法重建其执行前 state."
            )
        return self._rebuild_state(stop_at=stage_name)

    def _rebuild_state(self, stop_at: str | None = None) -> tuple[dict[str, Any], dict[str, str]]:
        """initial + done_stages 完成序 deltas 的 merge + 执行时点 producers.

        stop_at 命中断 (不含). Returns: (state, producers) — producers 记录每个
        merged delta 的 keys → 其 stage; initial keys → "<init>".
        """
        rebuilt = dict(self.initial_state)
        producers = {k: "<init>" for k in self.initial_state}
        for done in self.done_stages:
            if done == stop_at:
                break
            delta = self.stage_deltas.get(done)
            if delta:
                rebuilt.update(delta)
                for k in delta:
                    producers[k] = done
        # v0.8: fork overrides 最后 merge (覆盖前序 keep 产物; producer 标 <fork>)
        # v0.9 深合并: v0.8 浅 update 会把嵌套 dict 的兄弟键抹掉 (resume 重建
        # 才是 stage 实际看到的 state — 与 runtime.fork_run 应用语义保持一致)
        if stop_at is None and self.fork_overrides:
            rebuilt = apply_overrides(rebuilt, self.fork_overrides)
            for k in self.fork_overrides:
                producers[k] = "<fork>"
        return rebuilt, producers

    def state_stats(self, top_n: int = 10) -> dict:
        """state 体积观测 (#36): 各 key json 体积 top N + 总构成. 调试大 state 用."""
        import json as _json

        def _size(o: Any) -> int:
            try:
                return len(_json.dumps(o, ensure_ascii=False, default=str))
            except (TypeError, ValueError):  # 不可序列化 → 记 -1
                return -1

        _sizes = {k: _size(v) for k, v in self.state.items()}
        _top = sorted(_sizes.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
        return {
            "state_keys": len(self.state),
            "state_total_bytes": sum(_sizes.values()),
            "initial_state_bytes": _size(self.initial_state),
            "deltas_bytes": _size(self.stage_deltas),
            "done_stages": list(self.done_stages),
            "top_keys": [{"key": k, "bytes": v} for k, v in _top],
        }


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

    def prune(self, task_id: str, *, keep_last: int, dry_run: bool = False) -> dict:
        """按保留数修剪旧 run 的 checkpoint (R1, 2026-09-10 ai-research PR).

        语义:
        - 排序 = max(stage_ts) (run 最后活动时间, v0.7 epoch); 无 stage_ts 的 run
          视为最旧. 最新 keep_last 个保留; **latest 指针指向的 run 无条件保留**
          (resume/fork 基线), 指针文件本身不动.
        - 被删 run 的后续行为 = 既有边界: load(task, rid)→None, load_latest 在
          指针指向被删 run 时→None (本实现不会让指针悬空), fork-run 报
          "无 checkpoint, 不能 fork".
        - 删除以 key 为粒度 (StorageBackend.delete), 跨 task 不触碰.
        - approx_bytes = 重新序列化 JSON 的字节数 (近似值, dry-run 预估用).
- FileStorage 只删 key 文件, 可能残留空目录 (0 字节, 无碍).

        不做: max_runs_per_task 自动修剪 (引擎不替客户决定保留策略,
        v0.4 storage adapters REVERSED 同一先例); 跨 task GC / 引用计数.
        """
        if keep_last < 0:
            raise ValueError(f"keep_last 必须 >= 0, got {keep_last}")
        run_ids = self.list_runs(task_id)
        latest_d = self.storage.get(self._latest_key(task_id))
        latest_run = (latest_d or {}).get("run_id")

        def _mtime(rid: str) -> tuple[float, str]:
            cp = self.load(task_id, rid)
            ts = max(cp.stage_ts.values()) if cp and cp.stage_ts else 0.0
            return (ts, rid)

        ordered = sorted(run_ids, key=_mtime)  # 旧 → 新
        keep = set(ordered[-keep_last:]) if keep_last > 0 else set()
        if latest_run:
            keep.add(latest_run)
        victims = [r for r in ordered if r not in keep]

        deleted: list[dict] = []
        freed = 0
        for rid in victims:
            raw = self.storage.get(self._key(task_id, rid))
            size = len(json.dumps(raw).encode("utf-8")) if raw else 0
            if not dry_run:
                self.delete(task_id, rid)
            deleted.append({"run_id": rid, "approx_bytes": size})
            freed += size
        kept = [r for r in ordered if r in keep]
        return {
            "task_id": task_id,
            "dry_run": dry_run,
            "protected_latest": latest_run,
            "kept": kept,
            "deleted": deleted,
            "approx_bytes_freed": freed,
        }

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
