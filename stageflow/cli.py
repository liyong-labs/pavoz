"""CLI: run / trace / state (v0.1, 3 命令).

用法:
    python -m stageflow run dags/demo.py --task-id abc [--input '{"name": "x"}']
    python -m stageflow trace --task-id abc [--storage ./data]
    python -m stageflow state --task-id abc [--key k]

replay --prompt-patch 推 v0.2 (架构评估修正, 2026-09-05).
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
import sys
from pathlib import Path

from .checkpoint import CheckpointStore
from .runtime import Runtime
from .storage import FileStorage

DEFAULT_STORAGE = os.environ.get("STAGEFLOW_STORAGE", os.path.expanduser("~/.stageflow/data"))


def _load_dag(path: str):
    """import 一个 dag 文件, 返回其中唯一的 DAG 实例."""
    p = Path(path).resolve()
    if not p.exists():
        raise SystemExit(f"dag 文件不存在: {p}")
    spec = importlib.util.spec_from_file_location("_stageflow_dag", p)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_stageflow_dag"] = mod
    spec.loader.exec_module(mod)
    dags = [v for v in vars(mod).values() if type(v).__name__ == "DAG"]
    if not dags:
        raise SystemExit(f"{p} 里没有 DAG 实例")
    if len(dags) > 1:
        # 挑名字匹配的; 没有则第一个 (文档建议每文件 1 个 DAG)
        want = getattr(importlib.sys.modules["_stageflow_dag"], "DEFAULT_DAG", None)
        for d in dags:
            if d.name == want:
                return d
    return dags[0]


def _storage() -> FileStorage:
    return FileStorage(DEFAULT_STORAGE)


async def _cmd_run(args) -> int:
    dag = _load_dag(args.dag)
    task_id = args.task_id or f"run-{os.getpid()}"
    initial = {}
    if args.input:
        initial = json.loads(args.input)
    runtime = Runtime(checkpoint_store=CheckpointStore(_storage()) if args.resume else None)
    result = await runtime.run(
        dag, task_id, initial_state=initial, resume=args.resume
    )
    print(json.dumps({
        "task_id": result.task_id,
        "dag": result.dag_name,
        "status": result.status,
        "stage_statuses": result.stage_statuses,
        "error": result.error,
        "state": result.state,
    }, ensure_ascii=False, indent=2))
    return 0 if result.status == "done" else 1


async def _cmd_trace(args) -> int:
    """列 task 的所有 checkpoint + stage 记录."""
    store = CheckpointStore(_storage())
    cp = store.load(args.task_id)
    if cp is None:
        print(f"task {args.task_id} 无 checkpoint (没跑过或已完成已清)")
        return 1
    print(json.dumps({
        "task_id": cp.task_id,
        "dag": cp.dag_name,
        "workflow_hash": cp.workflow_hash,
        "stage_statuses": cp.stage_statuses,
        "done_stages": cp.done_stages,
        "state_keys": sorted(cp.state.keys()),
    }, ensure_ascii=False, indent=2))
    return 0


async def _cmd_state(args) -> int:
    store = CheckpointStore(_storage())
    cp = store.load(args.task_id)
    if cp is None:
        print(f"task {args.task_id} 无 checkpoint")
        return 1
    if args.key:
        print(json.dumps(cp.state.get(args.key, None), ensure_ascii=False, indent=2))
    else:
        print(json.dumps(cp.state, ensure_ascii=False, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="stageflow", description="LLM/SE/Extract 流程编排")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="跑 DAG")
    p_run.add_argument("dag", help="dag 文件路径 (含 DAG 实例)")
    p_run.add_argument("--task-id", default=None)
    p_run.add_argument("--input", default=None, help='JSON 初始 state, e.g. \'{"name": "x"}\'')
    p_run.add_argument("--resume", action="store_true", help="从 checkpoint 续跑 (若存在)")
    p_run.set_defaults(fn=_cmd_run)

    p_trace = sub.add_parser("trace", help="列 task checkpoint / stage 记录")
    p_trace.add_argument("--task-id", required=True)
    p_trace.set_defaults(fn=_cmd_trace)

    p_state = sub.add_parser("state", help="看 task state snapshot")
    p_state.add_argument("--task-id", required=True)
    p_state.add_argument("--key", default=None)
    p_state.set_defaults(fn=_cmd_state)

    args = parser.parse_args(argv)
    return asyncio.run(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(main())
