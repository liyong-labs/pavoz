"""CLI: run / trace / state / replay (v0.5.1, 4 命令).

用法:
    python -m stageflow run dags/demo.py --task-id abc [--input '{"name": "x"}']
    python -m stageflow trace --task-id abc [--storage ./data]
    python -m stageflow state --task-id abc [--key k]
    python -m stageflow replay dags/demo.py --task-id abc --stage s_b [--patch patch.py]

replay (--stage 单 stage 重放 + --patch 改 fn): v0.5.1 已 ship (2026-09-05).
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import inspect
import json
import os
import sys
from pathlib import Path

from .checkpoint import CheckpointMismatchError, CheckpointStore
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
    # v0.5.1 (R5 fix): 恒 attach store → CLI run 落盘, replay/trace/state 可用
    runtime = Runtime(checkpoint_store=CheckpointStore(_storage()))
    try:
        result = await runtime.run(
            dag, task_id, initial_state=initial, resume=args.resume
        )
    except (CheckpointMismatchError, RuntimeError, ValueError) as e:
        # v0.5: resume/校验错误 (无 cp / 已全部完成 / DAG hash 变了 / task_id 非法)
        # → 友好消息, 不炸 traceback
        print(f"run 失败: {e}")
        return 1
    print(json.dumps({
        "task_id": result.task_id,
        "run_id": result.run_id,
        "dag": result.dag_name,
        "status": result.status,
        "stage_statuses": result.stage_statuses,
        "error": result.error,
        "state": result.state,
    }, ensure_ascii=False, indent=2))
    return 0 if result.status == "done" else 1


async def _cmd_trace(args) -> int:
    """列 task 的最新 run checkpoint + stage 记录 (per-run, 看 load_latest)."""
    store = CheckpointStore(_storage())
    cp = store.load_latest(args.task_id)
    if cp is None:
        print(f"task {args.task_id} 无 checkpoint (没跑过)")
        return 1
    print(json.dumps({
        "task_id": cp.task_id,
        "run_id": cp.run_id,
        "dag": cp.dag_name,
        "workflow_hash": cp.workflow_hash,
        "stage_statuses": cp.stage_statuses,
        "done_stages": cp.done_stages,
        "state_keys": sorted(cp.state.keys()),
    }, ensure_ascii=False, indent=2))
    return 0


async def _cmd_state(args) -> int:
    store = CheckpointStore(_storage())
    cp = store.load_latest(args.task_id)
    if cp is None:
        print(f"task {args.task_id} 无 checkpoint")
        return 1
    if args.stats:
        print(json.dumps(cp.state_stats(), ensure_ascii=False, indent=2))
        return 0
    if args.key:
        print(json.dumps(cp.state.get(args.key, None), ensure_ascii=False, indent=2))
    else:
        print(json.dumps(cp.state, ensure_ascii=False, indent=2))
    return 0


async def _cmd_replay(args) -> int:
    """重放单 stage: stageflow replay <dag.py> --task-id X --stage Y [--patch P.py].

    patch 文件约定: 模块顶层暴露 patch(dag) -> None (import 后调用, 可改 stage fn).
    """
    dag = _load_dag(args.dag)
    if args.patch:
        patch_path = Path(args.patch).resolve()
        if not patch_path.exists():
            print(f"patch 文件不存在: {patch_path}")
            return 1
        spec = importlib.util.spec_from_file_location("_stageflow_patch", patch_path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules["_stageflow_patch"] = mod
        spec.loader.exec_module(mod)
        patcher = getattr(mod, "patch", None)
        if patcher is None:
            print(f"patch 文件 {patch_path} 需暴露 patch(dag) -> None")
            return 1
        if inspect.iscoroutinefunction(patcher):
            print(f"patch 文件 {patch_path} 的 patch 不能是 async — 用同步 def patch(dag)")
            return 1
        patcher(dag)
    runtime = Runtime(checkpoint_store=CheckpointStore(_storage()))
    try:
        result = await runtime.run_stage(dag, task_id=args.task_id, stage_name=args.stage)
    except (CheckpointMismatchError, RuntimeError, ValueError, KeyError) as e:
        print(f"replay 失败: {e}")
        return 1
    print(json.dumps({
        "task_id": result.task_id,
        "run_id": result.run_id,
        "dag": result.dag_name,
        "stage": args.stage,
        "status": result.status,
        "error": result.error,
        "state": result.state,
    }, ensure_ascii=False, indent=2))
    return 0 if result.status == "done" else 1


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
    p_state.add_argument("--stats", action="store_true", help="state 体积统计 (key top N + 构成)")
    p_state.set_defaults(fn=_cmd_state)

    p_replay = sub.add_parser("replay", help="重放单 stage (调试: 改 prompt/参数秒级看效果)")
    p_replay.add_argument("dag", help="dag 文件路径")
    p_replay.add_argument("--task-id", required=True)
    p_replay.add_argument("--stage", required=True, help="要重放的 stage 名")
    p_replay.add_argument("--patch", default=None,
                          help="patch 文件 (暴露 patch(dag) -> None, 改 stage fn)")
    p_replay.set_defaults(fn=_cmd_replay)

    args = parser.parse_args(argv)
    return asyncio.run(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(main())
