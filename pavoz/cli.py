"""CLI: run / trace / state / replay / export-input / fork-run (v0.6).

用法:
    python -m pavoz run dags/demo.py --task-id abc [--input '{"name": "x"}']
    python -m pavoz trace --task-id abc
    python -m pavoz state --task-id abc [--key k]
    (存储目录: PAVOZ_STORAGE env, 默认 ~/.pavoz/data)
    python -m pavoz replay dags/demo.py --task-id abc --stage s_b [--patch patch.py]

replay (--stage 单 stage 重放 + --patch 改 fn): v0.5.1 已 ship (2026-09-05).
export-input / fork-run (v0.6, 2026-09-06): 任意过去节点取输入 → 改 → 装回续跑
(LangGraph fork 范式, 见 docs/design/node-input-edit-fork.md).
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

DEFAULT_STORAGE = os.environ.get("PAVOZ_STORAGE", os.path.expanduser("~/.pavoz/data"))


def _load_dag(path: str):
    """import 一个 dag 文件, 返回其中唯一的 DAG 实例."""
    p = Path(path).resolve()
    if not p.exists():
        raise SystemExit(f"dag 文件不存在: {p}")
    spec = importlib.util.spec_from_file_location("_pavoz_dag", p)
    assert spec is not None and spec.loader is not None, f"无法加载 dag 文件: {p}"
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_pavoz_dag"] = mod
    spec.loader.exec_module(mod)
    dags = [v for v in vars(mod).values() if type(v).__name__ == "DAG"]
    if not dags:
        raise SystemExit(f"{p} 里没有 DAG 实例")
    if len(dags) > 1:
        # 挑名字匹配的; 没有则第一个 (文档建议每文件 1 个 DAG)
        want = getattr(sys.modules["_pavoz_dag"], "DEFAULT_DAG", None)
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
    """重放单 stage: pavoz replay <dag.py> --task-id X --stage Y [--patch P.py].

    patch 文件约定: 模块顶层暴露 patch(dag) -> None (import 后调用, 可改 stage fn).
    """
    dag = _load_dag(args.dag)
    if args.patch:
        patch_path = Path(args.patch).resolve()
        if not patch_path.exists():
            print(f"patch 文件不存在: {patch_path}")
            return 1
        spec = importlib.util.spec_from_file_location("_pavoz_patch", patch_path)
        assert spec is not None and spec.loader is not None, f"无法加载 patch 文件: {patch_path}"
        mod = importlib.util.module_from_spec(spec)
        sys.modules["_pavoz_patch"] = mod
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


async def _cmd_export_input(args) -> int:
    """export-input: 打印 stage 执行前 state (rebuild_state_before) — 人编辑的基座."""
    store = CheckpointStore(_storage())
    cp = store.load_latest(args.task_id)
    if cp is None:
        print(f"task {args.task_id} 无 checkpoint")
        return 1
    try:
        before = cp.rebuild_state_before(args.stage)
    except KeyError as e:
        print(f"export-input 失败: {e} (stage 须已执行过)")
        return 1
    print(json.dumps(before, ensure_ascii=False, indent=2))
    return 0


async def _cmd_fork_run(args) -> int:
    """fork-run: 装回 state → 从 stage 分支续跑.

    v0.9 新增: --set / --set-file / --compare-with / --dry-run.
    优先级 (后写覆盖前写): --set > --set-file > --overrides > --input
    (--overrides > --input 是 v0.8 组合使用时的既有胜者, 保持不变).
    """
    from pavoz.state import parse_set_args, parse_set_file, merge_overrides, _state_diff

    dag = _load_dag(args.dag)

    try:
        overrides = merge_overrides(
            json.loads(Path(args.input).read_text(encoding="utf-8")) if args.input else None,
            json.loads(args.overrides) if args.overrides else None,
            parse_set_file(args.set_file) if args.set_file else None,
            parse_set_args(args.set) if args.set else None,
        )
    except ValueError as e:
        print(f"参数错误: {e}")
        return 1

    if args.dry_run:
        print(json.dumps({
            "dry_run": True,
            "task_id": args.task_id,
            "stage": args.stage,
            "overrides": overrides,
        }, indent=2, ensure_ascii=False))
        return 0

    runtime = Runtime(checkpoint_store=CheckpointStore(_storage()))
    try:
        result = await runtime.fork_run(
            dag, task_id=args.task_id,
            from_stage=args.stage,
            overrides=overrides,
        )
    except (CheckpointMismatchError, RuntimeError, ValueError, KeyError, TimeoutError) as e:
        print(f"fork-run 失败: {e}")
        return 1

    output = {
        "task_id": result.task_id,
        "run_id": result.run_id,
        "dag": result.dag_name,
        "stage": args.stage,
        "status": result.status,
        "error": result.error,
        "overrides_applied": overrides,
    }

    if args.compare_with:
        store = CheckpointStore(_storage())
        orig_cp = store.load(args.task_id, args.compare_with)
        if orig_cp is None:
            print(f"compare-with: run {args.compare_with} 不存在")
            return 1
        diff = _state_diff(orig_cp.state, result.state)
        output["compare_with"] = args.compare_with
        output["diff_keys_count"] = len(diff)
        output["diff_sample"] = dict(list(diff.items())[:10])

    print(json.dumps(output, indent=2, ensure_ascii=False))
    return 0 if result.status == "done" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pavoz", description="LLM/SE/Extract 流程编排")
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

    p_export = sub.add_parser("export-input", help="导出某 stage 的执行前输入 (state JSON, 可编辑)")
    p_export.add_argument("--task-id", required=True)
    p_export.add_argument("--stage", required=True, help="要导出的 stage 名 (须已执行过)")
    p_export.set_defaults(fn=_cmd_export_input)

    p_fork = sub.add_parser("fork-run", help="从历史 stage 分支: 前序复用, 该 stage 起用改后输入重跑")
    p_fork.add_argument("dag", help="dag 文件路径")
    p_fork.add_argument("--task-id", required=True)
    p_fork.add_argument("--stage", required=True, help="分支点 stage (重跑起点)")
    p_fork.add_argument("--overrides", default=None,
                        help='JSON 顶层覆盖, e.g. \'{"topic": "chickens"}\'')
    p_fork.add_argument("--input", default=None,
                        help="编辑过的输入 JSON 文件 (export-input 产物, 全量 state 覆盖)")
    # v0.9 NEW — 4 flags
    p_fork.add_argument("--set", action="append", default=[],
                        help="点分路径 KEY=VALUE (可重复). 例: --set llm.model=longcat")
    p_fork.add_argument("--set-file", default=None,
                        help="JSON/YAML 文件批量覆盖. 1MB 上限, YAML 用 safe_load.")
    p_fork.add_argument("--compare-with", default=None,
                        help="跑完后输出 state leaf diff vs RUN_ID")
    p_fork.add_argument("--dry-run", action="store_true", default=False,
                        help="仅解析 + 显示 overrides, 不执行 stage")
    p_fork.set_defaults(fn=_cmd_fork_run)

    args = parser.parse_args(argv)
    return asyncio.run(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(main())
