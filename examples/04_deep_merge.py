#!/usr/bin/env python3
"""04_deep_merge.py — dot-path 深合并演示 (v0.9 behavior fix).

v0.8: fork-run --overrides '{"llm": {"model": "x"}}' → llm 下的兄弟键被浅替换全抹掉.
v0.9: --set llm_config.writer.model=x → 只覆盖 model 这一个 leaf, 同级的
      temperature / prompt (以及 reviewer 整支) 原样保留.

为什么这条值得单独演示: 少一个键不会报错, 只会让下游拿到缺字段的配置 —— 属于
"跑通了但结果不对"那一类。

用法:
    python examples/04_deep_merge.py <task_id> [stage]

    python -m pavoz run examples/demo_dag.py --task-id demo   # 先有 task
    python examples/04_deep_merge.py demo

⚠️ 2026-10-17: 原先硬编码消费方 dag 路径与 `llm_config.research_writer.*` key。
"""
import os
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent


def main():
    if len(sys.argv) < 2:
        print("usage: 04_deep_merge.py <task_id> [stage]")
        print("hint: 先 `python -m pavoz run examples/demo_dag.py --task-id demo` 造个 task")
        sys.exit(1)
    task_id = sys.argv[1]
    stage = sys.argv[2] if len(sys.argv) > 2 else "s_compose"
    dag = Path(os.environ.get("DAG", SCRIPT_DIR / "demo_dag.py"))
    if not dag.is_file():
        print(f"dag 文件不存在: {dag} (可用 DAG=<path> 指定你自己的)", file=sys.stderr)
        sys.exit(1)

    print("=== dry-run: 检查 overrides 解析 (只有 model 一个 leaf) ===")
    subprocess.run([
        "pavoz", "fork-run", str(dag),
        "--task-id", task_id,
        "--stage", stage,
        "--set", "llm_config.writer.model=tuned-model",
        "--dry-run",
    ], check=True)

    print("\n上面 overrides 只含 model 一个 leaf. v0.9 深合并保证 writer 下的其他键")
    print("(temperature / prompt) 与 reviewer 整支原样保留.")
    print("确认无误后, 去掉 --dry-run 真正执行 (见本文件末尾注释).")

    # subprocess.run([
    #     "pavoz", "fork-run", str(dag),
    #     "--task-id", task_id,
    #     "--stage", stage,
    #     "--set", "llm_config.writer.model=tuned-model",
    # ], check=True)


if __name__ == "__main__":
    main()
