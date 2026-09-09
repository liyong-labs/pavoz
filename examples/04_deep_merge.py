#!/usr/bin/env python3
"""04_deep_merge.py — dot-path 深合并演示 (v0.9 behavior fix).

v0.8: fork-run --overrides '{"llm": {"model": "x"}}' → llm 下兄弟键全被抹掉.
v0.9: --set llm.model=x → 只覆盖 model leaf, 兄弟键 (temperature 等) 保留.

用法:
    python examples/04_deep_merge.py <task_id> [stage]
"""
import sys
import subprocess


def main():
    if len(sys.argv) < 2:
        print("usage: 04_deep_merge.py <task_id> [stage]")
        sys.exit(1)
    task_id = sys.argv[1]
    stage = sys.argv[2] if len(sys.argv) > 2 else "s_compose"
    dag = "backend/integration/research_pipeline_dag.py"

    print("=== dry-run: 检查 overrides 解析 (只含 model leaf) ===")
    subprocess.run([
        "pavoz", "fork-run", dag,
        "--task-id", task_id,
        "--stage", stage,
        "--set", "llm_config.research_writer.model=longcat-official/longcat-2.0",
        "--dry-run",
    ], check=True)

    print()
    print("上面 overrides 只含 model 一个 leaf. v0.9 深合并保证 research_writer")
    print("下的其他键 (temperature / prompt 等) 原样保留.")
    print("确认无误后, 去掉 --dry-run 真正执行 (见本文件末尾注释).")

    # subprocess.run([
    #     "pavoz", "fork-run", dag,
    #     "--task-id", task_id,
    #     "--stage", stage,
    #     "--set", "llm_config.research_writer.model=longcat-official/longcat-2.0",
    # ], check=True)


if __name__ == "__main__":
    main()
