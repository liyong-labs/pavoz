#!/usr/bin/env python3
"""02_prompt_patch.py — 改 compose prompt 不重跑前序 stage (v0.9).

用例: LLM engineer 改了 prompt 模板, 在历史 task 上看效果 (前序 stage 复用).

用法:
    python examples/02_prompt_patch.py <task_id> <new_prompt_file>
"""
import sys
import subprocess
from pathlib import Path


def main():
    if len(sys.argv) < 3:
        print("usage: 02_prompt_patch.py <task_id> <new_prompt_file>")
        sys.exit(1)
    task_id = sys.argv[1]
    prompt_file = Path(sys.argv[2])
    new_prompt = prompt_file.read_text(encoding="utf-8").strip()

    # v0.9 declarative: --set-file 引用 prompt 文件
    prompt_meta_file = prompt_file.with_suffix(".meta.json")
    prompt_meta_file.write_text(
        '{"prompts": {"compose_synthesize": ' + repr(new_prompt) + '}}'
    )

    subprocess.run([
        "pavoz", "fork-run",
        "backend/integration/research_pipeline_dag.py",
        "--task-id", task_id,
        "--stage", "s_compose",
        "--set-file", str(prompt_meta_file),
    ], check=True)

    print(f"OK: task {task_id} composer stage re-ran with new prompt from {prompt_file}")


if __name__ == "__main__":
    main()
