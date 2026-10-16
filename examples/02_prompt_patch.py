#!/usr/bin/env python3
"""02_prompt_patch.py — 改 prompt 不重跑前序 stage (v0.9).

用例: 改了 prompt 模板, 在历史 task 上看效果 (前序 stage 复用其 checkpoint).

用法:
    python examples/02_prompt_patch.py <task_id> <new_prompt_file>

    # 先有 task:
    python -m pavoz run examples/demo_dag.py --task-id demo
    echo "v2: 更严格的写作要求" > /tmp/p.txt
    python examples/02_prompt_patch.py demo /tmp/p.txt

⚠️ 2026-10-17: 本文件原先写的是消费方 schema (`{"prompts": {"compose_synthesize": ...}}`)
+ 消费方 dag 路径 (`backend/integration/research_pipeline_dag.py`) —— 任何 clone
pavoz 的人照 README 跑都失败, 而它在公开仓里示范的却是一套**这里根本不存在**的
key 名。现在改成与 examples/demo_dag.py 一致的 `llm_config.writer.prompt`, dag 路径
按脚本自身目录解析。

用业务方的 dag 时注意: 生成的 meta JSON 的 key 必须**跟着那个 dag 的 state 结构走**,
否则 fork 会写进一个没人读的 key (静默无效)。想省事就直接 `--set llm.writer.prompt=...`。
"""
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent


def main():
    if len(sys.argv) < 3:
        print("usage: 02_prompt_patch.py <task_id> <new_prompt_file>")
        print("hint: 先 `python -m pavoz run examples/demo_dag.py --task-id demo` 造个 task")
        sys.exit(1)
    task_id = sys.argv[1]
    prompt_file = Path(sys.argv[2])
    new_prompt = prompt_file.read_text(encoding="utf-8").strip()
    dag = Path(os.environ.get("DAG", SCRIPT_DIR / "demo_dag.py"))
    if not dag.is_file():
        print(f"dag 文件不存在: {dag} (可用 DAG=<path> 指定你自己的)", file=sys.stderr)
        sys.exit(1)

    # v0.9 declarative: --set-file 引用 JSON 文件。key 与 demo_dag.py 的 state 对齐 ——
    # 覆盖 prompt 一个 leaf, 同级的 model / temperature 保留 (深合并)。
    fd, meta_path = tempfile.mkstemp(suffix=".json", prefix="prompt_patch_")
    os.close(fd)
    Path(meta_path).write_text(
        json.dumps({"llm_config": {"writer": {"prompt": new_prompt}}}, ensure_ascii=False),
        encoding="utf-8",
    )
    try:
        subprocess.run([
            "pavoz", "fork-run", str(dag),
            "--task-id", task_id,
            "--stage", "s_compose",
            "--set-file", meta_path,
        ], check=True)
    finally:
        Path(meta_path).unlink(missing_ok=True)

    print(f"OK: task {task_id} 的 s_compose 用新 prompt ({new_prompt[:30]!r}) 重跑过")
    print("  同级的 model/temperature 与 reviewer 整支都保留 —— 这就是 v0.9 的深合并")


if __name__ == "__main__":
    main()
