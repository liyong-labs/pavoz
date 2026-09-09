#!/usr/bin/env python3
"""03_compare_diff.py — 多 fork_run state diff (v0.9).

用法:
    python examples/03_compare_diff.py <task_id> <run_id_1> [<run_id_2> ...]

输出:
    每对 run 的 state 顶层 key 对比 (轻量, 不依赖内部 API).
"""
import sys
import json
import subprocess


def get_state(task_id, run_id):
    result = subprocess.run(
        ["pavoz", "state", "--task-id", task_id, "--run-id", run_id],
        capture_output=True, text=True, check=True,
    )
    return json.loads(result.stdout)


def main():
    if len(sys.argv) < 3:
        print("usage: 03_compare_diff.py <task_id> <run_id_1> [<run_id_2>...]")
        sys.exit(1)
    task_id = sys.argv[1]
    run_ids = sys.argv[2:]

    print(f"=== {task_id} 多 run state diff ===")
    states = {rid: get_state(task_id, rid) for rid in run_ids}

    for i, rid_a in enumerate(run_ids):
        for rid_b in run_ids[i + 1:]:
            sa, sb = states[rid_a], states[rid_b]
            diff_keys = sorted(set(sa.keys()) | set(sb.keys()))
            print(f"\n{rid_a[:8]} vs {rid_b[:8]}:")
            for k in diff_keys:
                va, vb = sa.get(k), sb.get(k)
                if va != vb:
                    print(f"  {k}: {str(va)[:60]!r} -> {str(vb)[:60]!r}")


if __name__ == "__main__":
    main()
