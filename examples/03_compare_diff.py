#!/usr/bin/env python3
"""03_compare_diff.py — 多 fork_run state diff (v0.9).

用法:
    # 先跑几次 fork (例: bash examples/01_model_switch.sh demo)
    python examples/03_compare_diff.py <task_id> [<run_id_2> <run_id_3> ...]

    run_id 从哪来:
        pavoz trace --task-id <task_id>          # 列 stage 记录 + total_runs
        python -c "from pavoz import CheckpointStore, FileStorage; \\
                   s=CheckpointStore(FileStorage('~/.pavoz/data')); \\
                   print(s.list_runs('<task_id>'))"

⚠️ 2026-10-17 本文件重写 —— 原来它调的是 `pavoz state --run-id <id>`, 而
**`state` 子命令从来没有 `--run-id` 这个 flag** (只有 --task-id / --key / --stats),
而且 CLI **没有任何命令能读指定 run 的 state** (`state` 只读 latest)。所以它对谁都
跑不通: argparse 直接 `unrecognized arguments: --run-id`。demo 里那句
"轻量, 不依赖内部 API" 在没有那个 flag 的前提下无法成立。

现在改用**公开**的 `CheckpointStore` API (`pavos.__init__` 里导出, 不是内部路径):
它本来就是 CLI 底层用的那一个。库 API 能读指定 run, CLI 读不了 —— 这个差在本文件
的 docstring 里说明, 免得下一个人以为是 CLI 少了个 flag。

⚠️ 2026-10-17 顺带修掉一个让本文件**必然读空**的 bug: `FileStorage` 原先不
`expanduser`, 于是这里 (和下面 `_default_store` 的默认值) 里的 `"~/.pvos/data"`
是字面量 —— 读的是 `./~/.pvos/data`, 而 CLI 写的是 `$HOME/.pvos/data`。照
docstring 用法跑 (不带 PAVOZ_STORAGE) 必然报"该 task 现有的 run: (无)",
且每读一次就在 cwd 造一棵字面量 `~` 目录树。现在 `FileStorage.__init__` 统一
展开, 本文件的默认值与 CLI 才指向同一处 —— 别再把它改回字面量。
"""
import json
import os
import sys

from pavoz import CheckpointStore, FileStorage


def _default_store() -> CheckpointStore:
    """CLI 同一套存储解析: PAVOZ_STORAGE (目录) > ~/.pavoz/data。"""
    return CheckpointStore(FileStorage(os.environ.get("PAVOZ_STORAGE", "~/.pavoz/data")))


def main():
    if len(sys.argv) < 3:
        print("usage: 03_compare_diff.py <task_id> <run_id_1> [<run_id_2> ...]")
        sys.exit(1)
    task_id = sys.argv[1]
    run_ids = sys.argv[2:]
    store = _default_store()

    states = {}
    for rid in run_ids:
        cp = store.load(task_id, rid)
        if cp is None:
            known = store.list_runs(task_id)
            print(f"run {rid} 不存在. 该 task 现有的 run: "
                  f"{[r[:8] for r in known] or '(无)'}", file=sys.stderr)
            sys.exit(1)
        states[rid] = cp.state

    print(f"=== {task_id} 多 run state diff ===")
    for i, rid_a in enumerate(run_ids):
        for rid_b in run_ids[i + 1:]:
            sa, sb = states[rid_a], states[rid_b]
            diff_keys = sorted(set(sa) | set(sb))
            print(f"\n{rid_a[:8]} vs {rid_b[:8]}:")
            changed = False
            for k in diff_keys:
                va, vb = sa.get(k), sb.get(k)
                if va != vb:
                    changed = True
                    print(f"  {k}:\n    {json.dumps(va, ensure_ascii=False)[:200]}\n"
                          f"    {json.dumps(vb, ensure_ascii=False)[:200]}")
            if not changed:
                print("  (无差异)")


if __name__ == "__main__":
    main()
