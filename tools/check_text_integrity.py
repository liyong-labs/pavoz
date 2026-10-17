"""CI guard: 仓库里的文本文件不许带 U+FFFD (替换字符).

起因 (2026-10-17 实测): 本会话写文件时, **4 个文件**的正文里各有一处字符在落盘
前被打坏, 写进去的就是 U+FFFD (`ef bf bd`) 本身 —— 不是"当时显示乱码", 是**真的
存成了替换字符**, 文件本身仍是合法 UTF-8, 所以 `read_text()` 一切正常, 语法也正常,
**没有任何现有门禁看得见**:

    tools/check_changelog.py                 L11   恒红[FFFD]门禁迟早被关掉
    tests/test_max_concurrent_runs_queue.py  L16   才开[FFFD]"是确定性事实
    tests/test_corrupt_record.py             L16   traceback 里[FFFD]文件和行号
    docs/cn/api.md                           L497  要[FFFD]**等待本身**设上界

`docs/cn/api.md` 那处是**用户可见**的: 读者看到的是一个替换字符方块。这不是排版
问题 —— 它意味着"这句话在传输/生成链路上被截断了", 而没人知道被截掉的是什么。

为什么这类损坏特别难靠肉眼发现: 它落在**注释和散文**里 (本仓 4 处全在 docstring /
中文说明), 不影响任何断言, 不影响任何 import, 所以"跑一遍测试全绿"完全正常。
换句话说, 凡是能跑出来的东西都验不到它 —— 只能显式扫字节。

扫**已跟踪 + 未跟踪但未被 ignore** 的文件 (`git ls-files --cached --others
--exclude-standard`)。带 `--others` 是 2026-10-17 补的: 原先只扫已跟踪的, 于是新写的
文件在 `git add` 之前完全不在范围内 —— 而本会话写 `tests/test_cli_dag_selection.py`
时自己带进 2 处替换字符, 门禁当时报 "OK: 147 个已跟踪文本文件" 因为**看不见那个还没
跟踪的文件**, 提交后才转红。门禁在"新东西即将落仓"那一刻瞎, 恰好抵消了它存在的理由。

`--exclude-standard` 保证 .gitignore 里的东西 (构建产物 / .venv / data/) 不被扫进来 ——
实测本仓 `.venv` 与那两棵垃圾目录树都在 ignore 里, 加了 `--others` 之后扫描数不变。

⚠️ 加 `--others` **不会**让 CI 变严: `actions/checkout` 给出的工作区是干净的, 那里
`--others --exclude-standard` 返回空集, 扫的还是同一批已跟踪文件。换句话说这个拓宽
纯粹是给**本地提交前**加的, 而提交前恰恰是损坏最该被拦住的那一刻。

Usage:  python tools/check_text_integrity.py     (exit 0 = 文本完好)
"""
from __future__ import annotations

import pathlib
import subprocess
import sys

# 只扫文本。扩展名白名单而不是"试着解码": 二进制文件解不出 utf-8 是正常的, 拿它
# 当坏会造出一堆假阳性, 而假阳性会把门禁关掉 —— 那比没有门禁更坏。
TEXT_SUFFIXES = frozenset({
    ".py", ".md", ".sh", ".bash", ".toml", ".yml", ".yaml",
    ".cfg", ".ini", ".txt", ".json",
})
# 写成转义而不是字面量: 字面的 U+FFFD 会让**本文件自己**被自己的判据判失败。
REPLACEMENT = "\ufffd"


def tracked_files() -> list[pathlib.Path]:
    """git 已跟踪的文件 (相对仓库根)。"""
    # ⚠️ `--others --exclude-standard` 不是可选项 —— 2026-10-17 实测: 本函数原先只扫
    # 已跟踪文件, 于是**新写的文件在 `git add` 之前完全不在扫描范围里**。而那正是最
    # 该拦的窗口: 本会话写 `tests/test_cli_dag_selection.py` 时自己带进了 2 处替换
    # 字符, 门禁当时报 "OK: 147 个已跟踪文本文件" —— 因为那个文件还没被跟踪, 门禁
    # **看不见它**。提交后才变成 "148 个" 并转红。
    # 也就是说: 只扫已跟踪文件的门禁, 恰好在"新东西即将落仓"那一刻是瞎的, 而那正是
    # 它存在的理由。`--exclude-standard` 保证 .gitignore 里的东西 (构建产物、.venv)
    # 不会被扫进来。
    p = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        capture_output=True,
    )
    if p.returncode != 0:
        print("FAIL: `git ls-files` 失败 —— 判据失明, 退出而不是恒过")
        raise SystemExit(1)
    root = pathlib.Path(subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True,
    ).stdout.strip())
    return [root / name for name in p.stdout.decode("utf-8", "replace").split("\0") if name]


def scan(path: pathlib.Path) -> list[str]:
    """返回该文件的问题描述列表 (空 = 完好)。"""
    try:
        raw = path.read_bytes()
    except OSError as e:                       # 文件在 ls-files 里却读不到
        return [f"L?: 读不了 ({e})"]

    problems = []
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        lineno = raw.count(b"\n", 0, e.start) + 1
        problems.append(f"L{lineno}: 不是合法 UTF-8 ({e.reason} @ byte {e.start})")
        return problems

    for i, line in enumerate(text.splitlines(), 1):
        if REPLACEMENT in line:
            col = line.index(REPLACEMENT) + 1
            n = line.count(REPLACEMENT)
            snippet = line.strip()
            if len(snippet) > 60:
                snippet = snippet[:57] + "..."
            problems.append(f"L{i}: {n} 个替换字符 (col {col}) | {snippet}")
    return problems


def main() -> int:
    files = [p for p in tracked_files() if p.suffix in TEXT_SUFFIXES]
    # 自检: 扫到 0 个文件 = 判据空转 (白名单写错 / 不在 git 仓里), 那它就永远绿。
    if len(files) < 20:
        print(f"FAIL: 只扫到 {len(files)} 个文本文件 —— 判据失明, 不该绿")
        return 1

    bad: list[str] = []
    for p in files:
        for msg in scan(p):
            try:
                rel = p.relative_to(pathlib.Path.cwd())
            except ValueError:
                rel = p
            bad.append(f"{rel}  {msg}")

    if bad:
        print(f"FAIL: {len(bad)} 处文本损坏 (U+FFFD / 非法 UTF-8):\n")
        print("\n".join(bad))
        print("\nU+FFFD 是字符在生成/传输链路上被截断的痕迹, 落盘后就是 ef bf bd 三个"
              "字节, 语法与测试都验不到它。按上下文把那句话补回去再提交 —— "
              "别用 sed 之类的方式批量删, 那会把句子删残。")
        return 1
    print(f"OK: {len(files)} 个文本文件均无替换字符 (已跟踪 + 未跟踪未忽略)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
