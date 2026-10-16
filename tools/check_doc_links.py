"""CI guard: relative links in markdown docs must resolve.

Docs are for humans — a dead link is where a reader stops. Checks every local
link/target in ``*.md`` (skipping ``docs/design/`` working notes, which link to
things outside the repo on purpose).

Usage:  python tools/check_doc_links.py     (exit 0 = all links resolve)
"""

from __future__ import annotations

import pathlib
import re
import sys

SKIP_DIRS = (".git", ".venv", "docs/design")
SKIP_PREFIXES = ("http://", "https://", "mailto:")
LINK = re.compile(r"\[[^\]]*\]\(([^)#][^)]*)\)")
# 锚点链接 (同文件内跳转): 原先被 SKIP_PREFIXES 里的 "#" 一起跳过, 于是
# `docs/cn/api.md` 里写错的小节名没人查 —— 读者点过去落在文件顶部, 而 CI 绿。
INPAGE = re.compile(r"\[[^\]]*\]\((#[^)]*)\)")
FENCE = re.compile(r"^(?:```|~~~)")


def strip_code_blocks(text: str) -> str:
    """去掉围栏代码块 —— 里面的 `# foo` 是注释, 不是标题."""
    out, in_fence = [], False
    for line in text.splitlines():
        if FENCE.match(line.lstrip()):
            in_fence = not in_fence
            continue
        if not in_fence:
            out.append(line)
    return "\n".join(out)


def slugify(heading: str) -> str:
    """GitHub 风格小节锚点: 小写; 去标点; 空格→连字符; 保留字母数字与 CJK."""
    s = heading.strip().lower()
    s = re.sub(r"[\s]+", "-", s)
    s = re.sub(r"[^\w\-\u4e00-\u9fff]", "", s)
    return s


def anchors_of(md_path: pathlib.Path) -> set[str] | None:
    """该文件所有小节锚点。**返回 None = 不检查** (取不到可靠的标题集).

    取不到的情形: 文件不存在、非 markdown、或一个 ATX 标题都没有 (比如只用
    setext 标题) —— 此时宁可放过, 也不误报。
    """
    if not md_path.exists():
        return None
    try:
        text = strip_code_blocks(md_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError):
        return None
    heads = [m.group(2).strip() for m in re.finditer(r"^(#{1,6})\s+(.*?)\s*#*$",
                                                   text, re.M)]
    if not heads:
        return None
    counts: dict[str, int] = {}
    out: set[str] = set()
    for h in heads:
        base = slugify(h)
        n = counts.get(base, 0)
        counts[base] = n + 1
        out.add(base if n == 0 else f"{base}-{n}")   # 同名标题 GitHub 加后缀
    return out


def markdown_files(root: pathlib.Path) -> list[pathlib.Path]:
    return [
        path
        for path in root.rglob("*.md")
        if not any(part in str(path) for part in SKIP_DIRS)
    ]


def broken_links(root: pathlib.Path) -> list[str]:
    problems: list[str] = []
    cache: dict[pathlib.Path, set[str] | None] = {}

    def _anchors(p: pathlib.Path):
        if p not in cache:
            cache[p] = anchors_of(p)
        return cache[p]

    for path in markdown_files(root):
        text = path.read_text(encoding="utf-8")
        for target in LINK.findall(text):
            target = target.strip()
            if target.startswith(SKIP_PREFIXES):
                continue
            file_part, _, anchor = target.partition("#")
            resolved = (path.parent / file_part).resolve()
            if not resolved.exists():
                problems.append(f"{path}: 链接失效 → {target}")
                continue
            if anchor:
                have = _anchors(resolved)
                if have is not None and anchor.lower() not in have:
                    problems.append(
                        f"{path}: 锚点失效 → {target} (该文件没有这个小节)")
        for target in INPAGE.findall(text):          # 同文件内锚点
            anchor = target.strip()[1:]
            if not anchor:
                continue
            have = _anchors(path)
            if have is not None and anchor.lower() not in have:
                problems.append(f"{path}: 锚点失效 → {target} (本文件没有这个小节)")
    return problems


def main() -> int:
    problems = broken_links(pathlib.Path("."))
    if problems:
        print("\n".join(problems))
        return 1
    print("OK: 所有 markdown 相对链接与锚点均有效")
    return 0


if __name__ == "__main__":
    sys.exit(main())
