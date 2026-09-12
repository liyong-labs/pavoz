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

SKIP_DIRS = (".git", "docs/design")
SKIP_PREFIXES = ("http://", "https://", "mailto:", "#")
LINK = re.compile(r"\[[^\]]*\]\(([^)#][^)]*)\)")


def markdown_files(root: pathlib.Path) -> list[pathlib.Path]:
    return [
        path
        for path in root.rglob("*.md")
        if not any(part in str(path) for part in SKIP_DIRS)
    ]


def broken_links(root: pathlib.Path) -> list[str]:
    problems: list[str] = []
    for path in markdown_files(root):
        for target in LINK.findall(path.read_text(encoding="utf-8")):
            target = target.strip()
            if target.startswith(SKIP_PREFIXES):
                continue
            if not (path.parent / target.split("#")[0]).resolve().exists():
                problems.append(f"{path}: 链接失效 → {target}")
    return problems


def main() -> int:
    problems = broken_links(pathlib.Path("."))
    if problems:
        print("\n".join(problems))
        return 1
    print("OK: 所有 markdown 相对链接均有效")
    return 0


if __name__ == "__main__":
    sys.exit(main())
