"""CI guard: mechanical doc-style checks (the greppable subset).

Rules (from the team doc-writing standard): no AI-cliché words; README headings
carry no version-number decorations (version info goes in the body); Chinese prose
ends sentences with 。, not an ASCII period.

Only high-precision checks belong here — a false positive that blocks a
contributor's PR costs more than the rule it guards.

Usage:  python tools/check_doc_style.py     (exit 0 = clean)
"""

from __future__ import annotations

import pathlib
import re
import sys

SKIP_PARTS = (".git", "docs/design", "docs/work-note", "docs/reference")
# 过程/历史文档: 版本号与日期是内容本身 (里程碑、变更记录), 套风格规则会误报
SKIP_FILES = ("CLAUDE.md", "CONTRIBUTING.md", "CHANGELOG.md", "ROADMAP.md")

AI_CLICHES = ("utilize", "ascertain", "endeavor", "seamless", "robust",
              "landscape", "paradigm", "empower", "foster")
# 只抓"装饰性"版本号 (README 里的新功能标榜); 设计文档里 "(v0.5)" 表示引入版本, 是信息
VERSION_DECORATION = re.compile(r"^#{1,4} .*[（(](new in |since |v\d+\.\d+ 新|v\d+\.\d+ new)",
                                re.IGNORECASE)
CN_ASCII_PERIOD = re.compile(r"[一-鿿]\.$")


def markdown_files(root: pathlib.Path) -> list[pathlib.Path]:
    return [
        path
        for path in root.rglob("*.md")
        if not any(part in str(path) for part in SKIP_PARTS)
        and path.name not in SKIP_FILES
    ]


def check(path: pathlib.Path) -> list[str]:
    problems: list[str] = []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        lowered = line.lower()
        if not line.lstrip().startswith(("|", ">", "```")):
            for word in AI_CLICHES:
                if re.search(rf"\b{word}\w*", lowered):
                    problems.append(f"{path}:{lineno} AI 禁用词 {word!r} — 换具体说法或删")
        if path.name.startswith("README") and VERSION_DECORATION.match(line):
            problems.append(f"{path}:{lineno} 标题带版本号 — 版本信息移进正文")
        if CN_ASCII_PERIOD.search(line):
            problems.append(f"{path}:{lineno} 中文句尾用了半角句号 — 用 。")
    return problems


def main() -> int:
    problems = [m for path in markdown_files(pathlib.Path(".")) for m in check(path)]
    if problems:
        print("\n".join(problems))
        return 1
    print("OK: 文档风格检查通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
