"""api.md 的目录必须与正文小节**双向一致**.

起因: `docs/{cn,en}/api.md` 都是 700+ 行的单文件参考, 顶部新增了目录 (竞品的
"documentation map" 做法)。手写清单必然漂移:

  · 加一节忘加目录 → 读者在长文里找不到它 (没有任何门禁会响);
  · 删/改一节忘删目录 → 链接指向不存在的锚点 (`check_doc_links` 会抓这一半)。

所以本判据从**正文派生**锚点集合, 与目录链接集合做双向比较。锚点算法直接复用
`tools/check_doc_links.slugify` —— 两处各写一份必然漂移, 而漂移的结果是
"判据绿、真链接坏"。
"""

from __future__ import annotations

import pathlib
import re

from tools.check_doc_links import slugify

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_DOCS = [_ROOT / "docs" / "cn" / "api.md", _ROOT / "docs" / "en" / "api.md"]
_TOC_HEADING = re.compile(r"^##\s+(目录|Contents)\s*$")
_LINK = re.compile(r"^\s*-\s*\[[^\]]+\]\(#([^)]+)\)\s*$")
_H2 = re.compile(r"^##\s+(.+?)\s*$")


def _lines(path: pathlib.Path) -> list[str]:
    """去掉围栏代码块 —— 代码里的 `## ` 是注释, 不是小节标题."""
    out: list[str] = []
    in_fence = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            continue
        if not in_fence:
            out.append(line)
    return out


def toc_and_sections(path: pathlib.Path) -> tuple[list[str], list[str]]:
    lines = _lines(path)
    start = next(i for i, line in enumerate(lines) if _TOC_HEADING.match(line))
    toc: list[str] = []
    for line in lines[start + 1:]:
        if line.startswith("## "):
            break                      # 目录段结束
        m = _LINK.match(line)
        if m:
            toc.append(m.group(1))

    counts: dict[str, int] = {}
    sections: list[str] = []
    for line in lines:
        m = _H2.match(line)
        if not m or _TOC_HEADING.match(line):
            continue
        base = slugify(m.group(1))
        n = counts.get(base, 0)
        counts[base] = n + 1
        sections.append(base if n == 0 else f"{base}-{n}")
    return toc, sections


def test_toc_matches_sections_both_ways():
    problems = {}
    for path in _DOCS:
        toc, sections = toc_and_sections(path)
        assert len(toc) >= 10, (
            f"{path.name}: 只解析到 {len(toc)} 条目录项 —— 解析逻辑坏了, 空集上会恒绿")
        missing = sorted(set(sections) - set(toc))
        stale = sorted(set(toc) - set(sections))
        if missing or stale:
            problems[path.name] = {"缺目录项": missing, "目录项无对应小节": stale}
    assert not problems, (
        f"api.md 目录与正文不一致: {problems}\n"
        f"  · 缺目录项 = 新加的小节读者找不到;"
        f"  · 目录项无小节 = 点过去落在文件顶部 (check_doc_links 也会抓).")
