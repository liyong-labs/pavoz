"""CI 的文档链接闸门自身要被测 — 否则它悄悄退化我们不会知道.

背景 (2026-10-16 巡检): `tools/check_doc_links.py` 原先只查**文件路径**, 而且
`SKIP_PREFIXES` 里带了个 "#", 于是 `docs/cn/api.md` 这种纯锚点跳转被整体跳过,
`other.md#小节` 也只验文件不验小节。结果: 写错或改名的锚点照发, 读者点过去落在
文件顶部, 而 **CI 绿**。docs 之间的互链密度很高 (架构 → API → 快速上手), 这类
死锚点就是读者流失的地方。

现 checker 加了锚点校验, 本文件钉住它**该抓的抓、不该报的不报** —— 后半句更重要:
一个满屏误报的检查等于没有检查, 下次有人就会开始忽略它。
"""
from __future__ import annotations

import importlib.util
import pathlib
import shutil
import tempfile

_CHECKER = pathlib.Path(__file__).resolve().parents[1] / "tools" / "check_doc_links.py"
spec = importlib.util.spec_from_file_location("doc_link_checker", _CHECKER)
chk = importlib.util.module_from_spec(spec)
spec.loader.exec_module(chk)


def _check(files: dict[str, str]) -> list[str]:
    root = pathlib.Path(tempfile.mkdtemp())
    for rel, content in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    try:
        return chk.broken_links(root)
    finally:
        shutil.rmtree(root)


TARGET = """# 顶层标题

## 扩展面 · 一个 stage 就是扩展点

正文。

### 小节 with Mixed CASE 与标点!
"""


def test_valid_cross_file_anchor_passes():
    """CJK + 空格 + 连字符的锚点必须认得 (GitHub 风格 slug)."""
    assert _check({
        "a.md": "[x](b.md#扩展面--一个-stage-就是扩展点)\n",
        "b.md": TARGET,
    }) == []


def test_valid_inpage_anchor_passes():
    """同文件内纯锚点跳转 —— 原先被 SKIP_PREFIXES 整体跳过的那类."""
    assert _check({"a.md": "[x](#小节-with-mixed-case-与标点)\n" + TARGET}) == []


def test_broken_cross_file_anchor_is_reported():
    problems = _check({"a.md": "[x](b.md#根本没有这一节)\n", "b.md": TARGET})
    assert len(problems) == 1 and "锚点失效" in problems[0]


def test_broken_inpage_anchor_is_reported():
    problems = _check({"a.md": "[x](#不存在)\n" + TARGET})
    assert len(problems) == 1 and "锚点失效" in problems[0]


def test_missing_file_is_still_reported():
    assert len(_check({"a.md": "[x](nope.md#某节)\n"})) == 1


def test_plain_file_link_passes():
    assert _check({"a.md": "[x](b.md)\n", "b.md": TARGET}) == []


def test_hash_inside_a_code_fence_is_not_a_heading():
    """围栏代码块里的 `# foo` 是注释 —— 拿它当标题会把合法链接判死."""
    src = "```bash\n# 这是注释不是标题\n```\n\n[x](#这是注释不是标题)\n"
    assert _check({"a.md": src}) == []


def test_target_without_headings_is_skipped_not_flagged():
    """取不到可靠的标题集 (如只用了 setext) 时**放过**, 不误报."""
    assert _check({"a.md": "[x](b.md#任意锚点)\n", "b.md": "只有正文没有标题\n"}) == []


def test_duplicate_headings_use_github_suffix():
    """同名标题 GitHub 会加 -1 后缀, 两个都要认."""
    assert _check({
        "a.md": "[x](b.md#重复)\n[y](b.md#重复-1)\n",
        "b.md": "# 重复\n\n# 重复\n",
    }) == []


def test_real_repo_docs_pass():
    """真实文档全过 —— 误报的代价 (下一个人开始忽略这个检查) 比漏报更高."""
    assert chk.broken_links(pathlib.Path(".")) == []
