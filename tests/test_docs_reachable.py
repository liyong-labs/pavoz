"""文档页必须可达 — 防止特性文档变成孤儿页.

背景: 2026-10 发现 docs/cn/conditional-edges.md 无人从任何 md 链入 —— 条件边
是本库旗舰特性 (跨 stage 循环 + max_visits 熔断), 但读者从 README 走不到它,
等同于没写。test_api_docs_coverage.py 抓不到这类问题: 它只查"公开 API 名字
有没有出现在 api.md", 不查页面之间通不通。

同源哲学: 文档漂移用测试钉住, 不靠人眼 (见 test_api_docs_coverage.py 的说明)。

**这条只管可达性, 不管内容对不对。** 孤儿页能被它抓住; 但"页面存在、却被
别人链接、内容却是过时的"这种更危险的情况它抓不到 —— 那类只能靠评审。
(2026-10 同一批审查里, conditional-edges.md 正是"可达性缺失 + 内容停留在
R1"两者同时发生, 所以两者都得看。)
"""
from __future__ import annotations

import pathlib

_ROOT = pathlib.Path(__file__).resolve().parent.parent

# 页面集合: docs/ 下所有 md + 仓库根的入口文档
_DOC_PAGES = sorted(_ROOT.glob("docs/**/*.md"))
_ENTRY_DOCS = sorted(
    p
    for p in _ROOT.glob("*.md")
    if p.name not in {"CHANGELOG.md"}  # CHANGELOG 不承担导航职责
)

# 按目录索引的页面: README 链到 docs/cn/use-cases/ 这类目录, 页面本身可达。
# 列在这里表示"确认过, 有意不逐页链接"。
_ALLOW_DIR_INDEXED = {
    "docs/cn/use-cases/ai-writer.md",  # README.cn.md 链 docs/cn/use-cases/ 目录
}


def _links_to(target: str, sources: list[pathlib.Path]) -> bool:
    """任一源文档的正文里出现该页面的相对路径即算可达."""
    rel = target[len("docs/") :] if target.startswith("docs/") else target
    for src in sources:
        if src.name == pathlib.Path(target).name and src == _ROOT / target:
            continue  # 不自链
        if rel in src.read_text(encoding="utf-8"):
            return True
        # 也接受从 docs/ 内部写的相对形式, 如 ../cn/conditional-edges.md
        if pathlib.Path(target).name in src.read_text(encoding="utf-8"):
            return True
    return False


def test_every_doc_page_is_reachable() -> None:
    orphans = [
        str(p.relative_to(_ROOT))
        for p in _DOC_PAGES
        if str(p.relative_to(_ROOT)) not in _ALLOW_DIR_INDEXED
        and not _links_to(str(p.relative_to(_ROOT)), _ENTRY_DOCS + _DOC_PAGES)
    ]
    assert not orphans, (
        f"无人链接的孤儿文档页: {orphans} — 新增 docs 页后从 README 或其他页链入, "
        f"否则读者走不到 (或有意不链则登记进 _ALLOW_DIR_INDEXED 并注明理由)"
    )
