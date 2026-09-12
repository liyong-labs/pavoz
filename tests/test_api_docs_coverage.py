"""API 参考文档必须覆盖公开 API — 防止文档悄悄落后于代码.

背景: 2026-09-13 发现 docs/{cn,en}/api.md 漏了 on_event / set_progress / fork_run /
error_class / stage_timings / prune 六项已 ship 的能力 (架构文档已写, API 参考没跟上).
读者按 API 参考找扩展面会一无所获, 所以这条用测试钉住, 不靠人眼。
"""
from __future__ import annotations

import pathlib

import pytest

import pavoz

_ROOT = pathlib.Path(__file__).resolve().parent.parent
DOC_FILES = ("docs/cn/api.md", "docs/en/api.md")


@pytest.mark.parametrize("doc", DOC_FILES)
def test_api_reference_covers_every_public_name(doc: str) -> None:
    text = (_ROOT / doc).read_text(encoding="utf-8")
    missing = [name for name in pavoz.__all__ if name not in text]
    assert not missing, f"{doc} 未覆盖公开 API: {missing} (新增公开名字后同步 API 参考)"
