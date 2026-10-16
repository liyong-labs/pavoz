"""API 参考文档必须覆盖公开 API — 防止文档悄悄落后于代码.

背景: 2026-09-13 发现 docs/{cn,en}/api.md 漏了 on_event / set_progress / fork_run /
error_class / stage_timings / prune 六项已 ship 的能力 (架构文档已写, API 参考没跟上).
读者按 API 参考找扩展面会一无所获, 所以这条用测试钉住, 不靠人眼。
"""
from __future__ import annotations

import importlib
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


# ── 门禁自身的盲区 (2026-10-16): 子模块声明了却没导出到顶层 ────────────────
#
# 上面那条只查"已在 `pavoz.__all__` 里的名字有没有写进文档"。它对
# **"某个公开名压根没被导出"** 完全无感 —— 因为那种名字压根不在 `__all__` 里,
# 循环压根不会碰到它。
#
# 实例: `pavoz.runtime.__all__` 一直把 `CallMeta` 列为公开名 (caller 的签名类型,
# 核心扩展点), 但顶层没导出 ⇒ 用户 `import pavoz` 拿不到, 写自定义 caller 时
# 无法标注签名, 只能走内部路径 `from pavoz.runtime import ...`。同族的
# `CallResult` / `Ctx` 都导出了, 只有它没有 —— 而文档门禁一路绿灯。

_SUBMODULES = ("runtime", "checkpoint", "dag", "state", "types", "policy",
               "storage", "events", "cancel", "viz", "testing", "_id")


def test_submodule_public_names_are_all_exported_at_top_level() -> None:
    """★ 子模块 `__all__` 声明的每个名字, 都必须能从 `pavoz` 顶层 import。

    "声明为公开" 和 "真的公开" 是两件事, 这条把后者钉住。
    """
    # 判据是"**能从文档里找到怎么拿到它**", 而不是"必须在顶层"。
    # 两种公开方式都成立: 顶层 `from pavoz import X`, 或模块级
    # `from pavoz.storage import key_sha256` (docs/storage.md 明确记了这种)。
    # 只查"顶层有没有"会误报后者。
    # 扫**整个 docs/** 而不是只 api.md —— 模块级公开面记在各自文档里
    # (例: key_sha256 记在 docs/storage.md 的"模块导出"一段)。
    doc_text = "\n".join(
        f.read_text(encoding="utf-8")
        for f in sorted((_ROOT / "docs").rglob("*.md")))
    missing: list[str] = []
    for mod_name in _SUBMODULES:
        mod = importlib.import_module(f"pavoz.{mod_name}")
        for name in getattr(mod, "__all__", ()):   # 没声明 __all__ 的模块不参与
            if hasattr(pavoz, name) or name in doc_text:
                continue
            missing.append(f"{mod_name}.{name}")
    assert not missing, (
        f"子模块 __all__ 声明为公开、但顶层导不出且**文档里也找不到**的名字: "
        f"{missing}。用户既 `from pavoz import X` 不到, 也没文档告诉他从哪拿。"
        f"要么顶层补导出 (加 __all__ 后同步 API 参考文档), 要么把它从子模块的 "
        f"__all__ 里去掉 —— 后者才是多数情况: 那些是内部工具。"
    )


def test_declared_public_names_are_importable_by_name() -> None:
    """反向约束: `from pavoz import X` 对每个公开名都要成立 (不只是 hasattr)。

    `hasattr` 与 `from ... import` 理论上可以不一致 (模块 `__getattr__` 之类的
    边角), 而用户写的是 `from pavoz import X`。这条按用户实际写法验。
    """
    unimportable = []
    for name in pavoz.__all__:
        try:
            exec(f"from pavoz import {name}", {})    # noqa: S102 — 验的就是这个语法
        except ImportError:
            unimportable.append(name)
    assert not unimportable, (
        f"在 pavoz.__all__ 里但 `from pavoz import X` 失败: {unimportable} —— "
        f"那等于对外承诺了一个拿不到的名字")
