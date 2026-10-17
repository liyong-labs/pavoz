"""architecture 的**事件表**必须与代码实际 emit 的事件**双向一致**.

起因 (2026-10-17 实测): 那张表叫"生命周期事件表" / "Lifecycle events", 而 `llms.txt`
把它指为**权威参考** ("core design decisions, extension surface, event table")。可它是
**不完整的**:

  · **`route` 事件完全不在表里** —— 它是条件边 (R1, 0.5.5) 的路由事件, 代码在
    `runtime.py` 里每次求值都 emit, payload `{task_id, run_id, from, key, to, declared}`。
    它只在 `docs/{cn,en}/conditional-edges.md` 的一个表格单元格里被顺带提到 ——
    于是"按 architecture 实现一个事件消费者"的人会**整个漏掉它**。
  · **stage 事件的字段表漏了 `visit`** —— 写的是
    `task_id/run_id/stage/attempt/status/duration`, 而 R1 之后一个 stage 可被访问多次,
    `stage_start`/`stage_end`/`stage_retry` 都带 `visit`。照那张表写代码的人拿不到
    访问次数, 而它在回路里是**判断是否转圈**的关键。
  · 而且那张表把 `status`/`duration` 归给 `stage_start`, 代码里 `stage_start` **不带**
    这两个 (它们随 `stage_end` 才来)。

三条同源: 表是**手写**的, 而没有任何判据拿它跟代码对过。判据从**代码派生**事件集:

  · 正向 (code ⊆ doc): 代码 emit 的每个事件都必须在**两种语言**的表里出现。漏一个
    就是上面那个 `route`。
  · 反向 (doc ⊆ code): 表里不许列代码根本不 emit 的事件 —— 那是许愿, 不是文档。

⚠️ 反向只取表格**第一格**, 且只扫表体行: 表下方的说明文字里会提到 `stage_end` /
`visit` / `declared` 这些**字段名**, 全局扫反引号标识符会把它们当成"编造的事件"。
这一条第一版就是这么写的, 变异时自己报了假红。

⚠️ 提取事件名要同时认 `self._emit("x", ...)` 与 `self.on_event("x", ...)` **两种调用**
—— `stage_progress` 走的是后者 (Ctx.set_progress 里直接调 on_event, 不经 `_emit`)。
第一版只搜 `_emit(`, 于是 `stage_progress` 被算成"代码没 emit", 而它其实在文档里 ——
判据会对着一个正确条目报红, 那比不报更让人不信任它。
"""
from __future__ import annotations

import pathlib
import re
from importlib import import_module

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_PKG = [d for d in _ROOT.iterdir() if d.is_dir() and (d / "runtime.py").is_file()][0].name
_PKG = import_module(_PKG).__name__  # 程序化取真包名, 不手打

ARCH_DOCS = [_ROOT / "docs" / "cn" / "architecture.md",
             _ROOT / "docs" / "en" / "architecture.md"]

# 两种 emit 入口: runtime 内部的 _emit, 与 Ctx.set_progress 直接调 on_event
_EMIT = re.compile(r'\b_emit\(\s*"(\w+)"')
_ON_EVENT = re.compile(r'\bon_event\(\s*"(\w+)"')
# 表格行**第一格**里的每一个反引号标识符。⚠️ 必须取**全部**而不是第一个: 表里有合并行
# (如 `run_start` / `run_end` 写在同一格), 只取第一个的话 `run_end` 与 `stage_end`
# 会被算成"代码 emit 了但表里没有" —— 而它们明明印在表上。
# 只取第一格是为了不把后面几格的**字段名** (attempt/duration/visit/...) 当成事件。
_CELL_TOKEN = re.compile(r"`(\w+)`")


def emitted_events() -> set[str]:
    out: set[str] = set()
    for f in sorted((_ROOT / _PKG).glob("*.py")):
        text = f.read_text(encoding="utf-8")
        out |= set(_EMIT.findall(text))
        out |= set(_ON_EVENT.findall(text))
    return out


# 事件表那一节的标题 (中英各一)。**必须限定在这一节内**取行 —— 同一份文档下面还有一张
# "异常契约" 表, 它的首格是 `StageError` / `RetryableError` / `FatalError`。第一版扫全文
# 所有"首格是反引号标识符"的表格行, 于是判据对着那三条**异常名**报"列了代码不 emit 的
# 事件" —— 纯假红, 而且是那种会让人把判据关掉的假红。
_HEADINGS = re.compile(r"生命周期事件表|Lifecycle events")


def table_rows(text: str) -> list[str]:
    """**事件表那一节**里, 首格为反引号事件名的数据行。"""
    lines = text.splitlines()
    start = next((i for i, l in enumerate(lines) if _HEADINGS.search(l)), None)
    assert start is not None, "architecture.md 里找不到事件表的标题"
    out: list[str] = []
    for l in lines[start + 1:]:
        if not l.startswith("|"):
            if out:            # 表体结束 (标题行之后先撞到非表格行才收)
                break
            continue          # 标题与表头分隔行之间的空行
        cells = l.split("|")
        if len(cells) >= 2:
            out.extend(_CELL_TOKEN.findall(cells[1]))
    return out


def test_every_emitted_event_is_in_the_table():
    """★ 代码 emit 的每个事件, 两种语言的表里都必须有。"""
    code = emitted_events()
    assert code, "从代码里一个事件都没提取到 —— 提取逻辑坏了"
    problems = {}
    for doc in ARCH_DOCS:
        listed = set(table_rows(doc.read_text(encoding="utf-8")))
        missing = sorted(code - listed)
        if missing:
            problems[doc.name] = missing
    assert not problems, (
        "architecture 的事件表漏了代码实际 emit 的事件: "
        f"{problems}\n  代码 emit 的全集: {sorted(code)}\n"
        f"  那张表是 llms.txt 指名的权威参考, 漏一个事件 = 按它实现消费者的人整个漏掉它。")


def test_table_invents_no_events():
    """★ 反面: 表里不许列代码根本不 emit 的事件 (那是许愿, 不是文档)。"""
    code = emitted_events()
    problems = {}
    for doc in ARCH_DOCS:
        invented = sorted(set(table_rows(doc.read_text(encoding="utf-8"))) - code)
        if invented:
            problems[doc.name] = invented
    assert not problems, (
        f"architecture 的事件表列了代码不 emit 的事件: {problems}\n"
        f"  代码 emit 的全集: {sorted(code)}")


def test_extraction_sees_both_emit_paths():
    """自检: 两种 emit 入口都要被认到 —— 少认一种, 上面两条的"全集"就是错的.

    `stage_progress` 走的是 `self.on_event("stage_progress", ...)` (Ctx.set_progress 里
    直接调 on_event, 不经 runtime 的 `_emit`)。只搜 `_emit(` 会把它漏掉, 于是"代码没
    emit 它" 而文档里明明有 —— 判据对一条**正确**条目报红。
    """
    code = emitted_events()
    assert "stage_progress" in code, (
        f"没提取到 stage_progress —— on_event 那条路径没被认。提取到: {sorted(code)}")
    assert "route" in code, f"没提取到 route —— 提取到: {sorted(code)}"
    assert len(code) >= 5, f"只提取到 {len(code)} 个事件, 提取多半坏了: {sorted(code)}"
