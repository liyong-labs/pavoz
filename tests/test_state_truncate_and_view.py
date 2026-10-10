"""diff 输出整形与 ReadOnlyState 访问器 — 用户可见输出形状, 此前零覆盖.

背景: 2026-10 覆盖率调查发现 state.py 的 _truncate_for_diff 与 ReadOnlyState
的 __iter__/values/__len__/__repr__ 全未覆盖。这两处都不是内部细节:

- _truncate_for_diff 决定 diff_runs 输出长什么样 (超长 list 只留 50 项 + 标记,
  超长 str 截到 200 并注明总长)。它是**静默塑形** —— 逻辑出错不会抛异常, 只会
  给用户一份看起来正常但少了内容的 diff, 而 diff 正是排查工具, 少内容 = 误导。
- 四个访问器是 ReadOnlyState 的公开面 (ctx.state 由它承载), 业务代码天天用。

先实测确认行为正确 (各边界 + 不就地改原 list), 再把形状钉住, 防止以后有人
改 _DIFF_LIST_MAX 或漏掉标记时无人察觉。
"""
from __future__ import annotations

from pavoz.state import (
    _DIFF_LIST_MAX,
    _DIFF_STR_MAX,
    _truncate_for_diff,
    ReadOnlyStateView,
)


# ── 截断: 边界与形状 ─────────────────────────────────────

def test_long_list_truncated_with_marker() -> None:
    out = _truncate_for_diff(list(range(100)))
    assert len(out) == _DIFF_LIST_MAX + 1
    assert out[-1] == "...(truncated)"
    assert out[:3] == [0, 1, 2]          # 前段保持原样


def test_list_at_limit_untouched() -> None:
    src = list(range(_DIFF_LIST_MAX))
    assert _truncate_for_diff(src) == src


def test_list_just_over_limit_truncated() -> None:
    src = list(range(_DIFF_LIST_MAX + 1))
    out = _truncate_for_diff(src)
    assert len(out) == _DIFF_LIST_MAX + 1
    assert out[-1] == "...(truncated)"


def test_long_string_truncated_with_total() -> None:
    out = _truncate_for_diff("x" * 500)
    assert out.startswith("x" * 10)
    assert "total 500 chars" in out
    assert len(out) == _DIFF_STR_MAX + len("...(truncated, total 500 chars)")


def test_string_at_limit_untouched() -> None:
    src = "x" * _DIFF_STR_MAX
    assert _truncate_for_diff(src) == src


def test_truncation_does_not_mutate_input() -> None:
    """截断只产出新对象 —— 就地改会污染 checkpoint 里的真实 state。"""
    src = list(range(100))
    _truncate_for_diff(src)
    assert len(src) == 100
    assert src[-1] == 99


def test_non_container_passthrough() -> None:
    for v in (1, 1.5, True, None, {"a": 1}, (1, 2)):
        assert _truncate_for_diff(v) == v


# ── ReadOnlyState 公开访问器 ──────────────────────────────

def test_len_iter_values_repr() -> None:
    st = ReadOnlyStateView({"a": 1, "b": 2})
    assert len(st) == 2
    assert sorted(st) == ["a", "b"]                    # __iter__
    assert sorted(st.values()) == [1, 2]               # values()
    assert "ReadOnlyState" in repr(st)                 # __repr__
    assert "'a': 1" in repr(st) or '"a": 1' in repr(st)


def test_empty_state() -> None:
    st = ReadOnlyStateView({})
    assert len(st) == 0
    assert list(st) == []
    assert list(st.values()) == []
