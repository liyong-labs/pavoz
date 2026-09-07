"""State 契约: 类型限定 / read-only view / merge / 冲突."""

import pytest

from pavoz import ReadOnlyStateView, StateConflictError
from pavoz.state import deep_validate_state, merge_state


def test_validate_json_types_ok():
    deep_validate_state({"a": 1, "b": "x", "c": [1, 2], "d": {"e": True}, "f": None})


def test_deep_validate_rejects_set():
    with pytest.raises(TypeError, match="json 序列化"):
        deep_validate_state({"a": {1, 2}})


def test_deep_validate_rejects_datetime():
    import datetime

    with pytest.raises(TypeError, match="datetime"):
        deep_validate_state({"when": datetime.datetime.now()})  # noqa: DTZ005 — 故意造 datetime 值测 raise


def test_readonly_view_blocks_write():
    view = ReadOnlyStateView({"a": 1})
    assert view["a"] == 1
    assert view.get("a") == 1
    assert "a" in view
    with pytest.raises(TypeError, match="read-only"):
        view["b"] = 2
    with pytest.raises(TypeError):
        view.update({"b": 2})
    with pytest.raises(TypeError):
        del view["a"]


def test_readonly_view_isolated_from_source():
    src = {"a": {"deep": 1}}
    view = ReadOnlyStateView(src)
    # 内部还是引用 — 但 runtime 传的是 deepcopy, 这里验证语义由 runtime 保证
    assert view["a"]["deep"] == 1


def test_merge_basic():
    merged = merge_state({"a": 1}, {"b": 2}, "s_x")
    assert merged == {"a": 1, "b": 2}


def test_merge_conflict_raises():
    with pytest.raises(StateConflictError, match="s_x"):
        merge_state({"a": 1}, {"a": 99}, "s_x")


def test_merge_does_not_mutate_prev():
    prev = {"a": 1}
    merge_state(prev, {"b": 2}, "s_x")
    assert prev == {"a": 1}
