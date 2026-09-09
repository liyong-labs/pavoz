"""v0.9: state.py apply_overrides utilities tests."""

import pytest
from pavoz.state import (
    _validate_path, _infer_type, _deep_set, _deep_merge,
)


class TestValidatePath:
    def test_simple_ok(self):
        _validate_path("topic")  # no raise
        _validate_path("llm.model")
        _validate_path("a.b.c.d.e")  # depth 5 OK

    def test_depth_over_5_rejected(self):
        with pytest.raises(ValueError, match="深度"):
            _validate_path("a.b.c.d.e.f")

    def test_dunder_rejected(self):
        for bad in ["__proto__", "__class__", "__init__", "__dict__"]:
            with pytest.raises(ValueError, match="禁词"):
                _validate_path(bad)

    def test_empty_segment_rejected(self):
        with pytest.raises(ValueError, match="空"):
            _validate_path("a..b")

    def test_special_chars_rejected(self):
        with pytest.raises(ValueError, match="标识符"):
            _validate_path("a-b")


class TestInferType:
    def test_int(self):
        assert _infer_type("42") == 42

    def test_float(self):
        assert _infer_type("3.14") == 3.14

    def test_bool(self):
        assert _infer_type("true") is True
        assert _infer_type("false") is False

    def test_null(self):
        assert _infer_type("null") is None

    def test_string_passthrough(self):
        assert _infer_type("hello") == "hello"
        assert _infer_type("2026-09-09") == "2026-09-09"

    def test_infer_types_false_passthrough(self):
        """Conservative mode: never infer, always return str."""
        assert _infer_type("42", infer_types=False) == "42"
        assert _infer_type("true", infer_types=False) == "true"


class TestDeepSet:
    def test_simple(self):
        d = {}
        _deep_set(d, "x", 1)
        assert d == {"x": 1}

    def test_nested_create(self):
        d = {}
        _deep_set(d, "a.b.c", 1)
        assert d == {"a": {"b": {"c": 1}}}

    def test_overwrite(self):
        d = {"a": {"b": 1}}
        _deep_set(d, "a.b", 2)
        assert d == {"a": {"b": 2}}

    def test_overwrite_dict(self):
        d = {"a": {"b": 1}}
        _deep_set(d, "a", "scalar")  # overwrite dict with scalar
        assert d == {"a": "scalar"}


class TestDeepMerge:
    def test_simple(self):
        into = {"a": 1}
        _deep_merge(into, {"b": 2})
        assert into == {"a": 1, "b": 2}

    def test_overwrite(self):
        into = {"a": 1, "b": 2}
        _deep_merge(into, {"a": 10})
        assert into == {"a": 10, "b": 2}

    def test_nested(self):
        into = {"a": {"x": 1, "y": 2}}
        _deep_merge(into, {"a": {"y": 20, "z": 3}})
        assert into == {"a": {"x": 1, "y": 20, "z": 3}}
