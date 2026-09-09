"""v0.9: state.py apply_overrides utilities tests."""

import pytest
from pavoz.state import (
    _validate_path, _infer_type, _deep_set, _deep_merge,
)


class TestParseSetFile:
    def test_json_file(self, tmp_path):
        import json
        from pavoz.state import parse_set_file
        f = tmp_path / "override.json"
        f.write_text(json.dumps({"llm": {"model": "longcat"}}))
        assert parse_set_file(str(f)) == {"llm": {"model": "longcat"}}

    def test_yaml_file_safe_load(self, tmp_path):
        pytest.importorskip("yaml")
        from pavoz.state import parse_set_file
        f = tmp_path / "override.yaml"
        f.write_text("llm:\n  model: longcat\n")
        assert parse_set_file(str(f)) == {"llm": {"model": "longcat"}}

    def test_yaml_python_object_rejected(self, tmp_path):
        """!!python/object must be rejected by yaml.safe_load → ValueError."""
        pytest.importorskip("yaml")
        from pavoz.state import parse_set_file
        f = tmp_path / "evil.yaml"
        f.write_text("!!python/object/apply:os.system ['echo evil']\n")
        with pytest.raises(ValueError, match="YAML 解析失败"):
            parse_set_file(str(f))

    def test_file_size_limit(self, tmp_path):
        from pavoz.state import parse_set_file
        f = tmp_path / "big.json"
        f.write_text('{"x": "' + 'a' * 1_100_000 + '"}')
        with pytest.raises(ValueError, match="1MB"):
            parse_set_file(str(f))

    def test_non_dict_top_level_rejected(self, tmp_path):
        import json
        from pavoz.state import parse_set_file
        f = tmp_path / "list.json"
        f.write_text(json.dumps([1, 2, 3]))
        with pytest.raises(ValueError, match="顶层必须是 dict"):
            parse_set_file(str(f))

    def test_dunder_in_file_rejected(self, tmp_path):
        import json
        from pavoz.state import parse_set_file
        f = tmp_path / "evil.json"
        f.write_text(json.dumps({"__proto__": {"x": 1}}))
        with pytest.raises(ValueError, match="禁词"):
            parse_set_file(str(f))

    def test_nonexistent_file_rejected(self):
        from pavoz.state import parse_set_file
        with pytest.raises(ValueError, match="不存在"):
            parse_set_file("/tmp/pavoz_nonexistent_file_42.json")

    def test_json_nonfinite_literal_rejected(self, tmp_path):
        """裁决 #2 回归: JSON NaN/Infinity 字面量必须被 parse_constant 拒."""
        from pavoz.state import parse_set_file
        f = tmp_path / "nan.json"
        f.write_text('{"x": NaN}')
        with pytest.raises(ValueError, match="非有限"):
            parse_set_file(str(f))

    def test_directory_path_rejected(self, tmp_path):
        """目录路径 → OSError 收敛为 ValueError (Task 7 CLI except ValueError)."""
        from pavoz.state import parse_set_file
        with pytest.raises(ValueError, match="读取失败"):
            parse_set_file(str(tmp_path))


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


class TestParseSetArgs:
    def test_single_top_level(self):
        from pavoz.state import parse_set_args
        assert parse_set_args(["topic=chickens"]) == {"topic": "chickens"}

    def test_single_nested(self):
        from pavoz.state import parse_set_args
        assert parse_set_args(["llm.model=longcat"]) == {
            "llm": {"model": "longcat"}
        }

    def test_multiple_merge(self):
        from pavoz.state import parse_set_args
        result = parse_set_args([
            "llm.model=longcat",
            "llm.temperature=0.7",
            "config.target_chars=5000",
        ])
        assert result == {
            "llm": {"model": "longcat", "temperature": 0.7},
            "config": {"target_chars": 5000},
        }

    def test_type_inference(self):
        from pavoz.state import parse_set_args
        assert parse_set_args(["a=42"]) == {"a": 42}
        assert parse_set_args(["b=3.14"]) == {"b": 3.14}
        assert parse_set_args(["c=true"]) == {"c": True}
        assert parse_set_args(["d=null"]) == {"d": None}
        assert parse_set_args(['e=[1,2,3]']) == {"e": [1, 2, 3]}
        assert parse_set_args(['f={"k":"v"}']) == {"f": {"k": "v"}}

    def test_missing_equals_rejected(self):
        from pavoz.state import parse_set_args
        with pytest.raises(ValueError, match="缺 '='"):
            parse_set_args(["no_equals_sign"])

    def test_dunder_rejected(self):
        from pavoz.state import parse_set_args
        with pytest.raises(ValueError, match="禁词"):
            parse_set_args(["__proto__.x=1"])

    def test_dunder_mid_path_rejected(self):
        from pavoz.state import parse_set_args
        with pytest.raises(ValueError, match="禁词"):
            parse_set_args(["a.__proto__.b=1"])

    def test_non_finite_nan_rejected(self):
        from pavoz.state import parse_set_args
        with pytest.raises(ValueError, match="非有限"):
            parse_set_args(["x=nan"])

    def test_non_finite_inf_rejected(self):
        from pavoz.state import parse_set_args
        with pytest.raises(ValueError, match="非有限"):
            parse_set_args(["x=inf"])

    def test_empty_list_returns_empty_dict(self):
        from pavoz.state import parse_set_args
        assert parse_set_args([]) == {}
