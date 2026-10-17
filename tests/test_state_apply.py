"""v0.9: state.py apply_overrides utilities tests."""

import pytest

from pavoz.state import (
    _deep_merge,
    _deep_set,
    _infer_type,
    _validate_path,
)


class TestParseSetFile:
    def test_json_file(self, tmp_path):
        import json

        from pavoz.state import parse_set_file
        f = tmp_path / "override.json"
        f.write_text(json.dumps({"llm": {"model": "my-model"}}))
        assert parse_set_file(str(f)) == {"llm": {"model": "my-model"}}

    def test_yaml_file_safe_load(self, tmp_path):
        pytest.importorskip("yaml")
        from pavoz.state import parse_set_file
        f = tmp_path / "override.yaml"
        f.write_text("llm:\n  model: my-model\n")
        assert parse_set_file(str(f)) == {"llm": {"model": "my-model"}}

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

    def test_nonexistent_file_rejected(self, tmp_path):
        from pavoz.state import parse_set_file
        with pytest.raises(ValueError, match="不存在"):
            parse_set_file(str(tmp_path / "definitely_missing.json"))

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
        assert parse_set_args(["llm.model=my-model"]) == {
            "llm": {"model": "my-model"}
        }

    def test_multiple_merge(self):
        from pavoz.state import parse_set_args
        result = parse_set_args([
            "llm.model=my-model",
            "llm.temperature=0.7",
            "config.target_chars=5000",
        ])
        assert result == {
            "llm": {"model": "my-model", "temperature": 0.7},
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


class TestMergeOverrides:
    def test_simple(self):
        from pavoz.state import merge_overrides
        assert merge_overrides({"a": 1}, {"b": 2}) == {"a": 1, "b": 2}

    def test_priority_later_wins(self):
        from pavoz.state import merge_overrides
        assert merge_overrides(
            {"a": 1, "b": 2},
            {"a": 10, "c": 3},
        ) == {"a": 10, "b": 2, "c": 3}

    def test_nested(self):
        from pavoz.state import merge_overrides
        assert merge_overrides(
            {"llm": {"model": "old", "temp": 0.7}},
            {"llm": {"model": "new"}},
        ) == {"llm": {"model": "new", "temp": 0.7}}

    def test_with_none(self):
        from pavoz.state import merge_overrides
        assert merge_overrides(None, {"a": 1}, None) == {"a": 1}

    def test_empty(self):
        from pavoz.state import merge_overrides
        assert merge_overrides() == {}
        assert merge_overrides({}) == {}


class TestApplyOverrides:
    def test_nested_dict_format(self):
        from pavoz.state import apply_overrides
        result = apply_overrides(
            {"a": 1},
            {"b": {"c": 2}},
        )
        assert result == {"a": 1, "b": {"c": 2}}

    def test_dot_path_format(self):
        from pavoz.state import apply_overrides
        result = apply_overrides(
            {"a": 1},
            {"b.c": 2},
        )
        assert result == {"a": 1, "b": {"c": 2}}

    def test_mixed(self):
        from pavoz.state import apply_overrides
        result = apply_overrides(
            {"llm": {"model": "old"}},
            {"llm.model": "new", "config": {"x": 1}},
        )
        assert result == {"llm": {"model": "new"}, "config": {"x": 1}}

    def test_does_not_mutate_base(self):
        from pavoz.state import apply_overrides
        base = {"a": 1}
        apply_overrides(base, {"a": 2, "b": 3})
        assert base == {"a": 1}  # base unchanged

    def test_does_not_mutate_base_nested(self):
        import copy

        from pavoz.state import apply_overrides
        base = {"llm": {"model": "old", "temperature": 0.5}}
        snapshot = copy.deepcopy(base)
        result = apply_overrides(base, {"llm": {"model": "new"}})
        assert base == snapshot  # base 深层不变
        assert result == {"llm": {"model": "new", "temperature": 0.5}}
        result["llm"]["temperature"] = 99
        assert base["llm"]["temperature"] == 0.5  # 结果嵌套不是 base 的引用

    def test_dot_path_does_not_mutate_base_nested(self):
        import copy

        from pavoz.state import apply_overrides
        base = {"cfg": {"x": 1}}
        snapshot = copy.deepcopy(base)
        apply_overrides(base, {"cfg.y": 2})
        assert base == snapshot


class TestStateDiff:
    def test_top_level_diff(self):
        from pavoz.state import _state_diff
        diff = _state_diff({"a": 1}, {"a": 2})
        assert diff == {"a": [1, 2]}

    def test_nested_diff(self):
        from pavoz.state import _state_diff
        diff = _state_diff(
            {"llm": {"model": "old"}},
            {"llm": {"model": "new"}},
        )
        assert diff == {"llm.model": ["old", "new"]}

    def test_added_key(self):
        from pavoz.state import _state_diff
        diff = _state_diff({"a": 1}, {"a": 1, "b": 2})
        assert diff == {"b": [None, 2]}

    def test_removed_key(self):
        from pavoz.state import _state_diff
        diff = _state_diff({"a": 1, "b": 2}, {"a": 1})
        assert diff == {"b": [2, None]}

    def test_no_diff_empty(self):
        from pavoz.state import _state_diff
        diff = _state_diff({"a": 1}, {"a": 1})
        assert diff == {}

    def test_truncate_long_strings(self):
        from pavoz.state import _state_diff
        long = "x" * 300
        diff = _state_diff({"k": long}, {"k": long + "y"})
        # v0, v1 in diff["k"], long v0 gets truncated
        assert "truncated" in str(diff["k"][0])


# ── key 校验的两个方向 (2026-10-16) ────────────────────────────────────────
#
# `_validate_dict_keys` 自称"递归校验 override 源 dict 的**所有** key", 但实现
# 与这句话有两处对不上, 且**方向相反**:
#
#   (A) 洞: 只递归 **dict** 值, 不进 list/tuple ⇒ 列表里的 dict 完全绕过校验。
#       同一个坏 key, 挂 dict 上拒、挂 list 里放行:
#           {"a": {"": 1}}          → 拒 (非空字符串)
#           {"a": [{"": 1}]}        → 放行
#           {"a": {"__proto__": 1}} → 拒 (禁词)
#           {"a": [{"__proto__": 1}]} → 放行
#
#   (B) 过头: 对**嵌套** dict 里的点号 key 也套了完整的 `_validate_path`
#       (深度 ≤5 / 非标识符字符)。可 `apply_overrides` 只在**顶层**把带点的 key
#       当路径展开 (`"." in k` → `_deep_set`); 嵌套走 `_deep_merge`, 该 key
#       **原样保留**。所以 `{"cfg": {"a.b.c.d.e.f": 1}}` 被 "深度 6 > 5" 拒掉,
#       而它本来会作为一个字面 key 干净落盘 —— 用户的合法输入被误拒。
#       (原注释写的正是"apply_overrides 会展开" —— 那是错的, 只有顶层会。)


class TestKeyValidationCoversWholeStructure:
    """(A) 列表里的 dict 与 dict 里的 dict 判定必须一致."""

    @pytest.mark.parametrize("bad", ["", "__proto__", "__class__", "__init__"])
    def test_bad_key_in_dict_rejected(self, bad, tmp_path):
        import json

        from pavoz.state import parse_set_file
        f = tmp_path / "d.json"
        f.write_text(json.dumps({"a": {bad: 1}}))
        with pytest.raises(ValueError):
            parse_set_file(str(f))

    @pytest.mark.parametrize("bad", ["", "__proto__", "__class__", "__init__"])
    def test_same_bad_key_in_list_rejected(self, bad, tmp_path):
        """★ 同一个坏 key 放进 list 里也必须拒 —— 此前它完全绕过校验。"""
        import json

        from pavoz.state import parse_set_file
        f = tmp_path / "l.json"
        f.write_text(json.dumps({"a": [{bad: 1}]}))
        with pytest.raises(ValueError):
            parse_set_file(str(f))

    def test_deeply_nested_in_lists_rejected(self, tmp_path):
        """两层 list 也不能是漏洞。"""
        import json

        from pavoz.state import parse_set_file
        f = tmp_path / "ll.json"
        f.write_text(json.dumps({"a": [{"b": [{"__proto__": 1}]}]}))
        with pytest.raises(ValueError):
            parse_set_file(str(f))

    def test_list_in_list_valid_still_accepted(self, tmp_path):
        """反向约束: 合法的 list 形状不能被这次收紧误伤。"""
        import json

        from pavoz.state import parse_set_file
        f = tmp_path / "ok.json"
        payload = {"a": [{"k": 1}, [2, 3], {"n": {"d": 4}}], "b": []}
        f.write_text(json.dumps(payload))
        assert parse_set_file(str(f)) == payload

    def test_non_str_key_from_python_caller_rejected(self):
        """非 JSON 路径 (library API 直接传 dict) 也走同一套校验。"""
        from pavoz.state import _validate_dict_keys
        with pytest.raises(ValueError, match="非空字符串"):
            _validate_dict_keys({"a": [{1: 2}]})


class TestNestedDotKeyIsALiteralKey:
    """(B) 嵌套点号 key 不会被展开 ⇒ 不该按"路径"判它."""

    def test_nested_deep_dot_key_accepted_as_literal(self, tmp_path):
        import json

        from pavoz.state import parse_set_file
        f = tmp_path / "lit.json"
        f.write_text(json.dumps({"cfg": {"a.b.c.d.e.f": 1}}))
        assert parse_set_file(str(f)) == {"cfg": {"a.b.c.d.e.f": 1}}

    def test_nested_dot_key_with_dunder_segment_still_rejected(self, tmp_path):
        """放宽的只是"深度/非标识符" —— **dunder 与空 segment 任何层级都拒**。

        否则这次收紧就成了"为了修过度拒绝而打开一个注入口"。
        """
        import json

        from pavoz.state import parse_set_file
        f = tmp_path / "d1.json"
        f.write_text(json.dumps({"cfg": {"__proto__.x": 1}}))
        with pytest.raises(ValueError):
            parse_set_file(str(f))

        f2 = tmp_path / "d2.json"
        f2.write_text(json.dumps({"cfg": {"a..b": 1}}))
        with pytest.raises(ValueError):
            parse_set_file(str(f2))

    def test_top_level_dot_path_still_fully_validated(self):
        """顶层 key 会真的被展开成路径 ⇒ 全套规则一个都不能少。"""
        from pavoz.state import _validate_dict_keys
        _validate_dict_keys({"a.b.c": 1})                       # 合法
        with pytest.raises(ValueError, match="深度"):
            _validate_dict_keys({"a.b.c.d.e.f": 1})             # 6 段
        with pytest.raises(ValueError, match="非标识符"):
            _validate_dict_keys({"my-key.v2": 1})               # 连字符
        with pytest.raises(ValueError, match="禁词"):
            _validate_dict_keys({"__proto__.x": 1})

    def test_nested_literal_key_with_hyphen_and_dot_accepted(self):
        """嵌套的字面 key 带连字符也没问题 —— 它不会被展开, 不必受标识符限制。"""
        from pavoz.state import _validate_dict_keys
        _validate_dict_keys({"cfg": {"my-key.v2": 1}})


# ── 文档表格 ↔ 实际行为 的双向绑定 (2026-10-17) ───────────────────────────
#
# `docs/replay-params.md` 的 "Type inference (auto)" 表列了哪些写法会被改写,
# 之前那张表**只列了安全的那些** (int/float/bool/None/JSON/日期), 于是一个用户
# 只会照着它判断"我的值安不安全" —— 而真正危险的写法 (`1.10`→`1.1`、`007`→`7`)
# 一个都没列。文档还写着"无逃生门", 并建议用 `--dry-run` 看解析结果: 可 `--dry-run`
# 只**显示**结果, 不给你把类型改回去的办法。
#
# 实际逃生门是**同一个命令的 `--set-file`**: JSON 里写了引号就是字符串, 不过推断。
# (library 层另有 `infer_types=False`, 但 CLI 不暴露这个开关。)
#
# 下面两条用例把文档表格的每一行钉住: 将来谁改了推断逻辑, 表格会**被迫**跟着更新,
# 而不是悄悄过期 —— 文档与代码各写一遍却互不校验, 就等于没写。


class TestDocumentedTypeInferenceTable:
    """逐行对应 docs/replay-params.md 的"静默有损"表。"""

    @pytest.mark.parametrize("raw,expected", [
        ("1.10", 1.1),          # 尾零没了
        ("007", 7),             # 前导零没了 (JSON 本来就不允许前导零)
        ("1_000", 1000),        # Python 式下划线
        ("+5", 5),              # 带正号
        (" 5 ", 5),             # 首尾空格被 int() 吃掉
        ("1e3", 1000.0),        # 科学计数法 → float 而不是 int
    ])
    def test_lossy_rewrites_behave_as_documented(self, raw, expected):
        assert _infer_type(raw) == expected
        assert type(_infer_type(raw)) is type(expected)

    def test_documented_lossy_cases_really_lose_through_the_cli_path(self):
        """走真实路径 (`--set K=V` → parse_set_args), 不只测内部函数。"""
        from pavoz.state import parse_set_args
        assert parse_set_args(["version=1.10", "code=007"]) == {
            "version": 1.1, "code": 7}

    def test_escape_hatch_set_file_keeps_the_string(self, tmp_path):
        """★ 文档承诺的逃生门必须真的能用: `--set-file` 的 JSON 字符串原样保留。

        这条是"文档说有路"与"文档说有路"之间的区别 —— 前者要能验。
        """
        import json

        from pavoz.state import parse_set_file
        f = tmp_path / "keep.json"
        f.write_text(json.dumps({"version": "1.10", "code": "007"}), encoding="utf-8")
        parsed = parse_set_file(str(f))
        assert parsed == {"version": "1.10", "code": "007"}, (
            f"`--set-file` 是文档给的唯一逃生门, 它若也开始推断类型, 文档就彻底没出路了; "
            f"实得 {parsed}")
        assert all(isinstance(v, str) for v in parsed.values())

    def test_json_nan_literal_rejected_at_parse_layer(self, tmp_path):
        """JSON 的 `NaN` 字面量在**解析层**就被拒 (`parse_constant`), 走不到推断。"""
        from pavoz.state import parse_set_file
        f = tmp_path / "nf.json"
        f.write_text('{"x": NaN}', encoding="utf-8")
        with pytest.raises(ValueError, match="非有限"):
            parse_set_file(str(f))

    def test_yaml_nan_rejected_by_the_state_chokepoint(self, tmp_path):
        """YAML 的 `.nan` 是**合法 YAML**, 解析层放行 —— 兜住它的是 deep_validate_state。

        两层各钉一条: 别以为"JSON 那条拒了, YAML 也一样" —— YAML 分支没有
        parse_constant, 靠的是下游 chokepoint (并行实现两份校验必然漂, 这里正是
        只有一份的理由)。
        """
        pytest.importorskip("yaml")
        from pavoz.state import parse_set_file
        f = tmp_path / "nf.yaml"
        f.write_text("x: .nan\n", encoding="utf-8")
        # 报错仍然发生在 parse_set_file 里, 但**机制不同**: YAML 分支没有
        # parse_constant, 靠的是 parse_set_file 末尾那次 deep_validate_state
        # (同一个 chokepoint, 没有平行实现第二份 walker —— 平行实现必然漂)。
        with pytest.raises(ValueError, match="非有限"):
            parse_set_file(str(f))
