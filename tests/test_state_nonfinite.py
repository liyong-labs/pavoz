"""非有限数值 (NaN / ±Inf) 不能进 state —— 落盘会产出**非法 JSON**.

背景 (2026-10-16 巡检发现): `deep_validate_state` 只查值**类型**, 而 float 在
允许集里, 于是 NaN / ±Inf 一路畅通直到 json.dump:

    stage 返回 float("nan") → run status=**done** → checkpoint.json 含字面量
    `NaN` → 严格解析器 (RFC 8259) 拒收整个文件

为什么本仓内看不出来: Python 的 json **load 和 dump 两侧都 lenient**, NaN
往返无损。所以问题在跨边界时才炸 —— 其他语言的 JSON 解析器、jq、PostgreSQL
JSONB 会拒收, 而 pavoz 自己读得好好的。

这与 `--set-file` 的不对称是同一件事: JSON 分支早就用 `parse_constant` 挡了
NaN (注释里明写 "JSON 无法持久化"), 但那是**解析层**的局部防护; YAML 分支
两样都没有 (`.nan` / `.inf` 是合法 YAML 标量), 而**任何**来源的 NaN 都能从
stage 返回值进来。故校验点落在 `deep_validate_state` 这个唯一 chokepoint 上。

判据覆盖三种来源 (stage 返回 / --set-file YAML / --set-file JSON) 与三个位置
(顶层 / 嵌套 dict / list 元素), 并反向验证"有限 float 一律放行"。
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from pavoz import DAG, CheckpointStore, FileStorage, Runtime
from pavoz.state import deep_validate_state, parse_set_file


def _strict_loads(text: str):
    """严格 RFC 8259 解析器 (默认 json 允许 NaN, 这里显式拒)."""
    def reject(c):
        raise ValueError(f"非标准 JSON 常量: {c}")
    return json.loads(text, parse_constant=reject)


# ── 单元层: deep_validate_state ──────────────────────────────────────────

@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_rejected_at_top_level(bad):
    with pytest.raises(ValueError, match="非有限数值"):
        deep_validate_state({"score": bad})


def test_nonfinite_rejected_when_nested_in_dict():
    with pytest.raises(ValueError, match="非有限数值"):
        deep_validate_state({"llm": {"temperature": float("nan")}})


def test_nonfinite_rejected_inside_list():
    """list 元素也必须查 —— overrides 里 list 是合法值, 别让 NaN 藏在里面."""
    with pytest.raises(ValueError, match="非有限数值"):
        deep_validate_state({"scores": [1.0, float("inf")]})


@pytest.mark.parametrize("ok", [0, -0.0, 1.5, -2.25, 1e300])
def test_finite_floats_still_pass(ok):
    """反向约束: 修的是"非有限", 不是"浮点" —— 有限 float 一律放行."""
    deep_validate_state({"v": ok})
    deep_validate_state({"v": [ok, {"k": ok}]})


def test_int_and_bool_not_treated_as_float():
    """bool 是 int 的子类, 但两者都该照常过 (isinstance(x, float) 不该误伤)."""
    deep_validate_state({"i": 3, "b": True, "f": False})


def test_error_message_names_the_key_and_path():
    """报错要能指到具体位置 —— 否则 NaN 混在几十个 key 里无从查起."""
    with pytest.raises(ValueError) as ei:
        deep_validate_state({"llm": {"nested": {"deep": float("nan")}}})
    msg = str(ei.value)
    assert "deep" in msg and "llm.nested" in msg


# ── 落盘层: NaN 不能变成非法 JSON ───────────────────────────────────────

async def test_stage_returning_nan_does_not_persist_illegal_json(tmp_path):
    store = CheckpointStore(FileStorage(tmp_path / "store"))
    dag = DAG("nan_stage")

    @dag.stage()
    async def s_a(ctx):
        return {"score": float("nan"), "ok": 1}

    r = await Runtime(checkpoint_store=store).run(dag, "nt")

    assert r.status != "done", (
        "stage 返回 NaN 必须失败而不是 done —— 修复前 run 报 done 且落盘出非法 JSON")
    assert "非有限" in (r.error or ""), f"报错应说清是非有限数值, 实得: {r.error}"

    for cp_file in Path(store.storage.root).rglob("checkpoint.json"):
        text = cp_file.read_text(encoding="utf-8")
        _strict_loads(text)   # 严格解析器拒收即抛 → 本用例红
        assert "NaN" not in text and "Infinity" not in text


# ── 文件层: --set-file 两个分支 ─────────────────────────────────────────

def test_set_file_yaml_nonfinite_rejected(tmp_path):
    pytest.importorskip("yaml")
    f = tmp_path / "over.yaml"
    f.write_text("score: .nan\n", encoding="utf-8")
    with pytest.raises(ValueError, match="非有限数值"):
        parse_set_file(str(f))


def test_set_file_yaml_inf_rejected(tmp_path):
    pytest.importorskip("yaml")
    f = tmp_path / "over.yaml"
    f.write_text("hi: .inf\nlo: -.inf\n", encoding="utf-8")
    with pytest.raises(ValueError, match="非有限数值"):
        parse_set_file(str(f))


def test_set_file_json_nonfinite_still_rejected_with_its_own_message(tmp_path):
    """JSON 分支原有 parse_constant 保持优先 (更早、更精确的报错措辞)."""
    f = tmp_path / "over.json"
    f.write_text('{"score": NaN}', encoding="utf-8")
    with pytest.raises(ValueError, match="含非有限数值"):
        parse_set_file(str(f))


def test_set_file_yaml_datetime_rejected_at_parse_site(tmp_path):
    """能解析但存不下来的类型 (YAML 的 date) 也在解析现场报, 不留到落盘时炸."""
    pytest.importorskip("yaml")
    f = tmp_path / "over.yaml"
    f.write_text("due: 2026-10-16\n", encoding="utf-8")
    with pytest.raises(TypeError, match="json 序列化"):
        parse_set_file(str(f))


def test_set_file_yaml_normal_values_unaffected(tmp_path):
    """反向约束: 正常 YAML override 不受影响."""
    pytest.importorskip("yaml")
    f = tmp_path / "over.yaml"
    f.write_text("llm:\n  model: my-model\n  temperature: 0.7\ntags:\n  - a\n  - b\n",
                 encoding="utf-8")
    got = parse_set_file(str(f))
    assert got == {"llm": {"model": "my-model", "temperature": 0.7}, "tags": ["a", "b"]}
    assert math.isfinite(got["llm"]["temperature"])
