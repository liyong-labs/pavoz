"""dot-path 覆盖的可见性 —— 行为**不改**, 但"静默"必须变成"可见"。

`_deep_set` 原本对两件事完全沉默 (docstring 自称 "warning implicit in behavior",
即没有 warning):

- **key 拼错**: `--set llm.modle=x` 而真 key 是 `model` ⇒ 静默新建 `modle`,
  真 `model` 保持旧值 ⇒ **配置看起来设了, 实际没生效**, 全程无提示
- **中间不是 dict**: `{"cfg": "a-string-value"}` 上写 `cfg.model=x` ⇒ 那个
  **原值被静默丢弃**, 变成 `{"model": "x"}` —— 这是数据丢失

行为之所以不改: pavoz 是通用编排库, **不该猜用户的 state schema** ——
往已有结构里加新配置项 (`llm.temp`) 与首次注入 (`--set` 到空 state) 都是合法
常态, 报错会把它们一起拒掉。所以修的是**可见性**, 分级如下:

| 情形 | 级别 | 理由 |
|------|------|------|
| 覆盖非 dict 中间值 | WARNING | 几乎总是写错, 且**丢数据** |
| 中间层新建 | DEBUG | 首次注入的常态, 不该刷屏 |
| 叶子 key 新建 | DEBUG | "加新配置项"合法, 但用户拼错的**正是叶子** |
| 覆盖已有叶子 | 无输出 | 正常路径 |
"""
from __future__ import annotations

import logging

import pytest

from pavoz.state import apply_overrides


@pytest.fixture
def caplog_pavoz(caplog):
    caplog.set_level(logging.DEBUG, logger="pavoz")
    return caplog


# ── WARNING: 覆盖非 dict (丢数据) ───────────────────────────────────────────

def test_overwriting_non_dict_warns_and_says_the_value_is_lost(caplog_pavoz):
    out = apply_overrides({"cfg": "a-string-value"}, {"cfg.model": "x"})

    assert out["cfg"] == {"model": "x"}, "行为不变: 覆盖成 dict"
    warns = [r.getMessage() for r in caplog_pavoz.records
             if r.levelno >= logging.WARNING]
    assert len(warns) == 1, f"丢数据必须 WARNING, 实得 {warns}"
    msg = warns[0]
    assert "原值丢失" in msg, f"消息要说明原值没了, 实得: {msg}"
    assert "str" in msg, f"消息要给出被覆盖值的类型, 实得: {msg}"
    assert "cfg" in msg, f"消息要点名路径位置, 实得: {msg}"


@pytest.mark.parametrize("existing", [
    "字符串", 123, ["列表"], None, True,
])
def test_overwriting_any_non_dict_warns(caplog_pavoz, existing):
    """不只是 str —— 任何非 dict 中间值被覆盖都该警告。"""
    apply_overrides({"k": existing}, {"k.child": 1})
    warns = [r.getMessage() for r in caplog_pavoz.records
             if r.levelno >= logging.WARNING]
    assert warns, f"覆盖 {type(existing).__name__} 也该警告"


# ── DEBUG: 路径新建 (合法常态, 但拼错也长这样) ─────────────────────────────

def test_leaf_creation_is_debug_not_warning(caplog_pavoz):
    """叶子 key 新建: 合法 (加新配置项) 但也可能是拼错 ⇒ DEBUG 而非 WARNING。

    断言**不出现** WARNING 是重点: 把它升成 WARNING 会让"给 llm 加一个 temp
    配置"这种每天都在做的操作开始刷警告, 那用户只会把日志关掉。
    """
    apply_overrides({"llm": {"model": "x"}}, {"llm.temp": 0.7})

    warns = [r.getMessage() for r in caplog_pavoz.records
             if r.levelno >= logging.WARNING]
    assert not warns, f"加新配置项是合法常态, 不该 WARNING: {warns}"

    debugs = [r.getMessage() for r in caplog_pavoz.records
              if r.levelno == logging.DEBUG]
    assert any("temp" in m for m in debugs), (
        f"开 DEBUG 的人应当能看见新叶子 (用户拼错的正是叶子), 实得 {debugs}")


def test_typo_in_leaf_is_visible_at_debug(caplog_pavoz):
    """★ 用户最常见的错误: `--set llm.modle=x` (真 key 是 model)。

    行为上它与"加新配置项"无法区分 (都是叶子不存在) —— 所以只能是 DEBUG。
    但消息必须说清**真 key 仍在、本次设置不生效**, 否则日志本身没用。
    """
    out = apply_overrides({"llm": {"model": "gpt-4"}}, {"llm.modle": "oops"})

    assert out["llm"]["model"] == "gpt-4", "真 key 的值不能被影响"
    assert out["llm"]["modle"] == "oops"

    debugs = [r.getMessage() for r in caplog_pavoz.records
              if r.levelno == logging.DEBUG]
    assert debugs, "叶子拼错必须留痕 (DEBUG)"
    assert "不生效" in debugs[0] or "拼错" in debugs[0], (
        f"消息要说清后果: 真 key 仍在, 本次设置不生效。实得: {debugs[0]}")


def test_intermediate_creation_is_debug(caplog_pavoz):
    apply_overrides({}, {"llm.model": "gpt-4"})
    debugs = [r.getMessage() for r in caplog_pavoz.records
              if r.levelno == logging.DEBUG]
    assert any("llm" in m for m in debugs), f"中间层新建也该留痕, 实得 {debugs}"


# ── 反向约束: 正常路径零噪声 ───────────────────────────────────────────────

def test_normal_overwrite_is_silent(caplog_pavoz):
    """反向约束: 正常路径一条日志都不出 —— 否则开 DEBUG 的人要被淹。"""
    apply_overrides({"llm": {"model": "x", "temp": 0.7}}, {"llm.model": "gpt-4"})
    assert not caplog_pavoz.records, f"正常路径不该有日志: {caplog_pavoz.records}"


def test_behaviour_is_unchanged_by_the_logging(caplog_pavoz):
    """日志是**唯一**改动: 同样的输入必须得到逐字节相同的输出。"""
    cases = [
        ({"cfg": "s"}, {"cfg.model": "x"}),
        ({"llm": {"model": "x"}}, {"llm.modle": "oops"}),
        ({}, {"a.b.c": 1}),
        ({"a": {"b": {"c": 1}}}, {"a.b.c": 2}),
    ]
    for base, patch in cases:
        import logging as _lg
        records: list = []

        class _Cap(_lg.Handler):
            def emit(self, r):
                records.append(r)

        logger = _lg.getLogger("pavoz")
        cap = _Cap()
        logger.addHandler(cap)
        old_level = logger.level
        logger.setLevel(_lg.DEBUG)
        try:
            out = apply_overrides(base, patch)
        finally:
            logger.removeHandler(cap)
            logger.setLevel(old_level)
        assert out is not None
        assert records is not None
        # 逐键重跑一次 (关掉日志) 结果必须一致
        logger.setLevel(_lg.WARNING + 10)
        try:
            out2 = apply_overrides(base, patch)
        finally:
            logger.setLevel(old_level)
        assert out == out2, f"{base} + {patch}: 两次结果不同"


def test_depth_limit_still_rejects(caplog_pavoz):
    """深度上限不受日志改动影响。"""
    with pytest.raises(ValueError, match="深度"):
        apply_overrides({}, {"a.b.c.d.e.f": 1})
