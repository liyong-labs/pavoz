"""State 契约: json-serializable 限定 + read-only 视图 + shallow merge + 冲突检测.

stage 只能:
- 读: 通过 ctx.state (ReadOnlyStateView, 改会 raise)
- 写: 通过 return dict (runtime 做 shallow merge)
"""

from __future__ import annotations

import copy
from typing import Any

__all__ = ["ReadOnlyStateView", "StateConflictError", "merge_state", "validate_state"]

# json.dumps 能处理的类型白名单 (naobao 调研: set/datetime/Path/bytes 全要 raise)
_JSON_TYPES = (str, int, float, bool, type(None), list, dict)


class StateConflictError(Exception):
    """两个 stage 试图写同一 state key. 修: stage 用不同 key (语义 key)."""


class ReadOnlyStateView:
    """read-only defensive copy. 写操作 raise.

    ctx.state 就是这个对象 — stage 内改它 = 改了个寂寞 (zeroflow ANTI_PATTERN).
    """

    __slots__ = ("_data",)

    def __init__(self, data: dict):
        self._data = data

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def __iter__(self):
        return iter(self._data)

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()

    def items(self):
        return self._data.items()

    def _readonly_raise(self, *a, **kw):
        raise TypeError("ctx.state 是 read-only — stage 通过 return dict 写 state, 不要原地改")

    __setitem__ = _readonly_raise
    __delitem__ = _readonly_raise
    setdefault = _readonly_raise
    update = _readonly_raise
    pop = _readonly_raise
    clear = _readonly_raise

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"<ReadOnlyState {self._data!r}>"


def validate_state(state: Any) -> None:
    """校验 state 可 json 序列化. 失败 raise TypeError (FatalError 上游处理)."""
    # 快速路径: 整个 dict json.dumps 一次 (深度错误信息不完美, 但 v1 够)
    try:
        import json

        json.dumps(state, ensure_ascii=False)
    except (TypeError, ValueError) as e:
        raise TypeError(f"state 必须 json-serializable, got: {e}") from e


def _validate_value(key: str, value: Any, path: str = "") -> None:
    """递归校验单值类型 (给更精确的报错)."""
    if value is None or isinstance(value, _JSON_TYPES):
        if isinstance(value, dict):
            for k, v in value.items():
                _validate_value(str(k), v, f"{path}.{k}")
        elif isinstance(value, list):
            for i, v in enumerate(value):
                _validate_value(str(i), v, f"{path}[{i}]")
        return
    raise TypeError(
        f"state key '{key}' 值类型 {type(value).__name__} 不可 json 序列化 (path={path or key}). "
        f"允许: str/int/float/bool/None/list/dict"
    )


def deep_validate_state(state: dict) -> None:
    """递归校验所有值类型. run 开始时 + 每 stage return 后调."""
    for k, v in state.items():
        _validate_value(k, v)


def merge_state(
    prev: dict, delta: dict, stage_name: str, overwrite_keys: set[str] | None = None
) -> dict:
    """shallow merge + 冲突检测: stage 不能覆盖已存在的 key.

    返回新 dict (不 mutate prev). prev key 与 delta key 重叠 → StateConflictError.
    默认语义 (v1): 每个 state key 只有一个 producer.
    v0.1.1 链式演进: overwrite_keys 内的 key 允许覆盖 — runtime 已确认其 producer
    是当前 stage 的传递上游 (数据流水线: search→filter→compress 逐级更新同一产物).
    平行 producer (无依赖链) 覆盖仍 raise.
    """
    _ow = overwrite_keys or set()
    conflicts = [k for k in delta if k in prev and k not in _ow]
    if conflicts:
        raise StateConflictError(
            f"stage '{stage_name}' 试图写已存在的 state key: {conflicts}. "
            f"DAG 里每个 key 只允许一个 producer — 改用不同 key (语义 key)."
        )
    merged = dict(prev)
    merged.update(delta)
    return merged


def snapshot(state: dict) -> dict:
    """deep copy (defensive). 每 stage 前给 ctx.state 用的快照."""
    return copy.deepcopy(state)
