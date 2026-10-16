"""State 契约: json-serializable 限定 + read-only 视图 + shallow merge + 冲突检测.

stage 只能:
- 读: 通过 ctx.state (ReadOnlyStateView, 改会 raise)
- 写: 通过 return dict (runtime 做 shallow merge)
"""

from __future__ import annotations

import copy
import math
from typing import Any

__all__ = [
    "ReadOnlyStateView",
    "StateConflictError",
    "apply_overrides",
    "merge_overrides",
    # "merge_state" 曾在这里: 它是 runtime 内部用来折叠 delta 的实现细节, 既没进
    # 顶层也没进文档 —— 公开面与实际可用面不一致 (2026-10-16 新门禁逮到)。用户要
    # 折叠/合并自己的 state 用 apply_overrides / merge_overrides。
    "parse_set_args",
    "parse_set_file",
]

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
        """ctx.state 的只读守卫 — 挂到 __setitem__ / __delitem__ / setdefault /
        update / pop / clear 上, 任何原地写都抛带指引的 TypeError。

        `*a, **kw` 是刻意的: 吞掉任意位置参数与关键字参数, 让调用方拿到
        "read-only" 这条有用信息, 而不是 "takes 1 positional argument but 2
        were given" 这种把人引向错误方向的签名报错。
        (vulture 会把 `kw` 报成未使用变量 —— 它看不到下面 6 处方法名赋值。)
        """
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


def _validate_value(key: str, value: Any, path: str = "") -> None:
    """递归校验单值类型 + 有限性 (给更精确的报错)."""
    # 有限性必须单独查: float 在 _JSON_TYPES 里, 类型检查挡不住 NaN/±Inf ——
    # 它们会照常被 json.dump 写成字面量 NaN/Infinity, 而那不是 RFC 8259 的合法
    # JSON: Python 自己 lenient 所以本仓内不显形, 任何严格解析器 (其他语言 /
    # jq / PostgreSQL JSONB) 拒收整个文件。2026-10-16 实测: stage 直接返回
    # float("nan") → run 报 done, checkpoint.json 含 NaN, 严格解析器拒收。
    # 故非有限值抛 **ValueError** (值的问题而非类型的问题 —— 报"类型 float 不可
    # 序列化"会指错方向); 调用方按 FatalError 语义处理 = 不重试直接失败。
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(
            f"state key '{key}' 是非有限数值 {value!r} (path={path or key}) — "
            f"无法持久化为合法 JSON"
        )
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
    """递归校验所有值类型 + 有限性. run 开始时 + 每 stage return 后调.

    顶层 key 直接作为 path 的起点 —— 否则递归里 f"{path}.{k}" 会从 ".llm"
    起拼, 报错给出的是 ".llm.nested.deep" 这种前导点路径 (既有 TypeError 报错
    一直是这个毛病: 在嵌套 key 上指错位置, 而那正是排查时最需要它说对的场景)。
    """
    for k, v in state.items():
        _validate_value(str(k), v, str(k))


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


# ── v0.9: apply_overrides utilities (CLI + Library API) ─────────────

import json as _json

_DUNDER_BLOCKLIST = frozenset({
    "__proto__", "__class__", "__init__", "__dict__", "__getattribute__",
    "__setattr__", "__delattr__", "__bases__", "__mro__", "__subclasses__",
    "__getstate__", "__setstate__", "__reduce__", "__reduce_ex__",
})

_MAX_PATH_DEPTH = 5
_MAX_INLINE_JSON_LEN = 500


def _validate_path(path: str) -> None:
    """Validate a dot-separated path is safe to use.

    Raises ValueError on:
    - empty path
    - depth > 5
    - empty segment
    - dunder key (Python object injection defense)
    - non-identifier characters
    """
    if not path:
        raise ValueError(f"path {path!r} 为空")
    parts = path.split(".")
    if len(parts) > _MAX_PATH_DEPTH:
        raise ValueError(
            f"path {path!r} 深度 {len(parts)} > {_MAX_PATH_DEPTH} 限制"
        )
    for p in parts:
        if not p:
            raise ValueError(f"path {path!r} 含空 segment")
        if p in _DUNDER_BLOCKLIST:
            raise ValueError(
                f"path {path!r} 含禁词 {p!r} (防 Python object injection)"
            )
        if not all(c.isalnum() or c == "_" for c in p):
            raise ValueError(
                f"path {path!r} 含非标识符字符 (只允许 [a-zA-Z0-9_])"
            )


def _infer_type(raw: str, *, infer_types: bool = True) -> Any:
    """Auto-infer type from string. Default conservative (infer_types=True).

    When infer_types=False, never infer — always return raw str. Use this
    when type drift between v0.8 and v0.9 is unacceptable.

    **下面三个 `except ...: pass` 不是吞错, 是类型试探阶梯**: 按
    int → float → JSON → 原样字符串 的顺序试, 每个 pass 的含义是
    "这个解释不适用, 试下一种", 兜底是末尾的 `return raw`。
    记日志/raise 都是错的 —— 每个字符串值都会刷一条噪声。
    (静默吞错扫描器会报这 3 行, 是已知误报。)
    """
    if not infer_types:
        return raw
    s = raw.strip()
    if not s:
        return s
    if s in ("null", "None"):
        return None
    if s == "true":
        return True
    if s == "false":
        return False
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    if len(s) <= _MAX_INLINE_JSON_LEN and s.startswith(("[", "{")):
        try:
            return _json.loads(s)
        except _json.JSONDecodeError:
            pass
    return raw


def _deep_set(d: dict, path: str, value: Any) -> None:
    """Set d[path.split('.')[0]][...][last] = value. Mutates d.

    Creates intermediate dicts as needed. Overwrites non-dict intermediate
    values (with a warning implicit in behavior — caller is responsible
    for understanding the schema).
    """
    parts = path.split(".")
    cur = d
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = value


def _deep_merge(into: dict, src: dict) -> None:
    """Recursively merge src into into (mutates into)."""
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(into.get(k), dict):
            _deep_merge(into[k], v)
        else:
            into[k] = v


def parse_set_args(items: list[str]) -> dict:
    """Parse --set KEY=VALUE list into a nested dict.

    Example:
        >>> parse_set_args(["llm.model=x", "config.x=5"])
        {'llm': {'model': 'x'}, 'config': {'x': 5}}

    Raises:
        ValueError: any item missing '=' or path unsafe.
    """
    result: dict = {}
    for item in items:
        if "=" not in item:
            raise ValueError(
                f"--set {item!r} 缺 '=' 分隔符. 格式: --set path.to.key=value"
            )
        path, raw = item.split("=", 1)
        _validate_path(path)
        value = _infer_type(raw)
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(
                f"--set {item!r} 值为非有限数值 (nan/inf) — JSON 无法持久化"
            )
        _deep_set(result, path, value)
    return result


_MAX_FILE_SIZE = 1_000_000  # 1MB


def parse_set_file(path: str) -> dict:
    """Parse --set-file PATH. JSON or YAML. 1MB limit, yaml.safe_load only.

    Raises:
        ValueError: file missing / too large / parse error / not dict / unsafe keys.
    """
    from pathlib import Path  # local import — Path not in state module yet

    p = Path(path)
    if not p.exists():
        raise ValueError(f"--set-file {path} 不存在")
    try:
        size = p.stat().st_size
    except OSError as e:
        raise ValueError(f"--set-file {path} 读取失败: {e}")
    if size > _MAX_FILE_SIZE:
        raise ValueError(
            f"--set-file {path} 大小 {size}B > {_MAX_FILE_SIZE}B (1MB) 限制"
        )
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as e:
        raise ValueError(f"--set-file {path} 读取失败: {e}")
    if path.endswith((".yaml", ".yml")):
        try:
            import yaml  # lazy import (stdlib-only 保持)
        except ImportError:
            raise ValueError(
                f"--set-file {path} 是 YAML 但 PyYAML 未安装. "
                "用 JSON 替代 OR pip install pyyaml."
            )
        try:
            data = yaml.safe_load(text)  # 强制 safe_load (防任意代码)
        except yaml.YAMLError as e:
            raise ValueError(f"--set-file {path} YAML 解析失败: {e}")
    else:
        def _reject_nonfinite(lit):
            raise ValueError(f"--set-file {path} 含非有限数值 {lit!r} — JSON 无法持久化")

        try:
            data = _json.loads(text, parse_constant=_reject_nonfinite)
        except _json.JSONDecodeError as e:
            raise ValueError(f"--set-file {path} JSON 解析失败: {e}")
    if not isinstance(data, dict):
        raise ValueError(
            f"--set-file {path} 顶层必须是 dict (实际 {type(data).__name__})"
        )
    _validate_dict_keys(data, path="")
    # 值层校验 (类型 + 有限性) 也在这里做: 文件是用户直接给的数据, 错误该在
    # 解析现场报, 而不是等 merge 进 state 后由 deep_validate_state 在更远处抛。
    # 复用同一个 chokepoint (不平行实现一份 walker —— 平行实现必然漂)。
    # JSON 分支的 parse_constant 只挡 NaN 这一类, 这层还挡 datetime 之类"能
    # 解析但存不下来"的类型, 且对 YAML 分支同样生效 (YAML 此前两样都没有)。
    deep_validate_state(data)
    return data


def _validate_dict_keys(d: dict, path: str = "") -> None:
    """递归校验 override 源 dict 的所有 key (file 来源不可信).

    只拒: 非字符串 / 空 key / dunder 禁词. 其余字符 (含 '-') 是合法 dict
    key, 不拒 — 严格逐段校验是 _validate_path 的职责 (dot-path 场景).
    含 '.' 的 key 走 _validate_path (dot-path 语义, apply_overrides 会展开).

    不变量: 本函数是 override key 校验的唯一入口 — parse_set_file /
    merge_overrides 共用; 新增校验路径时复用本函数, 不要平行实现.
    """
    for k, v in d.items():
        if not isinstance(k, str) or not k:
            raise ValueError(
                f"override key {k!r} 必须是非空字符串 (在 {path or '<root>'})"
            )
        if k in _DUNDER_BLOCKLIST:
            raise ValueError(
                f"override key {k!r} 含禁词 (在 {path or '<root>'})"
            )
        if "." in k:
            _validate_path(k)
        if isinstance(v, dict):
            _validate_dict_keys(v, f"{path}.{k}" if path else k)


# merge_overrides 的 key 校验复用 _validate_dict_keys (唯一入口) —
# 拒 非字符串/空 key/dunder; '-' 等 dict key 合法; 含 '.' 的 key 走
# _validate_path. 不要在此平行实现校验 (重复 def 会静默覆盖).


def merge_overrides(*sources: dict | None) -> dict:
    """Merge multiple override dicts by priority. Later sources win.

    注意: 结果 dict 的嵌套对象与输入 src 按引用共享 (不做 deepcopy) —
    merge 后不要再 mutate 输入源.

    Example:
        >>> merge_overrides({"a": 1}, {"a": 2, "b": 3})
        {'a': 2, 'b': 3}

    None sources are skipped.
    """
    merged: dict = {}
    for src in sources:
        if src:
            _validate_dict_keys(src, "")
            _deep_merge(merged, src)
    return merged


def apply_overrides(base: dict, *patch_dicts: dict) -> dict:
    """Library API: apply one or more patch dicts to base.

    Each patch dict accepts BOTH formats:
    - Nested dict (current pavoz convention): {"llm": {"model": "x"}}
    - Dot-path dict (new): {"llm.model": "x"}

    Returns: new dict (does not mutate base).
    base 的嵌套对象与结果完全独立 (deepcopy). 结果的嵌套对象可能与 patch
    的嵌套对象共享引用 — 调用方 apply 后不要再 mutate patch.

    Example:
        >>> apply_overrides({"a": 1}, {"b.c": 2})
        {'a': 1, 'b': {'c': 2}}
    """
    merged = copy.deepcopy(base)
    for patch in patch_dicts:
        if not patch:
            continue
        for k, v in patch.items():
            if "." in k:
                # dot-path: validate + set
                _validate_path(k)
                _deep_set(merged, k, v)
            else:
                # nested dict: merge
                if isinstance(v, dict) and isinstance(merged.get(k), dict):
                    _deep_merge(merged[k], v)
                else:
                    merged[k] = v
    return merged


_DIFF_STR_MAX = 200
_DIFF_LIST_MAX = 50


def _truncate_for_diff(v: Any) -> Any:
    """Truncate long strings / lists for readable diff output."""
    if isinstance(v, str) and len(v) > _DIFF_STR_MAX:
        return v[:_DIFF_STR_MAX] + f"...(truncated, total {len(v)} chars)"
    if isinstance(v, list) and len(v) > _DIFF_LIST_MAX:
        return v[:_DIFF_LIST_MAX] + ["...(truncated)"]
    return v


def _state_diff(a: dict, b: dict, path: str = "") -> dict[str, list]:
    """Recursively find leaf differences between two state dicts.

    Returns: {full.path: [before, after]} dict. Skips nested diffs
    when both sides are dicts (recurses instead).
    """
    diffs: dict[str, list] = {}
    keys = set(a.keys()) | set(b.keys())
    for k in keys:
        full = f"{path}.{k}" if path else k
        va, vb = a.get(k), b.get(k)
        if isinstance(va, dict) and isinstance(vb, dict):
            diffs.update(_state_diff(va, vb, full))
        elif va != vb:
            diffs[full] = [_truncate_for_diff(va), _truncate_for_diff(vb)]
    return diffs
