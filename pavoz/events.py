"""EventRecorder: Runtime.on_event 回调的收集器 (W2, 0.5.3).

用途: caller 不想手写 lambda 收集时, 直接 Runtime(on_event=EventRecorder()).
可选 sink_path 每事件追加一行 JSONL (调试留档 / debug_dir 的姊妹信号).
自身不抛异常 — Runtime._emit 已隔离 observer 异常, recorder 再兜一层不必要,
保持薄: 收集 + 可选落盘, 异常交给 _emit 的隔离层.
"""

from __future__ import annotations

import json
import math
from typing import Any

__all__ = ["EventRecorder"]


def _json_safe(obj: Any) -> Any:
    """递归把**非有限 float** 换成其 repr 字符串, 使落盘结果始终是合法 JSON.

    为什么需要: Python 的 json 默认 `allow_nan=True`, 会把 NaN/±Inf 写成**字面量**
    `NaN`/`Infinity` —— 那不是 RFC 8259 的合法 JSON, 于是这份"给日志工具读的 JSONL"
    里的行, jq / Go / Splunk / Loki 一行都读不了 (2026-10-16 实测: 一个上报
    `set_progress(float("nan"))` 的 stage, 就能让 7 行 sink 里 2 行不可解析)。
    注意 state 侧已在校验点拒掉非有限值 (deep_validate_state), 但**事件载荷不经
    那个 chokepoint** —— 它是 caller/stage 自报的数据。

    为什么不用 `allow_nan=False` 一刀切: 那会让 dumps 直接抛错, 该事件整条丢失
    (`Runtime._emit` 隔离 observer 异常 → 只剩一行日志)。sink 的职责是**留住观测
    记录**, 一个浮点怪值不该让记录消失 —— 与既有 `default=str` "JSON 表达不了的
    就字符串化" 的口径一致。

    只在**序列化时**替换: 内存里的 `recorder.events` 仍保留原始 float, 在进程内的
    observer 看到的值没有被改写。
    """
    if isinstance(obj, float) and not math.isfinite(obj):
        return repr(obj)          # 'nan' / 'inf' / '-inf'
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    return obj


class EventRecorder:
    """收集 (event, data) 事件流; 可选追加 JSONL 文件."""

    def __init__(self, sink_path: str | None = None):
        self._events: list[tuple[str, dict]] = []
        self._sink = sink_path

    def __call__(self, event: str, data: dict) -> None:
        self._events.append((event, data))
        if self._sink:
            # _json_safe + allow_nan=False 是一对: 前者把非有限值换成字符串,
            # 后者保证万一还有别的非标准值时**响**而不是悄悄写坏文件。
            payload = _json_safe({"event": event, "data": data})
            with open(self._sink, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False,
                                   default=str, allow_nan=False) + "\n")

    @property
    def events(self) -> list[tuple[str, dict]]:
        return list(self._events)

    def names(self) -> list[str]:
        return [e for e, _ in self._events]

    def __repr__(self) -> str:
        return f"<EventRecorder {len(self._events)} events>"
