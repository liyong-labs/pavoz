"""EventRecorder: Runtime.on_event 回调的收集器 (W2, 0.5.3).

用途: caller 不想手写 lambda 收集时, 直接 Runtime(on_event=EventRecorder()).
可选 sink_path 每事件追加一行 JSONL (调试留档 / debug_dir 的姊妹信号).
自身不抛异常 — Runtime._emit 已隔离 observer 异常, recorder 再兜一层不必要,
保持薄: 收集 + 可选落盘, 异常交给 _emit 的隔离层.
"""

from __future__ import annotations

import json

__all__ = ["EventRecorder"]


class EventRecorder:
    """收集 (event, data) 事件流; 可选追加 JSONL 文件."""

    def __init__(self, sink_path: str | None = None):
        self._events: list[tuple[str, dict]] = []
        self._sink = sink_path

    def __call__(self, event: str, data: dict) -> None:
        self._events.append((event, data))
        if self._sink:
            with open(self._sink, "a", encoding="utf-8") as f:
                f.write(json.dumps({"event": event, "data": data},
                                   ensure_ascii=False, default=str) + "\n")

    @property
    def events(self) -> list[tuple[str, dict]]:
        return list(self._events)

    def names(self) -> list[str]:
        return [e for e, _ in self._events]

    def __repr__(self) -> str:
        return f"<EventRecorder {len(self._events)} events>"
