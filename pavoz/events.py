"""EventRecorder: Runtime.on_event 回调的收集器 (W2, 0.5.3).

用途: caller 不想手写 lambda 收集时, 直接 Runtime(on_event=EventRecorder()).
可选 sink_path 每事件追加一行 JSONL (调试留档 / debug_dir 的姊妹信号).
自身不抛异常 — Runtime._emit 已隔离 observer 异常, recorder 再兜一层不必要,
保持薄: 收集 + 可选落盘, 异常交给 _emit 的隔离层.
"""

from __future__ import annotations

import json
import math
import os
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
        self._sink_dir_ready = False

    def _ensure_sink_dir(self, sink: str) -> None:
        # sink 由调用点传入而不是读 self._sink: 非空收窄发生在 `if self._sink:`
        # 那一行, 跨方法边界 pyright 看不到 (它会报 dirname(str|None))。
        """首次落盘前把父目录建出来 (只做一次)。

        为什么要有这一步: sink 路径指向一个**还不存在**的目录时, `open(..., "a")`
        每个事件都抛 FileNotFoundError —— 一个单 stage 的 run 就是 5 条完整
        traceback, 长 run 能刷几千行同样的错误 (把真正的错误淹掉), 而 run 照样
        报 done、磁盘上一个字节没有。

        这个失败是**永久性**的却按事件重复报, 是最坏的组合; 它也不是"该响就响"
        就够了 —— 用户要的是日志文件, 报一万条 traceback 不等于给了他日志。

        口径统一到 `_dump_debug` (debug_dir 快照): 那是同一类事 (用户给路径, 我们
        往里写 JSON), 它 `os.makedirs(..., exist_ok=True)` 把 task 子目录建了出来。
        两个功能对"父目录怎么办"给出一致的答案, 好过让每个调用方自己记得 mkdir。

        只在**有东西要写时**建 (不在 `__init__`): 一个从不落盘的 recorder 不该留下
        目录副作用。裸文件名 (`ev.jsonl`) 的 dirname 是空串 —— 跳过, 不去建 "."。
        """
        if self._sink_dir_ready:
            return
        parent = os.path.dirname(sink)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._sink_dir_ready = True

    def __call__(self, event: str, data: dict) -> None:
        self._events.append((event, data))
        if self._sink:
            # _json_safe + allow_nan=False 是一对: 前者把非有限值换成字符串,
            # 后者保证万一还有别的非标准值时**响**而不是悄悄写坏文件。
            self._ensure_sink_dir(self._sink)
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
