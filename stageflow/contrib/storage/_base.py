"""contrib Storage adapter 公共 helpers.

stageflow.contrib.storage 全部 adapter 共享:
- _validate_key: 路径安全 (只允许 [a-zA-Z0-9_-/]+)
- _encode_payload / _decode_payload: dict <-> bytes JSON 序列化
  (与 ai_writer MinioStorage 行为对齐: ensure_ascii=False + None 表示缺失)
"""

from __future__ import annotations

import json

# 与 stageflow/storage.py FileStorage._path 一致: 路径注入防御
_SAFE_KEY_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_/.")


def _validate_key(key: str) -> None:
    if not key:
        raise ValueError("storage key 不能为空 (empty)")
    if ".." in key:
        # char check 已通过, 但 '..' 是路径遍历 token — 单独拦截
        raise ValueError(f"storage key 含路径遍历: {key!r}")
    if not all(c in _SAFE_KEY_CHARS for c in key):
        raise ValueError(
            f"storage key 含非法字符: {key!r}. 只允许 [a-zA-Z0-9_-/.]"
        )


def _encode_payload(data: dict) -> bytes:
    """dict -> bytes (UTF-8 JSON). 用 ensure_ascii=False 保留中文."""
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


def _decode_payload(raw: bytes) -> dict | None:
    """bytes -> dict. 解析失败 (包括空) 返 None — 与 stageflow.core 同语义."""
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return None
