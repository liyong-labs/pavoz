"""Storage backend Protocol + file 实现.

core 零依赖 (不 import psycopg/redis/minio). v1 默认 file backend (本地测试/单机).
ai_writer 接入时实现自己的 backend (DB/MinIO) 注入.

接口只 3 个方法 — checkpoint 持久化 + trace 数据够用.
"""

from __future__ import annotations

import hashlib
import json
import os
from abc import ABC, abstractmethod

__all__ = ["FileStorage", "StorageBackend", "key_sha256"]


def key_sha256(parts: list[str]) -> str:
    """内容寻址 key: 把若干片段拼起来 sha256 前 16 hex.

    用法: key_sha256(["run", task_id, "checkpoint"]) → 稳定唯一 key.
    """
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


class StorageBackend(ABC):
    """checkpoint / trace 数据的持久化接口."""

    @abstractmethod
    def put(self, key: str, data: dict) -> None:
        """写一个 JSON 对象."""

    @abstractmethod
    def get(self, key: str) -> dict | None:
        """读. 不存在返 None."""

    @abstractmethod
    def list_keys(self, prefix: str) -> list[str]:
        """列 prefix 下的所有 key (按字典序)."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """删一个 key."""


class FileStorage(StorageBackend):
    """文件系统实现: root_dir/<key>.json. 测试 / 单机默认."""

    def __init__(self, root_dir: str | os.PathLike):
        self.root = os.fspath(root_dir)
        os.makedirs(self.root, exist_ok=True)

    def _path(self, key: str) -> str:
        # key 只允许 [a-z0-9_/.-] — 防路径注入
        if not all(c.isalnum() or c in "_-/." for c in key):
            raise ValueError(f"非法 storage key: {key!r}")
        return os.path.join(self.root, key + ".json")

    def put(self, key: str, data: dict) -> None:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    def get(self, key: str) -> dict | None:
        path = self._path(key)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def list_keys(self, prefix: str) -> list[str]:
        prefix_path = os.path.join(self.root, prefix)
        out: list[str] = []
        for dirpath, _dirs, files in os.walk(prefix_path):
            for fn in files:
                if fn.endswith(".json"):
                    rel = os.path.relpath(os.path.join(dirpath, fn), self.root)
                    # Storage keys use "/" regardless of host OS (Windows uses "\");
                    # callers (CheckpointStore) match against "runs/.../checkpoint".
                    out.append(rel.replace(os.sep, "/")[: -len(".json")])
        return sorted(out)

    def delete(self, key: str) -> None:
        path = self._path(key)
        if os.path.exists(path):
            os.remove(path)

    def __repr__(self) -> str:
        return f"<FileStorage {self.root}>"
