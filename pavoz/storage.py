"""Storage backend Protocol + file 实现.

core 零依赖 (不 import psycopg/redis/minio). v1 默认 file backend (本地测试/单机).
接入方实现自己的 backend (DB/MinIO) 注入.

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
        # ⚠️ 展开必须在**这一层**, 不能只放在 CLI 的默认值上: 实测两个入口
        # 都把字面量 `~` 写进了磁盘 (修复前) ——
        #   1) FileStorage("~/.pvos/data")          库 API 直传。examples/03_
        #      compare_diff.py 的 docstring 正是这么教的, 照抄就中招。
        #   2) PAVOZ_STORAGE='~/.pvos/data' pavos run  cli.py 只 expand 了**默认值**
        #      那一支 (`os.environ.get(spec, os.path.expanduser(...))`), env 命中时
        #      原样返回, 里的 `~` 没人展开。
        # 后果不是报错, 是**静默写到别处**: 在 cwd 里长出一棵字面量名叫 `~` 的目录树,
        # 数据全落进去。于是同一份数据, 写的一侧读 $HOME/.pvos/data, 读的一侧读
        # ./\~/.pvos/data —— `03_compare_diff.py` 对刚跑完的 task 报"该 task 现有的
        # run: (无)", 而且每读一次就在 cwd 留一棵垃圾目录。
        # 这里是路径的**唯一**汇合点 (CLI 的 _storage 也走 FileStorage), 放这一处
        # 两个入口一起修好; os.path.expanduser 只处理**行首**的 ~ (与 shell 一致),
        # 路径中间的 ~ 与绝对/相对路径都不受影响 —— 纯修正, 不改既有解析语义。
        self.root = os.path.expanduser(os.fspath(root_dir))
        os.makedirs(self.root, exist_ok=True)

    def _path(self, key: str) -> str:
        # 第一道: 粗筛, 挡掉明显不是 key 的东西 (空格 / 控制符 / 盘符等)。
        # ⚠️ 别把它当白名单读 —— `str.isalnum()` 对**大写**和**全部 Unicode
        # 字母/数字**都成立, 故 "UPPER" / "任务" / "café" / "٣" 一律放行 (实测)。
        # 本文件原先的注释写的是 "[a-z0-9_/.-]", 与实现不符, 会让读注释的人以为
        # 这道是白名单 —— 而真正的安全边界是下面的 realpath containment。
        # task_id 那侧的 ASCII-only 是另一处校验 (`runtime._validate_task_id`),
        # 那里才需要显式限定 (因为它把键直接交给用户当身份用)。
        if not all(c.isalnum() or c in "_-/." for c in key):
            raise ValueError(f"非法 storage key: {key!r}")
        # 第二道必须做: '.' 在允许集内, 故 '..' 段可穿过字符集过滤。字符集过滤
        # 挡不住路径穿越 —— 实测 get/delete 可越界读写 root 之外的文件 (put 只是
        # 被 os.makedirs 撞上字面量 '..' 目录而偶然失败, 不是防护)。这里把路径
        # 解析成 realpath 再断言仍落在 root 内, 对 get/put/delete 一视同仁。
        root = os.path.realpath(self.root)
        path = os.path.realpath(os.path.join(root, key + ".json"))
        if path != root and not path.startswith(root + os.sep):
            raise ValueError(f"storage key 越界 (路径穿越): {key!r}")
        return path

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
        prefix_path = self._path(prefix)[:-len(".json")]
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
