"""Storage backend Protocol + file 实现.

core 零依赖 (不 import psycopg/redis/minio). v1 默认 file backend (本地测试/单机).
接入方实现自己的 backend (DB/MinIO) 注入.

接口只 3 个方法 — checkpoint 持久化 + trace 数据够用.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from abc import ABC, abstractmethod

__all__ = ["CorruptRecordError", "FileStorage", "StorageBackend", "key_sha256"]


class CorruptRecordError(Exception):
    """磁盘上的记录不是合法 JSON —— 文件被截断/写坏, 读不出内容。

    **为什么不返回 None**: `get` 返回 None 的含义是"这条记录不存在"。若把损坏
    也当成 None, `resume` 会认为"没有 checkpoint, 从头开始" —— 于是**静默丢掉**
    这个 task 已有的全部进度, 而用户以为只是重跑了一遍。损坏必须**响**。

    消息自带路径与处置建议, 因为最常见的现场就是"崩溃后回来查" —— 那正是用户
    会跑 `pavos trace` / `resume` 的时刻 (实测: 未处理时这里打的是指向
    `json/decoder.py` 的裸 traceback, 对用户零信息量)。
    """


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
        """原子写一个 JSON 对象: 先写同目录临时文件, 再 `os.replace` 换名.

        为什么必须是原子的 (2026-10-17 实测): `open(path, "w")` 是**先截断再写**,
        而 checkpoint 正是 run 崩溃后**续跑的唯一凭据**。写到一半进程被杀时
        (SIGKILL, 实测 3/3), 磁盘上留下的既不是旧的也不是新的:

            写到一半被杀 → 12-17 MB 的半截 JSON (完整应有 60 MB), `get` 直接
            JSONDecodeError;

        更糟的是**连崩溃前那份本来合法的 checkpoint 也一起没了** —— 实测先写好一份
        825 字节的完整 checkpoint, 再跑一个大 run 写到一半 SIGKILL, 结果那个文件
        变成 17 MB 半截: 既不是旧的也不是新的。于是"崩溃后能 resume"这件事,
        被崩溃**本身**给毁了 —— 而这恰恰是这个库最核心的承诺。

        `os.replace` 是同文件系统内的原子改名 (POSIX rename / Windows
        MoveFileEx+REPLACE_EXISTING), 所以读者只可能看到旧值或新值, 永远不会看到
        半截; `get` 要么读到上一份, 要么读到这一份, 不会是坏的。

        临时文件**不带 `.json` 后缀** —— `list_keys` 是靠 `*.json` 收集 key 的, 带
        后缀的话崩溃残留的临时文件会变成一个谁都读不到的假 key。崩溃确实可能留下
        一个 `.pvos-tmp-*` 文件, 但它对 `get`/`list_keys` 都不可见, 也不会被误当成
        checkpoint。

        边界 (明说, 不假装覆盖): 这里保证的是**进程崩溃**下的完整性。机器掉电 /
        内核 panic 那档还需要 `fsync` 临时文件 + `fsync` 父目录 (且父目录 fsync 在
        Windows 上没有可移植做法), 本仓没有承诺那一档, 故不做 —— 不拿一个看不见的
        开销换一个证明不了的收益。
        """
        path = self._path(key)
        parent = os.path.dirname(path) or "."
        os.makedirs(parent, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=parent, prefix=".pvos-tmp-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            os.replace(tmp, path)
        except BaseException:
            # 失败就把临时文件收掉。**必须收**: 否则每次写失败都在数据目录里留一个
            # 垃圾文件, 而调用方 (比如 run 收尾) 往往还会继续失败很多次。
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def get(self, key: str) -> dict | None:
        path = self._path(key)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            # ⚠️ 这一层以前是裸 json.load: 损坏直接冒成 JSONDecodeError, 而 CLI 的
            # main() 没有顶层兜底 —— 用户看到的是一整段指向 stdlib 的 traceback
            # (实测: `pavos trace` 在崩溃留下的半截 checkpoint 上就这个下场),
            # 既没说哪个文件坏了, 也没说能怎么办。换成带路径与处置建议的错误。
            raise CorruptRecordError(
                f"记录损坏, 不是合法 JSON: {path}\n"
                f"  原始错误: {type(e).__name__}: {e}\n"
                f"  常见原因: 崩溃/断电时正写到一半 (半截文件)。该 run **无法恢复**。\n"
                f"  处置: 删掉这个文件再重跑 —— 或用 `prune --task-id <id> "
                f"--dry-run` 先看会删什么。注意: 本错误**不会**被当成'没有 "
                f"checkpoint', 否则 resume 会静默从头开始、丢掉已有进度。"
            ) from e

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
