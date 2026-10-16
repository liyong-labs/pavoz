"""Storage loader: 按 'pkg.module:Class' 字符串加载 StorageBackend 实现.

core 不带任何 driver — 用户自己 `pip install` 自己要的依赖 (psycopg / redis /
boto3 / etc.).
本模块只做 importlib + 友好错误, stdlib only.
"""

from __future__ import annotations

import importlib
from typing import Any

from .storage import StorageBackend


class PavozStorageError(ImportError):
    """统一存储加载错误. 消息含 module path + 安装提示."""


def load_storage(spec: str, **kwargs: Any) -> StorageBackend:
    """按 'pkg.module:Class' 字符串加载并实例化 StorageBackend.

    Args:
        spec: storage 类定位字符串. 两种等价形式:
              - `"pkg.module.path:ClassName"` (canonical, 显式 separator)
              - `"pkg.module.path.ClassName"` (dotted, 最后段是 class 名)
              不支持 URL (storage 不是网络协议).
        kwargs: 传给 ClassName.__init__ 的额外参数 (如 endpoint_url, bucket).

    Raises:
        PavozStorageError: 拼错 module/class 或缺依赖时. 消息含 pip 安装提示.

    Examples:
        >>> load_storage("pavoz.storage:FileStorage", root_dir="/data/kv")
        >>> load_storage("backend.integration.sf_storage.MinioStorage",
        ...              endpoint_url="http://localhost:9000", bucket="kv")
        >>> load_storage("myapp.adapters:PostgresStore", dsn="postgresql://...")
    """
    if ":" in spec:
        module_path, _, class_name = spec.rpartition(":")
    elif "." in spec:
        # dotted form: 最后一段是 class, 其余是 module
        module_path, _, class_name = spec.rpartition(".")
    else:
        raise PavozStorageError(
            f"storage spec {spec!r} 格式错误. 期望 'pkg.module:ClassName' 或 'pkg.module.ClassName'"
        )
    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        top_pkg = module_path.split(".")[0]
        raise PavozStorageError(
            f"storage {spec!r} 模块 {module_path!r} 不可用: {e}. "
            f"检查 (1) 模块名拼写; (2) 对应 pip 包是否已安装 (e.g. {top_pkg})."
        ) from e
    try:
        cls = getattr(module, class_name)
    except AttributeError as e:
        available = [n for n in dir(module) if not n.startswith("_")]
        raise PavozStorageError(
            f"storage {spec!r} 类 {class_name!r} 不在模块 {module_path!r}. "
            f"模块导出: {available[:10]}{'...' if len(available) > 10 else ''}"
        ) from e
    try:
        instance = cls(**kwargs)
    except TypeError as e:
        raise PavozStorageError(
            f"storage {spec!r} 实例化失败: {e}. "
            f"检查 kwargs={list(kwargs.keys())} 是否匹配 {class_name}.__init__."
        ) from e
    except Exception as e:
        # 真实 adapter 的 __init__ 基本都要连服务 (Redis / PG / MinIO / S3), 连不上
        # 时抛的是**驱动自己的**异常。此前只有 TypeError 被包住, 于是最常见的那类
        # 失败原样逃出去: 用户看到一句"无法连接 redis://…", 既不知道是哪个 storage
        # spec, 也不知道传了哪些 kwargs (实测 2026-10-16)。原始异常用 `from e`
        # 链住 —— 类型与 traceback 一个不少, 只是补上了上下文。
        raise PavozStorageError(
            f"storage {spec!r} 实例化失败 ({type(e).__name__}): {e}. "
            f"已传 kwargs={list(kwargs.keys())}; 检查该 adapter 的连接配置 / 凭据 / 依赖."
        ) from e
    if not isinstance(instance, StorageBackend):
        raise PavozStorageError(
            f"storage {spec!r} 实例不是 StorageBackend (类型 {type(instance).__name__})"
        )
    return instance
