"""Storage loader: 按 'pkg.module:Class' 字符串加载 StorageBackend 实现.

core 不带任何 driver — 用户自己 `pip install` 自己要的依赖 (psycopg / redis /
boto3 / etc.) + 自己写 adapter 或抄 ai_writer 的 `backend.integration.sf_storage`.
本模块只做 importlib + 友好错误, stdlib only.
"""

from __future__ import annotations

import importlib
from typing import Any

from .storage import StorageBackend


class StageflowStorageError(ImportError):
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
        StageflowStorageError: 拼错 module/class 或缺依赖时. 消息含 pip 安装提示.

    Examples:
        >>> load_storage("stageflow.storage:FileStorage", root_dir="/data/kv")
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
        raise StageflowStorageError(
            f"storage spec {spec!r} 格式错误. 期望 'pkg.module:ClassName' 或 'pkg.module.ClassName'"
        )
    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        top_pkg = module_path.split(".")[0]
        raise StageflowStorageError(
            f"storage {spec!r} 模块 {module_path!r} 不可用: {e}. "
            f"检查 (1) 模块名拼写; (2) 对应 pip 包是否已安装 (e.g. {top_pkg})."
        ) from e
    try:
        cls = getattr(module, class_name)
    except AttributeError as e:
        available = [n for n in dir(module) if not n.startswith("_")]
        raise StageflowStorageError(
            f"storage {spec!r} 类 {class_name!r} 不在模块 {module_path!r}. "
            f"模块导出: {available[:10]}{'...' if len(available) > 10 else ''}"
        ) from e
    try:
        instance = cls(**kwargs)
    except TypeError as e:
        raise StageflowStorageError(
            f"storage {spec!r} 实例化失败: {e}. "
            f"检查 kwargs={list(kwargs.keys())} 是否匹配 {class_name}.__init__."
        ) from e
    if not isinstance(instance, StorageBackend):
        raise StageflowStorageError(
            f"storage {spec!r} 实例不是 StorageBackend (类型 {type(instance).__name__})"
        )
    return instance
