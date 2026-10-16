"""storage_loader 加载测试."""
# ruff: noqa: S102 — exec() 故意用于动态构造 adapter 模块做 round-trip 测试
import pytest

from pavoz import PavozStorageError, load_storage
from pavoz.storage import FileStorage


def test_loads_builtin_file_storage(tmp_path):
    """pavoz 自带的 FileStorage 直接可加载."""
    storage = load_storage("pavoz.storage.FileStorage", root_dir=str(tmp_path))
    assert isinstance(storage, FileStorage)


def test_loads_with_kwargs():
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        storage = load_storage("pavoz.storage.FileStorage", root_dir=tmp)
        storage.put("k", {"v": 1})
        assert storage.get("k") == {"v": 1}


def test_format_error_missing_colon():
    with pytest.raises(PavozStorageError, match="格式错误"):
        load_storage("nocolon")


def test_missing_module_error_mentions_module_path():
    with pytest.raises(PavozStorageError, match="不可"):
        load_storage("nonexistent.package.path:SomeClass")


def test_wrong_class_name_error_lists_module_exports():
    """AttributeError → 提示可用类."""
    with pytest.raises(PavozStorageError, match="不在模块"):
        load_storage("pavoz.storage:WrongClassName")


def test_wrong_kwargs_error():
    """TypeError → 提示 kwargs 不匹配."""
    with pytest.raises(PavozStorageError, match="kwargs"):
        load_storage("pavoz.storage.FileStorage", totally_unknown_kwarg=True)


def test_non_storage_backend_returns_error():
    """实例不是 StorageBackend 报错."""
    with pytest.raises(PavozStorageError, match="StorageBackend"):
        load_storage("builtins:int")  # int() is not a StorageBackend


def test_loads_custom_user_adapter(tmp_path):
    """用户自定义 adapter 模块可加载."""
    # Create a fake adapter module on the fly
    import sys
    import textwrap
    import types
    adapter_code = textwrap.dedent("""
        from pavoz.storage import StorageBackend
        class MyLocalAdapter(StorageBackend):
            def __init__(self, path):
                self.path = path
            def put(self, key, data): pass
            def get(self, key): return None
            def list_keys(self, prefix): return []
            def delete(self, key): pass
    """)
    mod = types.ModuleType("my_test_adapter")
    exec(adapter_code, mod.__dict__)
    sys.modules["my_test_adapter"] = mod
    try:
        storage = load_storage("my_test_adapter:MyLocalAdapter", path=str(tmp_path))
        assert isinstance(storage, mod.MyLocalAdapter)
    finally:
        del sys.modules["my_test_adapter"]

# ── adapter 实例化失败必须带上下文 (2026-10-16 巡检) ────────────────────────
#
# 真实 adapter 的 __init__ 基本都要连服务 (Redis / PG / MinIO / S3), 连不上时抛的
# 是驱动自己的异常。此前 load_storage 只把 TypeError 包成 PavozStorageError, 于是
# **最常见的那类失败原样逃出去**: 用户看到一句"无法连接 redis://…", 既不知道是哪个
# storage spec, 也不知道传了哪些 kwargs — 而 spec 来自环境变量 PAVOZ_STORAGE_SPEC,
# 一旦配错, 这两样正是排查的第一现场。

def _register_adapter(name: str, source: str):
    """就地造一个 adapter 模块并注册进 sys.modules (与本文件既有写法一致)."""
    import sys
    import textwrap
    import types

    mod = types.ModuleType(name)
    exec(textwrap.dedent(source), mod.__dict__)
    sys.modules[name] = mod
    return mod


def test_driver_error_during_init_is_wrapped_with_context():
    _register_adapter("_probe_refuses", """
        class RefusingStore:
            def __init__(self, host="localhost", port=6379):
                raise ConnectionRefusedError(f"cannot connect {host}:{port}")
    """)
    try:
        with pytest.raises(PavozStorageError) as ei:
            load_storage("_probe_refuses:RefusingStore", host="10.0.0.1", port=6380)
        msg = str(ei.value)
        assert "_probe_refuses:RefusingStore" in msg, f"报错应点名是哪个 spec: {msg}"
        assert "host" in msg and "port" in msg, f"报错应列出已传的 kwargs: {msg}"
        assert "ConnectionRefusedError" in msg, f"报错应保留原异常类型: {msg}"
        # 原始异常必须链住 —— 类型与 traceback 一个不少, 只补上下文
        assert isinstance(ei.value.__cause__, ConnectionRefusedError), (
            "原始异常要在 __cause__ 里, 不能被吞掉")
    finally:
        import sys
        del sys.modules["_probe_refuses"]


def test_config_error_during_init_is_wrapped_too():
    """adapter 从配置取值失败 (KeyError/ValueError) 同样要带上下文。"""
    _register_adapter("_probe_cfg", """
        class CfgStore:
            def __init__(self, dsn):
                self.dsn = dsn
                if ":" not in dsn:
                    raise ValueError(f"bad dsn: {dsn!r}")
    """)
    try:
        with pytest.raises(PavozStorageError) as ei:
            load_storage("_probe_cfg:CfgStore", dsn="not-a-dsn")
        assert "_probe_cfg:CfgStore" in str(ei.value)
        assert isinstance(ei.value.__cause__, ValueError)
    finally:
        import sys
        del sys.modules["_probe_cfg"]


def test_typeerror_path_keeps_its_kwargs_hint():
    """反向约束: TypeError 那条路原本就给 kwargs 提示, 补了 except Exception 后
    不能被抢走 (TypeError 分支必须排在前面)。"""
    with pytest.raises(PavozStorageError, match="kwargs"):
        load_storage("pavoz.storage.FileStorage", totally_unknown_kwarg=True)


def test_kwargs_mismatch_still_raises_typeerror_not_something_else():
    """FileStorage 不吃 **kwargs, 未知参数是 TypeError → 走 kwargs 提示分支。"""
    with pytest.raises(PavozStorageError) as ei:
        load_storage("pavoz.storage.FileStorage", bogus=1)
    assert "kwargs" in str(ei.value)
    assert isinstance(ei.value.__cause__, TypeError)
