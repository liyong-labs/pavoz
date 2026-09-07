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
