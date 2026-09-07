# pavoz Storage Loader v0.4 Spec (反转: 取消 contrib, 改 config driver)

> v0.4 重新设计 (2026-09-05 user 拍板): 取消 `pavoz.contrib.storage` (已 ship 在
> commit `c7f760b`, 此 spec 描述反转方向). 改: core 加 `storage_loader.load_storage()` —
> 用户 config 写 `"pkg.module:Class"`, pavoz 运行时 importlib 加载.

## 背景 (反转原因)

v0.4 第一版 (`c7f760b`) 在 pavoz 内 vendor 5 个 adapter (Sqlite/Postgres/MySQL/Redis/Minio), 走
extras 机制 + contrib 子包. user 拍板**这是 over-engineering** — 我们不该替客户决定需要哪些
DB/S3 client. 客户知道自己的部署环境, 应该自己 pip install 自己要的 deps + 自己写 adapter (或抄 ai_writer
已有的 `sf_storage.py`).

naobao 调研 (2026-09-05) 4 角度结论:
- 业界主流 Python 库 (SQLAlchemy / pytest / MLflow / Kedro / langchain / pluggy / triton)
  都用"用户写字符串 → runtime importlib 加载" pattern
- vendor adapter 是经典 bug 源 (setuptools #3113 shadow 包名 / pytrim 报告 deps bloat)
- pluggy 为 1400+ plugin 设计, pavoz 一个 storage slot 用它 = 牛刀杀鸡
- 推荐组合: config string + importlib + 友好 ImportError + 可选 entry_points 注册

## 设计

### 核心 API: `pavoz.storage_loader.load_storage()`

```python
# pavoz/storage_loader.py — stdlib only
from __future__ import annotations

import importlib
from typing import Any

class PavozStorageError(ImportError):
    """统一存储加载错误. 消息含 module path + 安装提示."""

def load_storage(spec: str, **kwargs: Any) -> "StorageBackend":
    """按 'pkg.module:Class' 字符串加载并实例化 StorageBackend.

    Args:
        spec: 格式 "pkg.module.path:ClassName". 不支持 URL (storage 不是网络协议).
        kwargs: 传给 ClassName.__init__ 的额外参数 (如 endpoint_url, bucket).

    Raises:
        PavozStorageError: 拼错 module/class 或缺依赖时. 消息含 pip 安装提示.

    Examples:
        >>> load_storage("backend.integration.sf_storage.MinioStorage",
        ...              endpoint_url="http://localhost:9000", bucket="kv")
        >>> load_storage("myapp.adapters.PostgresStore", dsn="postgresql://...")
        >>> load_storage("pavoz.storage.FileStorage", root_dir="/data/kv")
    """
    if ":" not in spec:
        raise PavozStorageError(
            f"storage spec {spec!r} 格式错误. 期望 'pkg.module:ClassName'"
        )
    module_path, _, class_name = spec.rpartition(":")
    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise PavozStorageError(
            f"storage {spec!r} 模块 {module_path!r} 不可用: {e}. "
            f"检查 (1) 模块名拼写; (2) 对应 pip 包是否已安装 (e.g. {module_path.split('.')[0]})."
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
    if not isinstance(instance, StorageBackend):
        raise PavozStorageError(
            f"storage {spec!r} 实例不是 StorageBackend (类型 {type(instance).__name__})"
        )
    return instance
```

### __init__.py export

```python
from .storage_loader import PavozStorageError, load_storage

__all__ += ["load_storage", "PavozStorageError"]
```

### 使用方式

```python
# 用户 config (TOML/YAML/INI, 任意)
# storage = "backend.integration.sf_storage.MinioStorage"
# storage_kwargs = {endpoint_url: "...", bucket: "..."}

from pavoz import load_storage, CheckpointStore

storage = load_storage(
    "backend.integration.sf_storage.MinioStorage",
    endpoint_url="http://localhost:9000",
    bucket="pavoz-kv",
)
cp_store = CheckpointStore(storage)
```

### 友好错误模式

| 错类型 | 消息内容 |
|--------|---------|
| spec 格式错 | `storage spec 'xxx' 格式错误. 期望 'pkg.module:ClassName'` |
| module 未装 | `storage 'xxx' 模块 'yyy' 不可用: {ImportError}. 检查 (1) 模块名拼写; (2) 对应 pip 包是否已安装 (e.g. {top_pkg}).` |
| class 名错 | `storage 'xxx' 类 'Class' 不在模块 'yyy'. 模块导出: [...]` |
| kwargs 错 | `storage 'xxx' 实例化失败: {TypeError}. 检查 kwargs=[...] 是否匹配 Class.__init__.` |
| 类型错 | `storage 'xxx' 实例不是 StorageBackend (类型 Y)` |

### 反向方向 (v0.4 contrib)

`pavoz/contrib/storage/` 子包及 5 个 adapter + 7 个 tests + pyproject extras **全部删除**. 后续如果社区想用 contrib 模式, 走**独立包** (`pavoz-storage-postgres` 等), 自己维护, 不进 pavoz core.

## 不做 (YAGNI)

- entry_points 强制注册 — config 字符串足够, 强制注册增加维护成本
- pluggy hookspec/hookimpl — 一个 storage slot 用它是牛刀杀鸡
- vendor 任何 adapter — 客户自己管 deps
- URL scheme dispatch (`postgresql://` 自动 pick driver) — pavoz 不是 db connection, 用普通 dotted path 即可
- 自动发现 / 自动转换 — 增加复杂度无收益

## 验收

- `pip install pavoz` — 仅 stdlib + core, 不带任何 driver
- 用户自行 `pip install pymysql/boto3/redis/psycopg2-binary/etc.` (按需)
- 用户 config 写 `storage = "..."`, 调 `load_storage()` 即用
- 错误信息可操作 (含 module path + pip 提示)
- pytest tests pass (38 core + 新增 storage_loader tests)
- ruff 0
- ai_writer 集成: `backend.integration.sf_storage.MinioStorage` 直接可加载
- 删 `pavoz/contrib/` 后, 38 core tests + 新增 storage_loader tests 全过

## 互操作验收 (ai_writer, 2026-09-05 底线)

- `load_storage("backend.integration.sf_storage.MinioStorage", endpoint_url="http://...", bucket="kv")` 必须成功
- 生成的 MinioStorage 实例可直接传入 `CheckpointStore(storage)`
- ai_writer task 跑完 checkpoint 落 Minio → 同 load_storage 读回 → state 完全一致

## 与之前 v0.4 第一版的关系

- commit `9c39724` (contrib skeleton) → `c52380a` (5 adapters) → `5a60a75` (pyproject+docs+tag v0.4.0) → `42ddd83` (manifest version sync) → `c7f760b` (final-review fixes)
- **tag v0.4.0 还在 `c7f760b`** — 这意味着 `pip install pavoz==v0.4.0` 拉到的代码是"已废除的 contrib 设计"
- 反转需要**新 commit 链**:
  - 删 `pavoz/contrib/` 子包 (含 `_base.py`, `__init__.py`, `sqlite.py`, `postgres.py`, `mysql.py`, `redis.py`, `minio.py`)
  - 删 `tests/contrib/test_*.py` 和 `tests/contrib/__init__.py`
  - 删 `tests/test_base.py` (若已合并 — 实际在 `tests/contrib/test_base.py`)
  - 恢复 `pyproject.toml` (移除 7 个 extras; 移除 `[tool.setuptools]` 加的 `pavoz.contrib`; 移除 markers 注册)
  - 恢复 `pavoz/contrib/__init__.py` 删除 (或保留空目录?)
  - 移除 v0.4.0 tag (含删除 tag + 新 commit), 因为 v0.4.0 = contrib 设计, 已经废除
  - 用 v0.4.1 tag 标记 storage_loader 实现
  - 删 README/CHANGELOG/ROADMAP 中的 contrib 段
  - 加 storage_loader 文档 (docs/storage.md + docs/config.md)
  - 更新 spec/plan artifacts (写此 spec 为权威, 把之前的 contrib spec 标记为 "superseded")
- 注意: 这是**反转**, 不是 "新增". git history 保留两次方向, 作为 pavoz 设计演化的真实记录.