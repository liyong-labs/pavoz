# stageflow Storage Backend

🇨🇳 简体中文版 — 待补 (当前仅英文版)

stageflow 核心 **不带任何 storage driver** (`pip install stageflow` = 仅 stdlib)。
driver (psycopg / redis / boto3 / ...) 由用户自己 `pip install`, adapter 由用户
自己写 (或 vendor 第三方), stageflow 只提供:

1. **`StorageBackend` Protocol** — adapter 必须实现的 4 个方法 (`put / get /
   list_keys / delete`), 见 [`stageflow.storage`](../../stageflow/storage.py)。
2. **`FileStorage`** — 内置 stdlib 实现 (本地文件系统), 默认/测试用。
3. **`load_storage(spec, **kwargs)`** — 按 config 字符串加载任意 `StorageBackend`
   实现, 见下文。

## 为什么 core 不带 driver

vendor adapter 是经典 over-engineering: 用户可能用 Postgres 也可能用 Snowflake,
可能用 S3 也可能用 Cloudflare R2, deps 版本/版本组合各异。core 不该替客户
做这个决定 — 业界主流 (SQLAlchemy / pytest / MLflow / Kedro / langchain_community /
pluggy) 全部走"用户写字符串 → 运行时 importlib 加载" pattern。

## `load_storage(spec, **kwargs) -> StorageBackend`

按 config 字符串加载并实例化 `StorageBackend` 实现。

### Spec 格式

两种等价形式:

| 形式 | 示例 | 说明 |
|---|---|---|
| Canonical (colon) | `"pkg.module:ClassName"` | 显式 module/class 分隔 |
| Dotted | `"pkg.module.path.ClassName"` | 最后一段是 class 名, 其余是 module |

### 例子

```python
from stageflow import load_storage, CheckpointStore

# 1. 内置 FileStorage (stdlib, 无 deps)
storage = load_storage("stageflow.storage.FileStorage", root_dir="/data/cp")

# 2. 用户自定义 adapter (任意路径)
storage = load_storage(
    "backend.integration.sf_storage.MinioStorage",
    task_id="run-2026-09-05-001",
)

# 3. colon 形式
storage = load_storage("myapp.adapters:PostgresStore", dsn="postgresql://...")

cp_store = CheckpointStore(storage)
```

### 错误信息 (5 类, 均抛 `StageflowStorageError`)

```
# 1. spec 格式错 (没有 : 也没有 .)
StageflowStorageError: storage spec 'nocolon' 格式错误. 期望 'pkg.module:ClassName' 或 'pkg.module.ClassName'

# 2. module 未装
StageflowStorageError: storage 'foo.bar:Baz' 模块 'foo.bar' 不可用: No module named 'foo'.
  检查 (1) 模块名拼写; (2) 对应 pip 包是否已安装 (e.g. foo).

# 3. class 名错
StageflowStorageError: storage 'stageflow.storage:WrongClass' 类 'WrongClass' 不在模块 'stageflow.storage'.
  模块导出: ['FileStorage', 'StorageBackend', 'key_sha256']

# 4. kwargs 错
StageflowStorageError: storage 'stageflow.storage.FileStorage' 实例化失败:
  FileStorage.__init__() missing 1 required positional argument: 'root_dir'.
  检查 kwargs=['whatever'] 是否匹配 FileStorage.__init__.

# 5. 类型错 (实例不是 StorageBackend)
StageflowStorageError: storage 'builtins:int' 实例不是 StorageBackend (类型 int)
```

`StageflowStorageError` 是 `ImportError` 的子类 — 现有 `except ImportError`
的代码路径不会吞, 但语义更精确。

## ai_writer 互操作 (2026-09-05 底线)

ai_writer 项目已有 [`backend/integration/sf_storage.py`](../../../claude/backend/integration/sf_storage.py),
实现了 `MinioStorage(StorageBackend)`, 直接可加载:

```python
from stageflow import load_storage, CheckpointStore

storage = load_storage(
    "backend.integration.sf_storage.MinioStorage",
    task_id="abc-123",
)
# storage.put/get/list_keys/delete 直接用, 行为对齐 sf_storage 原 API
```

ai_writer 的 `MinioStorage` 内部用 `research_trace._put_json/_get_json/_list_dir`
(已带 bucket 配置 + 错误容错), `delete` 容忍 `NoSuchKey` — stageflow `CheckpointStore`
可直接用它, 不需改业务代码。

## 用户写 adapter (5-10 行)

```python
# myapp/adapters/postgres_store.py
from stageflow.storage import StorageBackend

class PostgresStore(StorageBackend):
    def __init__(self, dsn: str, table: str = "stageflow_kv"):
        import psycopg
        self._conn = psycopg.connect(dsn, autocommit=True)
        self._table = table

    def put(self, key: str, data: dict) -> None:
        import json
        with self._conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {self._table} (k, v) VALUES (%s, %s) "
                "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
                (key, json.dumps(data, ensure_ascii=False)),
            )

    def get(self, key: str) -> dict | None:
        import json
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT v FROM {self._table} WHERE k = %s", (key,))
            row = cur.fetchone()
            return json.loads(row[0]) if row else None

    def list_keys(self, prefix: str) -> list[str]:
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT k FROM {self._table} WHERE k LIKE %s", (prefix + "%",))
            return sorted(r[0] for r in cur.fetchall())

    def delete(self, key: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(f"DELETE FROM {self._table} WHERE k = %s", (key,))
```

然后:

```bash
pip install psycopg[binary]   # 用户自己装
```

```python
# 用户 config (TOML/YAML/INI, 任意格式)
# storage = "myapp.adapters:PostgresStore"
# storage_kwargs = {dsn: "postgresql://...", table: "stageflow_kv"}

storage = load_storage("myapp.adapters:PostgresStore",
                       dsn="postgresql://...", table="stageflow_kv")
```

## 与之前 v0.4 contrib 的关系

v0.4.0 (commit `c7f760b`) 在 stageflow 内 vendor 5 个 adapter (Sqlite/Postgres/
MySQL/Redis/MinIO), 走 extras 机制 + contrib 子包。**v0.4.1 (2026-09-05) 反转** —
这是 over-engineering, 我们不该替客户决定需要哪些 DB/S3 client。
`stageflow.contrib.storage` 子包及 5 个 adapter 已删除, 改走 `load_storage`。
git history 保留两次方向作为 stageflow 设计演化的真实记录。

如果社区想用 contrib 模式, 走**独立包** (`stageflow-storage-postgres` 等),
自己维护, 不进 stageflow core。
