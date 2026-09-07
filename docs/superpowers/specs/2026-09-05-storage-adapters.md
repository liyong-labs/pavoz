# pavoz Storage Adapters v0.4 Spec

> v0.4 计划 (2026-09-05 brainstorm 定稿): core stdlib-only 不变; 加 contrib 子包 + 4 个 StorageBackend adapter 作为可选 extras.

## 背景

pavoz core (12 模块) 仍是 stdlib-only + 零依赖. v0.3 之前 ai_writer 自己写 MinIOStorage adapter, 业务侧重复造轮子.
v0.4 提供 contrib 子包 + 4 个 adapter (Postgres / Redis / MinIO / SQLite) 作为**可选 extras**:
- core 永远干净, 不腐坏
- 业务侧不写重复 adapter
- LLM vendor / ai_writer 等"会变化的组件"绝不进入 contrib (deps 政策: 稳定基础设施 driver OK, 业务/LLM vendor 禁)

## 设计

### 包结构

```
pavoz/
  __init__.py
  ...                  # core 12 模块 (零变化)
  contrib/
    __init__.py         # 总入口 + 可用 adapter 检测
    storage/
      __init__.py       # 各 adapter 导出
      _base.py          # 公共 helpers (e.g. key sanitize)
      postgres.py       # PostgresStorage (psycopg[binary] v3)
      redis.py          # RedisStorage (redis-py)
      minio.py          # MinioStorage (boto3)
      sqlite.py         # SqliteStorage (stdlib sqlite3)
```

### pyproject.toml extras

```toml
[project.optional-dependencies]
dev = ["pytest>=8.0", "pytest-asyncio>=0.23", "moto[s3]>=5.0", "fakeredis>=2.20"]
postgres = ["psycopg[binary]>=3.1"]
mysql = ["pymysql>=1.1"]
redis = ["redis>=5.0"]
minio = ["boto3>=1.34"]
sqlite = []  # stdlib, 总是可用, 但保留入口 (depend on core only)
contrib = ["psycopg[binary]>=3.1", "pymysql>=1.1", "redis>=5.0", "boto3>=1.34"]
all = ["psycopg[binary]>=3.1", "pymysql>=1.1", "redis>=5.0", "boto3>=1.34"]
```

注: `sqlite` extras 故意为空 (stdlib), 保留供 deps 显式声明. 实际不需要 pip install 任何东西.

`mysql` driver 选择 `PyMySQL` (纯 Python, 安装简单, MySQL 5.7+/8.0+ 全兼容; 不用 `mysqlclient` 因其 C 绑定编译依赖重).

### StorageBackend API (无变化)

```python
class StorageBackend:
    def put(self, key: str, data: dict) -> None: ...
    def get(self, key: str) -> dict | None: ...
    def list_keys(self, prefix: str) -> list[str]: ...
    def delete(self, key: str) -> None: ...
```

每个 adapter 实现 4 方法. **不** 新增任何字段 / 方法.

### 依赖懒导入

每个 adapter 模块顶部:
```python
try:
    import psycopg
except ImportError as e:
    raise ImportError(
        "PostgresStorage 需要 psycopg. 安装: pip install 'pavoz[postgres]'"
    ) from e
```

`pavoz.contrib` 顶层 import 不强依赖任一 driver. 用户用哪个就装哪个.

### Key sanitize

4 个 adapter 共享的 key 字符限制 (Postgres/Redis 不允许任意字符):
- 用现有 `FileStorage._path` 同样的 `[a-zA-Z0-9_-/]+` 白名单 + raise ValueError
- 移到 `pavoz/contrib/storage/_base.py`

### 各 adapter 细节

| Adapter | Driver | Key strategy | put 语义 |
|---------|--------|--------------|----------|
| PostgresStorage | psycopg3 | `data:text/jsonb;base64,hex-encoded` (binary safety) | ON CONFLICT DO UPDATE (upsert) |
| MySQLStorage | PyMySQL | `data:LONGTEXT;base64,hex-encoded` (binary safety) | INSERT ... ON DUPLICATE KEY UPDATE (upsert) |
| RedisStorage | redis-py | 直接用 string key | SET (覆盖) |
| MinioStorage | boto3 (S3 API) | 直接用 string key | put_object (覆盖) |
| SqliteStorage | stdlib sqlite3 | `data:text/json;base64,hex` (binary safety) | INSERT OR REPLACE |

详细 SQL/key schema 见 plan 中各 task.

### API 冻结面

zero change: core 12 模块签名/语义不动. `StorageBackend` Protocol 4 方法签名不动.
新增的是 contrib 子包 + extras. 旧 FileStorage 不动, 与 contrib 平行存在.

## 不做 (YAGNI)

- connection pool (现用 ad-hoc connection, 业务需要再加)
- async 版本 (psycopg3 sync 已够, async 单独 extras v0.5)
- 其他 backend (DynamoDB / MongoDB / etcd / Consul — 社区 PR 可加)
- LLM vendor / 业务 SDK adapter (deps 政策禁止)

## 验收

- `pip install pavoz` — core 可装, 不带任何 driver
- `pip install pavoz[postgres]` — 可 import `pavoz.contrib.storage.PostgresStorage`
- `pip install pavoz[mysql]` — 可 import `pavoz.contrib.storage.MySQLStorage`
- `pip install pavoz[all]` — 5 个 adapter 全部可 import
- `pip install pavoz[redis]` + 不装 minio — `pavoz.contrib.storage.MinioStorage` 抛 ImportError (with install hint)
- tests: 5 个 adapter 各有 put/get/list/delete + key sanitize + round-trip 集成测
- moto[s3] 用于 MinioStorage 测试 (本地 s3 mock)
- fakeredis 用于 RedisStorage 测试 (本地 redis mock)
- SqliteStorage 用 tmp_path (无 dep)
- PostgresStorage + MySQLStorage 用 testcontainers 或 docker fixture (无 docker 时 skip, `@ `pytest.mark.integration`)
- pytest 默认 markers: `integration` 标记需要 docker 的 test, `unit` 默认跑
- ruff clean, 38 core tests 全过 + ≥35 contrib tests

### 互操作验收 (ai_writer 协同底线, 2026-09-05)

pavoz 与 ai_writer 相互不依赖 + 必须能协同工作. v0.4 验证:

- **MinioStorage 行为等价**: 必须复刻 ai_writer 现有 `backend/integration/sf_storage.py` 行为:
  - put / get (返回 None 当不存在) / list_keys / delete
  - delete 抛 `botocore.exceptions.ClientError` NoSuchKey 时静默 (ai_writer 现有容忍)
  - 前缀 `research/task_cp/pavoz/{task_id}/checkpoint` (与 ai_writer 现有路径一致)
  - integration test: 替换 ai_writer 的 MinioStorage 在真 task 上跑 (如华创 6000 字 pavoz 全链测试), checkpoint 序列化/反序列化完全兼容
- **PostgresStorage / MySQLStorage**: ai_writer 不使用, 但提供标准 SQL + JSON 序列化以供未来多环境部署
- **SqliteStorage**: ai_writer 不使用, 但提供本地测试 fallback
- **RedisStorage**: ai_writer 不使用, 但提供轻量替代选项 (比 PG/MySQL 简单)

如 MinioStorage 不能完全复刻 ai_writer 既有行为, v0.4 不 ship.