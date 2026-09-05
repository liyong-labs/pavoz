# stageflow.contrib.storage

`stageflow.contrib.storage` 提供 5 个 `StorageBackend` 可选 adapter, 通过 [pyproject extras](https://packaging.python.org/en/latest/specifications/dependency-specifiers/#extras) 按需安装 driver. core 永不引入第三方依赖 (stdlib only).

## Adapters

| Adapter | Driver | Extras | 典型场景 |
|---|---|---|---|
| `SqliteStorage` | stdlib `sqlite3` | `[sqlite]` (空) | 本地/单进程/测试; 无外部依赖 |
| `PostgresStorage` | psycopg3 | `[postgres]` | 服务化部署, JSONB + ON CONFLICT upsert |
| `MySQLStorage` | PyMySQL | `[mysql]` | 服务化部署, LONGTEXT + ON DUPLICATE KEY upsert |
| `RedisStorage` | redis-py | `[redis]` | 轻量 KV, prefix 隔离 + SCAN 列表 |
| `MinioStorage` | boto3 | `[minio]` | 对象存储 (AWS S3 / MinIO / Cloudflare R2) |

聚合: `[contrib]` = `[postgres] + [mysql] + [redis] + [minio]`; `[all]` = `[contrib]` 的别名.

## 用法

### Sqlite

```python
from stageflow.contrib.storage import SqliteStorage

store = SqliteStorage("./runs.db")
store.put("runs/task-1/checkpoint", {"stage": "search"})
assert store.get("runs/task-1/checkpoint") == {"stage": "search"}
store.list_keys("runs/")  # → ['runs/task-1/checkpoint']
store.delete("runs/task-1/checkpoint")
```

### Postgres / MySQL

```python
from stageflow.contrib.storage import PostgresStorage  # MySQLStorage 同形

store = PostgresStorage("postgresql://user:pass@localhost:5432/mydb", table="stageflow_kv")
store.put("cp:task-1", {"stage": "synth", "ok": True})
```

### Redis

```python
from stageflow.contrib.storage import RedisStorage

store = RedisStorage("redis://localhost:6379/0", prefix="myapp:")
store.put("cp:task-1", {"v": 1})
print(store.list_keys("cp:"))  # SCAN-based, 不阻塞 Redis
```

### MinIO / S3

```python
from stageflow.contrib.storage import MinioStorage

store = MinioStorage(
    bucket="my-bucket",
    endpoint_url="http://localhost:9000",  # MinIO; 留 None = AWS S3
    access_key="minioadmin", secret_key="minioadmin",
    prefix="stageflow:",
)
store.put("cp:task-1", {"v": 1})
```

## ai_writer 兼容

`MinioStorage.delete/get` **容忍 `NoSuchKey` / `404`** (静默返 None / 静默吞错), 与 ai_writer `backend/integration/sf_storage.py` 行为完全一致. ai_writer 可直接换用, 不需改业务代码.

`MinioStorage.put` 与 ai_writer `sf_storage.py` 同样使用 `json.dumps(ensure_ascii=False)` + UTF-8 bytes, 中文/emoji 透传无损.

## 测试策略

```bash
pip install stageflow[dev]  # 含所有测试依赖
pytest                       # 默认不含 integration marker
pytest -m integration        # 需 docker compose up postgres + mysql
```

- **MinioStorage**: `moto[s3]` mock, 无需 docker
- **RedisStorage**: `fakeredis` mock, 无需 docker
- **PostgresStorage + MySQLStorage**: 标 `@pytest.mark.integration`, 需本地 docker

CI 默认跑 `-m "not integration"` (72 tests), 集成测试本地手动触发.

## Key 安全 + Payload 编码

- `_validate_key`: 只允许 `[a-zA-Z0-9_\-/.]` + 拦截 `..` 路径遍历 (与 `FileStorage._path` 一致)
- `_encode_payload` / `_decode_payload`: UTF-8 JSON + `ensure_ascii=False` 保留中文; 解析失败返 `None` (与 core `FileStorage.get` 语义对齐). PostgresStorage 用 psycopg3 自带 `dict` ↔ JSONB, 不走该 helper.
