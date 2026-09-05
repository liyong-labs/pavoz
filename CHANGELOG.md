# Changelog

本项目遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [0.1.1] — 2026-09-05

### Added

- **链式覆盖 (chain overwrite)**: 下游 stage 可以覆盖**传递上游 producer** 写的
  state key。数据流水线 (如 `search → filter → compress` 逐级演进同一产物) 是
  通用模式, 不再被 one-producer-per-key 误判为平行冲突。
  - `DAG.reachable(upstream, downstream)`: 传递依赖判定
  - Runtime 追踪 `state key → producer stage`, checkpoint 持久化 `producers`
  - 平行 producer (无依赖链) 冲突仍抛 `StateConflictError` (v1 语义保留)
  - 旧 checkpoint (无 producers 字段) resume → 宽松模式 (视同单链), 向后兼容

### Changed

- `merge_state(prev, delta, stage_name, overwrite_keys=None)`: 新增可选参数
- `Checkpoint`: 新增 `producers` 字段 (序列化向后兼容, 缺省 `{}`)

## [0.1.0] — 2026-09-05

### Added

首个可用版本 (micro/in-process workflow engine):

- **DAG DSL**: `@dag.stage(depends_on, retries, timeout)` 静态声明, Kahn 拓扑
  排序, Tarjan SCC 环检测 (含自环), validate 后冻结
- **Runtime**: 顺序执行 + per-node 重试 (指数退避) + absolute deadline 传播 +
  state merge + 每 node checkpoint
- **State 契约**: json-serializable 类型白名单 (str/int/float/bool/None/
  list/dict), `ctx.state` 为只读深拷贝 (写即 raise), `return dict` 是唯一写
  路径, 平行 producer 冲突 → `StateConflictError`
- **异常契约**: `StageError` (业务失败, 不重试) / `RetryableError` (退避重试,
  耗尽 fail) / `FatalError` (程序 bug, 立即 fail)
- **Checkpoint**: 每 node 落盘, `workflow_hash` (stage 结构指纹) mismatch →
  拒 resume (只改函数体不影响 hash), run 完成自动清理
- **ctx.call(kind, op, params)**: 对外调用统一入口 (caller 由业务注入),
  默认 no-op echo (demo/TestPipe 可用)
- **TestPipe**: 第一天可用的回归 fixture — mock stage 输出跑全图
- **StorageBackend Protocol + FileStorage**: core 零依赖, 业务可注入
  DB/MinIO adapter
- **TaskTrigger Protocol**: 业务 task 表边界抽象
- **CLI**: `run / trace / state / inspect` (v0.1 面)
- 38 tests, ruff clean, 零运行时依赖 (stdlib only)

## [0.1.2] — 2026-09-05

### Removed (ponytail cleanup)

- `trigger.py` (`TaskTrigger` / `TaskRef` — zero callers)
- `Ctx.log` / `Ctx.remaining_seconds` (zero callers)
- `Stage.metadata` field + `@dag.stage` `**metadata` (zero callers)
- `DAG.entrypoints` / `DAG.depth_of` (zero callers)
- CLI `inspect` subcommand (空壳 — "v0.1 无 call 级 trace" 提示)

### Internal

- API 冻结面 zero change: `@dag.stage` / `Runtime.run` / `Ctx` / `State` / 异常 / Checkpoint 全部不动
- 38 tests 全绿, ruff 0 violation

## [0.4.0] — 2026-09-05

### Added (stageflow.contrib.storage)

- **PostgresStorage** (psycopg3 + JSONB, upsert via ON CONFLICT)
- **MySQLStorage** (PyMySQL + LONGTEXT, upsert via ON DUPLICATE KEY)
- **RedisStorage** (redis-py, prefix-scoped, SCAN-based list_keys)
- **MinioStorage** (boto3, **delete/get 容忍 NoSuchKey/404** 对齐 ai_writer `sf_storage.py`)
- **SqliteStorage** (stdlib sqlite3, 单文件 DB)
- 公共 helpers: `_validate_key` (路径安全) + `_encode_payload` / `_decode_payload` (UTF-8 JSON)

### Internal

- core 12 模块 + `StorageBackend` Protocol 零变化 (API 冻结); 0 行 core 功能修改 (fix1 仅同步 manifest 版本号)
- contrib 是独立 subpackage (`stageflow.contrib.storage`); core 不引入任何 driver
- pyproject extras 分组: `[postgres]` / `[mysql]` / `[redis]` / `[minio]` / `[sqlite]` (空) / `[contrib]` (4 个) / `[all]`
- 缺 driver 时 adapter 模块顶 try/except ImportError + 安装提示

### Tests

- 38 core tests + 44 contrib tests (10 标 `@pytest.mark.integration` 需 docker)
- MinioStorage 用 moto[s3] mock; RedisStorage 用 fakeredis mock (无 docker 依赖)

## [0.4.1] — 2026-09-05

### Removed (reversal of v0.4 contrib approach)

- `stageflow.contrib.storage` 5 个 vendor adapter (SqliteStorage / PostgresStorage / MySQLStorage / RedisStorage / MinioStorage) — over-engineering, user 自管 deps + 自写 adapter 更合适
- `stageflow.contrib` subpackage 完全移除
- pyproject.toml extras (`postgres` / `mysql` / `redis` / `minio` / `sqlite` / `contrib` / `all`) 移除 — 用户自己装自己要的
- 相关 tests (tests/contrib/) 移除

### Added

- `stageflow.storage_loader.load_storage(spec: str, **kwargs) -> StorageBackend` — 按 `"pkg.module:ClassName"` 字符串 importlib 加载
- `stageflow.StageflowStorageError` — 友好错误 (module 未装 / class 名错 / kwargs 错 / 类型错 5 类)
- 用户可自己写 adapter, stageflow 不带 driver, 通过 `pip install` 自管 deps
- ai_writer 兼容: `load_storage("backend.integration.sf_storage.MinioStorage", ...)` 直接可用

### Internal

- core 仍 stdlib-only + 零第三方依赖
- 0 行 core 修改 (除新增 storage_loader.py + 2 行 export)
- reversal 是真实设计演化: v0.4 contrib 是反向教材, git history 保留以做记录

## [Unreleased]

- (规划) `replay --prompt-patch` (v0.2): checkpoint 加载 + 单 stage 重放

## [0.1.3] — 2026-09-05

### Fixed

- `pyproject.toml` version + `stageflow/__init__.py` `__version__` 与 tag 同步 (0.1.1 → 0.1.3)。final-review B1: 之前 `pip install -e .` / `import stageflow; stageflow.__version__` 返 0.1.1,与 v0.1.2 tag 不符。
- 不 amend v0.1.2 (keep git history honest) — 走新 patch tag v0.1.3
