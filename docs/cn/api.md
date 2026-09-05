# API 参考

🇬🇧 [English version](../en/api.md)

公开面 = `stageflow/__init__.py` 的 `__all__`。core 零第三方依赖。

## DAG

```python
dag = DAG(name: str)
```

- `dag.stage(*, depends_on: list[str] | None = None, retries: int = 0,
  timeout: float | None = None)` — 装饰器注册 stage 函数
  - `depends_on`: 前置 stage 名列表
  - `retries`: `RetryableError`/超时可重试次数 (指数退避 2^n, 上限 30s)
  - `timeout`: 本 stage 硬超时 (秒); 实际超时 = `min(stage.timeout,
    run 剩余 deadline)`
- `dag.validate()` — 静态校验 (未知依赖 + 环检测), run 前自动调用, 通过即冻结
- `dag.topo_order() -> list[str]` — Kahn 拓扑序
- `dag.reachable(upstream, downstream) -> bool` — 传递依赖判定 (链式覆盖用)

异常: `UnknownDepError` / `CycleError`

## Runtime

```python
rt = Runtime(
    checkpoint_store: CheckpointStore | None = None,
    caller: Callable[[str, str, dict], Awaitable[dict]] | None = None,
    default_timeout: float | None = None,   # 整跑 absolute deadline (秒)
)

result: RunResult = await rt.run(
    dag, task_id: str,
    *,
    initial_state: dict | None = None,
    resume: bool = True,
    deadline: float | None = None,   # 覆盖 default_timeout
)
```

- `task_id` 是 caller 提供的 opaque key — 所有 checkpoint/state 按它隔离,
  多任务并行互不干扰
- resume: 有 checkpoint 且 workflow_hash 匹配 → 跳过已完成 stage
- 跑完 (`status == "done"`) → checkpoint 自动删除

### RunResult

- `status`: `"done" | "failed"`
- `state`: 最终 state
- `stage_statuses`: 每 stage 的 `done | failed`
- `error`: failed 原因 (异常转字符串)

### 失败语义 (不抛异常, 返回 failed)

| 异常 | 含义 | 行为 |
|---|---|---|
| `StageError` | 业务失败 | fail, 不重试 |
| `RetryableError` | 网络/限流/超时 | 退避重试, 耗尽 fail |
| `FatalError` / 未知异常 | 代码 bug | fail, 不消耗 retries |

## Ctx (stage 参数)

```python
ctx.task_id: str
ctx.stage_name: str
ctx.state: ReadOnlyStateView   # 只读深拷贝; 写任何属性 raise
ctx.deadline: float | None     # 本 stage absolute deadline

await ctx.call(kind: str, op: str, params: dict | None = None) -> dict
```

- `ctx.call` 是 stage 内**唯一**外部调用入口。`kind`/`op`/返回值结构由业务
  caller 决定 — 框架只透传, 默认 caller 是 no-op echo
  (`CallResult(kind=..., op=..., params=...)`)
- `ctx.logger`: logging.Logger (stage 名已注入)

## State

- 类型白名单: `str/int/float/bool/None/list/dict` (json-serializable),
  其他 (`set/datetime/Path/bytes/...`) → `StateValidationError`
- 写路径: stage `return dict` → shallow merge
- 冲突: delta key 已存在于 state 且 producer **不是**当前 stage 的传递上游
  → `StateConflictError` (平行 producer 冲突)
- 链式覆盖: producer 是传递上游 → 合法 (v0.1.1)

## Checkpoint

```python
store = CheckpointStore(storage: StorageBackend)
cp: Checkpoint | None = store.load_compatible(task_id, dag)   # hash 不匹配 raise
store.save(cp)
store.delete(task_id)
```

- `Checkpoint`: `task_id / dag_name / workflow_hash / stage_statuses / state /
  done_stages / producers`
- `workflow_hash(dag)`: stage 名+依赖+retries+timeout 的 sha256 指纹
  (改函数体不影响 hash)
- `CheckpointMismatchError`: resume 时结构变了 → 拒续跑 (删 checkpoint 或
  `resume=False` 强制重跑)
- 旧 checkpoint (无 `producers`) → resume 走宽松模式 (视同单链)

## StorageBackend (Protocol, 业务注入)

```python
class StorageBackend:
    def put(self, key: str, data: dict) -> None: ...
    def get(self, key: str) -> dict | None: ...
    def list_keys(self, prefix: str) -> list[str]: ...
    def delete(self, key: str) -> None: ...
```

内置: `FileStorage(dir)` — 本地文件系统实现 (测试/单机)。

## Contrib adapters

可选 `StorageBackend` 实现在 `stageflow.contrib.storage` (独立子包, core 保持 stdlib-only). 通过 pyproject extras 按需安装 driver:

```bash
pip install stageflow[postgres]   # psycopg3 + JSONB
pip install stageflow[mysql]      # PyMySQL + LONGTEXT
pip install stageflow[redis]      # redis-py
pip install stageflow[minio]      # boto3 (S3 / MinIO / R2)
pip install stageflow[contrib]    # 4 个全装
pip install stageflow[sqlite]     # stdlib, 无 extras
```

每个 adapter 懒导入 — `from stageflow.contrib.storage import PostgresStorage` 缺 driver 时抛 `ImportError` + 安装提示. 完整用法 / ai_writer 兼容说明 / 测试策略: [docs/contrib.md](../contrib.md).

## TestPipe

```python
pipe = TestPipe(dag, caller=None)
pipe.mock("s_search", lambda state: {"sources": [...]})   # 按需替换 stage
result: RunResult = await pipe.run(task_id="t", initial_state={...})
```

- 未 mock 的 stage 原样执行 (可 mock 外部调用密集段, 其余真跑)
- 回归场景: 用固定 mock 输出跑全图, 断言最终 state

## CLI

```bash
python -m stageflow run <dag.py> [--task-id X] [--input '{"k": "v"}'] [--resume]
python -m stageflow trace --task-id X
python -m stageflow state --task-id X [--key K]
```

- `run`: 跑 DAG 文件 (模块须暴露 `dag` 变量); `STAGEFLOW_STORAGE` 环境变量
  指向 FileStorage 目录 (缺省不落 checkpoint)
- `trace/state`: 读 FileStorage 里的 checkpoint

## 相关文档

- [quickstart.md](quickstart.md) — 5 分钟跑通
- [architecture.md](architecture.md) — 架构与设计决策
- [use-cases/](use-cases/) — 业务接入参考实现
