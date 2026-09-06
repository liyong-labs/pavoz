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
    caller: Callable[[str, str, dict, CallMeta], Awaitable[dict]] | None = None,
    default_timeout: float | None = None,   # 整跑 absolute deadline (秒)
)

result: RunResult = await rt.run(
    dag, task_id: str | None = None,
    *,
    initial_state: dict | None = None,
    resume: bool = False,
)
```

- `task_id` 可选: 省略 → stageflow 自动生成 UUID4 (36 字符)。显式传入时入口
  校验: 非空、≤128、字符集仅 `[A-Za-z0-9_.-]` (禁 `/` — task_id 直接进
  storage key 路径)。跨 retry/resume 稳定 — 幂等键。resume 必须有显式
  task_id (自动生成的 ID 不会有 checkpoint 可找)
- `run_id` 每次 `run()` 自动生成 (UUID4), 返回在 `RunResult.run_id`。resume
  **复用** checkpoint 的原 run_id (Temporal Continue-As-New 模式) — 同 task
  多次 run 互不覆盖
- `resume` (默认 `False`): `True` → 加载该 task 最新 checkpoint 从断点续跑
  (跳过已完成 stage, 恢复 state)。无 checkpoint → `RuntimeError`; run 已全部
  完成 (done guard) → `RuntimeError` 防静默 no-op。重跑 = `resume=False`
  (起新 run_id)
- resume 时 `workflow_hash` 不匹配 (DAG 结构变了) → `CheckpointMismatchError`
- run 级 absolute deadline 只来自 `Runtime(default_timeout=...)` — `run()` 无
  deadline 参数; stage 级 `timeout` 受 run 剩余 deadline 封顶
- run 完成后 checkpoint **保留** (不删除) — 它是该 task 的最新 run, 是未来
  resume / `load_latest` 的目标

### `Runtime.run_stage` (单 stage 重放, v0.5.1)

```python
result: RunResult = await rt.run_stage(dag, task_id: str, stage_name: str)
```

调试 helper: 从该 task 最新 checkpoint 重建 stage 执行前 state, **只跑这一个
stage** — 调 prompt/参数秒级看效果, 不重跑前序 stage。

- 执行前 state 重建: 已完成 stage → `cp.rebuild_state_before(stage_name)`;
  从未完成 (原始 run 失败/中断) 且依赖已全完成的 stage →
  `cp.rebuild_state()`; 依赖未完成 → `RuntimeError`
- **不落 checkpoint** 也不动 `latest` 指针 (replay 产出是开发临时物 —
  不能成为 resume 目标); `ctx.run_id` 是本次调用的一次性 UUID4
- 错误: 无 checkpoint → `RuntimeError`; DAG hash 不匹配 →
  `CheckpointMismatchError`; pre-M2 (v0.5.0) cp 无 `stage_deltas` →
  `RuntimeError` (重跑一次生成新 cp); stage 不存在 → `KeyError`

### RunResult

- `task_id`: caller 提供或自动生成 UUID4
- `run_id`: 本次 run 的 UUID4 (resume 时 = 复用的原 run_id)
- `status`: `"running" | "done" | "failed" | "cancelled"` (终态: `done` / `failed` / `cancelled`)
- `state`: 最终 state
- `stage_statuses`: 每 stage 的 `done | failed | cancelled`
- `error`: failed/cancelled 原因 (异常转字符串)

### 失败语义 (不抛异常, 返回 failed)

| 异常 | 含义 | 行为 |
|---|---|---|
| `StageError` | 业务失败 | fail, 不重试 |
| `RetryableError` | 网络/限流/超时 | 退避重试, 耗尽 fail |
| `FatalError` / 未知异常 | 代码 bug | fail, 不消耗 retries |

## Ctx (stage 参数)

```python
ctx.task_id: str
ctx.run_id: str          # 本次 run 的 UUID4 (resume 复用的原 run_id)
ctx.attempt: int         # 1-based; 本 stage 当前尝试次数 (retry 一次 +1)
ctx.stage_name: str
ctx.state: ReadOnlyStateView   # 只读深拷贝; 写任何属性 raise
ctx.deadline: float | None     # 本 stage absolute deadline

await ctx.call(kind: str, op: str, params: dict | None = None) -> dict
```

- `ctx.call` 是 stage 内**唯一**外部调用入口。`kind`/`op`/返回值结构由业务
  caller 决定 — 框架只透传, 默认 caller 是 no-op echo
  (`CallResult(kind=..., op=..., params=...)`)
- 注入的 caller 收**第 4 参** `meta: CallMeta` (`task_id` / `run_id` / `stage` /
  `attempt`) — caller 落 trace/llm_calls 时可直接关联执行上下文。
  `CallMeta` 在 `stageflow.runtime`。stage 函数签名不受影响
  (`async def fn(ctx) -> dict`)
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
store.save(cp)                                  # 写 checkpoint + 更新 latest 指针
cp: Checkpoint | None = store.load(task_id, run_id)       # 指定 run
cp: Checkpoint | None = store.load_latest(task_id)        # 按指针文件加载最新
run_ids: list[str] = store.list_runs(task_id)             # 有哪些 run? (调试/清理)
store.delete(task_id, run_id)                             # 指针不随删除更新
cp: Checkpoint | None = store.load_compatible(task_id, run_id, dag)
# hash 校验加载; run_id="" → 该 task 最新 run (load_latest 哨兵)
```

- 存储布局: 每次 run 存 `runs/{task_id}/{run_id}/checkpoint`; 每次 `save()`
  同时更新指针文件 `runs/{task_id}/latest` (`{"run_id": ...}`)。同 task 多次
  run 互不覆盖; "最新" 不靠字典序 (uuid4 字典序 ≠ 时间序)。删的恰是指针
  指向的 run → `load_latest` 返 `None` ("无 checkpoint" 明确信号, 可接受边界)
- `Checkpoint` 字段: `task_id / run_id / dag_name / workflow_hash /
  stage_statuses / state / done_stages / producers` + v0.5.1:
  `initial_state` (run 的起始 state) 和 `stage_deltas` (每个已完成 stage 的
  原始 return dict, 按完成序)。两者合起来可重建任意已记录 stage 执行前的
  state (`cp.state` 是最终合并态 — 无法从它倒推每 stage 的输入)。
  `run_id` 必填 — v0.5 之前的 checkpoint (无 `run_id`, 旧 key
  `runs/{task_id}/checkpoint`) 已 orphan (hard cut, 无 legacy loader);
  当前格式内缺可选字段走默认值 (schema 向前兼容 — v0.5.0 旧 cp 无新字段
  也可读可 resume)
- `cp.rebuild_state() -> dict` — 重建所有已完成 stage merge 后的 state
  (`initial_state` + done deltas 按完成序; 链式覆盖由顺序天然处理)。
  给从未完成 (依赖已全完成) 的 stage 当执行前 state (重放用)
- `cp.rebuild_state_before(stage_name) -> dict` — 重建 stage 执行前的 state
  (`initial_state` + 它之前已完成 stage 的 deltas)。stage 不在本次 run 的
  记录里 → `KeyError`。`run_stage` 用它 — 保证 stage 自己的旧输出不会污染
  重放输入
- `workflow_hash(dag)`: stage 名+依赖+retries+timeout 的 sha256 指纹
  (改函数体不影响 hash)
- `CheckpointMismatchError`: resume 时结构变了 → 拒续跑 (`resume=False`
  强制重跑, 或删该 run 的 checkpoint)

## StorageBackend (Protocol, 业务注入)

```python
class StorageBackend:
    def put(self, key: str, data: dict) -> None: ...
    def get(self, key: str) -> dict | None: ...
    def list_keys(self, prefix: str) -> list[str]: ...
    def delete(self, key: str) -> None: ...
```

内置: `FileStorage(dir)` — 本地文件系统实现 (测试/单机)。

## Storage loader

stageflow core **只**带 `StorageBackend` Protocol + `FileStorage`。driver
(psycopg / redis / boto3 / ...) 由用户自己装, 自己写 (或抄) 实现 Protocol 的
adapter, 然后用 config 字符串加载。

```python
from stageflow import load_storage

storage = load_storage("stageflow.storage.FileStorage", root_dir="/data/cp")
# 或:
storage = load_storage(
    "backend.integration.sf_storage.MinioStorage",
    task_id="run-2026-09-05-001",
)
cp_store = CheckpointStore(storage)
```

### `load_storage(spec: str, **kwargs) -> StorageBackend`

- `spec` — adapter 定位字符串。两种等价形式:
  - `"pkg.module:ClassName"` (canonical, 显式 separator)
  - `"pkg.module.path.ClassName"` (dotted, 最后一段是 class 名)
- `**kwargs` — 透传给 `ClassName.__init__`
- 失败抛 `StageflowStorageError` (`ImportError` 子类), 5 类错误信息友好:
  格式错 / 模块未装 / class 名错 / kwargs 错 / 非 `StorageBackend` 实例。

完整参考 + ai_writer 互操作 + adapter 写法: [docs/storage.md](../storage.md).

## TestPipe

```python
pipe = TestPipe(dag, initial_state={...})                  # caller=None 默认
pipe.mock("s_search", lambda state: {"sources": [...]})   # 按需替换 stage 输出
result: RunResult = await pipe.run()

# v0.5.1: 把真实 run 的 checkpoint 当回归夹具
pipe2 = TestPipe.replay_from(cp, dag)  # 已完成 stage 用存下的 delta 喂 (mock)
result2 = await pipe2.run()
assert result2.state == cp.state       # 相同 stage 输出进 → 相同图行为出
```

- 未 mock 的 stage 原样执行 (可 mock 外部调用密集段, 其余真跑)
- 回归场景: 用固定 mock 输出跑全图, 断言最终 state
- `replay_from(cp, dag)`: `cp.done_stages` 里每个已完成 stage 用存的
  `stage_deltas` 输出 mock; 从未完成的 stage 真跑。要求 cp 的
  `workflow_hash` 与 DAG 一致 (结构没变) — mismatch →
  `CheckpointMismatchError`; pre-M2 (v0.5.0) cp 无 `stage_deltas` →
  `RuntimeError` (重跑一次)。真实 run 的手写 mock 回归可被它替代

## CLI

```bash
python -m stageflow run <dag.py> [--task-id X] [--input '{"k": "v"}'] [--resume]
python -m stageflow trace --task-id X
python -m stageflow state --task-id X [--key K]
python -m stageflow replay <dag.py> --task-id X --stage Y [--patch P.py]   # v0.5.1
```

- `run`: 跑 DAG 文件 (模块须暴露 `dag` 变量); `STAGEFLOW_STORAGE` 环境变量
  指向 FileStorage 目录 — 每次 run 都落 per-stage checkpoint (单跑也算,
  v0.5.1 R5 fix), `--resume`/replay/trace/state 对 CLI 产物可用
- `trace/state`: 读 FileStorage 里的 checkpoint
- `replay`: 在重建的输入上重放单 stage (见 `Runtime.run_stage`); 可选
  `--patch P.py` 加载暴露 `patch(dag) -> None` 的模块先换实现 (必须 sync —
  async patch 拒绝)。输出 result JSON; 不落任何盘 (不写 cp, 不动 latest 指针)

## 相关文档

- [quickstart.md](quickstart.md) — 5 分钟跑通
- [architecture.md](architecture.md) — 架构与设计决策
- [use-cases/](use-cases/) — 业务接入参考实现
