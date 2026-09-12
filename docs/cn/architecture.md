# pavoz 架构

🇬🇧 [English version](../en/architecture.md)

## 定位

**micro/in-process workflow engine** — 不是 Airflow/Prefect/Dagster 那种 macro orchestrator (自带 scheduler/DB/UI/分布式).
只做: 图执行 + state 传递 + 失败重试 + checkpoint 恢复. 嵌入业务进程, 零运行时依赖,
**不绑定任何业务系统 / 模型 / 存储服务** (参考接入: `docs/use-cases/`).

```
┌─────────────────────────────────────────────┐
│ 业务系统 (任何项目)                          │
│   worker 拉 task → Runtime.run(dag, task_id)│
│   stage 函数 = 纯 async (读 ctx.state,      │
│     return dict 写 state)                    │
└──────────────┬──────────────────────────────┘
               │ ctx.call(kind, op, params)
               ▼
┌─────────────────────────────────────────────┐
│ pavoz core (本仓库)                     │
│   DAG (静态) + Runtime + State + Checkpoint │
│   零依赖: 不 import 任何第三方库             │
└──────────────┬──────────────────────────────┘
               │ StorageBackend Protocol
               ▼
┌─────────────────────────────────────────────┐
│ 存储 (业务注入)                             │
│   FileStorage (默认/测试)                   │
│   DB / MinIO / 其他 backend                 │
└─────────────────────────────────────────────┘
```

## 核心设计决策

### 1. 循环留在业务层 (最重要)

"反复执行同一阶段直到满足条件" 的流程 (质量收敛循环、人工确认轮询等) **不是图节点**,
是 stage 函数内的 Python while/for:

```python
@dag.stage(depends_on=["s_produce"])
async def s_quality_loop(ctx):
    """示例: 直到质检通过 (纯业务逻辑, 框架不提供循环原语)."""
    for attempt in range(3):
        result = await ctx.call("quality_check", "default", params={"item": ...})
        if result["verdict"] == "pass":
            return {"item": ..., "quality": "pass"}
        if attempt < 2:
            await ctx.call("fix", "default", params={"issues": result["issues"]})
    return {"quality": "fail"}
```

框架只有 3 能力: **DAG (线性图) + per-node retries + checkpoint**. 无
sub_dag/retry_budget/escalation/watchdog 原语。
参考: 最成熟引擎 (Airflow/Temporal/Prefect) 都没有 cross-stage retry 原语 —
循环和 gate 用 workflow 代码表达。

#### 扩展面: 一个 stage 就是扩展点

框架不提供循环原语, 但提供**扩展点** —— 你可以不碰框架源码, 用装饰器/注入实现自己的机制:

| 扩展点 | 机制 | 用途 |
|---|---|---|
| **stage 函数** | 任意 `async def(ctx) -> dict`, 注册前先包一层 | 质量门 (评分 + 重做循环) / 契约校验 / 埋点 / 自适应超时 |
| **生命周期事件** | `Runtime(on_event=fn)`, `fn(event: str, data: dict)` | 进度上报 / 指标采集 / 告警; observer 异常被隔离, 不影响 run |
| **出站调用** | `ctx.call(kind, op, params)` → caller 注入 | LLM / HTTP / DB 全走同一出口, trace、记账、重放都挂在这里 |
| **存储** | `StorageBackend` Protocol | 换 checkpoint 落盘位置 (文件 / DB / 对象存储) |
| **取消** | `Runtime(cancel_check=...)` + `ctx.cancelled()` | 协作式取消, 长 stage 自行轮询退出 |

生命周期事件表:

| 事件 | 时机 | 主要字段 |
|---|---|---|
| `run_start` / `run_end` | run 边界 | `task_id` / `run_id` / `status` |
| `stage_start` / `stage_end` | 每次 attempt | `task_id` / `run_id` / `stage` / `attempt` / `status` / `duration` |
| `stage_retry` | `RetryableError` 触发重试后 | 同上 + `error` |
| `stage_progress` | stage 内调 `ctx.set_progress(fraction, note)` | `fraction` / `note` (best-effort 瞬态信号, **不落 checkpoint**) |

**装饰器注意事项**: 包 stage 函数时必须用 `functools.wraps` —— stage 名取自 `fn.__name__`, 丢了它所有被装饰的 stage 会重名。

**版本策略**: 第三方扩展声明 `pavoz>=0.3,<0.4` (pip 解析即强制), CI 跑 "最低支持版本 × 最新版本" 矩阵; 0.x 期间 minor 版本可能包含破坏性变更 (semver 允许), 1.0 之后回归常规语义。

参考实现: [pavoz-extensions](https://github.com/liyong-labs/pavoz-extensions) —— `@gate` (worker → 多 lens 评审 → 评分 → 重做) 与 `@schema` (跨 stage 契约校验), 两个纯装饰器, 也是写你自己的 `pavoz-*` 扩展的模板。

### 2. Stage = async def(ctx) -> dict

- 读: `ctx.state` (ReadOnlyStateView, 深拷贝, 写会 raise)
- 写: `return dict` → runtime 做 shallow merge
- 冲突检测 (v0.1.1 链式覆盖语义):
  - stage 覆盖**传递上游 producer** 写的 key → 合法 (数据流水线逐级演进,
    `search → filter → compress` 更新同一产物)
  - 平行 producer (无依赖链) 写同一 key → `StateConflictError`
  - runtime 追踪 `key → producer stage`, 随 checkpoint 持久化

### 3. 异常契约

| 异常 | 语义 | runtime 行为 |
|---|---|---|
| `StageError` | 业务失败 (不可重试) | 直接 fail 终态 |
| `RetryableError` | 网络/429/5xx/超时 | 扣 retries 指数退避重试, 耗尽 fail |
| `FatalError` / 未知异常 | 代码 bug | 立即 fail, 不消耗 retries |

### 4. Checkpoint + workflow hash

- 每 node 完成后落盘 (StorageBackend)。checkpoint 是 **run 级**:
  存 `runs/{task_id}/{run_id}/checkpoint` + 指针文件 `runs/{task_id}/latest` —
  同 task 可有多 run (original / resume / 重跑), 互不覆盖
- checkpoint 含 `workflow_hash` (stage 名 + 依赖 + retries + timeout 的 sha256)
- resume 时 hash 不匹配 → `CheckpointMismatchError` (DAG 结构变了不能续跑; 只改函数体不影响 hash)
- checkpoint 持久化 `producers` (key → producer stage, 链式覆盖恢复判定用);
  当前格式内缺可选字段走默认值 (schema 向前兼容)。v0.5 之前的 checkpoint
  (无 `run_id`, 旧 key `runs/{task_id}/checkpoint`) 已 hard cut orphan — 无
  legacy loader
- run 完成后 checkpoint **保留** (不自动清) — 它是该 task 的最新 run;
  已完成 run 再 resume → done guard 明确报错 (防静默 no-op), 重跑用
  `resume=False` (起新 run_id)

### 5. 超时 = absolute deadline 传播

- run 级: `deadline` 或 `default_timeout`
- stage 级: `timeout` (秒) → 实际超时 = `min(stage_timeout, run_deadline - now)`
- 超时按 RetryableError 处理 (可重试)

### 6. State 契约

- 值类型限定: str/int/float/bool/None/list/dict (json-serializable). set/datetime/Path/bytes raise
- 每 stage 前: 深拷贝给 ctx.state (defensive — stage 原地改 = 改了个寂寞)

### 7. Run 身份 + 恢复模型 (v0.5)

三级身份, 对齐业界先例 (Temporal WorkflowId/RunId, DBOS workflow_id,
Airflow `dag_id + task_id + run_id + try_number`):

- `task_id` — caller 提供 (或自动 UUID4)。跨 retry/resume 的稳定幂等键。
  入口校验: 非空、≤128、字符集仅 `[A-Za-z0-9_.-]` (禁 `/` — 直接进 storage
  key 路径)
- `run_id` — 每次 `run()` pavoz 自动生成 UUID4。resume 复用 checkpoint
  的 run_id (Continue-As-New); 重跑 (`resume=False`) 起新 run_id
- `attempt` — pavoz 注入 Ctx 的 1-based int; stage 每 retry +1
  (Airflow try_number / Celery retries 模式)

恢复模式: pavoz `resume` 是**从 checkpoint 真续跑** (恢复 state、跳过
已完成节点、复用 run_id)。"从头重跑 + external cache 免单" (`resume=False`)
是首批接入方的日常默认 — 重复成本由业务 cache 吸收 — `resume` 留给真续跑
场景 (中断后重启)。

运行假设 (刻意不在 core 强制, 归业务层):

- **每 task 单写者** — pavoz 假定同 task 同时只有一个活跃 writer;
  并发由业务层防 (lease/epoch)。同 `(task_id, run_id)` 双写 = last-writer-wins,
  core 不做锁
- **不感知 cancel** — pavoz in-process: worker 死 = run 死 (checkpoint 存到
  最后一个完成节点)。cancel = 业务侧 kill + 重启 + `resume=True`
- **存储命名空间** — checkpoint 落在 `runs/` 前缀下
  (`runs/{task_id}/{run_id}/checkpoint` + `runs/{task_id}/latest`)。业务与
  pavoz 共用对象存储时前缀隔离 (如业务产物放 `research/{task_id}/v{v}/`)

### 8. Replay 语义 (v0.5.1, M2/M3)

两种重放模式, 都由 checkpoint 驱动 — 不靠 caller 重新喂数据:

- **checkpoint 存重放所需的一切**。v0.5.1 起 cp 存 `initial_state` + 每个
  已完成 stage 的原始 return (`stage_deltas`, 按完成序) — 任意已记录 stage
  执行前的 state 可精确重建 (`initial_state` + 它之前已完成 stage 的 deltas;
  链式覆盖由完成序天然处理), 不再 naive 地拿最终 `cp.state` 当输入 —
  那里面含 stage 自己的输出, 会污染重放输入。`rebuild_state()` /
  `rebuild_state_before(name)` 暴露此能力。旧 (v0.5.0) cp 缺新字段 —
  读和 resume 都兼容, 但重放 → 明确报错提示重跑一次
- **单 stage 重放** (`Runtime.run_stage` / CLI `replay <dag.py> --task-id X
  --stage Y [--patch P.py]`): 重建该 stage 执行前 state, 只跑它 — 调
  prompt/参数秒级看效果。原始 run 失败/中断、依赖已完成的 stage 也可重放。
  重放产出是开发临时物: **不写 checkpoint** 也不动 `latest` 指针 —
  replay 永远不会污染未来 resume/`load_latest` 的目标
- **图回归** (`TestPipe.replay_from(cp, dag)`): 已完成 stage 用存下的 delta
  当 mock, 其余真跑 — 真实 run 的 cp 直接当 fixture,"相同 stage 输出进 →
  相同终态出" (`result.state == cp.state`) 验证换实现后图行为 (拓扑/合并/
  冲突规则) 没坏。DAG 结构变了 (workflow_hash mismatch) → 明确报错拒
  replay, 不静默错配

## 模块

| 文件 | 职责 |
|---|---|
| `dag.py` | DAG 声明 + 拓扑排序 (Kahn) + 环检测 (Tarjan SCC + 自环) + 冻结 |
| `runtime.py` | 执行引擎: 顺序跑 + retry + deadline + state merge + checkpoint; `run_stage` 单 stage 重放 (v0.5.1) |
| `state.py` | 类型校验 + ReadOnlyStateView + shallow merge + 冲突检测 |
| `checkpoint.py` | CheckpointStore + workflow_hash + mismatch 检测; `stage_deltas`/`initial_state` + `rebuild_state[_before]` (v0.5.1) |
| `storage.py` | StorageBackend Protocol + FileStorage (默认) |
| `testing.py` | TestPipe (mock stage 跑全图; `replay_from(cp, dag)` checkpoint 回归, v0.5.1) |
| `cli.py` | run / trace / state / replay (v0.5.1) |
| `storage_loader.py` | `load_storage(spec, **kwargs)` — config-string 驱动 `StorageBackend` 加载 (importlib + 友好错误); 见 [docs/storage.md](../storage.md) |

## 不做什么 (YAGNI)

- ❌ scheduler / cron / 时间触发
- ❌ UI / 可视化 (v1)
- ❌ 分布式 (多 host lease 同步)
- ❌ dynamic DAG (运行时改图) — DAG 定义一次 parse 一次, validate 后冻结
- ❌ sub_dag 嵌套 (循环在业务层, 不需要)
- ❌ 业务 wrapper 实现 (外部调用 adapter 是业务系统的活; core 只定义 ctx.call Protocol, kind/op 由业务定)
- ❌ 业务表 schema (runs/task_state 是 caller 的 DB 的事)

## 相关文档

- [quickstart.md](quickstart.md) — 5 分钟跑通
- [api.md](api.md) — 公开 API 参考
- [use-cases/](use-cases/) — 业务接入参考实现
