# stageflow 架构

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
│ stageflow core (本仓库)                     │
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

## 核心设计决策 (2026-09-05 拍板)

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
sub_dag/retry_budget/escalation/watchdog 原语.
参考: 最成熟引擎 (Airflow/Temporal/Prefect) 都没有 cross-stage retry 原语 —
循环和 gate 用 workflow 代码表达.

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

- 每 node 完成后落盘 (StorageBackend)
- checkpoint 含 `workflow_hash` (stage 名 + 依赖 + retries + timeout 的 sha256)
- resume 时 hash 不匹配 → `CheckpointMismatchError` (DAG 结构变了不能续跑; 只改函数体不影响 hash)
- checkpoint 持久化 `producers` (key → producer stage, 链式覆盖恢复判定用);
  旧 checkpoint 无此字段 → resume 走宽松模式 (视同单链), 向后兼容
- run 全部完成 → checkpoint 自动清

### 5. 超时 = absolute deadline 传播

- run 级: `deadline` 或 `default_timeout`
- stage 级: `timeout` (秒) → 实际超时 = `min(stage_timeout, run_deadline - now)`
- 超时按 RetryableError 处理 (可重试)

### 6. State 契约

- 值类型限定: str/int/float/bool/None/list/dict (json-serializable). set/datetime/Path/bytes raise
- 每 stage 前: 深拷贝给 ctx.state (defensive — stage 原地改 = 改了个寂寞)

## 模块

| 文件 | 职责 |
|---|---|
| `dag.py` | DAG 声明 + 拓扑排序 (Kahn) + 环检测 (Tarjan SCC + 自环) + 冻结 |
| `runtime.py` | 执行引擎: 顺序跑 + retry + deadline + state merge + checkpoint |
| `state.py` | 类型校验 + ReadOnlyStateView + shallow merge + 冲突检测 |
| `checkpoint.py` | CheckpointStore + workflow_hash + mismatch 检测 |
| `storage.py` | StorageBackend Protocol + FileStorage (默认) |
| `testing.py` | TestPipe (mock stage 跑全图) |
| `cli.py` | run/trace/state (v0.1) |
| `contrib/storage/` | 可选 `StorageBackend` adapter (Postgres / MySQL / Redis / MinIO / SQLite) — 见 [docs/contrib.md](../contrib.md) |

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
