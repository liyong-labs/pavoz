# Use case: ai_writer 研究管线接入 (参考实现)

🇬🇧 [English version](../../en/use-cases/ai-writer.md)

> **这是 pavoz 的第 1 个业务接入, 作参考实现** — 展示一个真实业务如何用
> pavoz 表达"搜索 → 下载 → 过滤 → 合成 → 审阅 → 保存"类管线。
> pavoz 本身与 ai_writer 及其使用的模型/服务无关。

## 接入结果 (2026-09-05 接入 → 2026-09-06 全量迁移)

- 8 节点 DAG (first compose): `s_plan → s_search → s_download → s_filter →
  s_compress → s_compose → s_audit → s_save`
- revise (迭代修订) 是**单 super-node** (`s_revise`) 包住业务自己的迭代循环 —
  循环留在业务层的典型示范: 业务循环自带 save-per-iter + 终态落 DB,
  pavoz 只提供统一入口 + 异常契约 + 超时 (revise run 不挂 cp store —
  pavoz cp 只来自 first-compose run)
- **执行路径已唯一化 (2026-09-06)**: 业务旧 first-compose 阶段机
  (`_run_research_pipeline_impl` + `_impl_run_pipeline_phases`) 与单槽 checkpoint
  体系 (MinIO 单槽 CP / hash-skip / resume 槽) 全删 — subproc →
  `integration/runner.run_pipeline` → pavoz Runtime 跑 DAG; checkpoint 概念
  只剩 pavoz cp (`initial_state` + `stage_deltas`)
- cp 经 `MinioStorage` adapter (`sf_storage.py`) 落业务 MinIO 独立前缀
  `research/task_cp/pavoz/` 下 (`runs/{task_id}/{run_id}/checkpoint` +
  latest 指针), 与业务产物 (`research/{task_id}/v{v}/`) 同桶互不干扰
- 中断恢复: 生产路径**恒 `resume=False`** (重启起新 run_id) — 重启同 task 后
  stage 函数体从最近含对应 delta 的上一 run cp 读回 search/download/compress
  池产物 (`sf_restore.py`; 命中判定在业务 gate — search 池按 TTL 判新鲜,
  download/compress 池 use-time 校验), 命中即跳过
  已完成的搜索/下载/压缩段; 重复 LLM 成本由业务 external cache 免单. DB reset
  后重跑也不会被旧 cp 复活过期输入
- 调试重放: 业务暴露 `/api/research/replay-stage` → `Runtime.run_stage` 单 stage
  重放 (不落 cp / 不动 latest 指针)
- 端到端验证: 全量迁移后 first compose + revise 均正常产出, 中断重启恢复正常

代码位置 (业务侧, 非本仓库): `backend/integration/` — `research_pipeline_dag.py`
(两个 DAG) + `runner.py` (setup/route/异常边界) + `sf_storage.py`
(StorageBackend → 对象存储) + `sf_restore.py` (上一 run cp → 业务恢复 gate).

## 边界 (什么归谁)

| | pavoz (通用) | 业务 (ai_writer) |
|---|---|---|
| DAG 定义 | — | 业务侧文件 (8 节点 / s_revise) |
| 外部调用 | `ctx.call(kind, op, params)` Protocol | 业务 caller (复用自有 LLM/cache/计费) |
| checkpoint | StorageBackend Protocol + cp 格式 (`initial_state` + `stage_deltas` + `stage_ts`) | 业务 adapter (对象存储 / DB / 文件) + 独立 key 前缀 |
| 循环 (audit cascade) | — | stage 内 Python for/while |
| 日志 | stage 内业务自理 | 接业务既有日志/SSE |

## 落地的通用模式

### 1. 安装

```toml
# 业务 pyproject.toml
[tool.poetry.dependencies]  # 或 pip / uv
pavoz = { git = "ssh://git@github.com/liyong-labs/pavoz.git", tag = "v0.8.0" }
```

### 2. 定义 DAG (纯图, 阶段函数搬业务逻辑)

```python
from pavoz import DAG

dag = DAG("research_pipeline")

@dag.stage()
async def s_plan(ctx): ...          # 搬业务 plan 函数

@dag.stage(depends_on=["s_plan"], retries=2)
async def s_search(ctx): ...        # 搬业务 search (内部可 asyncio.gather 并行)

# ... s_download / s_filter / s_compress / s_compose / s_save 同构

@dag.stage(depends_on=["s_compose"])
async def s_audit(ctx):
    """质量循环留在 stage 内 (框架无循环原语)."""
    for attempt in range(3):
        report = await ctx.call("quality", "reviewer", params={"article": ...})
        if report["verdict"] == "pass":
            break
        if attempt < 2:
            await ctx.call("quality", "fixer", params={"issues": report["issues"]})
    return {"audit": report}
```

### 3. StorageBackend adapter (业务注入自己的存储)

```python
class ObjectStoreStorage(StorageBackend):
    def put(self, key, data): ...   # → 业务对象存储
    def get(self, key): ...         # 返回 dict | None
    def list_keys(self, prefix): ...  # → [str]
    def delete(self, key): ...
```

### 4. 入口 (业务 worker / subproc 内)

```python
from pavoz import Runtime, CheckpointStore

rt = Runtime(
    checkpoint_store=CheckpointStore(ObjectStoreStorage(task_id)),  # 每 task 隔离
    default_timeout=4 * 3600,   # 整跑 absolute deadline
)
# 生产路径恒 resume=False — 每次重启起新 run_id; 中断恢复 = 读上一 run 的 cp,
# 业务 gate 命中 (search 池 TTL 新鲜 / download、compress 池 use-time 校验) 即
# 跳过已完成段, 重复 LLM 成本由业务 external cache 免单
# (业务恢复读源: sf_restore.py — 从 stage_deltas 取 search/download/compress 产物).
result = asyncio.run(rt.run(
    dag, task_id=task_id,
    initial_state={...},
    resume=False,
))
if result.status != "done":
    raise RuntimeError(result.error or "run failed")
```

引擎的通用续跑 API 对其他接入方仍可用: `resume=True` 续最新 run (复用原 run_id,
跳过已完成节点; 无 checkpoint / run 已全部完成 → RuntimeError done guard) 与
`fork_run` (任意历史节点取输入 → 改 → 装回)。ai_writer 生产路径当前不使用它们 —
调试重放走 `Runtime.run_stage` (单 stage, 不落 cp / 不动 latest 指针)。

要点 (v0.8; ID model 自 v0.5):
- `task_id` = 业务 task id (opaque key, 稳定幂等键)
- `run_id`: 每次 run() 自动 UUID4 — cp 按 `runs/{task_id}/{run_id}/checkpoint`
  隔离 (+ `runs/{task_id}/latest` 指针)。ai_writer 的 `MinioStorage` adapter
  把 key 映射到自有 MinIO 前缀 `research/task_cp/pavoz/`, 与业务产物
  (`research/{task_id}/v{v}/`) 同桶互不干扰
- cp 内容 = `initial_state` + `stage_deltas` (每 node 原始 return, 按完成序)
  + `stage_ts` (v0.7+, stage 完成 epoch); 完整 state 由 `initial_state` +
  `stage_deltas` 重建 — v0.8 起落盘不再写冗余 state
- 恢复模式: 生产路径恒 `resume=False` (从头重跑 + external LLM cache 免单);
  中断恢复 = 读上一 run cp + 业务 gate (TTL / use-time 校验)。`resume=True` /
  `fork_run` 是引擎通用能力, ai_writer 暂未启用 (调试走 `run_stage`)。每 task
  单写者 — 并发由业务 worker 自己的 lease/heartbeat 防
- stage 函数体改动不影响 resume (workflow_hash 只含结构); 改依赖/retries → 拒续跑
- `Ctx.run_id` / `Ctx.attempt` + caller 第 4 参 `CallMeta` — 业务 caller 落
  trace 可直接关联执行现场 (task/run/stage/attempt)

### 5. 回归

```python
from pavoz import TestPipe

pipe = TestPipe(dag)
pipe.mock("s_search", lambda state: {...})   # mock 昂贵/外部段
result = await pipe.run()
assert result.state == {...}
```

## 已知注意

1. 业务侧模块级全局 (contextvar/缓存) 是迁移时最容易漏的点 — 搬函数时把跨阶段
   依赖显式化为 ctx.state
2. 子进程心跳/租约 (若有) 是业务 worker 的事, 与 pavoz 无关, 保留
3. ctx.state 值必须 json-serializable (str/int/float/bool/None/list/dict)

## 相关文档

- [quickstart.md](../quickstart.md) — 5 分钟跑通
- [architecture.md](../architecture.md) — 架构与设计决策
- [api.md](../api.md) — 公开 API 参考
