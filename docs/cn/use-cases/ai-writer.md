# Use case: ai_writer 研究管线接入 (参考实现)

🇬🇧 [English version](../../en/use-cases/ai-writer.md)

> **这是 stageflow 的第 1 个业务接入, 作参考实现** — 展示一个真实业务如何用
> stageflow 表达"搜索 → 下载 → 过滤 → 合成 → 审阅 → 保存"类管线。
> stageflow 本身与 ai_writer 及其使用的模型/服务无关。

## 接入结果 (2026-09-05 落地)

- 8 节点 DAG (first compose): `s_plan → s_search → s_download → s_filter →
  s_compress → s_compose → s_audit → s_save`
- revise (迭代修订) 是**单 super-node** (`s_revise`) 包住业务自己的迭代循环 —
  循环留在业务层的典型示范: 业务循环自带 save-per-iter + DB resume,
  stageflow 只提供统一入口 + 异常契约 + 超时
- checkpoint 落业务自己的对象存储 (StorageBackend adapter, 独立前缀)
- 断点续跑: subproc 中断 → 重启同 task → runtime resume 已完成节点
- 端到端验证: first compose + revise 均正常产出, 中断后 resume 正常

代码位置 (业务侧, 非本仓库): `backend/integration/` — `research_pipeline_dag.py`
(两个 DAG) + `runner.py` (setup/route/异常边界) + `sf_storage.py`
(StorageBackend → 对象存储).

## 边界 (什么归谁)

| | stageflow (通用) | 业务 (ai_writer) |
|---|---|---|
| DAG 定义 | — | 业务侧文件 (8 节点 / s_revise) |
| 外部调用 | `ctx.call(kind, op, params)` Protocol | 业务 caller (复用自有 LLM/cache/计费) |
| checkpoint 存储 | StorageBackend Protocol | 业务 adapter (对象存储 / DB / 文件) |
| 循环 (audit cascade) | — | stage 内 Python for/while |
| 日志 | stage 内业务自理 | 接业务既有日志/SSE |

## 落地的通用模式

### 1. 安装

```toml
# 业务 pyproject.toml
[tool.poetry.dependencies]  # 或 pip / uv
stageflow = { git = "ssh://git@github.com/ebziw/stageflow.git", tag = "v0.5.0" }
```

### 2. 定义 DAG (纯图, 阶段函数搬业务逻辑)

```python
from stageflow import DAG

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
from stageflow import Runtime, CheckpointStore

rt = Runtime(
    checkpoint_store=CheckpointStore(ObjectStoreStorage(task_id)),  # 每 task 隔离
    default_timeout=4 * 3600,   # 整跑 absolute deadline
)
# 首跑 / 日常重跑: resume=False (默认) — 起新 run_id; 重复 LLM 成本由业务
# external cache 免单, 从头重跑不心疼.
result = asyncio.run(rt.run(dag, task_id=task_id, initial_state={...}))
if result.status != "done":
    raise RuntimeError(result.error or "run failed")

# 真续跑 (中断后重启, 已有 checkpoint): resume=True — 跳过已完成节点,
# 复用原 run_id. 无 checkpoint / run 已全部完成 → RuntimeError (done guard),
# 回退 resume=False (起新 run_id).
result = asyncio.run(rt.run(dag, task_id=task_id, resume=True))
```

要点 (v0.5 ID model):
- `task_id` = 业务 task id (opaque key, 稳定幂等键)
- `run_id`: 每次 run() 自动 UUID4 — checkpoint 按 `runs/{task_id}/{run_id}/checkpoint`
  隔离 (+ `runs/{task_id}/latest` 指针)。与业务产物 (`research/{task_id}/v{v}/`)
  同桶时前缀隔离, 互不干扰
- 恢复模式: 默认 "从头重跑 + external LLM cache" (`resume=False`);
  stageflow `resume=True` 留给真续跑 (中断重启)。每 task 单写者 —
  并发由业务 worker 自己的 lease/heartbeat 防
- stage 函数体改动不影响 resume (workflow_hash 只含结构); 改依赖/retries → 拒续跑
- `Ctx.run_id` / `Ctx.attempt` + caller 第 4 参 `CallMeta` — 业务 caller 落
  trace 可直接关联执行现场 (task/run/stage/attempt)

### 5. 回归

```python
from stageflow import TestPipe

pipe = TestPipe(dag)
pipe.mock("s_search", lambda state: {...})   # mock 昂贵/外部段
result = await pipe.run()
assert result.state == {...}
```

## 已知注意

1. 业务侧模块级全局 (contextvar/缓存) 是迁移时最容易漏的点 — 搬函数时把跨阶段
   依赖显式化为 ctx.state
2. 子进程心跳/租约 (若有) 是业务 worker 的事, 与 stageflow 无关, 保留
3. ctx.state 值必须 json-serializable (str/int/float/bool/None/list/dict)

## 相关文档

- [quickstart.md](../quickstart.md) — 5 分钟跑通
- [architecture.md](../architecture.md) — 架构与设计决策
- [api.md](../api.md) — 公开 API 参考
