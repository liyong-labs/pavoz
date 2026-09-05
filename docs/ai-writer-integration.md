# ai_writer ↔ stageflow 接入指南

> 第 1 个 use case. 阶段 2 (接入) 执行时按此做. 设计决策见 `<local-config>/plans/humble-rolling-quilt.md` §两阶段执行模型.

## 边界 (什么归谁)

| | stageflow (通用) | ai_writer (业务) |
|---|---|---|
| DAG 定义 | — | `research_pipeline.py` (8 节点) |
| LLM/Search/Extract 调用 | `ctx.call(kind, op, params)` Protocol | adapter: BaseLLM / external_cache / llm_calls |
| 任务表 | TaskTrigger Protocol | `ResearchTaskTrigger` (research_tasks) |
| checkpoint 存储 | StorageBackend Protocol | MinIO / PG backend |
| 循环 (audit cascade) | — | `s_cascade` 内 Python for/while |
| 日志 | ctx.log → JSON trace | 接现有 research_task_logs 三写 |

## 接入步骤

### 1. 安装

```toml
# ai_writer pyproject.toml
[tool.poetry.dependencies]
stageflow = { git = "ssh://git@github.com/ebziw/stageflow.git", tag = "v0.1.0" }
```

### 2. DAG 定义 (新增 backend/integration/research_pipeline.py)

```python
from stageflow import DAG

dag = DAG("research_pipeline")

@dag.stage()
async def s_plan(ctx): ...          # 搬 _run_plan_phase

@dag.stage(depends_on=["s_plan"])
async def s_search(ctx): ...        # 搬 _run_search_phase (内部 asyncio.gather 并行)

@dag.stage(depends_on=["s_search"])
async def s_download(ctx): ...      # 搬 _run_download_phase

@dag.stage(depends_on=["s_download"])
async def s_filter(ctx): ...        # 搬 material_sieve

@dag.stage(depends_on=["s_filter"])
async def s_compress(ctx): ...      # 搬 material_compress

@dag.stage(depends_on=["s_compress"])
async def s_compose(ctx): ...       # 搬 outline + 章节 compose (循环章节内)

@dag.stage(depends_on=["s_compose"])
async def s_cascade(ctx):
    """audit → gap_search → fix_structural → re_audit → peer_review → proofread → editor_review
    循环. 搬 _run_unified_pipeline 的 for 循环 + convergence gate 判断 (ctx.state 里传)."""
    for attempt in range(3):
        report = await ctx.call("llm", op="research_auditor", params={"article": ...})
        ctx.log("audit_round", round=attempt, verdict=report["verdict"])
        if report["verdict"] == "pass":
            break
        if attempt < 2:
            await ctx.call("llm", op="fix_structural", params={"issues": report["issues"]})
    return {"article": ctx.state["article"], "verdict": ...}

@dag.stage(depends_on=["s_cascade"])
async def s_save(ctx): ...          # 搬 _save_revision_version (semver bump)
```

### 3. ctx.call adapter (新增 backend/integration/callers.py)

```python
async def research_caller(kind: str, op: str, params: dict) -> dict:
    if kind == "llm":
        # 复用 BaseLLM + external_cache (内容寻址免单) + log_llm_call
        ...
    elif kind == "search":
        ...
    elif kind == "extract":
        ...
```

**LLM 免单重放关键**: 复用 ai_writer `external_cache` — 恢复重跑 stage 时, ctx.call 内部同 input_hash 命中 → 直接返 response, 不重复计费.

### 4. TaskTrigger (新增 backend/integration/research_trigger.py)

实现 `stageflow.TaskTrigger`:
- `list_pending(max_concurrent)`: `SELECT task_id FROM research_tasks WHERE status='pending'` + in_progress 过滤
- `claim(task_id)`: `UPDATE research_tasks SET status='processing', lease_expires_at=NOW()+600 ... WHERE status='pending' RETURNING` (原子防双 worker)
- `mark_done/mark_failed`: 写终态

### 5. worker.py 改造

subproc 内 (保留 lease/hb/subproc 现有机制):

```python
# 原: _run_research_pipeline(task_row, conn)
# 改:
from backend.integration.research_pipeline import dag
from backend.integration.callers import research_caller
rt = Runtime(checkpoint_store=CheckpointStore(ai_writer_storage()), caller=research_caller)
result = await rt.run(dag, task_id=task_id, initial_state={...}, resume=True)
```

### 6. 回归验证

10 个已完成 task 的 MinIO stage 产物 (`research/{task_id}/v{v}/{stage}/`) 当 TestPipe fixture:
- mock 掉 LLM/Search (用已存的 payload_ref 响应)
- 对比: audit 5 维分数 / fix 触发路径 / convergence 停轮 / 字数 / refs

## 已知差异注意

1. 现在 stage 间状态靠 Python 全局/模块级 (contextvar `_TASK_VERSION`, `_audit_report_cell`, `_gates[task_id]`) — 切后全走 `ctx.state`. 搬函数时把这些显式化
2. subproc hb (60s lease 续) 是 subproc 模板的一部分, 与 stageflow 无关 — 保留
3. task_log 三写 (file + DB + SSE) 现在是 ai_writer `_task_log` 职责 — 切后 stage 内用 `ctx.log`, adapter 把 ctx.log 接回 `_task_log` (前端 SSE 协议不变)
