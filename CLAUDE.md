# CLAUDE.md — stageflow

> 本文件只放**铁律 + 指针**. 架构细节在 `docs/design/`, 阶段路线在 `ROADMAP.md`, 时序决策日志在 `docs/work-note/`, **使用文档**:
> - **30 秒上手**: `docs/QUICKSTART.md` (摘要 + 5 行示例 + gotchas)
> - **完整 doc**: `docs/handoff/usage.md` (10 节, 含端到端示例 + 决策树)
> - **架构**: `docs/design/architecture.md` (核心理念 / 模块 / 边界)
> - **AI 写作指南**: `docs/handoff/ai-writer-style-handoff.md` (供 LLM agent 看)

## 定位 (2026-09-05 user 拍板, 永久生效)

**stageflow = 独立通用 Python 工作流编排库, 准备公开发布的开源项目.**

**核心原则**: stageflow 是**通用**项目, ai_writer 仅是第 1 个 use case (不是专属用户, 不是设计中心).

**含义**:
- ❌ stageflow **不能**为 ai_writer 业务定制任何 feature (research-specific / writing-specific / 公众号-specific)
- ❌ stageflow **不能**假设 stage 名是 `s_plan / s_search / s_compose` — 这些是 ai_writer 的
- ❌ stageflow **不能**假设 storage 是 MinIO / PG / 业务 DB — 这些是 caller 的
- ❌ stageflow **不能**硬编码 ai_writer 的配置 / schema / 模型路由表
- ✅ stageflow **只暴露**通用原语 (DAG / Runtime / Ctx / State / Checkpoint / Protocol)
- ✅ ai_writer 通过 `StorageBackend` / `CallRecorder` / `TaskTrigger` Protocol **注入**自己的实现
- ✅ 任何其他项目 (RAG / 数据 pipeline / ETL / ML training / agent workflow) 都应**同等**使用 stageflow

**为什么**: stageflow 不绑业务 = 不会被业务拖死 (Uber Piper 反面教材, 8 年自研 EOL 2026 回流 Airflow). 这是 stageflow 的最大资产.

**开发纪律**:
- 改 stageflow 前先问: "这个改动对 ai_writer 之外的项目也有用吗?"
- 如果答案是否, 改 ai_writer 而不是 stageflow
- 如果答案是是, 加进 stageflow 但**通用化** (不能含 ai_writer 名字/术语/路径)
- docs 不能写 "stageflow 是 ai_writer 的子项目 / 配套工具 / 由 ai_writer 维护"
- examples/ 目录可以有 ai_writer 例子, 但必须有 ≥2 个其他 use case 平衡

**踩过的坑 (避免再犯)**:
- iter 5 v0.4 contrib storage adapters — 5 个 adapter (Sqlite/Postgres/MySQL/Redis/MinIO) vendor 进 stageflow, user 拍板反转 (c7f760b → 984ebfb). 反转理由: stageflow 不该替客户决定需要哪些 DB/S3 client. 教训: vendor adapter 是 over-engineering, user 自管 deps + 自写 adapter 更合适.

## 核心文档索引

- **架构**: `docs/design/architecture.md` — 5 原语 / Protocol 设计 / 边界
- **5 原语**: `docs/design/dag.md` / `runtime.md` / `state.md` / `checkpoint.md` / `protocols.md`
- **API 参考**: `docs/api.md`
- **路线**: `ROADMAP.md` (v0.2 freeze + 后续候选)
- **决策日志**: `docs/work-note/` (时序, 跨 session 沉淀)
- **变更历史**: `CHANGELOG.md`

## 项目结构

- 源码: `stageflow/` (git 跟踪)
- 部署: N/A — stageflow 是 library, 跟 caller 一起 ship
- 集成测试: 由 caller 项目承接 (library 无独立部署)

## 开发铁律

1. **stdlib only** — `pyproject.toml` 永远 `dependencies = []`. 0 第三方运行时依赖. (调研结论: zeroflow 同形竞品对齐)
2. **protocol 优先** — 跨项目差异走 Protocol (StorageBackend / CallRecorder / TaskTrigger), 不写默认实现
3. **YAGNI 严格** — 不加 dynamic DAG / subgraph / scheduler / UI / distributed / cron. (Uber Piper 反例)
4. **workflow_hash** — checkpoint 存 DAG 结构指纹, resume 时 mismatch 拒续跑
5. **YAGNI 上限** — M1 (state 观测) / M2 (replay fixture) / M3 (replay --prompt-patch) 是 v0.2 锁定范围. 不加 M4+

## 测试铁律

- 单测覆盖率 ≥ 80% (重点: DAG parser / state merge / retry_budget decrement)
- TestPipe fixture v0.1 第一天就有 (justpipe 启示)
- pytest markers: `unit` / `integration` / `slow` (integration 默认 skip)
- ruff 0 violations
- pyright strict 0 errors

## 发布铁律

- 语义化版本 (semver)
- v0.2 API freeze (见 ROADMAP.md)
- tag 必须 local + remote 一致
- CHANGELOG 每版本必写
- **push 永远由 user 决定**, AI 不主动 push

## 不做的事 (YAGNI 永久)

- ❌ 不引入重依赖 (Prefect / Airflow / Dagster 都是 100+ deps 的反面教材)
- ❌ 不做可视化 UI / Web server / scheduler (M4+ 候选, 不主动)
- ❌ 不做 conditional DAG (depends_on + retries 足够, 业务 while 在 stage 内)
- ❌ 不做 parallel stage (asyncio.gather 业务自管)
- ❌ 不做 distributed runtime (in-process 是核心定位)
- ❌ 不做 HITL (业务侧自己实现, 不进 stageflow)
- ❌ 不做 sub-DAG / nested DAG (循环在业务 super-node 内)
- ❌ 不做 plugin/entry_points 强制注册 (config string 足够, 借鉴 Kedro catalog)
