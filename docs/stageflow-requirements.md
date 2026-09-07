# ai_research 对 stageflow 框架的需求 (2026-09-07)

> 配套: [PRD.md](PRD.md) / [design.md](design.md)
> 设计依据: [handoff/ai-research/competitive-analysis.md](handoff/ai-research/competitive-analysis.md) §9

---

## 0. 命名约定 (2026-09-07)

| 层级 | 名字 | 用途 |
|---|---|---|
| **对外产品 / GitHub 仓库 / PyPI** | **`pavoz`** | ai_research 项目在 GitHub 上的仓库名 + 部署服务名 + 用户可见的 pip package 名 |
| **底层框架实现** | `stageflow` | `/home/ai/stageflow/` Python 库 (in-process workflow engine), ai_research 的 DAG 运行时 |
| **架构角色** | "orchestrator" | ai_research 项目里 stageflow 起的作用 (DAG 编排 + checkpoint + fork) |

**Why 两层命名**:
- `stageflow` 是通用库, 已被部署机其他项目用 (aics-platform / ai_writer 等)
- `pavoz` 是 ai_research 项目专属对外名, GitHub 0 完全同名 repo (18 描述命中, claim + graph 在 NLI/事实核查圈是核心术语, 听一次就懂), 2026-09-07 实测
- ai_research 文档/PR/commit 默认用 `pavoz`, 内部 doc 引用 stageflow 实现细节

**为什么叫 pavoz**:
- `claim` (断言) = deep-research 的核心产出 (5 段证据地图里每条都是 claim)
- `graph` (图) = learning 互相 supports/contradicts 关系图 (§12 论证层)
- 学界 (NLI / fact-checking) + 工程 (knowledge graph) 双圈都懂

**冲突解决**:
- PyPI 上传: `pip install pavoz` (顶层) → depends on `stageflow` (底层)
- 代码 import: `from stageflow import DAG, Runtime` (不变)
- README 开头: "# pavoz — powered by stageflow"
- GitHub repo: `liyong-labs/pavoz`

---

## 1. 现状: stageflow 已提供什么

stageflow (部署机 `/home/ai/stageflow/`, 仓未同步本地) 已支持:

| 原语 | API | ai_research 怎么用 |
|---|---|---|
| DAG | `@dag.stage` 装饰器 | 5 stage 装饰 |
| ctx.call | `await ctx.call("llm", op="...", params={...})` | 跨 stage 调 LLM, op 路由到 STRATEGIC/SMART/FAST 3 档 |
| state | `ctx.state: TypedDict` | stage 间传 learnings/contradictions/sources |
| checkpoint | `cp.json` + latest 指针 | s_research_loop 每轮 cp 落盘, 支持 fork_run |
| fork_run | `fork_run(from_stage, overrides)` | 用户加 query 时合并 frontier |
| per-node retry | 内建 | LLM call 失败 retry 1 次 |

**M0 阶段: ai_research 不动 stageflow 一个原语** — 所有扩展在 ai_research 仓内自建 (`research/loop.py` / `llm_router.py` / `knowledge.py` / `merge.py`)。

---

## 2. M0 阶段自建 (在 ai_research 仓内, 不污染 stageflow)

### 2.1 RevisionLoop + no_progress

**位置**: `research/loop.py`

```python
# 跨 stage 控循环
async def run_research_loop(ctx, personas, frontier, *, max_rounds=10, no_progress_threshold=2):
    no_progress_count = 0
    for round_n in range(max_rounds):
        prev_size = len(frontier["learnings"])
        for persona in personas:
            frontier = await one_round(ctx, persona, frontier)
        new_size = len(frontier["learnings"])
        if new_size - prev_size < no_progress_threshold:
            no_progress_count += 1
            if no_progress_count >= 2:
                frontier["converged_reason"] = "no_progress"
                break
        else:
            no_progress_count = 0
    return frontier
```

### 2.2 STRATEGIC LLM 注入 (tier routing)

**位置**: `research/llm_router.py`

```python
LLM_TIERS = {
    "FAST":      "deepseek-v3-flash",
    "SMART":     "deepseek-v4",
    "STRATEGIC": "deepseek-r1",
}

async def strategic_call(prompt, *, reasoning_effort="high"):
    """STRATEGIC 默认 R1 (无 fn-call). 抄 ADR fn_call_converter ~150 行降级层."""
    if LLM_TIERS["STRATEGIC"] in {"deepseek-r1", "o1", "o3-pro"}:
        text_prompt = _tools_to_text_suffix(prompt)
        return await _call_text(LLM_TIERS["STRATEGIC"], text_prompt, reasoning_effort)
    return await _call_fn(prompt)
```

### 2.3 StateEmbedding 旁路

**位置**: `research/state.py` 加 `embedding: list[float]` 字段 (沿用 SearchedQuery)

```python
class SearchedQuery(TypedDict):
    query: str
    goal: str
    embedding: list[float]  # 饱和判 (与已搜相似度 > 0.85 跳过)
    persona_id: str
```

### 2.4 SoftTarget (饱和熔断)

**位置**: `research/loop.py` 内嵌

```python
async def is_frontier_saturated(frontier, new_query) -> bool:
    new_emb = await embed(new_query)
    for sq in frontier["searched_queries"]:
        sim = cosine_sim(new_emb, sq["embedding"])
        if sim > 0.85:
            return True
    return False
```

### 2.5 KnowledgeBaseNode (state 字段化)

**位置**: `research/state.py` 直接在 TypedDict 表达 supports/contradicts

```python
class Learning(TypedDict):
    insight_id: str
    insight: str
    src_ids: list[str]
    grade: Grade
    supports: list[str]       # L_id 印证
    contradicts: list[str]    # L_id 反驳
    counter_searched: bool
```

### 2.6 fork_run merge_frontier

**位置**: `research/merge.py`

```python
async def merge_frontier(prev_state: dict, new_results: list[Learning]) -> dict:
    """跨 session 跑同 query 时合并 frontier.
    Ponytail: dict lookup, 不上向量检索 (量级 O(100)).
    """
    seen_insights = {l["insight_id"] for l in prev_state["learnings"]}
    new_learnings = [l for l in new_results if l["insight_id"] not in seen_insights]
    prev_state["learnings"].extend(new_learnings)
    return prev_state
```

---

## 3. M1+ 候选: TaskInterrupt (durable)

**为什么值得沉淀 stageflow**: 跨项目 ai_writer + ai_research 都需要"等用户回复"路径

| 项目 | 路径 | 出现频率 |
|---|---|---|
| ai_writer | ReviseChannel (用户改产物) | 高频 |
| ai_research | s_clarify 等待用户反问 | 中频 |

**前置条件** (M1 才提 PR):
1. 核验 ai_writer revise 路径是真痛 (work-note 已有 ReviseChannel 通道)
2. ai_research s_clarify 跑 ≥5 次生产数据, 看 wait 路径是否真的频繁

**API 提议**:

```python
@dag.stage(interruptible=True)  # 新原语
async def s_clarify(ctx):
    result = await ctx.call("llm", op="clarify", ...)
    if result["need_clarification"]:
        ctx.interrupt(
            reason="awaiting_user_input",
            question=result["question"],
            resume_token=ctx.token(),  # 用户回复后凭 token 续跑
        )
    return result

# 续跑 (用户回复后)
await dag.resume(s_clarify, token, override={"user_answer": "..."})
```

**stageflow 改动**:
- `dag.stage(interruptible=True)` 装饰器
- `ctx.interrupt()` API
- `dag.resume(stage, token, overrides)` CLI

---

## 4. M2+ 候选 (不写具体方案)

- **stream output**: 用户想看 frontier 边挖边长 (UI 阶段才真有用)
- **multi-tenant ctx**: 不同用户并发 (web 阶段)
- **LLM provider abstraction**: M0 写死 3 档 + 抄 ADR fn_call_converter, 不引 litellm

---

## 5. 不动 stageflow 的 6 项原语 (M0 自建, 见 §2)

| 原语 | 为什么不沉淀 |
|---|---|
| RevisionLoop + no_progress | ai_writer 已有 audit cascade, 不通用 |
| STRATEGIC LLM 注入 | ai_writer 单 LLM 不分档, 跨项目不通用 |
| StateEmbedding 旁路 | ai_writer state 无 embedding 字段 |
| SoftTarget | ai_writer 单数值收敛, 不通用 |
| KnowledgeBaseNode | ai_writer 素材池是 URL 列表, 不是 evidence graph |
| fork_run merge_frontier | ai_writer fork 是"改输入重跑", 跨 session 合并是 ai_research 独有 |

**M0 决策**: 跨项目不通用 → 在 ai_research 仓内实现, 不污染 stageflow.

---

## 6. 与 stageflow 维护者的协作约定

1. ai_research 不直接 PR 到 stageflow (避免 scope 蔓延)
2. 任何 stageflow 改动由 stageflow owner 评估 (M1+ 提 TaskInterrupt PR)
3. ai_research 复用 stageflow 时遇到痛点 → 写 work-note, 在 stageflow 0.x 路线图 review 时提
4. 文档镜像: ai_research `docs/stageflow-requirements.md` 与 stageflow `README.md` 互相 link

---

(全局 work-note 铁律见 `~/.claude/common/claude-common.md`)
