# ai_research 5 stage 详细设计 (2026-09-07)

> 配套: [PRD.md](PRD.md) / [pavoz-requirements.md](pavoz-requirements.md)
> 设计依据: [handoff/ai-research/competitive-analysis.md](handoff/ai-research/competitive-analysis.md) §3 / §10 / §11 / §12

---

## DAG 骨架

```
START
  → s_clarify              # ODR 模式: structured {need, question, verification}
  → s_warm_search          # ODR grounded planning: 先真搜 1 轮
  → s_personas             # STORM: 2 calls 产出 3-5 persona
  → s_research_loop × N    # gpt-researcher 三元组 + §12 论证层
  → s_render_map           # §8 证据分级地图
END

外层 RevisionLoop:
  generate  = s_research_loop 一轮
  critique  = 评估 frontier 边际 (新 learnings 数 + followup 相似度)
  converged = 边际归零 / followup 全相似
  no_progress = 连续 2 轮新发现 < 阈值 → 熔断, 输出"已知未知"段
```

---

## Stage 1: `s_clarify` (ODR 模式)

**目的**: 用户 query 进入 DAG 第一关, 决定是直接搜还是等用户回复

**触发**: 每次 query 进入 DAG (CLI `--query` / future web UI)

**输入**: `state["query"]` (str), `state.get("messages", [])` (历史)

**LLM call**: `op="clarify"` → structured output

```python
{
  "need_clarification": bool,
  "question": str,         # 仅 need=True 时非空
  "verification": str,     # LLM 自己答的问题 (让用户 verify 我们的理解对)
  "verified_query": str    # 改写后的精确 query (用户原话基础上)
}
```

**输出**:
- `need=True` → `state["pending_clarification"] = {question, verification}` + task 置 `waiting_user_input`
- `need=False` → `state["query"] = verified_query`, 进入 s_warm_search

**LLM 档位**: SMART (clarify 推理要稳但不必极致)

**失败处理**: LLM_NO_FALLBACK — structured output 解析失败 → 视为 need=False, 用原 query 进 s_warm_search

**借鉴 / 不抄**:
- 抄 ODR 的 structured-output 门禁 + verification 字段
- 不抄 ADR 的 "NEVER ASK" (用户原话"挖不到就标挖不到", 但 clarify 不算挖, 是用户主动改 query 的窗口)
- 不抄 STORM Co-STORM 完整 turn 协议 (过重)

---

## Stage 2: `s_warm_search` (ODR grounded planning)

**目的**: 不先列 plan, 先真搜 1 轮, 拿到 ground truth 再扩

**触发**: s_clarify 输出 verified_query

**输入**: `state["query"]`

**流程**:
1. gen_seed_queries (STRATEGIC 档 LLM) → 3-5 个 seed query 覆盖主轴
2. 并发跑 search:
   ```python
   for q in seed_queries:
       async with semaphore(3):
           results = await safe_search(q, top_k=8)  # FALLBACK_ORDER = [searxng, duckduckgo]
           compressed = await compress_evidence(results, goal=q)  # ODR 风格 25-30% 摘要
           sources.extend(parse_sources(results))
   ```
3. dedupe by URL
4. 写 cp

**LLM 档位**:
- STRATEGIC: gen_seed_queries
- FAST: compress_evidence (parallel)

**输出**:
```python
{
    "warm_results": list[SearchResult],  # O(30) sources
    "seed_queries": list[str],            # 给 s_personas 用
    "compressed_corpus": str,             # 给 s_personas prompt 作 context
}
```

**失败处理**:
- safe_search raise SearchExhausted → LLM_NO_FALLBACK 报错, DAG 终止, 让用户改 query
- compress 失败 → 跳过该 query 的压缩, 但 sources 仍入 (用户可手动降级)

**借鉴 / 不抄**:
- 抄 ODR `compress_evidence` (mini 模型 + key_excerpts 25-30%)
- 抄 gpt-researcher `requires_scraping` 协议 (snippet vs 全文分流)
- 抄 gpt-researcher `FALLBACK_ORDER` 多 retriever (但 M0 只 2 个, 不引 Tavily)
- 不抄 STORM "只取 snippet" (snippet-only 信息量不够)

---

## Stage 3: `s_personas` (STORM 多视角)

**目的**: 多视角并发挖, 避免单线偏差

**触发**: s_warm_search 输出 warm_results

**输入**: `state["query"]`, `state["warm_results"]`, `state["compressed_corpus"]`

**LLM calls**: 2 calls
1. STRATEGIC 档 `gen_personas` → 3-5 persona, 每个有:
   ```python
   {
       "persona_id": str,         # p_001, p_002, ...
       "focus": str,              # 这条 persona 关注什么
       "motivation": str,          # 假设的视角动机 (用户角色 / 厂商视角 / 学术 ...)
       "expertise": str,          # 专长领域
   }
   ```
2. SMART 档 `assign_seed_to_persona` → 把 warm_results 按 persona 切 (不是物理切, 是 persona 优先级 list)

**输出**:
```python
{
    "personas": list[Persona],  # 3-5 个
    "seed_assignment": dict[persona_id, list[seed_query_id]],
}
```

**借鉴 / 不抄**:
- 抄 STORM persona pattern
- 不抄 STORM 固定 4 视角 × 3 turn (太死板)
- 不抄 dspy Signature 抽象 (过工程化, 我们用 raw prompt)

---

## Stage 4: `s_research_loop` (gpt-researcher + §12 论证层)

**目的**: 每 persona 跑循环, 直到 frontier 饱和; 含反向证伪 + 矛盾检测

**触发**: s_personas 输出 personas

**输入**: 上述所有 state + warm_results 作为起点

**循环** (`while not converged`):

```python
for persona in personas:
    # A. 前向 (gpt-researcher 三元组)
    q = await gen_query(persona, open_questions)  # STRATEGIC 档
    results = await safe_search(q)              # FALLBACK
    compressed = await compress_evidence(results, goal=q)  # FAST 档
    new_learnings = await extract_learnings(compressed, persona)  # SMART 档
    for nl in new_learnings:
        nl["src_ids"] = [map_to_ref(r.url) for r in results[:3]]
        nl["persona_id"] = persona.id
        nl["grade"] = initial_grade(nl, sources)
        nl["as_of_date"] = max(r.date for r in results)
    learnings.extend(new_learnings)
    
    # B. 反向证伪 (新 insight 才跑, 节省)
    if new_learnings:
        last = new_learnings[-1]
        counter_q = await gen_counter_evidence(last.insight, last.src_ids)  # STRATEGIC
        counter_results = await safe_search(counter_q)
        counter_learnings = await extract_learnings(compress(counter_results), mode="counter")
        for cl in counter_learnings:
            cl["contradicts"] = [last.insight_id]
            cl["counter_searched"] = True
        learnings.extend(counter_learnings)
    
    # C. 矛盾检测 (跨 persona 跑, 学习后做)
    for nl in new_learnings:
        contradicts = detect_contradiction(nl, learnings)
        for c in contradicts:
            contradictions.append({
                "claim_a": nl.insight, "claim_b": c.insight,
                "src_a": nl.src_ids, "src_b": c.src_ids,
            })

# D. 升级 (≥2 独立域 → 升一档)
for nl in learnings:
    nl = upgrade_grade(nl, learnings)
```

**LLM 档位**:
- STRATEGIC: gen_query, gen_counter_evidence (推理深度)
- FAST: compress_evidence
- SMART: extract_learnings

**停止条件** (`converged`):
- 连续 2 轮 new_learnings < 阈值 (默认 2)
- frontier embedding 饱和 (新 query 全部与已有 query cosine > 0.85)
- 反向证伪全部空手 (即所有 claim 都被支持, 不再有反方)

**输出**:
```python
{
    "learnings": list[Learning],
    "contradictions": list[Contradiction],
    "open_questions": list[OpenQuestion],   # 留给下一轮
    "iterations": int,
    "converged_reason": str,  # "no_progress" / "embedding_saturated" / "all_supported"
}
```

**借鉴 / 不抄**:
- 抄 gpt-researcher `deep_research` 三元组 (goal → learnings → followup → loop)
- 抄 gpt-researcher `followUpQuestions` 思路, 但**改方向为反向** (gen_counter_evidence)
- 抄 STORM `KnowledgeBaseNode` 概念, 但作为 state field 而非新原语
- 不抄 ODR `compress_research_system_prompt` 长度限制 (我们用 key_excerpts 替代)
- 不抄 gpt-researcher 纯预算熔断 (我们用 frontier 饱和)
- 不抄 ADR `or True` 吞错

---

## Stage 5: `s_render_map` (证据地图)

**目的**: 把 frontier state 渲染成 5 段 markdown, [ref:xxxx] 全量引用

**触发**: s_research_loop 输出 learnings + contradictions

**输入**: learnings + contradictions + sources + query

**5 段产物**:

```markdown
# 课题: <query>

## ✅ 已证实 (T1/T2, ≥1 源)
- [T2] Qwen3-4B GGUF Q4 在 3080 10G 实测显存 8.2GB — [ref:79aa] HF model card

## 🟡 社区声称 (T3/T4, 单一源)
- [T4] VibeThinker-3B Q4 在 10G 显存跑得动 — [ref:b778] 单一博客, 缺第二源

## 🔵 理论可跑 (推算无实测)
- 7B Q2 (量化损失大) — 推算基于 [ref:79aa] 模型尺寸 + [ref:08f9] ollama 量化规则

## ⚠️ 已知未知 (挖不到, 诚实标)
- 3080/10G 跑 13B Q4 的具体推理速度 (搜 12 轮无 benchmark)

## 矛盾仲裁 (挖到时显式呈现, 不允许"忽略")
- claim A "Q4 量化无可见精度损失" [ref:79aa] vs claim B "Q4 量化下降" [ref:3fa4]
  → A 权威 + 时效新; B 仅 ≤2B 模型, 不可外推 → A 胜

## Sources
[ref:79aa] HF Qwen3-4B-GGUF model card
```

**citation_precheck** (用 ai_writer 同款):
```python
for line in md_lines:
    if not line.startswith("#") and not line.startswith("##"):
        refs = extract_refs(line)
        assert refs, f"line lacks [ref:xxxx]: {line}"
```

**grade 分级**:
- T1 (官方文档/原始数据)
- T2 (权威媒体/可信社区)
- T3 (博客/个人)
- T4 (社交/单源)
- T5 (无源, 拒绝入库)

**输出**: 单个 markdown 文件 + `.json` (state dump)

**LLM 档位**: 无 (纯模板渲染)

**借鉴 / 不抄**:
- 抄 ai_writer 5 段 + Sources 形式
- 抄 STORM index 一致性 (但我们是 line-level, 不是 doc-level)
- 不抄 ODR Sources 段编号 (我们用 4hex ref id)
- 不抄 STORM mind map (M1+ 再考虑)

---

## 状态 schema (research/state.py)

```python
from typing import Literal, TypedDict

Grade = Literal["T1", "T2", "T3", "T4", "T5"]  # 复用 ai_writer

class Source(TypedDict):
    src_id: str          # 4hex, [ref:xxxx]
    url: str
    title: str
    domain: str
    grade: Grade
    as_of_date: str      # ISO

class Learning(TypedDict):
    insight_id: str      # 4hex, [L:xxxx]
    insight: str
    src_ids: list[str]
    grade: Grade         # 取 src_ids 中最弱源为 grade
    persona_id: str
    as_of_date: str
    # === 论证层 (4 仓 + ai_writer 都没做的) ===
    staleness_score: float    # 0-1
    supports: list[str]       # 其他 insight_id
    contradicts: list[str]    # 其他 insight_id
    counter_searched: bool
    counter_evidence: list[str]

class OpenQuestion(TypedDict):
    text: str
    raised_by: str           # persona_id / round

class SearchedQuery(TypedDict):
    query: str
    goal: str                # researchGoal
    embedding: list[float]   # 用于饱和判
    persona_id: str

class Contradiction(TypedDict):
    claim_a: str
    src_a: list[str]
    claim_b: str
    src_b: list[str]
    verdict: str             # "a" / "b" / "abstain"
    rationale: str
```

---

## 失败处理总则 (LLM_NO_FALLBACK)

| 层 | 失败 | 处理 |
|---|---|---|
| LLM call | 结构化输出失败 | retry 1 次, 仍失败 → raise (不静默 fallback) |
| 搜索 | 全部 backend raise | raise SearchExhausted (不静默返回空) |
| 压缩 | FAST LLM 失败 | skip 该 query (sources 仍入), 不静默降级 |
| 渲染 | citation_precheck 失败 | raise (产物拒绝落盘) |
| 反向证伪 | counter_q 不存在 | skip 该 learning (记 counter_searched=False) |

---

(全局 work-note 铁律见 `~/.claude/common/claude-common.md`)
