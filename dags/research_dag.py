"""ai-research M0 DAG. 课题 → frontier 挖掘 → 证据分级地图.

骨架:
  s_clarify → s_warm_search → s_personas → s_research_loop × N → s_render_map

外层 RevisionLoop 由 stageflow 调用方控制 (stage 内 Python while).
停止: 连续 N 轮新 learnings < 阈值 或 followup 全相似.

设计依据: docs/handoff/ai-research/competitive-analysis.md §3.
"""
from __future__ import annotations

import asyncio
try:
    from stageflow import dag, Context  # 部署机 editable install
except ImportError:
    from ai_research.research.stageflow_stub import dag, Context  # 本地 Pyright

from ai_research.research.state import Learning, OpenQuestion


# === 1. clarify: ODR 模式 structured-output 门禁 ===
@dag.stage
async def s_clarify(ctx: Context) -> dict:
    state = ctx.state
    query = state["query"]

    # 一次 LLM call 输出 {need_clarification, question, verification}
    result = await ctx.call(
        "llm",
        op="clarify",
        params={"query": query, "history": state.get("messages", [])},
    )

    if result["need_clarification"]:
        # ODR 同款: 把 question 当 AIMessage 吐给用户, task 置 waiting_user_input
        return {
            "pending_clarification": {
                "question": result["question"],
                "verification": result["verification"],
            },
            # 不动其他字段, 等用户回复走 revise 通道合并进 brief
        }

    return {
        "research_brief": result["verification"],  # "我理解的是 X" = 唯一意图载体
        "pending_clarification": None,
    }


# === 2. warm_search: ODR grounded planning, 先真搜一轮 ===
@dag.stage
async def s_warm_search(ctx: Context) -> dict:
    """真搜 1 次拿 SERP, 喂给后续 personas / clarify 校准.
    ponytail: this exists — gpt-researcher 证明 grounded planning 比裸 query 强.
    """
    state = ctx.state
    brief = state["research_brief"]

    serp = await ctx.call(
        "search",
        op="searxng",
        params={"query": brief, "top_k": 8},
    )
    # 把 SERP 当已知上下文, 给 personas 阶段用
    return {"serp_snapshot": serp[:8]}


# === 3. personas: STORM 2 calls 产出 3-5 视角 ===
@dag.stage
async def s_personas(ctx: Context) -> dict:
    """找 3-5 个视角, 每视角一条独立研究线程. ponytail: hardcode 4 视角而非动态生成."""
    state = ctx.state
    brief = state["research_brief"]
    serp = state.get("serp_snapshot", [])

    # 抄 STORM FindRelatedTopic + GenPersona, 但用一次 LLM call 完成
    personas = await ctx.call(
        "llm",
        op="gen_personas",
        params={
            "brief": brief,
            "serp_titles": [s["title"] for s in serp[:5]],
            "max": 4,
        },
    )
    return {"personas": personas}


# === 4. research_loop: gpt-researcher 三元组核心, fan-out per persona ===
@dag.stage
async def s_research_loop(ctx: Context) -> dict:
    """每 persona 并发跑 research_loop:
       goal → search → distill → learnings → frontier_update → followup → next goal.
    RevisionLoop 形式: 内部 while, 直到 saturated.
    ponytail: this exists — gpt-researcher deep_research.py 是行业最值得抄的循环 (~200 行).
    """
    state = ctx.state
    personas = state["personas"]
    brief = state["research_brief"]
    serp = state.get("serp_snapshot", [])

    # 并发跑每个 persona
    persona_results = await asyncio.gather(*[
        _persona_loop(ctx, p, brief, serp, max_turns=3)
        for p in personas
    ])

    # 合并所有 persona 的 learnings + 追问
    new_learnings: list[Learning] = []
    new_open_questions: list[OpenQuestion] = []
    for r in persona_results:
        new_learnings.extend(r["learnings"])
        new_open_questions.extend(r["open_questions"])

    # frontier 更新 (单 key producer: learnings)
    existing = state.get("learnings", [])
    deduped = _dedupe_learnings(existing + new_learnings)

    # 矛盾检测 (新 vs 老) — 抄 STORM "同一事实不同源不同结论"
    contradictions = _detect_contradictions(new_learnings, existing)

    return {
        "learnings": deduped,
        "open_questions": new_open_questions,
        "contradictions": contradictions + state.get("contradictions", []),
        "iteration": state.get("iteration", 0) + 1,
    }


async def _persona_loop(
    ctx: Context, persona: dict, brief: str, serp: list, max_turns: int,
) -> dict:
    """单 persona 的研究循环. 抄 deep_research 三元组."""
    learnings: list[Learning] = []
    open_questions: list[OpenQuestion] = []

    goal = await ctx.call(
        "llm",
        op="initial_goal",
        params={"brief": brief, "persona": persona, "serp": serp[:3]},
    )

    for _ in range(max_turns):
        # 1. 生成 queries (goal → query)
        queries = await ctx.call(
            "llm",
            op="gen_queries",
            params={"goal": goal, "learned_so_far": learnings[-5:], "max": 3},
        )

        # 2. 并发搜 (SearXNG)
        results = await asyncio.gather(*[
            ctx.call("search", op="searxng", params={"query": q, "top_k": 5})
            for q in queries
        ])

        # 3. distill: 每结果 → ODR 风格 compress (逐字保 key_excerpts)
        compressed = await ctx.call(
            "llm",
            op="compress_evidence",
            params={"goal": goal, "results": [r for batch in results for r in batch]},
        )

        # 4. 抽取 learnings (绑 src_ids, 落条目级 grade)
        new = await ctx.call(
            "llm",
            op="extract_learnings",
            params={
                "compressed": compressed,
                "persona": persona,
                "existing_insights": [l["insight"] for l in learnings],
            },
        )
        learnings.extend(new["learnings"])

        # 5. 产出 followup (下一轮 goal = 当前 goal + followup)
        if not new["followup_questions"]:
            break  # LLM 自评: 够了
        goal = f"Goal: {goal}\nFollowups: {new['followup_questions']}"
        open_questions.extend(
            [{"text": q, "raised_by": persona["id"]} for q in new["followup_questions"]]
        )

    return {"learnings": learnings, "open_questions": open_questions}


def _dedupe_learnings(learnings: list[Learning]) -> list[Learning]:
    """insight 字面去重, 同 insight 取 grade 最强 (T1 > T2 > ...) 的版本."""
    seen: dict[str, Learning] = {}
    grade_rank = {"T1": 5, "T2": 4, "T3": 3, "T4": 2, "T5": 1}
    for l in learnings:
        key = l["insight"].strip().lower()
        if key not in seen or grade_rank[l["grade"]] > grade_rank[seen[key]["grade"]]:
            seen[key] = l
    return list(seen.values())


def _detect_contradictions(
    new: list[Learning], existing: list[Learning],
) -> list[list]:
    """STORM Moderator 同款: 同一议题多源不同结论 → contradictions 段."""
    # ponytail: 极简版 — 仅做"同关键词 + 反向词"启发, 不上 NLI
    contradicts = []
    reverse_pairs = [
        ("能跑", "不能跑"), ("可以", "不可以"), ("支持", "不支持"),
        ("是", "不是"), ("有", "没有"), ("快", "慢"), ("小", "大"),
    ]
    for n in new:
        for e in existing:
            for a, b in reverse_pairs:
                if a in n["insight"] and b in e["insight"] and n["insight"][:20] == e["insight"][:20]:
                    contradicts.append([n["insight"], n["src_ids"][0], e["insight"], e["src_ids"][0]])
                    break
    return contradicts


# === 5. render_map: 产物 = 证据分级地图 ===
@dag.stage
async def s_render_map(ctx: Context) -> dict:
    """✅ 已证实 / 🟡 社区声称 / 🔵 理论可跑 / ⚠️ 已知未知 / 矛盾. 每条挂 [ref:xxxx]."""
    state = ctx.state
    learnings = state.get("learnings", [])
    open_questions = state.get("open_questions", [])
    contradictions = state.get("contradictions", [])

    # 按 grade 分桶
    buckets: dict[str, list[Learning]] = {"T1": [], "T2": [], "T3": [], "T4": [], "T5": []}
    for l in learnings:
        buckets[l["grade"]].append(l)

    # 调用 LLM 渲染, prompt 注入 5 段模板 + 已知未知段
    report = await ctx.call(
        "llm",
        op="render_evidence_map",
        params={
            "query": state["query"],
            "brief": state["research_brief"],
            "buckets": buckets,
            "open_questions": open_questions,
            "contradictions": contradictions,
        },
    )
    return {"final_report": report}


# === DAG 注册 ===
research_dag = dag.DAG(
    name="ai_research_v0",
    stages=[
        s_clarify,
        s_warm_search,
        s_personas,
        s_research_loop,
        s_render_map,
    ],
)