"""ai-research 状态 schema. json-serializable, stageflow cp 落盘用.

设计依据: gpt-researcher deep_research 三元组 (goal/learnings/followup)
+ ODR research_brief 唯一意图载体 + 用户 T1-T5 条目级分级.
"""
from __future__ import annotations

from typing import Literal, TypedDict

Grade = Literal["T1", "T2", "T3", "T4", "T5"]  # 复用 ai_writer 五档分级


class Source(TypedDict):
    src_id: str          # 4hex, [ref:xxxx] 同款
    url: str
    title: str
    domain: str
    grade: Grade


class Learning(TypedDict):
    insight: str         # 一句话洞察
    src_ids: list[str]   # 绑源 (条目级, 不是文章级)
    grade: Grade         # 取 src_ids 中最弱源为条目 grade
    persona_id: str      # 哪条 persona 线挖出的


class OpenQuestion(TypedDict):
    text: str
    raised_by: str       # 哪轮 / 哪个 persona


class SearchedQuery(TypedDict):
    query: str
    goal: str            # researchGoal: 这条查询想搞清楚什么
    embedding: list[float]  # 用于饱和判 (与已搜相似度)
    persona_id: str


class Contradiction(TypedDict):
    claim_a: str
    src_a: str
    claim_b: str
    src_b: str


class FrontierState(TypedDict, total=False):
    """stageflow state 单 key producer 约束: 全程 frontier_update 一个 stage 写."""
    query: str
    research_brief: str
    personas: list[dict]
    learnings: list[Learning]
    open_questions: list[OpenQuestion]
    searched_queries: list[SearchedQuery]
    contradictions: list[list]          # Contradiction 不参与 merge, 序列
    pending_clarification: dict | None
    iteration: int
    saturated: bool
    known_unknowns: list[str]