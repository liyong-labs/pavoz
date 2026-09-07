"""ai_research LLM 路由. 4 模型 3 档 + 1 fallback. ponytail: this exists.

用户原话 (2026-09-07):
- minimax M3 = 主力 (token plan)
- GLM v5.3 flash = 深入思考
- deepseek v4 flash = 关键位置
- longcat 2.0 = minimax 伙伴 (fallback)
"""
from __future__ import annotations
import os
from typing import Literal

Tier = Literal["FAST", "SMART", "STRATEGIC"]

# 模型配置 (PRD §11 + design §5.3)
MODELS: dict[str, dict] = {
    "FAST": {
        "model": "minimax-m3",        # 主力, token plan, 摘要
        "max_tokens": 500,
        "temp": 0.1,
    },
    "SMART": {
        "model": "deepseek-v4-flash", # 关键位置, extract_learnings / render_map
        "max_tokens": 2000,
        "temp": 0.3,
    },
    "STRATEGIC": {
        "model": "glm-v5.3-flash",    # 深入思考, gen_query / counter_evidence / personas
        "max_tokens": 4000,
        "temp": 0.3,
        "reasoning_effort": "high",  # GLM v5.3 flash 支持 reasoning effort
    },
}

# Fallback (FAST 同档备用, 当 minimax 失败/限速时降级)
FALLBACK_FAST = {
    "model": "longcat-2.0",
    "max_tokens": 500,
    "temp": 0.1,
}


def _get_api_base() -> str:
    """统一 base URL. ai_writer 已有 env var 兼容. ponytail: env-first, hardcode fallback."""
    return os.environ.get("LLM_API_BASE", "https://api.deepseek.com")


async def call(tier: Tier, *, prompt: str, **overrides) -> str:
    """调对应档 LLM. fallback 默认关 (失败 raise, 不静默, LLM_NO_FALLBACK 铁律).

    用法:
        answer = await call("STRATEGIC", prompt=query)
        summary = await call("FAST", prompt=text, max_tokens=300)  # 临时覆盖
    """
    config = {**MODELS[tier], **overrides}
    return await _dispatch(config, prompt, _get_api_base())


async def _dispatch(config: dict, prompt: str, base: str) -> str:
    """实际 LLM call. 由 ai_writer BaseLLM 适配 (复用, 不重写)."""
    # TODO: 接 ai_writer backend/core/llm_providers/base.py 的 call 协议
    # M0 stub: raise NotImplementedError, 等接入
    raise NotImplementedError(
        f"LLM call 待接入 ai_writer BaseLLM. model={config['model']}, base={base}, "
        f"prompt[:50]={prompt[:50]!r}"
    )


# === 工具函数 ===

def estimate_cost_per_query() -> dict:
    """单 query 成本估 (74 calls, 估 ¥0.5).

    Returns:
        dict with per-tier breakdown
    """
    # 实测优先于理论 — M0 跑通后用真实 token 消耗反推校准
    return {
        "FAST":      {"calls": 24, "model": "minimax-m3",        "est_yuan": 0.024},
        "SMART":     {"calls": 25, "model": "deepseek-v4-flash", "est_yuan": 0.20},
        "STRATEGIC": {"calls": 25, "model": "glm-v5.3-flash",    "est_yuan": 0.25},
        "total_yuan": 0.474,
        "note": "flash 版成本. M0 跑通后用真实 token 消耗反推校准",
    }


__all__ = ["call", "MODELS", "FALLBACK_FAST", "estimate_cost_per_query", "Tier"]