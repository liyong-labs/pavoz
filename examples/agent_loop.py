"""Example 2 — an agent-style convergence loop, expressed as business code.

pavoz has no framework-level "retry_budget" or loop DSL: a review →
fix → re-review loop is just a Python `while` inside one stage. The loop is
checkpointed at the node boundary — kill the process mid-loop and
`resume=True` restarts it, not from scratch, but from the review step.

It also shows the debugging payoff: after this run you can fork it from
s_agent and re-run the *whole loop* on edited input without touching s_fetch:

    pavoz export-input examples/agent_loop.py --task-id agent-1 --stage s_agent
    pavoz fork-run      examples/agent_loop.py --task-id agent-1 --stage s_agent \\
        --overrides '{"topic": "new topic"}'

Run:  python examples/agent_loop.py
"""

import asyncio

from pavoz import DAG, Runtime, StageError

dag = DAG("agent")


async def _review(topic: str, draft: str, attempt: int) -> dict:
    """Stand-in for a review LLM: passes when the draft mentions the topic
    and has been fixed at least once (so the loop converges visibly)."""
    ok = topic.lower() in draft.lower() and attempt >= 2
    return {"pass": ok, "note": f"attempt {attempt}: draft={len(draft)} chars"}


@dag.stage()
async def s_fetch(ctx):
    return {"topic": ctx.state.get("topic", "durable agents")}


@dag.stage(depends_on=["s_fetch"], retries=2, timeout=300)
async def s_agent(ctx):
    """The whole agent loop in one stage — plain Python control flow."""
    topic = ctx.state["topic"]
    draft = ""
    report = None
    for attempt in range(1, 4):                       # your max iterations
        draft = draft or f"First draft about {topic}."  # gen step (LLM here)
        review = await _review(topic, draft, attempt)   # review step (LLM here)
        if review["pass"]:
            return {"draft": draft, "verdict": "pass", "attempts": attempt}
        draft = draft + f" [fixed after {review['note']}]"   # fix step
    raise StageError("did not converge in 3 attempts")


@dag.stage(depends_on=["s_agent"])
async def s_save(ctx):
    print(f"verdict={ctx.state['verdict']} attempts={ctx.state['attempts']}")
    print(ctx.state["draft"])
    return {"saved": True}


async def main() -> None:
    rt = Runtime()
    result = await rt.run(dag, task_id="agent-1")
    assert result.status == "done"


if __name__ == "__main__":
    asyncio.run(main())
