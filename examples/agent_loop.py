"""Example 2 — an agent-style convergence loop, expressed as business code.

pavoz has no framework-level "retry_budget" or loop DSL: a review →
fix → re-review loop is just a Python `while` inside one stage. The loop is
checkpointed at the node boundary — kill the process mid-loop and
`resume=True` restarts it, not from scratch, but from the review step.

Part 1 (`dag`) writes the loop inline.
Part 2 (`gate_dag`) factors the same pattern into a reusable `quality_gate`
decorator with **multiple review lenses**, a score threshold, strictest-lens
aggregation, and score events on `on_event` — the shape a production quality
gate takes. Pure stdlib: the reviewers here are plain functions, no LLM.

It also shows the debugging payoff: after this run you can fork it from
s_agent and re-run the *whole loop* on edited input without touching s_fetch:

    pavoz export-input examples/agent_loop.py --task-id agent-1 --stage s_agent
    pavoz fork-run      examples/agent_loop.py --task-id agent-1 --stage s_agent \\
        --overrides '{"topic": "new topic"}'

Run:  python examples/agent_loop.py
"""

import asyncio
import functools

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


# ── Part 2: the same loop, factored into a reusable gate decorator ───────────


def quality_gate(reviewers, revise, threshold, max_iter=3, on_exhaustion="raise"):
    """Wrap a stage fn in a review → score → revise loop.

    reviewers: {lens_name: async (result, ctx) -> (score, critique)}
    revise:    async (result, critique, ctx) -> result
    """
    assert reviewers and max_iter >= 1, "quality_gate: reviewers 非空 + max_iter >= 1"

    def deco(fn):
        @functools.wraps(fn)            # keep the stage name — dag registers fn.__name__
        async def wrapper(ctx):
            draft, best, best_score = await fn(ctx), None, -1.0
            for i in range(max_iter):
                scored = {name: await r(draft, ctx) for name, r in reviewers.items()}
                score = min(s for s, _ in scored.values())      # strictest lens decides
                if ctx.on_event:
                    ctx.on_event("gate_score", {
                        "stage": ctx.stage_name, "iter": i + 1, "score": score,
                        "lenses": {name: s for name, (s, _) in scored.items()},
                    })
                if score > best_score:
                    best, best_score = draft, score
                if score >= threshold:
                    return best
                if i < max_iter - 1:                            # no revise on the last pass
                    critique = "\n".join(c for _, c in scored.values())
                    draft = await revise(draft, critique, ctx)
            if on_exhaustion == "best_effort":
                return best
            raise StageError(f"gate exhausted: best={best_score:.1f} < {threshold} "
                             f"({max_iter} iters)")
        return wrapper
    return deco


def _lens(name, ok):
    """Stand-in reviewer: 9.0 when its single requirement is met, else 4.0."""
    async def review(result, ctx):
        passed = ok(result["report"])
        return (9.0 if passed else 4.0), (f"{name}: {'ok' if passed else 'missing'}")
    return review


async def _revise_report(result, critique, ctx):
    """Fix step: append whatever the lenses flagged."""
    fixes = []
    if "mentions-topic" in critique:
        fixes.append("Section: convergence and checkpointing.")
    if "has-signoff" in critique:
        fixes.append("SIGNED-OFF")
    return {**result, "report": result["report"] + " " + " ".join(fixes)}


gate_dag = DAG("agent_gate")


@gate_dag.stage()
async def g_fetch(ctx):
    return {"topic": ctx.state.get("topic", "durable agents")}


@gate_dag.stage(depends_on=["g_fetch"], retries=0)   # retries=0: the gate owns rework
@quality_gate(
    reviewers={
        "mentions-topic": _lens("mentions-topic",
                                lambda r: "convergence" in r.lower()),
        "has-signoff": _lens("has-signoff", lambda r: r.rstrip().endswith("SIGNED-OFF")),
    },
    revise=_revise_report,
    threshold=7.0,
    max_iter=3,
)
async def g_author(ctx):
    """Initial draft — the gate's reviewers decide whether it ships."""
    return {"report": f"Notes on {ctx.state['topic']}."}


@gate_dag.stage(depends_on=["g_author"])
async def g_publish(ctx):
    print(f"published after gate: {ctx.state['report']!r}")
    return {"published": True}


def _log_event(event: str, data: dict) -> None:
    """Observability: gate scores ride the same on_event stream as runtime events."""
    if event == "gate_score":
        print(f"  [gate] iter={data['iter']} score={data['score']} lenses={data['lenses']}")
    elif event == "stage_end":
        print(f"  [event] stage_end {data['stage']} status={data['status']}")


async def main() -> None:
    rt = Runtime(on_event=_log_event)

    result = await rt.run(dag, task_id="agent-1")
    assert result.status == "done"

    print()
    gate_result = await rt.run(gate_dag, task_id="agent-gate-1")
    assert gate_result.status == "done"


if __name__ == "__main__":
    asyncio.run(main())
