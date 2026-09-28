"""Example — conditional edges: a quality-gate loop with rework.

The pipeline refines a draft until a checker accepts it (or the visit cap
fuses the loop). Demonstrates the four things conditional edges give you:

1. declare routes once on the DAG — stage functions stay pure data handlers
2. loop back to an already-executed stage: the static chain re-runs (new
   visit each round), capped by max_visits
3. single-point debugging: run_stage replays one stage, fork_run re-runs
   from any executed stage with injected overrides — no full restart
4. observability: route events expose every decision (chosen key, the full
   declared key set → what was skipped)

Run:  python examples/conditional_loop.py
"""

import asyncio
import tempfile
from pathlib import Path

from pavoz import CheckpointStore, DAG, FileStorage, Runtime

dag = DAG("refinery")

ROUNDS = {"build": 0, "check": 0}   # in-process call counters (demo only)


@dag.stage()
async def s_fetch(ctx):
    return {"draft": "rough draft v0"}


@dag.stage(depends_on=["s_fetch"])
async def s_build(ctx):
    ROUNDS["build"] += 1
    return {"draft": f"draft v{ROUNDS['build']}"}


@dag.stage(depends_on=["s_build"])
async def s_check(ctx):
    ROUNDS["check"] += 1
    quality = "ok" if ROUNDS["check"] >= 2 else "rough"
    return {"quality": quality}


@dag.stage()
async def s_ship(ctx):
    return {"shipped": ctx.state["draft"]}

# Routes declared on the DAG, not inside the stages: after s_check finishes,
# the orchestrator evaluates route_fn and jumps. "rework" loops back to an
# executed stage — its static chain re-runs as a fresh visit. max_visits is
# the fuse: exceeding it fails the run (a guard, never a business fallback).
dag.add_conditional_edges(
    "s_check",
    lambda s: "accept" if s["quality"] == "ok" else "rework",
    {"accept": "s_ship", "rework": "s_build"},
    max_visits=5,
)


async def main():
    with tempfile.TemporaryDirectory() as d:
        store = CheckpointStore(FileStorage(str(Path(d) / "cp")))
        events = []
        rt = Runtime(checkpoint_store=store,
                     on_event=lambda e, data: events.append((e, data)))

        r = await rt.run(dag, task_id="demo")
        print(f"run: status={r.status} state={r.state}")

        # 3) single-point debugging — replay one stage on its historical input
        #    (no checkpoint write), then re-run the tail from any stage:
        single = await rt.run_stage(dag, "demo", "s_check")
        print(f"run_stage s_check -> {single.status}, quality={single.state['quality']}")
        forked = await rt.fork_run(dag, "demo", from_stage="s_build",
                                   overrides={"draft": "hand-edited draft"})
        print(f"fork_run from s_build -> {forked.status}, shipped={forked.state['shipped']}")

        # 4) route decisions: key = chosen, declared = all keys, so
        #    skipped == set(declared) - {key} is readable at a glance
        for _, data in [e for e in events if e[0] == "route"]:
            skipped = set(data["declared"]) - {data["key"]}
            print(f"route {data['from']} --{data['key']}--> {data['to']}"
                  f"  (skipped: {sorted(skipped) or 'none'})")


if __name__ == "__main__":
    asyncio.run(main())
