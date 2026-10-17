# Conditional edges: the orchestrator owns every edge (R1, v0.5.5)

Review-and-revise is the archetypal control flow
(`review → fail → revision → review again`). Historically it had to live *inside*
a stage as a `while` loop — which makes the node impure and costs each round its
checkpoint / observability / diff boundary.

A conditional edge gives control flow back to the orchestrator: **stages only do
data work and LLM calls; the orchestrator decides where the run goes next; every
round is a boundary you can resume, observe, and diff.**

## Minimal example

```python
dag = DAG("story")

@dag.stage()
async def s_write(ctx): ...

@dag.stage(depends_on=["s_write"])
async def s_review(ctx):
    r = int(ctx.state.get("round", 0)) + 1
    verdict = "pass" if r >= 2 else "fail"
    return {"round": r, "verdict": verdict}          # data only — no routing

@dag.stage()
async def s_revision(ctx): ...

@dag.stage()
async def s_save(ctx): ...

dag.add_conditional_edges(
    "s_review",
    lambda s: s["verdict"],                          # sync pure fn: reads state
    {"pass": "s_save", "fail": "s_revision"},        # key → target (a closed set)
    max_visits=5,                                    # required on a cycle (fuse)
)
dag.add_conditional_edges(
    "s_revision",
    lambda s: "re_review",
    {"re_review": "s_review"},
    max_visits=5,
)
```

Execution: `s_write → s_review → fail → s_revision → back to s_review → pass → s_save`.
Hitting `max_visits` → `MaxVisitsExceeded` (run fails, `failed_stage` = the router).

## LLM judgment: the judge-stage pattern (recommended)

When routing needs fuzzy LLM judgment, **do not call an LLM on the edge** — make
the judgment an ordinary stage:

```python
@dag.stage(depends_on=["s_audit"])
async def s_route_judge(ctx):
    """LLM reads the audit report + round, returns routing data."""
    decision = await judge_llm(ctx.state["audit_report"], ctx.state["round"])
    return {"route": decision, "route_reason": ..., "route_score": ...}

dag.add_conditional_edges(
    "s_route_judge",
    lambda s: s["route"],                            # pure read of state
    {"ship": "s_save", "revise": "s_revision", "escalate": "s_save"},
    max_visits=4,
)
```

Why the judge stage is strictly better: the LLM call then gets a checkpoint (a
crash doesn't re-run upstream), automatic backoff retry on `RetryableError`,
`stage_timings`, and diffability. An edge route function gets none of those.
This is the same layering LangGraph (router node + conditional edge), Temporal
(LLM work must live in an Activity) and Step Functions (decisions in a Task,
Choice evaluates state) converged on.

Convention: name judge stages `s_route_judge` / `s_decide_judge`; have them write
`state["route"]` (what the edge reads) plus `state["route_reason"]` /
`state["route_score"]` for tracing.

## Semantics at a glance

| Topic | Rule |
|---|---|
| `route_fn` | Sync pure function, `(state) -> key`; same state must yield the same key — the precondition for replay/resume determinism |
| `mapping` | Declared as a closed set; an undeclared key → `UnmappedRouteError` (fail-loud, no default) |
| `max_visits` | Cap on how many times a router stage executes in one run; required on cycles, optional on branching graphs; `MaxVisitsExceeded` is the fuse |
| Global backstop | `EnginePolicy.max_steps` still applies (two independent guardrails) |
| Validation | A target may not statically depend on its router; a router's static downstream must be a mapping target; cycles produce a WARNING |
| Triggering | A cycle target with a static parent is triggered by topo order (that's the loop head); branch targets and loop bodies only run via routing |
| `workflow_hash` | `route_fn` source + mapping + `max_visits` all count — changing routing is a structural change, so stale resumes are refused |
| `resume` | Re-evaluates the route of the last completed stage (a sync pure fn makes this deterministic); visit numbers continue |
| fork / replay | Supported (R2) — see below |
| Observability | A `route` event `{from, key, to, declared}` (`declared` arrived in R2: branches not taken this round are listed too, so mis-wiring is visible); stage events carry `visit`; `stage_timings` accumulates |
| viz | mermaid draws dashed edges labelled with the key; `graph_json` edges carry `kind` / `key` / `max_visits` |

## Interaction with fork_run (supported since R2)

v0.5.5 shipped with "on conditional-edge graphs `fork_run` raises RuntimeError".
**That restriction was lifted in R2** — the debugging story depends on it:

- `fork_run(from_stage=X)` truncates the run at X's **first completion**, deep-merges
  `overrides` into its pre-run state, then continues along the routing. The subtle
  part is tail alignment: folded historical deltas are aligned by visit, so re-running
  one round doesn't collapse round 2 into round 1 (after a fork on a two-round run,
  `round` is still 2, not 1).
- If X never executed in that task, `fork_run` raises `RuntimeError` explicitly —
  there is no truncation point, and routing may never reach it.
- `run_stage(X)` replays a single stage on reconstructed input and does **not**
  write a checkpoint (the latest-run pointer is unchanged) — the cheapest way to
  try a prompt edit on one node.
- Recommended debugging ladder: `run_stage` to check one node → `fork_run` with a
  changed input to re-walk the downstream → `diff_runs` to compare two runs.

Note that `fork_run` remains a *deterministic replay* primitive: it reuses upstream
results and does not re-evaluate routes it skips. To re-decide a route, change the
`route_fn` or start a fresh `run`.
