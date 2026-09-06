# stageflow

🇨🇳 [简体中文](README.cn.md)

**The in-process Python workflow engine — durable, replayable, forkable pipelines with zero runtime dependencies.**

No server. No scheduler. No YAML. No vendor lock-in. Define a DAG with `@dag.stage`, run it in your own process, and get per-stage **checkpoints**, **resume**, **replay**, and **time-travel forks** for free.

```text
Python 3.12+  |  MIT License  |  stdlib only  |  92 tests
```

## Why another workflow engine?

You've written a pipeline like `fetch → transform → review → save` before. You know the pain:

- Flow control (retries, timeouts, state) tangled up with business logic — one big function that's scary to touch
- A crash at step 4 means the expensive steps 1–3 run again
- Debugging an LLM stage means re-running the whole pipeline — you can't see the prompt that stage actually got, or change it and re-run *just that stage*
- Regression-testing a stage change means mocking everything by hand

stageflow pulls **graph execution** out of your business code and gives you durable, *replayable* runs — while staying out of your way:

- **In-process, zero runtime deps** — it's a library, not a platform. No cluster to self-host, no server to start, no DB required (bring your own persistence via a 5-method `StorageBackend` adapter when you want resume across restarts).
- **Never makes business decisions for you** — model selection, audit loops, and prompt templates are your code. External calls go through a `ctx.call(kind, op, params)` seam you wire to *your* LLM/search/HTTP stack; nothing is hard-coded to any vendor.
- **Every run is debuggable** — each stage's exact input is reconstructible from the checkpoint. Replay one stage after editing a prompt, or fork a past run from any stage with edited input (see [time-travel debugging](#time-travel-debugging)).

## 30-second quickstart

```bash
pip install stageflow            # not on PyPI yet → git clone + pip install -e .
```

```python
# pipeline.py
import asyncio
from stageflow import DAG, Runtime, CheckpointStore, FileStorage

dag = DAG("demo")

@dag.stage()
async def s_fetch(ctx):
    return {"items": ["a", "b", "c"]}

@dag.stage(depends_on=["s_fetch"], retries=2, timeout=60)
async def s_process(ctx):
    return {"count": len(ctx.state["items"]), "upper": [i.upper() for i in ctx.state["items"]]}

@dag.stage(depends_on=["s_process"])
async def s_save(ctx):
    print("saved:", ctx.state["count"], ctx.state["upper"])   # → saved: 3 ['A', 'B', 'C']
    return {"done": True}

async def main():
    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage("./data")))
    result = await rt.run(dag, task_id="job-1")
    print(result.status)          # → done

asyncio.run(main())
```

That's the whole API. State flows as a plain JSON-serializable dict; `ctx.state` is read-only, and each stage *returns* its delta. That's it — no base classes, no context-injection magic, no framework telling you how to structure code.

Or run it from the CLI (same checkpoint store, so every command below composes):

```bash
stageflow run pipeline.py --task-id job-1
stageflow trace  --task-id job-1            # stage statuses + run_id
stageflow state  --task-id job-1 --key count
```

## What you get

| | |
|---|---|
| **Checkpoint per stage** | State is persisted after every node. Kill the process mid-run, `run(..., resume=True)` continues from the first unfinished stage — nothing before it re-runs. `workflow_hash` fingerprints the DAG structure, so a structural change refuses a stale resume instead of silently corrupting it. |
| **Retries with a real contract** | Per-node `retries` + `timeout`, and three exception types with unambiguous semantics: `StageError` (business failure — no retry), `RetryableError` (backoff + retry), `FatalError` (program bug — stop now). |
| **Honest state model** | JSON-serializable state enforced at every write; read-only `ctx.state` views; two stages writing the same key raises `StateConflictError` instead of last-write-wins surprises. Chain overwrites (downstream refining an upstream value) are tracked via producers. |
| **Loops stay in business code** | No framework-level `retry_budget`/loop DSL to learn. Need an audit→fix→re-audit loop? Write a `while` inside one stage — it's your logic, expressed in Python, checkpointed at the node boundary. |
| **Regression testing built in** | `TestPipe` mocks any stage's output and runs the whole graph; `TestPipe.replay_from(cp, dag)` feeds a *real run's* saved outputs back as mocks — refactor a stage, prove the graph still converges. |
| **Storage is pluggable** | Core is stdlib-only. Default `FileStorage` needs nothing; for durable/cross-machine runs implement the 5-method `StorageBackend` (get/put/list/delete) over Postgres, MinIO, Redis, whatever you already run — or load it from a config string with `load_storage()`. |

## Time-travel debugging

This is the feature we built stageflow for: **any past run is an object you can inspect, edit, and fork.** Because every checkpoint stores each stage's raw delta (not just the merged end state), the exact input any stage saw can be rebuilt:

```bash
# 1. What did s_compose actually see? (full input JSON, human-editable)
stageflow export-input --task-id job-1 --stage s_compose > input.json

# 2. Tweak it, fork the run from that stage: upstream stages are reused (not re-run),
#    s_compose and its successors re-run on the edited input. Original run untouched.
stageflow fork-run pipeline.py --task-id job-1 --stage s_compose --input input.json
```

Or in code:

```python
result = await rt.run(dag, task_id="job-1")                 # original run
result = await rt.fork_run(dag, task_id="job-1",            # fork: reuse s_fetch+s_process
                           from_stage="s_compose",          # rerun s_compose → on
                           overrides={"items": ["x", "y"]})
```

Single-stage replay for prompt-tuning iterations (no fork, no new run):

```bash
stageflow replay pipeline.py --task-id job-1 --stage s_compose     # rerun one stage on its rebuilt input
```

Each fork gets a new `run_id` and becomes the task's latest run; the original checkpoint stays intact — multiple forks of the same history coexist. If your LLM stage misbehaves, you're seconds away from *"what did it see → change one sentence → re-run just that stage"* instead of a 30-minute pipeline rerun.

## When to use stageflow — and when not

**Use it** for sequential/stateful pipelines in a single process: LLM research & writing pipelines, agent steps with durable state, ETL with per-step checkpoints, anything where stage boundaries are known ahead of time and you want retries + resume + replay without adopting a platform.

**Don't use it** for:

- **Distributed scheduling at scale** — cron, a fleet of workers, multi-tenant queues: use Temporal / Prefect / Airflow, which own that layer. stageflow deliberately doesn't.
- **Dynamic graphs** — if your graph shape must change at runtime (agents that spawn agents, data-driven branching): LangGraph / Burr own that space. stageflow graphs are static; dynamic control flow lives *inside* a stage as ordinary Python.
- **A web UI / observability platform** — see Temporal, Hatchet, Windmill. stageflow gives you CLI + JSON checkpoints to build on.

## How it compares

<details>
<summary><b>vs. Temporal / Prefect / Airflow (durable-execution platforms)</b></summary>

They run your workflows on *their* infra (a server, workers, a scheduler). stageflow runs in *your* process as a library — you adopt it by decorating functions, not by deploying a platform. If you need cross-machine durability, multi-tenant queues, or a UI today, use them; stageflow is the right size when your pipeline is one process and you want durable primitives without infrastructure.
</details>

<details>
<summary><b>vs. LangGraph / Burr (agent graph frameworks)</b></summary>

Both are excellent for *state machines and dynamic agent graphs* and ship their own persistence/UI opinions. stageflow's graph is a static, validated DAG and it ships *no* opinion on how you call LLMs — no model catalog, no prompt abstractions, no vendor integrations. It targets the "pipeline with known stages" majority of LLM work and the frameworks stay composable with it (call them inside a stage if you like).
</details>

<details>
<summary><b>vs. DBOS Transact</b></summary>

DBOS pioneered "lightweight durable workflows" and its `fork_workflow(id, step)` is the industry precedent for restart-from-any-step — but it *requires Postgres* as its durability layer. stageflow's equivalent (`fork_run`) works on any `StorageBackend` you already have, including a plain directory. Postgres is a great adapter; it shouldn't be a requirement.
</details>

## Known limitations

- **Checkpoints store stage deltas, not full state** (v0.8): full state is rebuilt
  from `initial_state` + `stage_deltas` on load. If a stage returns very large
  payloads (e.g. full document content) into shared state, checkpoints grow with
  them — keep big blobs in external storage and pass references/summaries through
  state. `Checkpoint.state_stats()` helps you see what is actually large.
- **`resume=True` continues the latest run only**: it refuses to resume a run that
  already finished (`done` guard) — rerun with `resume=False` (new run id), or use
  `fork_run` to branch from a historical stage with edited inputs.
- **One process, one run at a time**: stageflow is an in-process engine with no
  built-in queue, scheduler, or multi-worker coordination. Lease/heartbeat on the
  task row is your integration's job (see the ai_writer reference integration).

## Use cases

- **LLM content pipelines** — search → download → filter → compress → compose → audit → save, with per-stage checkpointing so a failed audit never repeats 40 minutes of upstream work. ([reference integration](docs/en/use-cases/ai-writer.md))
- **Agent steps with durable state** — long-running multi-step agents where each step's output must survive process restarts.
- **Any multi-stage Python pipeline** that outgrows a script but doesn't need a platform.

## Docs & development

- [Architecture](docs/en/architecture.md) · [API reference](docs/en/api.md) · [Quickstart](docs/en/quickstart.md) · [Storage backends](docs/storage.md)
- [Contributing](CONTRIBUTING.md) · [Changelog](CHANGELOG.md) · [Roadmap](ROADMAP.md) · MIT License

---

**stageflow** — graph execution, extracted. Everything else stays yours.
