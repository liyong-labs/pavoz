# Use case: ai_writer research pipeline integration (reference implementation)

🇨🇳 [简体中文](../../cn/use-cases/ai-writer.md)

> **This is stageflow's first business integration, presented as a reference implementation** — it demonstrates how a real business system expresses a "search → download → filter → synthesize → review → save" pipeline with stageflow.
> stageflow itself has no coupling to ai_writer or the models/services it uses.

## Integration outcome (shipped 2026-09-05, fully migrated 2026-09-06)

- 8-node DAG (first compose): `s_plan → s_search → s_download → s_filter → s_compress → s_compose → s_audit → s_save`
- Revise (iterative refinement) is a **single super-node** (`s_revise`) wrapping the business's own iteration loop — a textbook example of "loops stay in business code": the loop carries its own save-per-iter and lands final state in the DB; stageflow only provides the unified entry point + exception contract + timeout (revise runs attach no checkpoint store, so stageflow checkpoints exist only for first-compose runs)
- **Execution path unified (2026-09-06)**: the business's legacy first-compose phase machine (`_run_research_pipeline_impl` + `_impl_run_pipeline_phases`) and its single-slot checkpoint system (MinIO single-slot CP / hash-skip / resume slots) are deleted — subprocess → `integration/runner.run_pipeline` → stageflow Runtime runs the DAG. "Checkpoint" now means only the stageflow checkpoint (`initial_state` + `stage_deltas`)
- Checkpoints persist under the business's own MinIO prefix `research/task_cp/stageflow/` via the `MinioStorage` adapter (`sf_storage.py`), at `runs/{task_id}/{run_id}/checkpoint` + a latest pointer — isolated from business artifacts (`research/{task_id}/v{v}/`) in the same bucket
- Interrupted-run recovery: the production path **always runs `resume=False`** (restart creates a fresh run_id) — after restarting the same task, stage bodies read the previous run's checkpoint holding the relevant deltas (`sf_restore.py`) and skip the completed search/download/compress segments when the business-side gates hit (TTL freshness for the search pool, use-time checks for the download/compress pools); repeated LLM costs are absorbed by the business's external cache. A DB reset + rerun is never polluted by a stale checkpoint either
- Debug replay: the business exposes `/api/research/replay-stage` → `Runtime.run_stage` single-stage replay (writes no checkpoint, leaves the latest pointer untouched)
- End-to-end verification: after the full migration, first compose + revise both produce output normally; interrupted-run restarts recover correctly

Code location (business side, not in this repo): `backend/integration/` — `research_pipeline_dag.py` (two DAGs) + `runner.py` (setup/route/exception boundary) + `sf_storage.py` (`StorageBackend` → object storage) + `sf_restore.py` (previous-run checkpoints → business restore gates).

## Boundary (what belongs to whom)

| | stageflow (generic) | Business (ai_writer) |
|---|---|---|
| DAG definition | — | Business-side file (8 nodes / `s_revise`) |
| External calls | `ctx.call(kind, op, params)` Protocol | Business caller (reuses its own LLM/cache/billing) |
| Checkpoint | `StorageBackend` Protocol + cp format (`initial_state` + `stage_deltas` + `stage_ts`) | Business adapter (object storage / DB / file) under its own key prefix |
| Loops (audit cascade) | — | Python `for`/`while` inside the stage |
| Logging | Handled inside the stage | Plugs into existing business logs / SSE |

## Patterns that landed

### 1. Installation

```toml
# Business pyproject.toml
[tool.poetry.dependencies]  # or pip / uv
stageflow = { git = "ssh://git@github.com/ebziw/stageflow.git", tag = "v0.8.0" }
```

### 2. Define the DAG (pure graph; stage functions carry business logic)

```python
from stageflow import DAG

dag = DAG("research_pipeline")

@dag.stage()
async def s_plan(ctx): ...          # port business plan function

@dag.stage(depends_on=["s_plan"], retries=2)
async def s_search(ctx): ...        # port business search (asyncio.gather OK internally)

# ... s_download / s_filter / s_compress / s_compose / s_save follow the same shape

@dag.stage(depends_on=["s_compose"])
async def s_audit(ctx):
    """Quality loop stays inside the stage (the framework has no loop primitives)."""
    for attempt in range(3):
        report = await ctx.call("quality", "reviewer", params={"article": ...})
        if report["verdict"] == "pass":
            break
        if attempt < 2:
            await ctx.call("quality", "fixer", params={"issues": report["issues"]})
    return {"audit": report}
```

### 3. `StorageBackend` adapter (inject your own storage)

```python
class ObjectStoreStorage(StorageBackend):
    def put(self, key, data): ...   # → business object storage
    def get(self, key): ...         # returns dict | None
    def list_keys(self, prefix): ...  # → [str]
    def delete(self, key): ...
```

### 4. Entry point (inside your business worker / subprocess)

```python
from stageflow import Runtime, CheckpointStore

rt = Runtime(
    checkpoint_store=CheckpointStore(ObjectStoreStorage(task_id)),  # per-task isolation
    default_timeout=4 * 3600,   # absolute deadline for the whole run
)
# Production path always runs resume=False — every restart gets a fresh run_id.
# Interrupted-run recovery reads the previous run's checkpoint and skips
# completed segments when the business-side gates hit (TTL freshness for the
# search pool, use-time checks for download/compress); repeat LLM costs are
# absorbed by the business's external cache.
# (Business read-side: sf_restore.py pulls search/download/compress artifacts
# out of the previous run's stage_deltas.)
result = asyncio.run(rt.run(
    dag, task_id=task_id,
    initial_state={...},
    resume=False,
))
if result.status != "done":
    raise RuntimeError(result.error or "run failed")
```

The engine's generic continuation APIs remain available to other adopters:
`resume=True` (continue the latest run, reusing its run_id and skipping
completed nodes; no checkpoint, or the run already finished → `RuntimeError`
done guard) and `fork_run` (branch from any historical stage with edited
inputs). ai_writer's production path does not use them today — its debug
replay goes through `Runtime.run_stage` (single stage, no checkpoint written,
latest pointer untouched).

Key points (v0.8; the ID model landed in v0.5):
- `task_id` = business task id (opaque key; the stable idempotency key)
- `run_id`: auto-generated UUID4 per `run()` — checkpoints are isolated by
  `runs/{task_id}/{run_id}/checkpoint` (+ a `runs/{task_id}/latest` pointer).
  ai_writer's `MinioStorage` adapter maps these keys under its own MinIO prefix
  `research/task_cp/stageflow/`, isolated from business artifacts
  (`research/{task_id}/v{v}/`) when both live in the same bucket
- Checkpoint contents = `initial_state` + `stage_deltas` (each node's raw
  return, in completion order) + `stage_ts` (v0.7+, stage completion epoch);
  full state is rebuilt from `initial_state` + `stage_deltas` — no redundant
  state is written to disk since v0.8
- Recovery modes: the production path always runs `resume=False` (restart from
  scratch + external LLM cache); interrupted-run recovery reads the previous
  run's checkpoint behind business gates (TTL freshness / use-time checks).
  `resume=True` / `fork_run` are generic engine capabilities not exercised by
  ai_writer today (debugging uses `run_stage`). Single-writer per task is
  assumed — the business worker's own lease/heartbeat guards concurrent runs
- Editing a stage's function body does not affect resume (`workflow_hash` only covers structure); changing dependencies/retries → refuse to resume
- `Ctx.run_id` / `Ctx.attempt` and the caller's 4th `CallMeta` arg give business
  callers the execution context (task/run/stage/attempt) for trace recording

### 5. Regression

```python
from stageflow import TestPipe

pipe = TestPipe(dag)
pipe.mock("s_search", lambda state: {...})   # mock expensive / external segments
result = await pipe.run()
assert result.state == {...}
```

## Known caveats

1. Module-level globals on the business side (`contextvar` / caches) are the easiest thing to miss when porting — make cross-stage dependencies explicit via `ctx.state` while moving functions
2. Subprocess heartbeat / lease (if any) belongs to the business worker, not stageflow — leave it as-is
3. `ctx.state` values must be json-serializable (`str` / `int` / `float` / `bool` / `None` / `list` / `dict`)

## Related documents

- [quickstart.md](../quickstart.md) — 5-minute walkthrough
- [architecture.md](../architecture.md) — architecture and design decisions
- [api.md](../api.md) — public API reference
