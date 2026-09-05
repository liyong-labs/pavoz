# Use case: ai_writer research pipeline integration (reference implementation)

🇨🇳 [简体中文](../../cn/use-cases/ai-writer.md)

> **This is stageflow's first business integration, presented as a reference implementation** — it demonstrates how a real business system expresses a "search → download → filter → synthesize → review → save" pipeline with stageflow.
> stageflow itself has no coupling to ai_writer or the models/services it uses.

## Integration outcome (shipped 2026-09-05)

- 8-node DAG (first compose): `s_plan → s_search → s_download → s_filter → s_compress → s_compose → s_audit → s_save`
- Revise (iterative refinement) is a **single super-node** (`s_revise`) wrapping the business's own iteration loop — a textbook example of "loops stay in business code": the loop carries its own save-per-iter + DB resume; stageflow only provides the unified entry point + exception contract + timeout
- Checkpoints persist to the business's own object storage (via a `StorageBackend` adapter under an isolated prefix)
- Resume: subprocess interrupted → restart the same task → runtime resumes completed nodes
- End-to-end verification: first compose + revise both produce output normally; resume after interruption works

Code location (business side, not in this repo): `backend/integration/` — `research_pipeline_dag.py` (two DAGs) + `runner.py` (setup/route/exception boundary) + `sf_storage.py` (`StorageBackend` → object storage).

## Boundary (what belongs to whom)

| | stageflow (generic) | Business (ai_writer) |
|---|---|---|
| DAG definition | — | Business-side file (8 nodes / `s_revise`) |
| External calls | `ctx.call(kind, op, params)` Protocol | Business caller (reuses its own LLM/cache/billing) |
| Checkpoint storage | `StorageBackend` Protocol | Business adapter (object storage / DB / file) |
| Loops (audit cascade) | — | Python `for`/`while` inside the stage |
| Logging | Handled inside the stage | Plugs into existing business logs / SSE |

## Patterns that landed

### 1. Installation

```toml
# Business pyproject.toml
[tool.poetry.dependencies]  # or pip / uv
stageflow = { git = "ssh://git@github.com/ebziw/stageflow.git", tag = "v0.1.1" }
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
result = asyncio.run(rt.run(dag, task_id=task_id,
                            initial_state={...}, resume=True))
if result.status != "done":
    raise RuntimeError(result.error or "run failed")
```

Key points:
- `task_id` = business task id (opaque key; checkpoint/state isolated by it)
- `resume=True`: on restart after interruption, completed nodes are skipped automatically; chained evolution relies on `producers` for restoration
- Editing a stage's function body does not affect resume (`workflow_hash` only covers structure); changing dependencies/retries → refuse to resume

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
