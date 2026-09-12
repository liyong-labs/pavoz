# pavoz architecture

🇨🇳 [简体中文](../cn/architecture.md)

## Positioning

**micro/in-process workflow engine** — not a macro orchestrator like Airflow/Prefect/Dagster (no built-in scheduler/DB/UI/distributed runtime).
Does only: graph execution + state passing + failure retry + checkpoint recovery. Embeds in your business process, zero runtime dependencies,
**no binding to any business system / model / storage service** (reference integration: `docs/use-cases/`).

```
┌─────────────────────────────────────────────┐
│ Business system (any project)               │
│   worker pulls task → Runtime.run(dag, task_id)│
│   stage functions = pure async (read ctx.state,│
│     return dict to write state)              │
└──────────────┬──────────────────────────────┘
               │ ctx.call(kind, op, params)
               ▼
┌─────────────────────────────────────────────┐
│ pavoz core (this repo)                  │
│   DAG (static) + Runtime + State + Checkpoint│
│   zero deps: imports no third-party libs    │
└──────────────┬──────────────────────────────┘
               │ StorageBackend Protocol
               ▼
┌─────────────────────────────────────────────┐
│ Storage (business-injected)                 │
│   FileStorage (default / tests)             │
│   DB / MinIO / other backends               │
└─────────────────────────────────────────────┘
```

## Core design decisions (locked in 2026-09-05)

### 1. Loops stay in business code (most important)

Processes that need to "rerun the same stage until a condition is met" (quality convergence loops, manual confirmation polling, etc.) are **not graph nodes** — they are Python `while`/`for` loops inside a stage function:

```python
@dag.stage(depends_on=["s_produce"])
async def s_quality_loop(ctx):
    """Example: loop until quality check passes (pure business logic; framework provides no loop primitives)."""
    for attempt in range(3):
        result = await ctx.call("quality_check", "default", params={"item": ...})
        if result["verdict"] == "pass":
            return {"item": ..., "quality": "pass"}
        if attempt < 2:
            await ctx.call("fix", "default", params={"issues": result["issues"]})
    return {"quality": "fail"}
```

The framework offers only three capabilities: **DAG (linear graph) + per-node retries + checkpoint**. No `sub_dag`, `retry_budget`, `escalation`, or `watchdog` primitives.
Reference: the most mature engines (Airflow/Temporal/Prefect) likewise lack cross-stage retry primitives — loops and gates are expressed in workflow code.

#### Extension surface: a stage function *is* the extension point

The framework ships no loop primitives, but it does ship **extension points** — you can build your own mechanisms with decorators and injection, without touching framework source:

| Extension point | Mechanism | Use for |
|---|---|---|
| **Stage function** | any `async def(ctx) -> dict`; wrap it before registration | Quality gates (score + redo loop) / contract validation / instrumentation / adaptive timeouts |
| **Lifecycle events** | `Runtime(on_event=fn)`, `fn(event: str, data: dict)` | Progress reporting / metrics / alerting; observer exceptions are isolated and never affect the run |
| **Outbound calls** | `ctx.call(kind, op, params)` → injected caller | Route every LLM / HTTP / DB call through one seam — tracing, accounting and replay all hang off it |
| **Storage** | `StorageBackend` Protocol | Swap where checkpoints land (files / DB / object storage) |
| **Cancellation** | `Runtime(cancel_check=...)` + `ctx.cancelled()` | Cooperative cancellation; long stages poll and exit on their own |

Lifecycle events:

| Event | When | Key fields |
|---|---|---|
| `run_start` / `run_end` | Run boundaries | `task_id` / `run_id` / `status` |
| `stage_start` / `stage_end` | Every attempt | `task_id` / `run_id` / `stage` / `attempt` / `status` / `duration` |
| `stage_retry` | After a `RetryableError` schedules a retry | Same as above + `error` |
| `stage_progress` | Stage calls `ctx.set_progress(fraction, note)` | `fraction` / `note` (best-effort transient signal, **never checkpointed**) |

**Decorator gotcha**: always wrap stage functions with `functools.wraps` — the stage name comes from `fn.__name__`, and dropping it makes every decorated stage collide on the same name.

**Version policy**: third-party extensions declare `pavoz>=0.3,<0.4` (enforced by pip resolution) and run a CI matrix of *(lowest supported × latest)*. During 0.x, minor releases may contain breaking changes (semver permits it); regular semantics return at 1.0.

Reference implementation: [pavoz-extensions](https://github.com/liyong-labs/pavoz-extensions) — `@gate` (worker → multi-lens review → score → redo) and `@schema` (cross-stage contract validation). Two plain decorators, and the template for writing your own `pavoz-*` extension.

### 2. Stage = `async def(ctx) -> dict`

- Read: `ctx.state` (a `ReadOnlyStateView` — deep copy; writes raise)
- Write: `return dict` → runtime performs shallow merge
- Conflict detection (chained-overwrite semantics, v0.1.1):
  - A stage overwriting a key whose producer is a **transitive upstream** → allowed (data pipelines evolve the artifact stage by stage, e.g. `search → filter → compress`)
  - Parallel producers (no dependency chain) writing the same key → `StateConflictError`
  - Runtime tracks `key → producer stage`, persisted alongside checkpoint

### 3. Exception contract

| Exception | Semantics | Runtime behavior |
|---|---|---|
| `StageError` | Business failure (not retryable) | Fail terminally |
| `RetryableError` | Network / 429 / 5xx / timeout | Consume a retry, exponential backoff; fail when exhausted |
| `FatalError` / unknown exception | Code bug | Fail immediately, does not consume retries |

### 4. Checkpoint + workflow hash

- Each node persists a checkpoint to storage after completion (via `StorageBackend`).
  Checkpoints are **per-run**: stored at `runs/{task_id}/{run_id}/checkpoint`,
  plus a pointer file `runs/{task_id}/latest` — one task can have many runs
  (original / resume / rerun) without overwriting each other
- Checkpoint includes `workflow_hash` (sha256 over stage names + dependencies + retries + timeout)
- On resume, hash mismatch → `CheckpointMismatchError` (DAG structure changed, cannot resume; editing the function body alone does not affect the hash)
- Checkpoint persists `producers` (key → producer stage, used to restore chained-overwrite judgment); within the current format, missing optional fields load with defaults (forward-tolerant schema). Pre-v0.5 checkpoints (no `run_id`, old key `runs/{task_id}/checkpoint`) are hard-cut orphaned — no legacy loader
- On completion the checkpoint is **kept** (no auto-delete) — it is the task's latest run. `resume=True` on a completed run raises a done guard (no silent no-op); rerun with `resume=False` (fresh run_id)

### 5. Timeout = absolute deadline propagation

- Run-level: `deadline` or `default_timeout`
- Stage-level: `timeout` (seconds) → effective timeout = `min(stage_timeout, run_deadline - now)`
- Timeouts are treated as `RetryableError` (retryable)

### 6. State contract

- Value types limited to: `str`/`int`/`float`/`bool`/`None`/`list`/`dict` (json-serializable); `set`/`datetime`/`Path`/`bytes` raise
- Before each stage: deep-copy into `ctx.state` (defensive — in-place mutation by the stage is a no-op)

### 7. Run identity + recovery model (v0.5)

Three-level identity, following industry precedents (Temporal WorkflowId/RunId,
DBOS workflow_id, Airflow `dag_id + task_id + run_id + try_number`):

- `task_id` — caller-supplied (or auto UUID4). The stable idempotency key across
  retries/resumes. Validated at entry: non-empty, ≤ 128 chars,
  `[A-Za-z0-9_.-]` only (no `/` — it is embedded in the storage key path)
- `run_id` — pavoz-generated UUID4 per `run()` invocation. Resume reuses the
  checkpoint's run_id (Continue-As-New); a rerun (`resume=False`) starts a new one
- `attempt` — pavoz-injected 1-based int into `Ctx`; +1 per stage retry
  (Airflow try_number / Celery retries)

Recovery modes: pavoz `resume` is **real continuation** from the last
checkpoint (state restored, completed nodes skipped, same run_id). "Restart from
scratch + external cache" (`resume=False`) is the default the first integrations
use for routine reruns — the caller's cache absorbs repeat costs — and `resume`
is reserved for genuine continuation scenarios (interrupted run restart).

Operational assumptions (deliberately not enforced in core):

- **Single writer per task** — pavoz assumes one active writer per
  `(task_id)`; concurrent writers are the business layer's concern (lease /
  epoch). Two writers on the same `(task_id, run_id)` = last-writer-wins, no locking
- **No cancellation sense** — pavoz is in-process: if the worker dies the run
  dies (checkpoint up to the last completed node). Cancellation = business-side
  kill + restart + `resume=True`
- **Storage namespace** — checkpoints live under a `runs/` prefix
  (`runs/{task_id}/{run_id}/checkpoint` + `runs/{task_id}/latest`). Callers
  sharing an object store between pavoz checkpoints and business artifacts
  keep the prefixes isolated (e.g. business artifacts under
  `research/{task_id}/v{v}/`)

### 8. Replay semantics (v0.5.1, M2/M3)

Two replay modes, both driven by the checkpoint — never by the caller
re-supplying data:

- **Checkpoint stores what replay needs.** Since v0.5.1 the checkpoint keeps
  `initial_state` plus each completed stage's raw return (`stage_deltas`, in
  completion order). The state before any recorded stage can then be rebuilt
  exactly (`initial_state` + the deltas of stages completed earlier — chain
  overwrites resolve naturally by completion order), instead of naively using
  the final `cp.state`, which contains the stage's own output and would
  pollute its replay input. `rebuild_state()` / `rebuild_state_before(name)`
  expose this. Old (v0.5.0) checkpoints lack the new fields — they load via
  defaults and resume fine, but replaying them fails with a clear error
  telling you to run once more.
- **Single-stage replay** (`Runtime.run_stage` / CLI `replay <dag.py>
  --task-id X --stage Y [--patch P.py]`): rebuild the stage's pre-run state,
  run only that stage — tune a prompt/parameters and see the effect in
  seconds. A stage that never completed (failed/interrupted run) is replayable
  once its dependencies are done. Replay output is a developer artifact: it is
  **not** written as a checkpoint and does **not** move the `latest` pointer,
  so a replay can never pollute the target of a future `resume`/`load_latest`.
- **Graph regression** (`TestPipe.replay_from(cp, dag)`): completed stages are
  fed their saved deltas as mocks, everything else runs for real — a real run's
  checkpoint becomes the fixture, and the assertion "same stage outputs in →
  same final state out" (`result.state == cp.state`) verifies that graph
  behavior (topology, merging, conflict rules) didn't break while implementations
  changed. A DAG-structure change (workflow_hash mismatch) is refused with a
  clear error rather than silently mis-replayed.

## Modules

| File | Responsibility |
|---|---|
| `dag.py` | DAG declaration + topological sort (Kahn) + cycle detection (Tarjan SCC + self-loops) + freezing |
| `runtime.py` | Execution engine: sequential run + retry + deadline + state merge + checkpoint; `run_stage` single-stage replay (v0.5.1) |
| `state.py` | Type validation + `ReadOnlyStateView` + shallow merge + conflict detection |
| `checkpoint.py` | `CheckpointStore` + `workflow_hash` + mismatch detection; `stage_deltas`/`initial_state` + `rebuild_state[_before]` (v0.5.1) |
| `storage.py` | `StorageBackend` Protocol + `FileStorage` (default) |
| `testing.py` | `TestPipe` (mock stages to run the whole graph; `replay_from(cp, dag)` checkpoint regression, v0.5.1) |
| `cli.py` | `run` / `trace` / `state` / `replay` (v0.5.1) |
| `storage_loader.py` | `load_storage(spec, **kwargs)` — config-string driven `StorageBackend` loader (importlib + friendly errors); see [docs/storage.md](storage.md) |

## What we don't do (YAGNI)

- ❌ scheduler / cron / time-based triggers
- ❌ UI / visualization (v1)
- ❌ distributed execution (multi-host lease coordination)
- ❌ dynamic DAG (runtime graph mutation) — DAG defined once, parsed once, frozen after validation
- ❌ sub-dag nesting (loops live in business code, no need)
- ❌ business wrapper implementations (external-call adapters belong to the business system; core only defines the `ctx.call` Protocol, `kind`/`op` defined by business)
- ❌ business table schemas (runs/task_state are the caller's DB concern)

## Related documents

- [quickstart.md](quickstart.md) — 5-minute walkthrough
- [api.md](api.md) — public API reference
- [use-cases/](use-cases/) — reference business integrations
