# stageflow architecture

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
│ stageflow core (this repo)                  │
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

- Each node persists to disk after completion (via `StorageBackend`)
- Checkpoint includes `workflow_hash` (sha256 over stage names + dependencies + retries + timeout)
- On resume, hash mismatch → `CheckpointMismatchError` (DAG structure changed, cannot resume; editing the function body alone does not affect the hash)
- Checkpoint persists `producers` (key → producer stage, used to restore chained-overwrite judgment); older checkpoints lacking this field → resume falls back to permissive mode (treat as single-chain), backward compatible
- Run completes → checkpoint auto-deleted

### 5. Timeout = absolute deadline propagation

- Run-level: `deadline` or `default_timeout`
- Stage-level: `timeout` (seconds) → effective timeout = `min(stage_timeout, run_deadline - now)`
- Timeouts are treated as `RetryableError` (retryable)

### 6. State contract

- Value types limited to: `str`/`int`/`float`/`bool`/`None`/`list`/`dict` (json-serializable); `set`/`datetime`/`Path`/`bytes` raise
- Before each stage: deep-copy into `ctx.state` (defensive — in-place mutation by the stage is a no-op)

## Modules

| File | Responsibility |
|---|---|
| `dag.py` | DAG declaration + topological sort (Kahn) + cycle detection (Tarjan SCC + self-loops) + freezing |
| `runtime.py` | Execution engine: sequential run + retry + deadline + state merge + checkpoint |
| `state.py` | Type validation + `ReadOnlyStateView` + shallow merge + conflict detection |
| `checkpoint.py` | `CheckpointStore` + `workflow_hash` + mismatch detection |
| `storage.py` | `StorageBackend` Protocol + `FileStorage` (default) |
| `testing.py` | `TestPipe` (mock stages to run the whole graph) |
| `cli.py` | `run` / `trace` / `state` (v0.1) |

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
