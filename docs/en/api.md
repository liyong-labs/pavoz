# API reference

🇨🇳 [简体中文](../cn/api.md)

The public surface is `__all__` in `pavoz/__init__.py`. The core has zero third-party dependencies.

## DAG

```python
dag = DAG(name: str)
```

- `dag.stage(*, depends_on: list[str] | None = None, retries: int = 0,
  timeout: float | None = None)` — decorator to register a stage function
  - `depends_on`: list of prerequisite stage names
  - `retries`: retry budget for `RetryableError` / timeout (exponential backoff `2^n`, capped at 30s)
  - `timeout`: hard timeout for this stage (seconds); effective timeout = `min(stage.timeout, remaining run deadline)`
- `dag.validate()` — static validation (unknown deps + cycle detection); auto-called before run, freezes on success
- `dag.topo_order() -> list[str]` — Kahn topological order
- `dag.reachable(upstream, downstream) -> bool` — transitive dependency check (used by chained overwrite)

Exceptions: `UnknownDepError` / `CycleError`

## Runtime

```python
rt = Runtime(
    checkpoint_store: CheckpointStore | None = None,
    caller: Callable[[str, str, dict, CallMeta], Awaitable[dict]] | None = None,
    default_timeout: float | None = None,   # absolute deadline for the whole run (seconds)
)

result: RunResult = await rt.run(
    dag, task_id: str | None = None,
    *,
    initial_state: dict | None = None,
    resume: bool = False,
)
```

- `task_id` is optional. If omitted, pavoz auto-generates a UUID4 (36 chars).
  If supplied, it is validated at entry: non-empty, ≤ 128 chars, charset
  `[A-Za-z0-9_.-]` only (`/` rejected — the task_id is embedded in the storage
  key path). Stable across retries/resumes — the idempotency key. Resume needs
  an explicit task_id (an auto-generated one has no checkpoint to find).
- `run_id` is auto-generated (UUID4) per `run()` invocation and returned in
  `RunResult.run_id`. Resume **reuses** the checkpoint's original run_id
  (Temporal Continue-As-New pattern) — one task can have multiple runs without
  overwriting each other.
- `resume` (default `False`): `True` → load this task's latest checkpoint and
  continue from where it stopped (skipping completed stages, restoring state).
  Raises `RuntimeError` if there is no checkpoint, and raises if the run already
  completed (done guard — no silent no-op; start over with `resume=False`, which
  begins a new run_id).
- On `resume`, a `workflow_hash` mismatch (DAG structure changed) →
  `CheckpointMismatchError`.
- Run-level absolute deadline comes from `Runtime(default_timeout=...)`; there
  is no per-`run()` deadline parameter. Per-stage `timeout` is capped by the
  remaining run deadline.
- On completion the checkpoint is kept (not deleted) — it is the task's latest
  run and the target of future resumes/`load_latest`.

### `Runtime.run_stage` (single-stage replay, v0.5.1)

```python
result: RunResult = await rt.run_stage(dag, task_id: str, stage_name: str)
```

Debug helper: rebuild the stage's pre-run state from the task's latest
checkpoint and run **only that stage** — tune a prompt/parameters and see the
effect in seconds without re-running upstream stages.

- Pre-state rebuild: completed stages → `cp.rebuild_state_before(stage_name)`;
  a stage that never completed (failed/interrupted original run) whose deps are
  all done → `cp.rebuild_state()`; deps incomplete → `RuntimeError`
- Does **not** write a checkpoint and does **not** move the `latest` pointer
  (replay output is a dev artifact — it must not become the resume target);
  `ctx.run_id` is a throwaway UUID4 for the call
- Errors: no checkpoint → `RuntimeError`; DAG hash mismatch →
  `CheckpointMismatchError`; pre-M2 (v0.5.0) checkpoint without
  `stage_deltas` → `RuntimeError` (re-run once to produce a fresh checkpoint);
  unknown stage → `KeyError`

### `RunResult`

- `task_id`: caller-supplied or auto-generated UUID4
- `run_id`: this run's UUID4 (or the reused original on resume)
- `status`: `"running" | "done" | "failed" | "cancelled"` (terminal: `done` / `failed` / `cancelled`)
- `state`: final state
- `stage_statuses`: each stage's `done`, `failed`, or `cancelled`
- `error`: failure or cancellation reason (exception converted to string)

### Failure semantics (no exception raised; returns `failed`)

| Exception | Meaning | Behavior |
|---|---|---|
| `StageError` | Business failure | Fail, no retry |
| `RetryableError` | Network / rate-limit / timeout | Backoff retry, fail when budget exhausted |
| `FatalError` / unknown exception | Code bug | Fail immediately, does not consume retries |

## Ctx (stage argument)

```python
ctx.task_id: str
ctx.run_id: str          # UUID4 of the current run (reused on resume)
ctx.attempt: int         # 1-based; current attempt for this stage (+1 per retry)
ctx.stage_name: str
ctx.state: ReadOnlyStateView   # read-only deep copy; any write raises
ctx.deadline: float | None     # absolute deadline for this stage

await ctx.call(kind: str, op: str, params: dict | None = None) -> dict
```

- `ctx.call` is the **only** entry point for external calls inside a stage. The `kind`/`op`/return-value shape is defined by the business caller — the framework just passes them through. The default caller is a no-op echo (`CallResult(kind=..., op=..., params=...)`)
- The injected caller receives a 4th argument: `meta: CallMeta` (`task_id` /
  `run_id` / `stage` / `attempt`) — callers that record traces/`llm_calls` can
  correlate the execution context without extra plumbing. `CallMeta` lives in
  `pavoz.runtime`. Stage functions are unaffected (`async def fn(ctx) -> dict`).
- `ctx.logger`: `logging.Logger` (with the stage name already injected)

## State

- Allowed types: `str`/`int`/`float`/`bool`/`None`/`list`/`dict` (json-serializable). Anything else (`set`/`datetime`/`Path`/`bytes`/...) → `StateValidationError`
- Write path: stage `return dict` → shallow merge
- Conflict: a delta key already exists in state and its producer is **not** a transitive upstream of the current stage → `StateConflictError` (parallel-producer conflict)
- Chained overwrite: the producer is a transitive upstream → allowed (v0.1.1)

## Checkpoint

```python
store = CheckpointStore(storage: StorageBackend)
store.save(cp)                                  # write checkpoint + latest pointer
cp: Checkpoint | None = store.load(task_id, run_id)       # a specific run
cp: Checkpoint | None = store.load_latest(task_id)        # via the latest pointer
run_ids: list[str] = store.list_runs(task_id)             # what runs exist? (debug/cleanup)
store.delete(task_id, run_id)                             # pointer is NOT updated on delete
cp: Checkpoint | None = store.load_compatible(task_id, run_id, dag)
# hash-checked load; run_id="" → load the task's latest run (load_latest sentinel)
```

- Storage layout: each run is saved at `runs/{task_id}/{run_id}/checkpoint`;
  every `save()` also updates a pointer file `runs/{task_id}/latest`
  (`{"run_id": ...}`). Multiple runs of the same task never overwrite each
  other, and "latest" does not rely on lexicographic ordering (UUID4 lex order
  ≠ chronological). Deleting the run the pointer names → `load_latest` returns
  `None` (clear "no checkpoint" signal — acceptable edge).
- `Checkpoint` fields: `task_id / run_id / dag_name / workflow_hash /
  stage_statuses / state / done_stages / producers` + v0.5.1:
  `initial_state` (the run's starting state) and `stage_deltas` (each
  completed stage's raw return dict, in completion order). Together they can
  rebuild the state as it was before any recorded stage (`cp.state` is the
  final merged state — you cannot derive per-stage inputs from it).
  `run_id` is required — pre-v0.5 checkpoints (no `run_id`, old key
  `runs/{task_id}/checkpoint`) are orphaned (hard cut, no legacy loader);
  within the current format, missing optional fields load via defaults
  (forward-compatible schema — v0.5.0 checkpoints without the new fields
  load and resume fine).
- `cp.rebuild_state() -> dict` — reconstruct the merged state after all
  completed stages (`initial_state` + done deltas in completion order;
  chain overwrites resolve by order). This is the input state for replaying
  a stage that never completed (its deps are done).
- `cp.rebuild_state_before(stage_name) -> dict` — reconstruct the state as it
  was just before `stage_name` ran (`initial_state` + deltas of stages
  completed before it). `KeyError` if the stage is not in this run's record.
  Used by `run_stage` so a stage's own earlier output can never pollute its
  replay input.
- `workflow_hash(dag)`: sha256 over stage names + dependencies + retries + timeout (function-body changes don't affect the hash)
- `CheckpointMismatchError`: structure changed on resume → refuse to resume (rerun with `resume=False`, or delete the run's checkpoint)

## `StorageBackend` (Protocol, business-injected)

```python
class StorageBackend:
    def put(self, key: str, data: dict) -> None: ...
    def get(self, key: str) -> dict | None: ...
    def list_keys(self, prefix: str) -> list[str]: ...
    def delete(self, key: str) -> None: ...
```

Built-in: `FileStorage(dir)` — local filesystem implementation (tests / single-host).

## Storage loader

pavoz core ships **only** the `StorageBackend` Protocol + `FileStorage`. Drivers
(psycopg / redis / boto3 / etc.) are the user's responsibility — install what you
need, write (or vendor) an adapter that implements the Protocol, then load it by
config string.

```python
from pavoz import load_storage

storage = load_storage("pavoz.storage.FileStorage", root_dir="/data/cp")
# or:
storage = load_storage(
    "backend.integration.sf_storage.MinioStorage",
    task_id="run-2026-09-05-001",
)
cp_store = CheckpointStore(storage)
```

### `load_storage(spec: str, **kwargs) -> StorageBackend`

- `spec` — adapter locator. Two equivalent forms:
  - `"pkg.module:ClassName"` (canonical, explicit separator)
  - `"pkg.module.path.ClassName"` (dotted, last segment = class name)
- `**kwargs` — forwarded to `ClassName.__init__`
- Raises `PavozStorageError` (subclass of `ImportError`) with friendly messages
  for 5 failure modes: format error / module not installed / wrong class name /
  wrong kwargs / non-`StorageBackend` instance.

Full reference + storage interop contract + adapter recipe: [docs/storage.md](../storage.md).

## `TestPipe`

```python
pipe = TestPipe(dag, initial_state={...})                  # caller=None default
pipe.mock("s_search", lambda state: {"sources": [...]})   # replace stage output on demand
result: RunResult = await pipe.run()

# v0.5.1: replay a real run's checkpoint as a regression fixture
pipe2 = TestPipe.replay_from(cp, dag)  # completed stages fed with their saved deltas
result2 = await pipe2.run()
assert result2.state == cp.state       # same stage outputs in → same graph behavior out
```

- Unmocked stages run normally (you can mock only the heavy external-call segments and let the rest execute for real)
- Regression scenario: drive the full graph with fixed mock outputs and assert the final state
- `replay_from(cp, dag)`: every completed stage in `cp.done_stages` is mocked
  with its saved `stage_deltas` output; stages that never completed run for
  real. Requires the checkpoint's `workflow_hash` to match the DAG (structure
  unchanged) → mismatch raises `CheckpointMismatchError`; a pre-M2 (v0.5.0)
  checkpoint without `stage_deltas` raises `RuntimeError` (re-run once).
  Hand-written mocks for real runs can be replaced by replaying the checkpoint.

## CLI

```bash
python -m pavoz run <dag.py> [--task-id X] [--input '{"k": "v"}'] [--resume]
python -m pavoz trace --task-id X
python -m pavoz state --task-id X [--key K]
python -m pavoz replay <dag.py> --task-id X --stage Y [--patch P.py]   # v0.5.1
```

- `run`: runs a DAG file (the module must expose a `dag` variable);
  `PAVOZ_STORAGE` env var points at the `FileStorage` directory — every
  run persists per-stage checkpoints there (single runs included, v0.5.1 R5
  fix), so `--resume` / replay / trace / state work on CLI runs
- `trace`/`state`: read checkpoints from `FileStorage`
- `replay`: replays one stage on its checkpoint-rebuilt input (see
  `Runtime.run_stage`); optional `--patch P.py` loads a module exposing
  `patch(dag) -> None` to swap implementations in before replaying (must be
  sync — async patch is rejected). Output is the result JSON; nothing is
  persisted (no checkpoint written, `latest` pointer untouched)

## Related documents

- [quickstart.md](quickstart.md) — 5-minute walkthrough
- [architecture.md](architecture.md) — architecture and design decisions
- [use-cases/](use-cases/) — reference business integrations
