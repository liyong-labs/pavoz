# API reference

🇨🇳 [简体中文](../cn/api.md)

The public surface is `__all__` in `stageflow/__init__.py`. The core has zero third-party dependencies.

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
    caller: Callable[[str, str, dict], Awaitable[dict]] | None = None,
    default_timeout: float | None = None,   # absolute deadline for the whole run (seconds)
)

result: RunResult = await rt.run(
    dag, task_id: str,
    *,
    initial_state: dict | None = None,
    resume: bool = True,
    deadline: float | None = None,   # overrides default_timeout
)
```

- `task_id` is an opaque key supplied by the caller — checkpoints/state are isolated by it, so parallel tasks don't interfere
- `resume`: if a checkpoint exists and the `workflow_hash` matches → skip completed stages
- When the run completes (`status == "done"`) → checkpoint is auto-deleted

### `RunResult`

- `status`: `"done" | "failed"`
- `state`: final state
- `stage_statuses`: each stage's `done` or `failed`
- `error`: failure reason (exception converted to string)

### Failure semantics (no exception raised; returns `failed`)

| Exception | Meaning | Behavior |
|---|---|---|
| `StageError` | Business failure | Fail, no retry |
| `RetryableError` | Network / rate-limit / timeout | Backoff retry, fail when budget exhausted |
| `FatalError` / unknown exception | Code bug | Fail immediately, does not consume retries |

## Ctx (stage argument)

```python
ctx.task_id: str
ctx.stage_name: str
ctx.state: ReadOnlyStateView   # read-only deep copy; any write raises
ctx.deadline: float | None     # absolute deadline for this stage

await ctx.call(kind: str, op: str, params: dict | None = None) -> dict
```

- `ctx.call` is the **only** entry point for external calls inside a stage. The `kind`/`op`/return-value shape is defined by the business caller — the framework just passes them through. The default caller is a no-op echo (`CallResult(kind=..., op=..., params=...)`)
- `ctx.logger`: `logging.Logger` (with the stage name already injected)

## State

- Allowed types: `str`/`int`/`float`/`bool`/`None`/`list`/`dict` (json-serializable). Anything else (`set`/`datetime`/`Path`/`bytes`/...) → `StateValidationError`
- Write path: stage `return dict` → shallow merge
- Conflict: a delta key already exists in state and its producer is **not** a transitive upstream of the current stage → `StateConflictError` (parallel-producer conflict)
- Chained overwrite: the producer is a transitive upstream → allowed (v0.1.1)

## Checkpoint

```python
store = CheckpointStore(storage: StorageBackend)
cp: Checkpoint | None = store.load_compatible(task_id, dag)   # raises on hash mismatch
store.save(cp)
store.delete(task_id)
```

- `Checkpoint`: `task_id / dag_name / workflow_hash / stage_statuses / state / done_stages / producers`
- `workflow_hash(dag)`: sha256 over stage names + dependencies + retries + timeout (function-body changes don't affect the hash)
- `CheckpointMismatchError`: structure changed on resume → refuse to resume (delete the checkpoint or force `resume=False` to rerun)
- Old checkpoints (no `producers` field) → resume falls back to permissive mode (treat as single-chain)

## `StorageBackend` (Protocol, business-injected)

```python
class StorageBackend:
    def put(self, key: str, data: dict) -> None: ...
    def get(self, key: str) -> dict | None: ...
    def list_keys(self, prefix: str) -> list[str]: ...
    def delete(self, key: str) -> None: ...
```

Built-in: `FileStorage(dir)` — local filesystem implementation (tests / single-host).

## Contrib adapters

Optional `StorageBackend` implementations live in `stageflow.contrib.storage` (separate subpackage; core stays stdlib-only). Install drivers via pyproject extras:

```bash
pip install stageflow[postgres]   # psycopg3 + JSONB
pip install stageflow[mysql]      # PyMySQL + LONGTEXT
pip install stageflow[redis]      # redis-py
pip install stageflow[minio]      # boto3 (S3 / MinIO / R2)
pip install stageflow[contrib]    # all 4 above
pip install stageflow[sqlite]     # stdlib, no extras
```

Each adapter is lazily imported — `from stageflow.contrib.storage import PostgresStorage` raises `ImportError` with install hint if the driver is missing. Full usage, ai_writer compatibility notes, and test strategy: [docs/contrib.md](contrib.md).

## `TestPipe`

```python
pipe = TestPipe(dag, caller=None)
pipe.mock("s_search", lambda state: {"sources": [...]})   # replace stage output on demand
result: RunResult = await pipe.run(task_id="t", initial_state={...})
```

- Unmocked stages run normally (you can mock only the heavy external-call segments and let the rest execute for real)
- Regression scenario: drive the full graph with fixed mock outputs and assert the final state

## CLI

```bash
python -m stageflow run <dag.py> [--task-id X] [--input '{"k": "v"}'] [--resume]
python -m stageflow trace --task-id X
python -m stageflow state --task-id X [--key K]
```

- `run`: runs a DAG file (the module must expose a `dag` variable); `STAGEFLOW_STORAGE` env var points at the `FileStorage` directory (no checkpoint if unset)
- `trace`/`state`: read checkpoints from `FileStorage`

## Related documents

- [quickstart.md](quickstart.md) — 5-minute walkthrough
- [architecture.md](architecture.md) — architecture and design decisions
- [use-cases/](use-cases/) — reference business integrations
