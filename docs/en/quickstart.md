# quickstart — 5-minute walkthrough

🇨🇳 [简体中文](../cn/quickstart.md)

## 1. Define a DAG

```python
# dags/my_pipeline.py
from stageflow import DAG

dag = DAG("my_pipeline")

@dag.stage()
async def s_fetch(ctx):
    # ctx.state is read-only. return dict writes state.
    return {"items": ["a", "b", "c"]}

@dag.stage(depends_on=["s_fetch"], retries=2, timeout=120)
async def s_process(ctx):
    items = ctx.state["items"]
    return {"count": len(items)}

@dag.stage(depends_on=["s_process"])
async def s_save(ctx):
    return {"saved": ctx.state["count"] > 0}
```

## 2. Run via CLI

```bash
python -m stageflow run dags/my_pipeline.py
# Outputs JSON: {task_id, dag, status, stage_statuses, state}
```

With initial state + checkpoint (resume):

```bash
STAGEFLOW_STORAGE=/data/sf \
python -m stageflow run dags/my_pipeline.py \
  --task-id task-1 --input '{"query": "north-china-tech"}'
# If interrupted, rerun with --resume to skip completed stages:
STAGEFLOW_STORAGE=/data/sf \
python -m stageflow run dags/my_pipeline.py --task-id task-1 --resume
```

## 3. Run from code

```python
import asyncio
from stageflow import Runtime, CheckpointStore
from stageflow.storage import FileStorage
from dags.my_pipeline import dag

async def main():
    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage("/data/sf")))
    result = await rt.run(dag, task_id="task-1", initial_state={"query": "x"}, resume=True)
    print(result.status, result.state)

asyncio.run(main())
```

## 4. External calls (`ctx.call`)

Stages don't import `requests`/`httpx` directly — all external calls go through `ctx.call(kind, op, params)`:

```python
@dag.stage()
async def s_search(ctx):
    results = await ctx.call("search", "my_engine", {"query": ctx.state["query"]})  # kind/op defined by business caller
    return {"sources": results["items"]}
```

The default caller is a no-op echo. Real calls are injected by your business code:

```python
rt = Runtime(caller=my_business_caller)  # async (kind, op, params) -> dict
```

## 5. Regression testing (`TestPipe`)

```python
import pytest
from stageflow import TestPipe
from dags.my_pipeline import dag

@pytest.mark.asyncio
async def test_pipeline_with_mocked_search():
    pipe = TestPipe(dag)
    pipe.mock("s_fetch", lambda state: {"items": ["a"]})
    result = await pipe.run()
    assert result.state["count"] == 1
```

## CLI commands (v0.1)

| Command | Purpose |
|---|---|
| `run <dag.py> [--task-id X] [--input JSON] [--resume]` | Run a DAG |
| `trace --task-id X` | Inspect checkpoint / stage records |
| `state --task-id X [--key K]` | Inspect state snapshot |

Further reading:
- [architecture.md](architecture.md) — architecture and design decisions
- [api.md](api.md) — public API reference
- [use-cases/](use-cases/) — reference business integrations
