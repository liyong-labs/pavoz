# quickstart — 5 分钟跑通

## 1. 定义 DAG

```python
# dags/my_pipeline.py
from stageflow import DAG

dag = DAG("my_pipeline")

@dag.stage()
async def s_fetch(ctx):
    # ctx.state 只读. return dict 写 state.
    return {"items": ["a", "b", "c"]}

@dag.stage(depends_on=["s_fetch"], retries=2, timeout=120)
async def s_process(ctx):
    items = ctx.state["items"]
    return {"count": len(items)}

@dag.stage(depends_on=["s_process"])
async def s_save(ctx):
    return {"saved": ctx.state["count"] > 0}
```

## 2. 跑 (CLI)

```bash
python -m stageflow run dags/my_pipeline.py
# 输出 JSON: {task_id, dag, status, stage_statuses, state}
```

带初始 state + checkpoint (resume):

```bash
STAGEFLOW_STORAGE=/data/sf \
python -m stageflow run dags/my_pipeline.py \
  --task-id task-1 --input '{"query": "北方华创"}'
# 中途断了再跑 (--resume): 已完成 stage 跳过
STAGEFLOW_STORAGE=/data/sf \
python -m stageflow run dags/my_pipeline.py --task-id task-1 --resume
```

## 3. 跑 (代码)

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

## 4. 对外调用 (ctx.call)

stage 不直接 import requests/httpx — 通过 `ctx.call(kind, op, params)`:

```python
@dag.stage()
async def s_search(ctx):
    results = await ctx.call("search", "my_engine", {"query": ctx.state["query"]})  # kind/op 由业务 caller 定义
    return {"sources": results["items"]}
```

默认 caller 是 no-op echo. 真调用由业务注入:

```python
rt = Runtime(caller=my_business_caller)  # async (kind, op, params) -> dict
```

## 5. 回归测试 (TestPipe)

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

## CLI 命令 (v0.1)

| 命令 | 用途 |
|---|---|
| `run <dag.py> [--task-id X] [--input JSON] [--resume]` | 跑 DAG |
| `trace --task-id X` | 看 checkpoint / stage 记录 |
| `state --task-id X [--key K]` | 看 state snapshot |
| `inspect --task-id X` | wrapper call 详情 (v0.2, adapter 接入后) |
