# quickstart — 5 分钟跑通

🇬🇧 [English version](../en/quickstart.md)

## 1. 定义 DAG

```python
# dags/my_pipeline.py
from pavoz import DAG

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
python -m pavoz run dags/my_pipeline.py
# 输出 JSON: {task_id, dag, status, stage_statuses, state}
```

带初始 state + checkpoint (resume):

```bash
PAVOZ_STORAGE=/data/sf \
python -m pavoz run dags/my_pipeline.py \
  --task-id task-1 --input '{"query": "北方华创"}'
# 中途断了再跑 (--resume): 已完成 stage 跳过
PAVOZ_STORAGE=/data/sf \
python -m pavoz run dags/my_pipeline.py --task-id task-1 --resume
```

## 3. 跑 (代码)

```python
import asyncio
from pavoz import Runtime, CheckpointStore
from pavoz.storage import FileStorage
from dags.my_pipeline import dag

async def main():
    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage("/data/sf")))
    # 首跑: resume=False (默认) — 起新 run_id.
    # 中途断了再跑: resume=True 跳过已完成 stage (复用原 run_id;
    # run 已全部完成 → RuntimeError, 重跑 resume=False 起新 run_id).
    result = await rt.run(dag, task_id="task-1", initial_state={"query": "x"})
    print(result.status, result.state, result.run_id)

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
from pavoz import TestPipe
from dags.my_pipeline import dag

@pytest.mark.asyncio
async def test_pipeline_with_mocked_search():
    pipe = TestPipe(dag)
    pipe.mock("s_fetch", lambda state: {"items": ["a"]})
    result = await pipe.run()
    assert result.state["count"] == 1
```

## CLI 命令 (v0.5.1)

每次 `run` 都会在每 stage 完成后落 checkpoint (单跑也算, v0.5.1) —
`trace`/`state`/`replay` 对任意 CLI run 直接可用:

| 命令 | 用途 |
|---|---|
| `run <dag.py> [--task-id X] [--input JSON] [--resume]` | 跑 DAG (每 stage 落 checkpoint) |
| `trace --task-id X` | 看 checkpoint / stage 记录 |
| `state --task-id X [--key K]` | 看 state snapshot |
| `replay <dag.py> --task-id X --stage S [--patch P.py]` | 在重建输入上重放单 stage (前序 stage 不重跑) |

重放示例 — 改完某个 stage 的 prompt/参数后, 只重跑 `s_process`:

```bash
python -m pavoz replay dags/my_pipeline.py --task-id task-1 --stage s_process
```

更多参考:
- [architecture.md](architecture.md) — 架构与设计决策
- [api.md](api.md) — 公开 API 参考
- [use-cases/](use-cases/) — 业务接入参考实现
