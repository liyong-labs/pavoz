# stageflow

🇨🇳 [简体中文](README.cn.md)

**micro/in-process workflow engine** — a lightweight Python DAG-based engine for orchestrating multi-stage pipelines.
Zero runtime dependencies, no business system lock-in, no binding to any model/search/storage service.

```text
Python 3.12+  |  MIT License  |  stdlib only  |  38 tests
```

stageflow tackles the most common layer of pipeline orchestration: **declarative DAG + sequential execution + retry on failure + checkpoint resume + regression testing**. It deliberately avoids macro orchestrator concerns (scheduler / UI / distributed execution) and never makes business decisions for you (model selection, audit loops, and prompt templates are business code).

## Why another orchestration library

If you've ever written a pipeline like "search → download → filter → synthesize → review" in code, you've probably run into:

- Flow control (state machine / retry / timeout) tangled with data logic — changing one stage means scrolling through the whole file
- A failure at step 4 forces the expensive calls from steps 1-3 to rerun
- No way to rerun a single stage in isolation or run regression tests with mock data

stageflow pulls **graph execution** out of business code:

```python
from stageflow import DAG

dag = DAG("research")

@dag.stage()
async def s_search(ctx):
    # ctx.state is read-only (deep copy); all external calls go through ctx.call (caller-injected)
    results = await ctx.call("search", "default", {"query": ctx.state["query"]})
    return {"sources": results["items"]}

@dag.stage(depends_on=["s_search"], retries=3, timeout=120)
async def s_analyze(ctx):
    sources = ctx.state["sources"]
    return {"summary": f"analyzed {len(sources)} items"}

@dag.stage(depends_on=["s_analyze"])
async def s_save(ctx):
    return {"saved": True}
```

Each stage is a pure `async def fn(ctx) -> dict` — read `ctx.state`, `return` to write.
Automatic retry on transient failures, checkpoint after each node, resume from where you left off on the next run.

## Features

| Capability | Description |
|---|---|
| **DAG DSL** | `@dag.stage(depends_on, retries, timeout)` decorator declaration; Kahn topological + Tarjan cycle detection; frozen after declaration (no runtime graph mutation) |
| **Exception contract** | `StageError` (business failure, no retry) / `RetryableError` (backoff retry) / `FatalError` (program bug, terminate immediately) — no more guessing semantics from raw `raise` |
| **Checkpoint resume** | Checkpoint after each node completes; resume validates `workflow_hash` (function body changes don't matter, structural changes refuse resume) |
| **Chained data evolution** | Downstream stages may overwrite keys passed through by upstream producers (pipeline pattern); parallel producer conflicts raise explicitly |
| **Deterministic regression** | `TestPipe` — mock any stage output to run the whole graph; a safety net for prompt/stage changes |
| **Business decoupled** | Storage (`StorageBackend`) and external calls (`ctx.call` caller) are Protocols, injected by business code |
| **Zero dependencies** | Core uses only Python stdlib; no drift into psycopg/redis/boto3 |

## Installation

```bash
# Direct install
pip install git+ssh://git@github.com/ebziw/stageflow.git

# Development mode
git clone git@github.com:ebziw/stageflow.git
pip install -e ".[dev]"
```

## Optional Contrib (StorageBackend adapters)

```bash
pip install stageflow[postgres]   # psycopg3 + JSONB
pip install stageflow[mysql]      # PyMySQL + LONGTEXT
pip install stageflow[redis]      # redis-py
pip install stageflow[minio]      # boto3 (AWS S3 / MinIO / R2)
pip install stageflow[contrib]    # 4 above together
pip install stageflow[all]        # alias for [contrib]
pip install stageflow[sqlite]     # stdlib, no extra deps
```

用法: `from stageflow.contrib.storage import PostgresStorage, MinioStorage, ...`

每个 adapter 只装对应 deps 时可 import, 缺 driver 抛 ImportError + `pip install 'stageflow[<name>]'` 提示. 详见 [`docs/contrib.md`](docs/contrib.md).

## Quick Start

```python
import asyncio
from stageflow import DAG, Runtime
from stageflow.storage import FileStorage, CheckpointStore  # noqa: F401

dag = DAG("demo")

@dag.stage()
async def s_hello(ctx):
    return {"greeting": f"hello, {ctx.state.get('name', 'world')}"}

@dag.stage(depends_on=["s_hello"], retries=2)
async def s_upper(ctx):
    return {"shout": ctx.state["greeting"].upper()}

async def main():
    rt = Runtime()
    result = await rt.run(dag, task_id="demo-1", initial_state={"name": "stageflow"})
    print(result.status, result.state)
    # done {'greeting': 'hello, stageflow', 'shout': 'HELLO, STAGEFLOW'}

asyncio.run(main())
```

CLI and complete examples (checkpoint resume / `ctx.call` / `TestPipe`) in
[`docs/en/quickstart.md`](docs/en/quickstart.md).
中文版示例见 [`docs/cn/quickstart.md`](docs/cn/quickstart.md).

## Core Concepts

- **DAG**: a statically declared, topologically ordered directed acyclic graph
- **Stage**: `async def fn(ctx) -> dict`; `ctx.state` is read-only, return value is the only write path
- **State**: must be json-serializable; conflict detection (parallel producers raise, chained evolution allowed)
- **Checkpoint**: every node persists + workflow hash validated, auto-cleanup after run completes
- **ctx.call**: the sole entry point for external calls (`kind`/`op` defined by business code, framework is agnostic)
- **TestPipe**: mock-based regression

See [`docs/en/architecture.md`](docs/en/architecture.md) and [`docs/en/api.md`](docs/en/api.md).
中文版见 [`docs/cn/architecture.md`](docs/cn/architecture.md) 和 [`docs/cn/api.md`](docs/cn/api.md)。

## Design Tradeoffs

**Loops stay in business code** — this is stageflow's most important design decision. Processes that need to "rerun the same stage until a condition is met" (audit cascades, quality convergence) are expressed with normal Python `for`/`while` loops inside a stage function, not as graph primitives. The framework offers only three capabilities: DAG + per-node retries + checkpoint. Mature engines (Airflow/Temporal/Prefect) also lack cross-stage retry primitives, for the same reason.

**What we don't do (YAGNI)**: scheduler/cron, UI/visualization, distributed execution, dynamic DAG, sub-DAG nesting, business wrapper implementations (LLM/Search/Extract adapters), business table schemas.

## Documentation

| Document | Content |
|---|---|
| [docs/en/quickstart.md](docs/en/quickstart.md) | 5-minute walkthrough (CLI/code/ctx.call/TestPipe) |
| [docs/en/architecture.md](docs/en/architecture.md) | Architecture and design decisions |
| [docs/en/api.md](docs/en/api.md) | Public API reference |
| [docs/en/use-cases/](docs/en/use-cases/) | Reference business integrations |
| [docs/cn/quickstart.md](docs/cn/quickstart.md) | 中文版快速上手 |
| [docs/cn/architecture.md](docs/cn/architecture.md) | 中文版架构说明 |
| [docs/cn/api.md](docs/cn/api.md) | 中文版 API 参考 |
| [docs/cn/use-cases/](docs/cn/use-cases/) | 中文版业务接入参考 |
| [CHANGELOG.md](CHANGELOG.md) | Version history |

## Testing

```bash
pytest            # 38 tests
ruff check .      # lint
```

## License

[MIT](LICENSE)
