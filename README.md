# stageflow

**micro/in-process workflow engine** — 用 Python DAG 编排多阶段流程的轻量引擎。
零运行时依赖, 不绑定任何业务系统, 不绑定任何模型/搜索/存储服务。

```text
Python 3.12+  |  MIT License  |  stdlib only  |  38 tests
```

stageflow 解决的是流程编排里最常用的一层: **声明式 DAG + 顺序执行 + 失败重试 +
断点续跑 + 可回归测试**。它刻意不做 macro orchestrator 的事 (调度器 / UI / 分布式),
也不替业务做决策 (模型选型、审核循环、prompt 模板都是业务代码)。

## 为什么还需要一个编排库

如果你用代码写过"搜索 → 下载 → 过滤 → 合成 → 审阅"这类多阶段流程, 大概率遇到过:

- 流程控制 (状态机/重试/超时) 和数据逻辑写在一起, 改一个阶段要翻整个文件
- 跑到第 4 步失败, 前 3 步的昂贵调用全部重来
- 想单独重跑某个阶段 / 用假数据回归, 无从下手

stageflow 把**图执行**从业务里抽出来:

```python
from stageflow import DAG

dag = DAG("research")

@dag.stage()
async def s_search(ctx):
    # ctx.state 只读 (深拷贝); 所有外部调用走 ctx.call (caller 业务注入)
    results = await ctx.call("search", "default", {"query": ctx.state["query"]})
    return {"sources": results["items"]}

@dag.stage(depends_on=["s_search"], retries=3, timeout=120)
async def s_analyze(ctx):
    sources = ctx.state["sources"]
    return {"summary": f"分析 {len(sources)} 条素材"}

@dag.stage(depends_on=["s_analyze"])
async def s_save(ctx):
    return {"saved": True}
```

每个 stage 是纯 `async def fn(ctx) -> dict` — 读 `ctx.state`, `return` 写入。
中间失败自动重试, 每完成一个节点 checkpoint 落盘, 下次 run 断点续跑。

## 特性

| 能力 | 说明 |
|---|---|
| **DAG DSL** | `@dag.stage(depends_on, retries, timeout)` 装饰器声明; Kahn 拓扑 + Tarjan 环检测; 声明后冻结 (不支持运行时改图) |
| **异常契约** | `StageError` (业务失败不重试) / `RetryableError` (退避重试) / `FatalError` (程序 bug 立即终) — 不再用裸 `raise` 猜语义 |
| **断点续跑** | 每 node 完成即 checkpoint; resume 时校验 `workflow_hash` (改函数体不影响, 改结构拒续跑) |
| **链式数据演进** | 下游 stage 可覆盖传递上游 producer 的 key (流水线模式); 平行 producer 冲突显式报错 |
| **确定性回归** | `TestPipe` — mock 任意 stage 输出跑全图, prompt/stage 改动有保护网 |
| **与业务解耦** | 存储 (`StorageBackend`)、外部调用 (`ctx.call` caller) 全部 Protocol, 业务侧注入 |
| **零依赖** | core 仅 Python 标准库; 不发散到 psycopg/redis/boto3 等 |

## 安装

```bash
# 直接装
pip install git+ssh://git@github.com/ebziw/stageflow.git

# 开发模式
git clone git@github.com:ebziw/stageflow.git
pip install -e ".[dev]"
```

## 快速开始

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

CLI 与完整示例 (checkpoint 续跑 / ctx.call / TestPipe) 见
[`docs/quickstart.md`](docs/quickstart.md)。

## 核心概念

- **DAG**: 静态声明、按拓扑顺序执行的有向无环图
- **Stage**: `async def fn(ctx) -> dict`; `ctx.state` 只读, 返回值是唯一写路径
- **State**: json-serializable; 冲突检测 (平行 producer raise, 链式演进允许)
- **Checkpoint**: 每 node 落盘 + workflow hash 校验, 跑完自动清理
- **ctx.call**: 外部调用唯一入口 (`kind`/`op` 由业务定义, 框架不感知)
- **TestPipe**: mock 回归

详见 [`docs/architecture.md`](docs/architecture.md) 和 [`docs/api.md`](docs/api.md)。

## 设计取舍

**循环留在业务层** — 这是 stageflow 最重要的设计决策。审计级联、质量收敛这类
"同一阶段反复执行直到满足条件"的流程, 用 stage 函数内的普通 Python
`for`/`while` 表达, 而不是图原语。框架只提供 3 种能力: DAG + per-node
retries + checkpoint。成熟引擎 (Airflow/Temporal/Prefect) 也没有
cross-stage retry 原语, 原因相同。

**不做的事 (YAGNI)**: scheduler/cron、UI/可视化、分布式执行、dynamic DAG、
sub-DAG 嵌套、业务 wrapper 实现 (LLM/Search/Extract adapter)、业务表 schema。

## 文档

| 文档 | 内容 |
|---|---|
| [docs/quickstart.md](docs/quickstart.md) | 5 分钟跑通 (CLI/代码/ctx.call/TestPipe) |
| [docs/architecture.md](docs/architecture.md) | 架构与设计决策 |
| [docs/api.md](docs/api.md) | 公开 API 参考 |
| [docs/use-cases/](docs/use-cases/) | 业务接入参考实现 |
| [CHANGELOG.md](CHANGELOG.md) | 版本历史 |

## 测试

```bash
pytest            # 38 tests
ruff check .      # lint
```

## License

[MIT](LICENSE)
