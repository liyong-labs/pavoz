# pavoz

🇬🇧 [English version](README.md)

**进程内的 Python 工作流引擎 — 零运行时依赖, 每次运行都可 checkpoint、replay、fork。**

无服务端 / 无调度器 / 无 YAML / 无厂商锁定。用 `@dag.stage` 声明 DAG, 在自己的进程里跑,
白拿按 stage 的**断点续跑**、**单节点重放**与**时间旅行 fork**(任意历史节点取输入 → 改 → 装回)。
仅标准库, 不绑定任何模型/搜索/存储服务。

```text
Python 3.12+  |  MIT License  |  stdlib only  |  111 tests
```

pavoz 解决的是流程编排里最常用的一层: **声明式 DAG + 顺序执行 + 失败重试 +
断点续跑 + 可回归测试**。它刻意不做 macro orchestrator 的事 (调度器 / UI / 分布式),
也不替业务做决策 (模型选型、审核循环、prompt 模板都是业务代码)。

## 为什么还需要一个编排库

如果你用代码写过"搜索 → 下载 → 过滤 → 合成 → 审阅"这类多阶段流程, 大概率遇到过:

- 流程控制 (状态机/重试/超时) 和数据逻辑写在一起, 改一个阶段要翻整个文件
- 跑到第 4 步失败, 前 3 步的昂贵调用全部重来
- 想单独重跑某个阶段 / 用假数据回归, 无从下手

pavoz 把**图执行**从业务里抽出来:

```python
from pavoz import DAG

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
| **Stage 重放** | 从 checkpoint 重放: `Runtime.run_stage` 在重建的输入上重跑单 stage (调 prompt/参数不重跑前序); `TestPipe.replay_from(cp, dag)` 把真实 run 存下的 stage 输出当 mock 喂回去 — 对真实 run 做图回归 |
| **与业务解耦** | 存储 (`StorageBackend`)、外部调用 (`ctx.call` caller) 全部 Protocol, 业务侧注入 |
| **协作式取消** | `Runtime(cancel_check=...)` — stage 间/重试间检查点拦截 (status="cancelled", 已完成 stage 照常落 cp, resume 无缝续跑); stage 内长循环 `ctx.cancelled()` 轮询自退出 |
| **事件钩子** | `Runtime(on_event=...)` — run_start / stage_start / stage_end / stage_retry / run_end 五种结构化事件 (observer 异常隔离); `RunResult.stage_timings` 每 stage 墙钟耗时 |
| **零依赖** | core 仅 Python 标准库; 不发散到 psycopg/redis/boto3 等 |

## 安装

```bash
# PyPI 上线后: pip install pavoz
# 目前: git clone + 本地装
git clone git@github.com:liyong-labs/pavoz.git
cd pavoz && pip install -e ".[dev]"
```

## 存储后端 (配置驱动)

core 不带任何 storage driver — 用户自己 `pip install` 自己要的依赖
(psycopg / redis / boto3 / ...), 自己写 adapter (或抄 ai_writer 既有
`backend/integration/sf_storage.py`), 通过 config 字符串 + `load_storage()` 加载.

```python
from pavoz import load_storage, CheckpointStore

# 内置 FileStorage (stdlib, 无额外依赖)
storage = load_storage(
    "pavoz.storage.FileStorage",
    root_dir="/var/lib/pavoz/cp",
)
cp_store = CheckpointStore(storage)
```

```python
# 用户自定义 adapter — 配置里写自己的 dotted path
storage = load_storage(
    "backend.integration.sf_storage.MinioStorage",
    task_id="run-2026-09-05-001",
)
```

`load_storage("pkg.module:ClassName", **kwargs)` 用 `importlib` 加载并实例化,
错误信息含 module path + pip 安装提示. 详见 [`docs/storage.md`](docs/storage.md).

## 快速开始

```python
import asyncio
from pavoz import DAG, Runtime, FileStorage, CheckpointStore  # noqa: F401

dag = DAG("demo")

@dag.stage()
async def s_hello(ctx):
    return {"greeting": f"hello, {ctx.state.get('name', 'world')}"}

@dag.stage(depends_on=["s_hello"], retries=2)
async def s_upper(ctx):
    return {"shout": ctx.state["greeting"].upper()}

async def main():
    rt = Runtime()
    result = await rt.run(dag, task_id="demo-1", initial_state={"name": "pavoz"})
    print(result.status, result.state)
    # done {'greeting': 'hello, pavoz', 'shout': 'HELLO, PAVOZ'}

asyncio.run(main())
```

CLI 与完整示例 (checkpoint 续跑 / ctx.call / TestPipe) 见
[`docs/cn/quickstart.md`](docs/cn/quickstart.md)。
English version: [`docs/en/quickstart.md`](docs/en/quickstart.md).

## CLI

每次 `run` 都落 per-stage checkpoint (单跑也算, v0.5.1) 到
`~/.pavoz/data` (`PAVOZ_STORAGE` 可覆盖目录) — replay/trace/state
对 CLI 产物直接可用:

```bash
python -m pavoz run dags/demo.py --task-id demo-1 --input '{"query": "北方华创"}'
python -m pavoz replay dags/demo.py --task-id demo-1 --stage s_compose
#   ^-- 在重建的输入上重放单 stage (前序 stage 不重跑)
python -m pavoz trace --task-id demo-1
python -m pavoz state --task-id demo-1 --key saved
```

`replay` 还支持 `--patch P.py` (模块暴露 `patch(dag) -> None`) — 重放前
换上新实现, prompt/参数秒级迭代。`run --resume` 续跑中断的 run (跳过已
完成 stage, 复用同 run_id)。

## 时间旅行调试 (v0.6)

pavoz 为这个场景而生: **任何一次历史 run 都是可检视、可编辑、可 fork 的对象**。
每个 checkpoint 存每 stage 的原始 delta (不只是合并终态) → 任意 stage 当时的输入
可精确重建:

```bash
# 1. 某 stage 当时看到了什么? (完整输入 JSON, 人可编辑)
pavoz export-input --task-id job-1 --stage s_compose > input.json

# 2. 改完装回: 从该 stage 分支续跑 — 前序 stage 复用 (不重跑),
#    该 stage 及后继用改后输入重跑。原 run 的 checkpoint 不动。
pavoz fork-run pipeline.py --task-id job-1 --stage s_compose --input input.json
```

代码等价物:

```python
await rt.fork_run(dag, task_id="job-1", from_stage="s_compose",
                  overrides={"items": ["x", "y"]})   # 前序复用, s_compose 起重跑
```

单 stage 重放 (改 prompt 调参迭代, 不起新 run):

```bash
pavoz replay pipeline.py --task-id job-1 --stage s_compose
```

每次 fork 是新 run_id 并成为该 task 的 latest; 原历史可反复 fork 出多分支。
LLM stage 出错时, 从"当时输入是什么 → 改一句 → 只重跑那个 stage"只需几秒,
而不是重跑整条 30 分钟管线。

## 核心概念

- **DAG**: 静态声明、按拓扑顺序执行的有向无环图
- **Stage**: `async def fn(ctx) -> dict`; `ctx.state` 只读, 返回值是唯一写路径
- **State**: json-serializable; 冲突检测 (平行 producer raise, 链式演进允许)
- **Checkpoint**: 每 node 落盘 per-run checkpoint (key 含 `task_id + run_id`,
  `latest` 指针); resume 沿用同 run_id, run 已全部完成则拒续跑 (重跑 = 新 run_id)
- **ctx.call**: 外部调用唯一入口 (`kind`/`op` 由业务定义, 框架不感知)
- **TestPipe**: mock 回归

详见 [`docs/cn/architecture.md`](docs/cn/architecture.md) 和 [`docs/cn/api.md`](docs/cn/api.md)。
See also: [`docs/en/architecture.md`](docs/en/architecture.md) and [`docs/en/api.md`](docs/en/api.md).

## 设计取舍

**循环留在业务层** — 这是 pavoz 最重要的设计决策。审计级联、质量收敛这类
"同一阶段反复执行直到满足条件"的流程, 用 stage 函数内的普通 Python
`for`/`while` 表达, 而不是图原语。框架只提供 3 种能力: DAG + per-node
retries + checkpoint。成熟引擎 (Airflow/Temporal/Prefect) 也没有
cross-stage retry 原语, 原因相同。

**不做的事 (YAGNI)**: scheduler/cron、UI/可视化、分布式执行、dynamic DAG、
sub-DAG 嵌套、业务 wrapper 实现 (LLM/Search/Extract adapter)、业务表 schema。

## 什么时候用它 — 什么时候不要

**用**: 单进程内的顺序/有状态管线 — LLM 研究写作管线、需要持久状态的 agent 步骤、
要 checkpoint 的 ETL, 阶段边界事先已知、想要重试+续跑+重放但不想因此引入一个平台。

**不要用**:
- **分布式规模调度** (cron/worker 集群/多租户队列) → Temporal / Prefect / Airflow, 那是它们的地盘
- **动态图** (运行期改图形状、agent 递归 spawn) → LangGraph / Burr; pavoz 图是静态的,
  动态控制流请写在 stage 内部的普通 Python 里
- **Web UI / 可观测平台** → Temporal / Hatchet / Windmill; pavoz 给 CLI + JSON checkpoint 供你搭

## 文档

| 文档 | 内容 |
|---|---|
| [docs/cn/quickstart.md](docs/cn/quickstart.md) | 5 分钟跑通 (CLI/代码/ctx.call/TestPipe) |
| [docs/cn/architecture.md](docs/cn/architecture.md) | 架构与设计决策 |
| [docs/cn/api.md](docs/cn/api.md) | 公开 API 参考 |
| [docs/cn/use-cases/](docs/cn/use-cases/) | 业务接入参考实现 |
| [docs/en/quickstart.md](docs/en/quickstart.md) | Quickstart (English) |
| [docs/en/architecture.md](docs/en/architecture.md) | Architecture (English) |
| [docs/en/api.md](docs/en/api.md) | API Reference (English) |
| [docs/en/use-cases/](docs/en/use-cases/) | Reference integrations (English) |
| [CHANGELOG.md](CHANGELOG.md) | 版本历史 |

## 测试

```bash
pytest            # 111 tests
ruff check .      # lint
```

## License

[MIT](LICENSE)
