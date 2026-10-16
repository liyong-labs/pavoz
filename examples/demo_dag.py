#!/usr/bin/env python3
"""demo_dag.py — fork-run 系列 example 用的**自包含** dag (v0.5.7).

为什么有这个文件: 01/02/04/05 这四个 example 原先硬编码了
`backend/integration/research_pipeline_dag.py` —— 那是**消费方仓库**的路径, pavoz 仓里
根本没有。于是"照着 examples/README 跑一遍"对任何 clone 的人都是 `dag 文件不存在`
(2026-10-17 实测)。它们连同 `llm_config.research_writer.*` 这个 key 与 LongCat 模型名
一起, 都是业务侧的具体值漏进了开源仓 —— 既跑不通, 也不符合 pavoz 不绑业务的口径。

所以: dag 收进本仓, key 改成中性的 `llm_config.writer.*`, 模型名改成占位值。
**业务方要用自己的 dag, 传自己的路径即可** (下面每个 example 都支持 `DAG=` 环境变量
或第一个参数覆盖)。

用法:
    python -m pavoz run examples/demo_dag.py --task-id demo    # 先跑一次, 留下 cp
    # 之后 01/02/04/05 都在这个 task 上演示 fork-run
"""
import asyncio
import os

from pavoz import DAG

dag = DAG("demo_pipeline")


@dag.stage()
async def s_plan(ctx):
    """产出一个**嵌套**配置 —— 深合并演示要的就是这种形状。

    三层嵌套 + 多个兄弟键: 只覆盖 `model` 一个 leaf 时, `temperature` / `prompt`
    必须原样保留 (v0.8 的浅替换会把它们全抹掉)。
    """
    return {
        "topic": "chickens",
        "llm_config": {
            "writer": {
                "model": "base-model",
                "temperature": 0.7,
                "prompt": "v1",
            },
            "reviewer": {"model": "base-model", "temperature": 0.3},
        },
    }


@dag.stage(depends_on=["s_plan"])
async def s_compose(ctx):
    """读全部三个配置项 —— 少一个都能立刻看出"深合并把兄弟键抹掉了"。"""
    w = ctx.state["llm_config"]["writer"]
    r = ctx.state["llm_config"]["reviewer"]
    return {
        "doc": (f"{ctx.state['topic']} | writer={w['model']}@{w['temperature']} "
                f"prompt={w['prompt']} | reviewer={r['model']}"),
    }


if __name__ == "__main__":
    task_id = os.environ.get("TASK_ID", "demo")
    from pavoz import Runtime

    result = asyncio.run(Runtime().run(dag, task_id))
    print(f"status={result.status}")
    print(f"doc={result.state.get('doc')}")
    print(f"\n下一步: python examples/04_deep_merge.py {task_id}")
