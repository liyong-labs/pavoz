# stageflow
LLM/SE/Extract 流程编排系统 — micro/in-process workflow engine.

独立于业务系统 (ai_writer / 未来其他项目) 的编排层: DAG 声明 + Runtime 执行 + checkpoint 恢复 + TestPipe 回归.
业务循环 (audit cascade / revise 等) 留在 stage 函数内用普通 Python 表达, 框架只管线性图.

```python
from stageflow import DAG

dag = DAG("demo")

@dag.stage()
async def s_hello(ctx):
    return {"greeting": f"hello, {ctx.state.get('name', 'world')}"}

@dag.stage(depends_on=["s_hello"], retries=2)
async def s_upper(ctx):
    return {"shout": ctx.state["greeting"].upper()}
```

快速开始: `docs/quickstart.md`. 架构: `docs/architecture.md`. ai_writer 对接: `docs/ai-writer-integration.md`.
