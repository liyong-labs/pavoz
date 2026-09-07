"""demo DAG: 模拟 research pipeline 的形状 (纯 mock, 不依赖真 LLM/SE).

    python -m pavoz run dags/demo.py --input '{"query": "北方华创"}'
"""

from pavoz import DAG

dag = DAG("demo")

# 真 caller 由业务注入; demo 用 default no-op caller (ctx.call 回显)
# 想演示 ctx.call 注入: Runtime(caller=my_caller).run(dag, ...)


@dag.stage()
async def s_plan(ctx):
    query = ctx.state.get("query", "demo query")
    return {"plan": {"queries": [query + " 背景", query + " 最新进展"]}}


@dag.stage(depends_on=["s_plan"], retries=2)
async def s_search(ctx):
    # demo: 不真搜, 造 2 条假 source (真实现里这里 ctx.call("search", ...))
    queries = ctx.state["plan"]["queries"]
    return {"sources": [{"url": f"https://example.com/{i}", "title": f"结果 {q}"} for i, q in enumerate(queries)]}


@dag.stage(depends_on=["s_search"])
async def s_compose(ctx):
    sources = ctx.state["sources"]
    return {"article": f"基于 {len(sources)} 条素材的 demo 文章"}


@dag.stage(depends_on=["s_compose"])
async def s_save(ctx):
    article = ctx.state["article"]
    return {"saved": True, "article_path": f"runs/demo/article.md", "chars": len(article)}
