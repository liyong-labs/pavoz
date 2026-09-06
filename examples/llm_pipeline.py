"""Example 1 — LLM research pipeline with a pluggable caller.

Shows the three ideas that make stageflow different:
1. Graph execution is extracted from business code (@dag.stage).
2. External calls (LLM/search/...) go through ctx.call — the *caller*
   is injected by you, so stageflow never binds a model vendor.
3. Every stage is checkpointed; a crash never repeats upstream work.

Run:  python examples/llm_pipeline.py
      (prints a fake 4-stage research pipeline end to end)
"""

import asyncio

from stageflow import CheckpointStore, DAG, FileStorage, Runtime

# ── Your caller: the only place stageflow touches the outside world ──────
# Implement any ops your stages need ("llm", "search", ...). stageflow does
# not know what an op means — that is your business layer.
async def my_caller(kind: str, op: str, params: dict, meta: dict | None = None) -> dict:
    if kind == "search":
        # A real implementation would call SearXNG / Brave / your engine.
        query = params["query"]
        return {"items": [{"url": f"https://example.com/{i}", "title": f"result {i} for {query}"}
                          for i in range(3)]}
    if kind == "llm":
        prompt = params["prompt"]
        return {"text": f"[{op} summary of: {prompt[:60]}...]"}
    raise KeyError(f"unknown call kind {kind!r}")


dag = DAG("research")


@dag.stage()
async def s_search(ctx):
    query = ctx.state.get("query", "default query")
    results = await ctx.call("search", "web", {"query": query, "top_k": 3})
    return {"sources": results["items"]}


@dag.stage(depends_on=["s_search"], retries=2, timeout=120)
async def s_analyze(ctx):
    sources = ctx.state["sources"]
    llm = await ctx.call("llm", "analyze", {
        "prompt": "Synthesize these sources: " + ", ".join(s["title"] for s in sources),
    })
    return {"analysis": llm["text"]}


@dag.stage(depends_on=["s_analyze"])
async def s_save(ctx):
    return {"report": f"sources={len(ctx.state['sources'])}\n{ctx.state['analysis']}"}


async def main() -> None:
    rt = Runtime(caller=my_caller, checkpoint_store=CheckpointStore(FileStorage("./data")))
    result = await rt.run(dag, task_id="pipeline-1", initial_state={"query": "durable execution"})
    assert result.status == "done"
    print("saved report:\n", result.state["report"])
    print("\nnext: stageflow replay examples/llm_pipeline.py --task-id pipeline-1 --stage s_analyze")


if __name__ == "__main__":
    asyncio.run(main())
