# examples

Runnable, dependency-free demos — each is a complete program:

| example | shows | run |
|---|---|---|
| [`llm_pipeline.py`](llm_pipeline.py) | DAG + `ctx.call` caller seam (vendor-neutral LLM/search calls), checkpointed multi-stage pipeline | `python examples/llm_pipeline.py` |
| [`agent_loop.py`](agent_loop.py) | review → fix → re-review convergence loop as a plain `while` inside one stage (no framework loop DSL) + forking the whole loop | `python examples/agent_loop.py` |
| [`resume_after_crash.py`](resume_after_crash.py) | crash at stage 2 → `--resume` continues from stage 2, upstream stage not re-run | `python examples/resume_after_crash.py` then `python examples/resume_after_crash.py --resume` |

A full business integration (an AI writing pipeline of
search → download → filter → compress → compose → audit → save with
MinIO-backed checkpoints and stage-level replay/fork in production) lives in
[`docs/en/use-cases/ai-writer.md`](../docs/en/use-cases/ai-writer.md).
