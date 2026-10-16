# examples

Runnable, dependency-free demos — each is a complete program:

| example | shows | run |
|---|---|---|
| [`llm_pipeline.py`](llm_pipeline.py) | DAG + `ctx.call` caller seam (vendor-neutral LLM/search calls), checkpointed multi-stage pipeline | `python examples/llm_pipeline.py` |
| [`agent_loop.py`](agent_loop.py) | convergence loop as a plain `while` inside one stage (no framework loop DSL), then the same loop factored into a reusable multi-lens `quality_gate` decorator with score events; + forking the whole loop | `python examples/agent_loop.py` |
| [`conditional_loop.py`](conditional_loop.py) | conditional edges: declare routes on the DAG (stages stay pure), quality-gate loop with rework re-entry (`max_visits` fuse), single-point debug via `run_stage`/`fork_run`, route events with skipped branches | `python examples/conditional_loop.py` |
| [`resume_after_crash.py`](resume_after_crash.py) | crash at stage 2 → `--resume` continues from stage 2, upstream stage not re-run | `python examples/resume_after_crash.py` then `python examples/resume_after_crash.py --resume` |

A full business integration (an AI writing pipeline of
search → download → filter → compress → compose → audit → save with
MinIO-backed checkpoints and stage-level replay/fork in production) lives in
[`docs/en/use-cases/ai-writer.md`](../docs/en/use-cases/ai-writer.md).

# pavoz fork-run Examples (v0.3.0)

5 runnable scenarios showing declarative param override. 全部对着仓内的
[`demo_dag.py`](demo_dag.py) 跑 —— **不需要另一个仓库**.

> 2026-10-17: 这 5 个原先硬编码了消费方 (ai-research) 的
> `backend/integration/research_pipeline_dag.py` 与 `llm_config.research_writer.*`,
> 任何 clone pavoz 的人照着跑都是 `dag 文件不存在`; `03` 还调了 CLI 上不存在的
> `state --run-id`。现在改成自带的 demo dag + 中性 key, 并由
> `tests/test_examples_runnable.py` 把"引用的路径必须在仓内""flag 必须真存在于该
> 子命令"钉成门禁。

## Quick start

```bash
# 0. 先跑一次 demo dag, 留下 checkpoint (下面 5 个都在它上面演示)
python -m pavoz run examples/demo_dag.py --task-id demo

# 1. Model A/B (同一个 task 上换 4 个模型 fork)
bash examples/01_model_switch.sh demo s_compose

# 2. Change prompt without rerunning prior stages
echo "v2: 更严格的写作要求" > /tmp/p.txt
python examples/02_prompt_patch.py demo /tmp/p.txt

# 3. Compare two fork runs (state diff) —— run_id 从 store.list_runs / trace 拿
python examples/03_compare_diff.py demo <run_id_1> [<run_id_2> ...]

# 4. Deep-merge demo (sibling keys preserved)
python examples/04_deep_merge.py demo [stage]

# 5. YAML file batch (with dry-run first)
bash examples/05_yaml_file.sh demo overrides.yaml
```

用自己的 dag: `DAG=/path/to/your_dag.py` 前置即可 (01/02/04/05 都读它)。
注意 02/05 用 `--set-file` 写进哪个 key 得**跟着你那个 dag 的 state 结构走**,
否则 fork 会写进一个没人读的 key (静默无效)。

03 走 `CheckpointStore` 公开 API 而不是 CLI: **CLI 没有任何命令能读指定 run 的
state** (`state` 只读 latest, `trace` 只给 stage 记录) —— 库 API 能, CLI 不能。

## Requirements

- `pavoz >= 0.3.0`
- For YAML (`05_yaml_file.sh`): `pip install "pavoz[yaml]"`
