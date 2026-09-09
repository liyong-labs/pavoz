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

# pavoz fork-run Examples (v0.3.0)

5 runnable scenarios showing declarative param override.

## Quick start

```bash
# 1. Model A/B (4 LLMs)
bash 01_model_switch.sh <task_id> s_compose

# 2. Change prompt without rerunning prior stages
python 02_prompt_patch.py <task_id> new_prompt.txt

# 3. Compare two fork runs (state diff)
python 03_compare_diff.py <task_id> <run_id_1> [<run_id_2> ...]

# 4. Deep-merge demo (sibling keys preserved)
python 04_deep_merge.py <task_id> [stage]

# 5. YAML file batch (with dry-run first)
bash 05_yaml_file.sh <task_id> overrides.yaml
```

## Requirements

- `pavoz >= 0.3.0`
- For YAML (`05_yaml_file.sh`): `pip install pavoz[yaml]`
