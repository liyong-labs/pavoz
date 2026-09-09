#!/bin/bash
# 01_model_switch.sh — ai-research 4-model composer A/B (v0.9 declarative)
#
# 用法: bash examples/01_model_switch.sh <task_id> [stage]
# 例:   bash examples/01_model_switch.sh 37b0145c02ea s_compose
#
# v0.9 新: dot-path + 自动类型推断, 不再写嵌套 JSON string

set -euo pipefail

TASK_ID="${1:?need task_id}"
STAGE="${2:-s_compose}"
DAG="${DAG:-backend/integration/research_pipeline_dag.py}"

MODELS=(
  "deepseek/v4-pro-1m"
  "minimax/m3-1m"
  "longcat-official/longcat-2.0"
  "glm/glm-5.3-flash"
)

echo "=== 4-model composer A/B (v0.9 declarative) ==="
echo "task_id=$TASK_ID  stage=$STAGE"
echo

for model in "${MODELS[@]}"; do
  echo "--- Running with model: $model ---"
  pavoz fork-run "$DAG" \
    --task-id "$TASK_ID" \
    --stage "$STAGE" \
    --set "llm_config.research_writer.model=$model" \
    --compare-with "${COMPARE_WITH:-}"
  echo
done

echo "=== Done. Compare fork_run_ids in checkpoint dir. ==="
