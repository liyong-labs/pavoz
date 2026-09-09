#!/bin/bash
# 05_yaml_file.sh — YAML 文件批量 (v0.9 + [yaml] extra)

set -euo pipefail

TASK_ID="${1:?need task_id}"
YAML_FILE="${2:?need yaml file}"

if ! command -v pavoz >/dev/null 2>&1; then
  echo "pavoz 未安装"
  exit 1
fi

# yaml lazy import — 缺 yaml 时 pavoz 自动报错 + 提示 install
YAML_SIZE=$(stat -c%s "$YAML_FILE")
if [ "$YAML_SIZE" -gt 1000000 ]; then
  echo "YAML file $YAML_SIZE > 1MB, 拒绝"
  exit 1
fi

echo "=== Dry-run first (recommended) ==="
pavoz fork-run backend/integration/research_pipeline_dag.py \
  --task-id "$TASK_ID" \
  --stage "s_compose" \
  --set-file "$YAML_FILE" \
  --dry-run

echo
echo "如果上面输出正确, 去掉 --dry-run 重跑 (去掉本行注释)"
# pavoz fork-run backend/integration/research_pipeline_dag.py --task-id "$TASK_ID" --stage s_compose --set-file "$YAML_FILE"
