#!/bin/bash
# 05_yaml_file.sh — YAML 文件批量覆盖 (v0.9 + pyyaml extra)
#
# 用法: bash examples/05_yaml_file.sh <task_id> <yaml_file>
#   python -m pavoz run examples/demo_dag.py --task-id demo   # 先有 task
#   cat > /tmp/ov.yaml <<'YAML'
#   llm_config:
#     writer:
#       model: yaml-model
#       temperature: 0.9
#   YAML
#   bash examples/05_yaml_file.sh demo /tmp/ov.yaml
#
# ⚠️ 2026-10-17: 原先硬编码消费方 dag 路径; 现在默认本仓 examples/demo_dag.py
# (按脚本所在目录解析, 与 cwd 无关), 业务方用 DAG=<path> 覆盖。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

TASK_ID="${1:?need task_id (先跑一次 demo_dag.py 留下 checkpoint)}"
YAML_FILE="${2:?need yaml file}"
DAG="${DAG:-$SCRIPT_DIR/demo_dag.py}"

if ! command -v pavoz >/dev/null 2>&1; then
  echo "pavoz 未安装 (pip install pavoz)" >&2
  exit 1
fi
[ -f "$DAG" ] || { echo "dag 文件不存在: $DAG (可用 DAG=<path> 指定你自己的)" >&2; exit 1; }
[ -f "$YAML_FILE" ] || { echo "yaml 文件不存在: $YAML_FILE" >&2; exit 1; }

# 1MB 上限与 pavoz 内部一致 —— 这里先挡一次, 免得 dry-run 才报错
YAML_SIZE=$(stat -c%s "$YAML_FILE")
if [ "$YAML_SIZE" -gt 1000000 ]; then
  echo "YAML file $YAML_SIZE > 1MB, 拒绝" >&2
  exit 1
fi

echo "=== Dry-run first (recommended) ==="
pavoz fork-run "$DAG" \
  --task-id "$TASK_ID" \
  --stage "s_compose" \
  --set-file "$YAML_FILE" \
  --dry-run

echo
echo "如果上面输出正确, 去掉 --dry-run 重跑 (去掉下一行注释)"
# pavoz fork-run "$DAG" --task-id "$TASK_ID" --stage s_compose --set-file "$YAML_FILE"
