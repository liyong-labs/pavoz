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

# 用 `python -m pavos` 而不是裸 `pavos`: 裸命令要求 venv 的 bin 在 PATH 上,
# 而 venv 没激活 (或用 pipx / 另一个解释器) 时它是 not found —— 而这条消息对
# 读者毫无指向性 (2026-10-17 实测)。解释器可用 PAVOZ_PYTHON 覆盖。
PAVOZ_PY="${PAVOZ_PYTHON:-python}"


SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

TASK_ID="${1:?need task_id (先跑一次 demo_dag.py 留下 checkpoint)}"
YAML_FILE="${2:?need yaml file}"
DAG="${DAG:-$SCRIPT_DIR/demo_dag.py}"

# 存在性检查也跟着换成解释器形式 —— 查 `command -v pavoz` 只在 venv 的 bin 在 PATH 上
# 才成立, 而失败时那句"pavoz 未安装"会把读者引向"我没装吗", 而真实原因常是"装了但
# 这个解释器看不到"。这里直接问解释器能不能 import。
if ! "$PAVOZ_PY" -c "import pavoz" >/dev/null 2>&1; then
  echo "当前解释器 ($PAVOZ_PY) 里没有 pavoz —— 装了别的解释器也会这样。" >&2
  echo "  试: PAVOZ_PYTHON=/path/to/venv/bin/python bash $0 ... (或先激活对应 venv)" >&2
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
"$PAVOZ_PY" -m pavoz fork-run "$DAG" \
  --task-id "$TASK_ID" \
  --stage "s_compose" \
  --set-file "$YAML_FILE" \
  --dry-run

echo
echo "如果上面输出正确, 去掉 --dry-run 重跑 (去掉下一行注释)"
# pavoz fork-run "$DAG" --task-id "$TASK_ID" --stage s_compose --set-file "$YAML_FILE"
