#!/bin/bash
# 01_model_switch.sh — 同一个 task 上换 4 个模型跑 A/B (v0.9 declarative)
#
# 用法: bash examples/01_model_switch.sh <task_id> [stage]
# 例:   python -m pavoz run examples/demo_dag.py --task-id demo   # 先有 task
#       bash examples/01_model_switch.sh demo s_compose
#
# v0.9 新: dot-path + 自动类型推断, 不再写嵌套 JSON string
#
# ⚠️ 2026-10-17: 本文件原先硬编码 `backend/integration/research_pipeline_dag.py`
# (消费方仓库的路径) 与 `llm_config.research_writer.*` —— 任何 clone pavoz 的人
# 照 README 跑都会 "dag 文件不存在"。现在默认用本仓自带的 examples/demo_dag.py,
# 业务方要用自己的 dag 就 `DAG=/path/to/your_dag.py` 覆盖 (路径按**脚本所在目录**
# 解析, 所以在哪个 cwd 跑都对)。
set -euo pipefail

# 用 `python -m pavos` 而不是裸 `pavos`: 裸命令要求 venv 的 bin 在 PATH 上,
# 而 venv 没激活 (或用 pipx / 另一个解释器) 时它是 not found —— 而这条消息对
# 读者毫无指向性 (2026-10-17 实测)。解释器可用 PAVOZ_PYTHON 覆盖。
PAVOZ_PY="${PAVOZ_PYTHON:-python}"


SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

TASK_ID="${1:?need task_id (先跑一次 demo_dag.py 留下 checkpoint)}"
STAGE="${2:-s_compose}"
DAG="${DAG:-$SCRIPT_DIR/demo_dag.py}"

[ -f "$DAG" ] || { echo "dag 文件不存在: $DAG (可用 DAG=<path> 指定你自己的)" >&2; exit 1; }

# 中性占位模型名 —— 本仓不替用户绑 vendor; 想试真模型就改这里
MODELS=(
  "model-a"
  "model-b"
  "model-c"
  "model-d"
)

echo "=== 4-model composer A/B (v0.9 declarative) ==="
echo "task_id=$TASK_ID  stage=$STAGE"
echo "dag=$DAG"
echo

for model in "${MODELS[@]}"; do
  echo "--- fork-run with model: $model ---"
  if [ -n "${COMPARE_WITH:-}" ]; then
    "$PAVOZ_PY" -m pavoz fork-run "$DAG" \
      --task-id "$TASK_ID" \
      --stage "$STAGE" \
      --set "llm_config.writer.model=$model" \
      --compare-with "$COMPARE_WITH"
  else
    # COMPARE_WITH 没给时**不能**传空串占位 (本文件第一版就是 `--compare-with "${...:-}"`,
    # 靠下游 falsy 判断兜住) —— 显式两分支更清楚, 也不给"空 run_id"留歧义
    "$PAVOZ_PY" -m pavoz fork-run "$DAG" \
      --task-id "$TASK_ID" \
      --stage "$STAGE" \
      --set "llm_config.writer.model=$model"
  fi
  echo
done

echo "=== Done. 每个 fork 都留下一个 run; 用 examples/03_compare_diff.py 对比 ==="
