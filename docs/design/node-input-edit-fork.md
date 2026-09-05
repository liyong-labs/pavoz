# Stage 输入取改回 — Time-Travel Fork (v0.6)

> user 拍板 (2026-09-06): **任意选一个过去的完成节点, 可以立刻取当时的输入修改一下,
> 再装回去调试** — 甚至非工程师可操作. 本文 = 设计 + 竞品依据 + 已落地实现.

## 需求拆解

| 动作 | 含义 | stageflow 落点 |
|---|---|---|
| 任意选一个过去节点 | 已 checkpoint 的 stage (每 stage 后落盘 = 天然边界) | `CheckpointStore.load_latest` + stage 列表 |
| 取当时的输入 | stage 执行前 state = initial + 该 stage 前全部 deltas | `Checkpoint.rebuild_state_before(stage)` (M3 已有) |
| 修改 | 编辑 JSON (json-serializable state 强制 → 天然可编辑) | `export-input` CLI 输出 / 人或工具改 |
| 装回去 | 注入编辑后的 state, 从该 stage 续跑 | `Runtime.fork_run` (v0.6 新增) |

## 竞品调研 (2026-09-06, exa)

| 系统 | 能力 | 与我们的差距/借鉴 |
|---|---|---|
| **LangGraph** | Time travel 双模式: **replay** (历史 cp 只读重放) / **fork** (`update_state` 在历史 cp 分支, `invoke(None)` 续跑; 原历史不动; fork checkpoint 成为 latest; 状态更新按 node writers/reducers 应用, `as_node` 指定归因) | stageflow 已有 replay (run_stage); 缺 fork → **v0.6 照此实现**: fork cp (新 run_id) + latest 指向 fork + overrides 注入 = "as_node" 归因的简化 (producers 记 `<fork>`) |
| **Prefect** | **无** "restart from task X / 改参后 UI retry" 内置. 官方答案: copy run + 改 flow 参数全量重跑 + 靠 task caching 复用; 同 run 改参数 "unusual operationally" | 印证: fork-as-new-run + 前序结果复用是正确范式 (stageflow 前序 deltas 复用 = 缓存超集) |
| Temporal / Airflow | workflow input 不可变; 修输入 = 新 run | fork 语义一致 |

**设计取向**: LangGraph fork 范式 + stageflow 线性 topo 简化 (无并行 super-step, done_stages
即执行序 → truncate 为纯后缀操作, 无 reducer 需求, 顶层 key 覆盖即可).

## 已落地实现 (v0.6, 全部测试绿)

### `Runtime.fork_run(dag, task_id, *, from_stage, overrides=None, run_id=None) -> RunResult`

1. `load_latest` + workflow_hash 校验 (同 run_stage, mismatch → CheckpointMismatchError)
2. 校验: from_stage 在 DAG 内; 其依赖全部 done (执行前 state 可重建)
3. **truncate**: done_stages 截到 from_stage 之前 (线性执行序 → 前缀保留)
   - from_stage 已 done → 移除它及其后继 → 重跑
   - from_stage 失败/未完成 (依赖已完成) → 前缀即全部 done → 等价"注入式 resume"
4. **state 重建**: initial + 保留 deltas (按序) + `overrides` (用户注入, 最后生效,
   producers 记 `<fork>`) — 全程 `deep_validate_state` (json-serializable 保证)
5. save fork cp (新 run_id) → **latest 指针移到 fork** (LangGraph: fork is latest)
6. 委托 `run(resume=True)`: 跳过保留前缀, 从 from_stage 顺序执行, 逐 stage 落 cp

原 run 的 checkpoint 全程不动 (`list_runs` 可见多 run 分支). ai_writer 的
runner 恒 `resume=False` → latest 指针移动不影响其业务语义.

### CLI

```bash
stageflow export-input --task-id X --stage s_b        # 打印 s_b 执行前输入 JSON
stageflow export-input --task-id X --stage s_b > in.json   # 存文件给人编辑
stageflow fork-run dags/demo.py --task-id X --stage s_b --input in.json   # 装回续跑
stageflow fork-run dags/demo.py --task-id X --stage s_b --overrides '{"k":"v"}'
```

### 边界与语义

- overrides = **顶层 key 覆盖** (LangGraph channel 值语义); 嵌套结构编辑 = 整值替换.
  JSON-pointer patch 留 v0.7 (有 reducer/嵌套需求时)
- fork 可对同一历史反复做 (多次 fork = 多分支), 各自新 run_id; 不清理
- `from_stage` 未完成 (失败点): overrides 注入失败 stage 的执行前 state →
  修输入重跑失败点 (对齐 "装回去调试")
- export-input 仅支持已执行过 (有 delta/完成记录) 的 stage — 失败 stage 的
  "输入" = 全部已完成前缀的重建 (rebuild_state), 与 run_stage 同语义

## 测试 (tests/test_fork.py, 6 例)

前缀复用不重跑 / latest 移动 + resume done-guard / 失败点 fork 重试 /
export-input→编辑→fork roundtrip / 未知 stage + 依赖缺失拒绝.

## 后续 (非 v0.6 范围)

- **Web/可视化** (user: "甚至个人都可以做"): fork 树浏览 + 输入 JSON 编辑框 +
  新旧 diff — 依赖 v0.6 primitives, 放 stageflow server 或业务侧 UI
- **reducer/嵌套 patch**: 有真实需求再加 (LangGraph reducers 是 channel 级,
  我们的 json-state 顶层覆盖已覆盖 90% 场景)
