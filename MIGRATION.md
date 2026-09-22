# MIGRATION

版本升级指南 (从旧版升到当前版要动什么)。当前覆盖 0.5.2 → 0.5.3。

## 0.5.2 → 0.5.3

**结论: 全部 additive, 没有 caller 必改项。** 不接新能力 = 引擎行为与 0.5.2 完全一致。

### 数值差异 (仅一处, 且默认值不变)

- RetryableError 重试退避上限从硬编码 30s 改为 `EnginePolicy.backoff_max` (默认仍 30.0) — 不传 policy 则数值不变。

### 新能力 (opt-in, 按需接)

| 能力 | 最小用法 | 说明 |
|---|---|---|
| 错误上下文 (W1) | `result.failed_stage / retryable / state_summary` | 失败/取消即得 "哪个 stage 错 / 能否重试 / state 现场 (截断 ≤2048 字符)" |
| 事件流 (W2) | `Runtime(on_event=EventRecorder(sink_path="events.jsonl"))` | 生命周期事件流 (`stage_end` 带 stage / status / duration), 可选 JSONL 落盘 |
| debug 快照 (W2) | `Runtime(debug_dir="...")` | 每 stage 后落 state 快照 JSON, 失败不杀 run |
| EnginePolicy (W3) | `Runtime(policy=EnginePolicy(max_steps=50))` | run 级护栏 (deadline / 熔断 / 退避 / 并发闸 / skip 默认); 字段默认值与何时覆写见 `EnginePolicy` docstring |
| fork 跳过未变 stage (R2) | `await rt.fork_run(dag, task_id, from_stage="s_compose", skip_unchanged=True)` | stage 输入指纹比对, 命中即重放历史 delta; 副作用 stage 标 `Stage.skip_unchanged=False` 拒跳 |
| 取消 (R1a) | `reg = CancelRegistry()` → `Runtime(cancel_registry=reg, ...)` → `reg.cancel(task_id, mode="hard")` | graceful = stage 边界拦截; hard = 立即打断 (前提 stage 幂等), `Stage.killable=False` 拒硬杀 → `NotKillable` |
| 状态查询 (R1b) | `rt.get_state(task_id)` / `store.list_tasks()` | 查最新 run 全量业务 state / 跨 task 巡检 (per-stage status+duration 在 `RunResult.stage_statuses / stage_timings`, 不在 state) |

完整 API 细节见 `docs/cn/api.md` (English: `docs/en/api.md`), 变更明细见 `CHANGELOG.md`。
