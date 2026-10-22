# MIGRATION

版本升级指南 (从旧版升到当前版要动什么)。当前覆盖 0.5.2 → 0.5.9。
每版的完整明细 (含判别力记录) 见 `CHANGELOG.md` 对应段; 这里只写 caller 视角的结论。

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

## 0.5.3 → 0.5.4

**结论: 全部 additive, 没有 caller 必改项。**

新增: `diff_runs` (两 run 的 stage 级 structured diff, 纯数据不做 AI 摘要)、`pavoz.viz` (`to_mermaid` / `to_graph_json`, 只读导出, 零依赖)、`StageError.category` + `RunResult.error_category` (业务错误分类透传, 未传/不适用为 `None`)。

## 0.5.4 → 0.5.5

**结论: additive (条件边是 opt-in 新原语), 没有 caller 必改项。**

- `DAG.add_conditional_edges(from_node, route_fn, mapping, *, max_visits=None)` (R1): 框架级条件路由/循环。未声明的路由 key → `UnmappedRouteError`; 回路边必须显式 `max_visits`; 正常终止时图不闭合 → `OrphanStagesError` (fail-loud, 决不静默报 done)。不用它 = 行为不变。
- 新事件 `route`; stage 事件加 `visit` 序号。指南: `docs/cn/conditional-edges.md`。
- 注意: 早期文档 "不做条件边" 的表述自本版作废 (YAGNI 推翻, caller 需求确认出现)。

## 0.5.5 → 0.5.6

**结论: 1 个安全修复, 建议所有更早版本升级; 其余 additive。**

- ⚠️ **`FileStorage` 路径穿越修复**: 0.5.5 及更早, `get("../../x/secret")` 可读出 storage root 之外的文件, `delete` 可将其删除。现按 `realpath` 断言仍落在 root 内, get/put/delete/list_keys 一视同仁。
- `from pavoz import PavozError`: 库自身异常统一基类, `UnmappedRouteError` / `MaxVisitsExceeded` / `OrphanStagesError` 归入其下 (原先直接继承 `Exception`)。精确 `except XxxError` 的代码不受影响; `except PavozError` 可一次兜住。
- `stage_alive` 心跳事件 (默认 30s, `None` 关); cp 增 `stage_fn_sources` (stage 函数源码漂移打 WARNING, 不阻断不拒续)。

## 0.5.6 → 0.5.7

**结论: 本仓至今行为变化最多的一版, 有必改项。** 逐条 "升级后你会看到什么 / 怎么办" 的升级注意表见 `CHANGELOG.md` 的 `[0.5.7]` 段, **升级前先读它**。高频要点:

| 变化 | caller 要做什么 |
|---|---|
| `workflow_hash` 开始含 **dag 名** → 已在盘的旧 cp 一律拒续 (`CheckpointMismatchError`; resume / fork_run / replay 三入口都查) | 该 task 重跑一次生成新 cp; 只是改过 dag 名的改回名字或换 task_id |
| 并发 run 共用 `task_id` (仅当配置了 `cancel_registry`) 从静默撞车改为抛 `TaskIdInUse` | 并发 run 各自 task_id; 或相关 Runtime 共用同一个 registry 实例 |
| `asyncio.wait_for(rt.run(...), t)` 超时改抛 `TimeoutError` (此前可能返回 cancelled 而不抛, 超时保护静默失效) | 依赖"不抛异常、返回 cancelled"行为的改判返回值 |
| `state --key K` 缺 K 时退出码 `0` → `1`, 并点名缺失 key | 拿 stdout 值做判断的脚本改判退出码 |
| `EnginePolicy` 构造期校验: 负值 / 限值类字段的 `0` 抛 `ValueError` | 想"不限 / 关闭"显式写 `None` (`default_timeout` 同理, 只拒 `0`, 亚秒合法) |
| `NaN` / `±Inf` 拒入 state | 用 `None` / 字符串表达"无值" (旧 JSON 的 `NaN` 字面量不是合法 RFC 8259) |
| `diff_runs` 输出新增键 (stage 条目加 `visits`, 顶层加 `dag_names`) | 按 key 取值不受影响; 按"键个数"做断言的消费者需放宽 |
| 循环图上的旧 fork cp 重建取错 visit / `fork_keep_counts` 缺失 | 受影响的 cp 重跑一次 (旧格式缺截断点信息, 无法推断自愈) |

## 0.5.7 → 0.5.8

**结论: 1 个 catch 点要改, 其余语义不变。**

- `FileStorage.get` 在记录损坏时抛新的 `CorruptRecordError` (已顶层导出), 不再是裸 `json.JSONDecodeError`。按 JSON 解析错误 catch 的调用点改名字。语义不变: 仍是**抛错**而不是返回 `None`。
- `FileStorage.put` 改原子写 (同目录临时文件 + `os.replace`): 崩溃/断电时磁盘留下的是上一份完整记录而非半截文件; 实测代价在噪声内。
- 顺带修复: storage 路径不展开 `~` —— 数据曾静默落进 cwd 里的字面量 `~` 目录。

## 0.5.8 → 0.5.9

**结论: 全部 additive, 没有 caller 必改项。**

- `Checkpoint.stage_attempts`: 每 stage 每 attempt 的 status / 耗时 / 错误摘要 (截断 ≤500 字符) 落盘; 旧 cp 读作 `{}`, resume 追加不覆盖; `pavoz trace` 直接可见。
- 修复: 库自己抛的用户可处置错误被报成"未预期异常"+ 整屏 traceback; 被 `skip_unchanged` 跳过的 stage 从结果与事件上可与真跑过的区分。
- 仓里删除了消费方业务包 `research/` 与 `dags/research_dag.py` — 对库的使用者无影响 (从来不在 wheel 里, 也不是公开 API)。
- examples 不再裸调 `pavos` 命令 (改经解释器调用, venv 未激活也能跑); quickstart 补 `--resume` 陷阱说明。

完整 API 细节见 `docs/cn/api.md` (English: `docs/en/api.md`), 变更明细见 `CHANGELOG.md`。
