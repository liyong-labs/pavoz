# Roadmap

> v0.2 计划 (2026-09-05 brainstorm 定稿)。每 milestone 独立可 ship + 可 tag。

## v0.4: StorageBackend contrib adapters (2026-09-05, REVERSED)

- 早期决策: vendor 5 个 adapter (Sqlite / Postgres / MySQL / Redis / MinIO)
- user 拍板反转: 这是 over-engineering, 我们不该替客户决定需要哪些 DB/S3 client
- git history 保留作为反向教材

## v0.4.1: Storage loader (config-driven, 2026-09-05, 已 ship)

- `pavoz.storage_loader.load_storage(spec: str, **kwargs) -> StorageBackend`
- spec 格式: `"pkg.module:ClassName"` — 用户 config 写自己 adapter 的 dotted path
- core 不带任何 driver, 用户自己 pip install 自己要的 deps
- ai_writer 兼容: `load_storage("backend.integration.sf_storage.MinioStorage", ...)` 直接可用
- 业界借鉴: SQLAlchemy URL prefix / Kedro catalog type / langchain_community vectorstores / pluggy hooks

## v0.2: API 冻结版

**总纲**: 核心语义锁死, 只加观测/测试/调试层能力, 且不动现有 API 语义。

### API 冻结宣言 (v0.2 起生效)

以下**核心语义不再变更** (新原语/改语义必须有真实 use case + 走决策流程):

| 面 | 冻结内容 |
|---|---|
| DAG | `@dag.stage(depends_on, retries, timeout)` 签名; 静态声明 + 冻结语义 |
| Runtime | `run(dag, task_id, initial_state, resume, deadline)` 行为; 失败返 RunResult 不抛 |
| Ctx | `ctx.state` 只读深拷贝; `return dict` 唯一写路径 |
| State | json-serializable 白名单; 平行 producer 冲突 raise, 链式覆盖合法 |
| 异常契约 | StageError (不重试) / RetryableError (退避) / FatalError (立即终) |
| Checkpoint | workflow_hash 结构指纹; resume mismatch 拒续跑 |

**允许新增** (非核心层): CLI 命令、Runtime 工具方法 (如 `run_stage`)、
观测回调 (如 `on_checkpoint`) — 新增不改既有语义即可。

**背景**: Uber Piper 8 年自研复盘 — "自研功能蔓延是死因, 静态 DAG + 无版本化
被点名为缺项" (astronomer.io/blog/airflow-in-action-uber-2026)。workflow_hash
已补版本化, 冻结宣言补功能蔓延。

---

### M1: state 体积观测 (观测层)

**为什么**: LangGraph 生产教训 — 每次 node 切换全量序列化 state,
50 文档 → 180KB/checkpoint → PG 写入 400ms (dev.to LangGraph 5 大坑)。
pavoz 目前对 checkpoint 膨胀无感知, 等膨胀到拖慢 resume 才发现就晚了。

**设计**:
- `Runtime(on_checkpoint: Callable[[CheckpointStats], None] | None = None)`
  - 每次 checkpoint 落盘后调用 (含 done/failed 两处 _save_cp)
  - `CheckpointStats`: `dataclass(size_bytes: int, save_ms: float, stage_name: str | None, task_id: str)`
- `size_bytes` = `len(json.dumps(cp.to_dict(), ensure_ascii=False))` (落盘前算);
  `save_ms` = storage.put 耗时
- 默认 None → 完全零开销 (不加 if 之外的路径)
- 阈值判断/告警归业务侧 (callback 里自己判) — core 不预设阈值

**改动文件**: `runtime.py` (Runtime 签名 + _save_cp 埋点), `types.py` 或 runtime
(CheckpointStats), `__init__.py` (export)

**测试**: 单测 — 构造 1KB/10KB state 跑 DAG, 断言 callback 收到 size ≈ json 长度、
save_ms ≥ 0、stage_name 顺序正确; on_checkpoint=None 时不崩。

**验收**: 真实 DAG run 打一行观测即可接 ai_writer 告警; 38 tests 全绿 + 新增 ≥3。

---

### M2: DAG 级集成测试夹具 (测试层) — 已 ship (v0.5.1)

> **已 ship (v0.5.1)**: 最终形态 `TestPipe.replay_from(cp, dag)` classmethod
> (cp = `Checkpoint`, 非 dict; checkpoint 存每 stage `stage_deltas` + `initial_state`)。
> `run_from` 参数未做 — "从某 stage 起真跑" 由 M3 `Runtime.run_stage` + CLI replay 承担。
> workflow_hash mismatch / v0.5.0 旧 cp (无 stage_deltas) → 明确报错不静默错配。
> 见 CHANGELOG [0.5.1]。

**为什么**: "unit test 抓不到 DAG 级 bug (if/elif 静默丢分支), 长流程 debug 需
per-node 状态快照" (调研角度 3 共识)。TestPipe 已能 mock 任意 stage 跑全图,
但 mock 数据要手写 — 真实 run 的 checkpoint 本身就是最好的 fixture。

**设计**:
- `TestPipe.replay_from(checkpoint: dict, dag, *, run_from: str | None = None)`
  - 从 checkpoint dict (Checkpoint.from_dict 可读的格式) 恢复 state + done_stages
  - 生成 mock 表: 每个 done stage 的输出 (从 cp.state 按 producer 倒推不可靠 —
    **cp.state 是最终合并态不是每 stage 输出**) → 需要 checkpoint 存每 stage 的
    return delta? **方案**: M2 顺带扩展 checkpoint 存 `stage_deltas: dict[stage, dict]`
    (每 node 成功后 return 的原始 delta, 序列化向后兼容, 旧 cp 缺字段 → replay
    报明确错误提示重跑一次生成新 cp)
  - **体积权衡**: stage_deltas 可能显著大于最终 state (流水线逐级演进同一大产物
    时, delta 总和 ≈ N×最终体积)。这正是 M1 观测要盯的指标; 若真实 use case
    膨胀明显, 再考虑只存 run_from 所需的最小 delta 集 (v0.2 不做, 先观测)
  - `run_from="s_x"`: s_x 之前全部用真实历史 delta mock, s_x 起真跑
  - 无 run_from: 全 mock 跑通断言与 checkpoint 一致 (回归 = DAG 结构/编排没坏)
- ai_writer 等 use case: 已完成 task 的 pavoz checkpoint 直接当 fixture;
  断言的不是"同一 LLM 输出"而是"给定相同 stage 输出, 图行为不变"

**改动文件**: `checkpoint.py` (+stage_deltas 字段), `runtime.py` (_save_cp 存 delta),
`testing.py` (TestPipe.replay_from), `__init__.py`

**测试**: 真跑 demo DAG (带 mock caller) → 存 checkpoint → replay_from 全 mock
断言 state 全等; run_from 后半真跑前半 mock 断言前半没重跑 (caller 计数为 0)。

**验收**: 手写 mock 回归可被 checkpoint 回放替代; workflow_hash mismatch 时
replay_from 明确报错 (不静默错配)。

> **依赖注记 (A7, 2026-09-05)**: M3 replay **依赖**本 milestone 的
> `stage_deltas` — 执行顺序锁定 M2 → M3, 不可跳。v0.5 checkpoint schema 已保证
> 向后兼容 (to_dict/from_dict, 未来字段 .get 默认), 届时加 stage_deltas 字段
> 不 breaking, 现无需动作。

---

### M3: CLI replay --prompt-patch (调试层) — 已 ship (v0.5.1)

> **已 ship (v0.5.1)**: CLI 命令 `replay <dag.py> --task-id X --stage Y
> [--patch P.py]` (选项 `--stage` / `--patch`); `Runtime.run_stage(dag, task_id,
> stage_name)` — 无 initial_state/resume 参数, 输入一律从 cp 重建;
> patch 约定 `patch(dag) -> None`, 仅 sync (async → 明确拒绝防静默 no-op)。
> 见 CHANGELOG [0.5.1]。

> **依赖注记 (A7, 2026-09-05)**: M3 重放 stage N 需要 "stage N-1 完成后" 的 state,
> 但 checkpoint 每 stage 后覆盖保存累计态 → 最新 cp 只有终态 (含 stage N 自身的
> 输出), 用终态重放 = 输入污染。`producers` 字段无法替代 (chain overwrite 场景
> 逆向剥离不可靠)。replay 必须从 `initial_state + stage_deltas[0..N-1]` 重建 —
> stage_deltas 由 M2 提供。
>
> **重放不落 checkpoint (A6, 2026-09-05)**: replay 是临时实验, 产出给开发者看,
> 不需要可 resume — **不写 checkpoint**, 落盘会错误更新 `runs/{task_id}/latest`
> 指针, 污染后续 load_latest/resume (指针污染防护)。

**为什么**: 调试场景 1 (调 prompt/参数) 的闭环 — 改 prompt 不用重跑全图,
单 stage 用 checkpoint state 重放, 分钟级迭代 (调研角度 4: 长流程 debug 无
per-node 快照是自研引擎通病)。

**设计**:
- Runtime 工具方法: `async run_stage(dag, task_id, stage_name, *, initial_state=None,
  resume=True) -> RunResult`
  - 加载 checkpoint (hash 校验同 run) → 跳过目标 stage 之前所有节点 → 只跑目标
    stage → 返回 RunResult (state = 目标 stage 合并后)
  - 目标 stage 的依赖输出已在 checkpoint state 里 (done_stages 必须覆盖其全部
    depends_on, 否则明确报错)
- CLI: `python -m pavoz replay <dag.py> --task-id X --stage s_x [--patch P.py]`
  - 加载 dag 模块 → patch 文件 (import 后 monkey-patch dag.stages 的 fn / caller)
  - 输出: RunResult JSON (state 里目标 stage 的 delta keys) + 目标 stage 单独 delta
- patch 约定: P.py 暴露 `patch(dag) -> None` (或直接副作用 import — import 即生效)

**改动文件**: `runtime.py` (run_stage), `cli.py` (replay 命令), `docs/api.md`,
`docs/quickstart.md`

**测试**: 集成测试 — 真跑 DAG 落 checkpoint → replay 某 stage 断言输出与首跑
一致 (caller 同 mock); patch 文件替换 stage fn 断言新输出生效; 无 checkpoint /
hash mismatch / 依赖未完成 → 明确错误。

**验收**: ai_writer 场景 — 改写作 prompt → patch 文件 → replay s_compose → 秒级
看新输出, 不重跑 search/download。

---

## v0.3.0-rc1: Declarative param override (2026-09-09, 已 ship)

- `fork-run` 4 新 flag: `--set` / `--set-file` / `--compare-with` / `--dry-run`
  (点分路径 + 类型推断 + state leaf diff; 见 docs/replay-params.md)
- `pavoz.state` 新 helper: parse_set_args / parse_set_file / merge_overrides /
  apply_overrides (+ _state_diff 内部)
- `fork_run` overrides 应用改深合并 (behavior fix — 兄弟键保留;
  runtime fork_run + Checkpoint._rebuild_state 两处应用点同步)
- 安全门: dunder 拒绝 / 路径深度 ≤5 / 文件 ≤1MB / yaml.safe_load; CLI rc 契约保留
- 消费端验收: ai-write composer 3-model A/B 端到端 (llm_calls 实证 override 达路由;
  详见 liyong-labs/ai_writer docs/handoff/pavoz-pr-v0.9/ACCEPTANCE.md)
- 0.3.0 正式版: soak ~1 周后 cut; PyPI publish 等 repo 公开

## v0.2 之后 (候选项, 未排期)


- 对外发布准备 (PyPI publish 流程, 若走开源分发)
- "pipeline 留 plain code / 动态留业务循环" 的差异化叙事正式化 (docs)
- 真实第二 use case (验证通用性)
- fork override 压过 stage 重产出 key 的语义 (当前 StateConflictError —
  override 语义 = 注入执行前输入; 有真实使用诉求再议, 见 replay-params.md footgun 段)

## 不做 (YAGNI 延续)

- 不加 scheduler/UI/分布式/dynamic DAG/sub-DAG 嵌套
- 不加 retry_budget/escalation/watchdog 等流程原语 (循环在业务层)
- core 保持零第三方依赖

## v0.3 候选追加 (2026-09-06 triage, use case #2: ai_writer 引用塌缩 debug)

### 需求提案 (ai_writer → pavoz)

**场景**: ai_writer 14d8842 引用塌缩 debug — 终稿 26 个 [ref:79aa] 全同 + 44×[来源待补]。
debug 摩擦: 回答"audit 看到的素材池 vs compose 用的素材池为什么不同"需手工翻 40+ trace JSON +
多个 CP 文件; 回答"v1.3→v1.4 正文被谁改的"需逐 trace 对比全文。

**提案能力**: stage 输入快照血缘查询 — `pavoz state --task X --at-stage s_audit --show-inputs`:
给定 task + stage, 直接返回该 stage 实际收到的输入 (素材池/state) + 该 stage 每次 LLM 调用
的输入输出摘要。

### Triage 三问裁决 (过滤器 2026-09-06 user 拍板)

| 问 | 裁决 |
|---|---|
| 1. 通用吗? | **部分** — "查任一 stage 的前置 state" 通用 (任何 DAG 用户 debug 都要) |
| 2. 竞品先例? | **有** — LangGraph checkpoint state 全量可查 / Prefect artifact / Temporal event history |
| 3. 绑 ai_writer? | **会绑** — ai_writer 编排壳模式下素材池/正文**不经 ctx.state** (stage 函数体直调旧 _run_xxx_phase, 业务数据走 ai_writer 自己 CP/DB) → pavoz 层面根本看不到素材池 |

**裁决**: **拒** (回 ai_writer adapter 层) + 1 条收 backlog:
1. **拒**: "素材池输入血缘" — 根因是 ai_writer 素材池**双路径加载** (compose 用 outliner selected 7 条,
   audit/save 用另 5 条), 是业务 bug 不是编排缺口. pavoz 不该为"业务数据不经 state"的架构
   买单 — 真修法是 ai_writer 素材池单一加载源. 附带方向: 编排壳 stage 若想让业务产物进可观测层,
   应显式 ctx.log/ctx.state 声明, 而非让 pavoz 猜.
2. **收 backlog (v0.3)**: **checkpoint state edit** — 任意已完成 stage 的 delta 可编辑后重放
   下游 (debug 改中间产物). 通用 (任何 DAG 调试图), 先例 (LangGraph state update / Temporal
   patch), 不绑 vendor. 与 M3 replay 互补: replay=改代码重跑, state-edit=改数据重放.
3. **拒 (YAGNI)**: run_stage 声明"外部产物版本依赖" — 单 use case, 无先例, 编排壳改造前无意义.

### 引用塌缩 bug 本身 → 回 ai_writer (不在本 repo 修)
