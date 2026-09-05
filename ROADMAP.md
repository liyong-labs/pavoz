# Roadmap

> v0.2 计划 (2026-09-05 brainstorm 定稿)。每 milestone 独立可 ship + 可 tag。

## v0.4: StorageBackend contrib adapters (2026-09-05, 已 ship)

- stageflow.contrib.storage: 5 个可选 extras adapter (Postgres / MySQL / Redis / MinIO / SQLite)
- core 永远 stdlib-only; contrib 是可选 (extras 机制)
- MinioStorage 行为对齐 ai_writer 既有 sf_storage.py (delete/get 容忍 NoSuchKey)
- deps 政策 (2026-09-05 拍板): 稳定基础设施 driver (DB/Redis/S3) OK; LLM vendor / 业务 SDK 禁入 contrib

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
stageflow 目前对 checkpoint 膨胀无感知, 等膨胀到拖慢 resume 才发现就晚了。

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

### M2: DAG 级集成测试夹具 (测试层)

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
- ai_writer 等 use case: 已完成 task 的 stageflow checkpoint 直接当 fixture;
  断言的不是"同一 LLM 输出"而是"给定相同 stage 输出, 图行为不变"

**改动文件**: `checkpoint.py` (+stage_deltas 字段), `runtime.py` (_save_cp 存 delta),
`testing.py` (TestPipe.replay_from), `__init__.py`

**测试**: 真跑 demo DAG (带 mock caller) → 存 checkpoint → replay_from 全 mock
断言 state 全等; run_from 后半真跑前半 mock 断言前半没重跑 (caller 计数为 0)。

**验收**: 手写 mock 回归可被 checkpoint 回放替代; workflow_hash mismatch 时
replay_from 明确报错 (不静默错配)。

---

### M3: CLI replay --prompt-patch (调试层)

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
- CLI: `python -m stageflow replay <dag.py> --task-id X --stage s_x [--patch P.py]`
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

## v0.2 之后 (v0.3 候选, 未排期)

- 对外发布准备 (PyPI publish 流程, 若走开源分发)
- "pipeline 留 plain code / 动态留业务循环" 的差异化叙事正式化 (docs)
- 真实第二 use case (验证通用性)

## 不做 (YAGNI 延续)

- 不加 scheduler/UI/分布式/dynamic DAG/sub-DAG 嵌套
- 不加 retry_budget/escalation/watchdog 等流程原语 (循环在业务层)
- core 保持零第三方依赖
