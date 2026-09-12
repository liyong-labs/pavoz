# Changelog

本项目遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Added (文档工程 — 2026-09-13)

- **`llms.txt`**: 仓库根的 agent 自读入口 — 这是什么 / 从哪读起 / 不可变规则 / 提交前跑什么命令。
- **`tools/check_doc_links.py` + CI 步骤**: markdown 相对链接必须可解析 (上线即抓出 3 处死链,
  含 `docs/storage.md` 与 `docs/en/*.md` 的错路径)。
- **`tools/check_doc_style.py` + CI 步骤**: 只做高精度风格检查 — AI 禁用词、README 装饰性版本号、
  中文句尾半角句号; 过程类文档 (CLAUDE/CHANGELOG/ROADMAP) 与 `docs/design/` 显式豁免, 误报挡 PR 比漏报贵。
- **`tests/test_api_docs_coverage.py`**: 断言 `pavoz.__all__` 每个名字都出现在
  `docs/{cn,en}/api.md` — 防"文档悄悄落后代码"。

### Changed (文档工程 — 2026-09-13)

- **API 参考补齐滞后项** (`docs/cn/api.md` + `docs/en/api.md`): 补上已 ship 但未写进参考的能力 —
  `Runtime(on_event=...)` / `cancel_check`(含事件表)、`Runtime.fork_run`、
  `ctx.set_progress` / `cancelled` / `on_event`、`RunResult.error_class` / `stage_timings`、
  `CheckpointStore.prune`、参数覆盖四个函数 (`parse_set_args` / `parse_set_file` /
  `merge_overrides` / `apply_overrides`) 与 `__version__`。
- **CONTRIBUTING 对齐 CI**: 质量门槛改为 CI 实际执行的两条命令 (原写 `ruff check` 与
  "38+ tests" 均与现状不符); 修正 3 处指向不存在文件的文档链接。
- **CI lint 范围加入 `examples/` 与 `tools/`**。

### Changed (扩展面文档 + 示例升级 — 2026-09-13)

- **架构文档新增《扩展面》一节** (`docs/cn/architecture.md` + `docs/en/architecture.md`):
  五个扩展点 (stage 函数 / 生命周期事件 / `ctx.call` / `StorageBackend` / 取消) + 事件表 +
  装饰器注意事项 (`functools.wraps`) + 版本策略。**核心 API 无改动。**
- **`examples/agent_loop.py` 升级**: 保留原内联循环写法, 新增 `quality_gate` 装饰器版本
  (多 lens 评分 / `min` 聚合 / 阈值 / 分数写 `on_event`), 并加回归测试 (`tests/test_examples_agent_loop.py`)
  防止示例腐化。
- **pyflakes 范围加入 `examples/`**; 顺带修掉 `tests/` 中一处未使用 import (存量 lint 失败)。
- 修正 `pavoz/dag.py` docstring 中过期的 stage 签名 (`async def s_x(req, ctx)` → `async def s_x(ctx)`,
  实际调用见 `runtime.py` 的 ctx-only 约定)。

### Rejected (2026-09-13 评审决议 — 见 `docs/design/extension-plan-2026-09-13.md`)

- 核心不新增全局 hook 注册表: `Runtime(on_event=...)` 已提供同等观察能力, 且注入式优于进程级全局状态。
- 核心不新增 `ctx._meta`: `Ctx` 在每次 attempt 重建, 不能承载 run 级记账 (计数会被重试绕过)。
- 下游质量门 / 契约校验以**独立扩展包**形式提供 (pavoz-extensions), 核心保持零依赖、零改动。

### Added (consumer PR: ai-research R1/R2/R3 — 2026-09-10, 见 docs/design/pr-reply-ai-research-2026-09-10.md)

- **R2 `RunResult.error_class`**: 原始异常类名 (失败时保留, cancelled/done 为 None)。
  未知业务异常 (如 PipelineError) 保真原始类名; `error` 字符串格式不变 (API 冻结)。
  用途: 错误分诊 (业务失败→调参重跑 vs 代码 bug→找人)。
- **R1 `CheckpointStore.prune(task_id, *, keep_last, dry_run)` + `pavoz prune` CLI**:
  按保留数修剪旧 run checkpoint。排序 = max(stage_ts); **latest 指针指向的 run
  无条件保留**; `--dry-run` 列将删 run + 近似字节。不做 max_runs_per_task 自动修剪
  (v0.4 adapters REVERSED 先例: 不替客户决定保留策略)。
- **R3 `Ctx.set_progress(fraction, note=None)`**: stage 内进度心跳 → `stage_progress`
  生命周期事件 (第 5 种)。best-effort 瞬态信号: 不落 checkpoint, 不碰 deadline/重试,
  同 fraction 引擎内去抖, 未启用 on_event 时 no-op。

### 2026-09-10 之前 (已在 0.3.0-rc1 tag 之后, 未入版本)

- ai-write 需求 PR (530f4c2): CLI storage 解耦 `PAVOZ_STORAGE_SPEC` + fork-run `--dry-run` 预演。

## [0.3.0-rc1] — 2026-09-09

### Added (fork-run 通用 param override — Declarative stage-level debug)

**4 个新 CLI 标志** (`pavoz fork-run`):
- `--set KEY=VALUE` (可重复, 点分路径 KEY, 类型自动推断)
- `--set-file PATH` (JSON/YAML 文件, 1MB 上限, `yaml.safe_load`)
- `--compare-with RUN_ID` (跑完后输出 state leaf diff)
- `--dry-run` flag (仅解析 + 显示, 不执行 stage)

**新模块级函数** (`parse_set_args` / `parse_set_file` / `merge_overrides` /
`apply_overrides` 顶层也导出; `_state_diff` 是 `pavoz.state` 内部 helper,
仅 compare-with 内部使用, 不在顶层导出面):
- `parse_set_args(items)` — `--set` 列表解析
- `parse_set_file(path)` — JSON/YAML 文件解析 (1MB + safe_load + dunder 校验)
- `merge_overrides(*sources)` — 多源合并 (按优先级)
- `apply_overrides(base, *patches)` — 接受 dot-path 或 nested dict, 不 mutate base

**CLI storage 解耦 (R1)**: `PAVOZ_STORAGE_SPEC` env (`pkg.module:Class`) +
`PAVOZ_STORAGE_KWARGS` (JSON, `{task_id}` 插值) — CLI 全子命令可操作任意
StorageBackend (如 consumer 的 MinIO adapter); 不设时现 FileStorage 行为不变。

**dry-run 预演 (R2)**: `fork-run --dry-run` 从 "显示解析结果" 升级为
"预演 apply 效果" — 基于 `rebuild_state_before(stage)` 的执行前 state 做
leaf 级 diff (`preview.diff_keys_count` / `diff_sample` / `would_rerun`),
试错不再需要真跑。

**Priority merge**: `--set > --set-file > --overrides > --input` (后写覆盖前写;
`--overrides` > `--input` 沿袭 v0.8 组合使用时的既有胜者)

**Safety**:
- 拒绝 dunder key (`__proto__`, `__class__` 等) — 防 Python object injection
- 路径深度 ≤ 5 限制
- YAML 用 `safe_load` (不 `yaml.load`) — 防任意代码执行
- 文件大小 ≤ 1MB 限制

**新 examples** (`examples/`):
- `01_model_switch.sh` — 4-model composer A/B (ai-research 场景)
- `02_prompt_patch.py` — 改 prompt 不重跑前序 stage
- `03_compare_diff.py` — 多 run state diff 对比
- `04_deep_merge.py` — dot-path 兄弟键保留演示
- `05_yaml_file.sh` — YAML 文件批改 (dry-run first)

**新 tests**:
- `tests/test_state_apply.py` (54 case) — state.py utilities
- `tests/test_fork_overrides.py` (6 case) — 深合并语义
- `tests/test_cli_fork_set.py` (8 case) — CLI 直接调用

**新 docs**:
- `docs/replay-params.md` (完整指南)

### Fixed

- `fork_run` overrides 应用从顶层浅替换 (`state.update`) 改为深合并
  (`apply_overrides`) — 嵌套 patch 不再抹掉兄弟键 (如 `--set llm.model=x`
  不再丢 `llm.temperature`). 顶层标量覆盖行为不变.

### Compatibility

| 0.2.0 | 0.3.0 |
|---|---|
| `--overrides '{"k":"v"}'` | ✅ unchanged |
| `--input edited.json` | ✅ unchanged |
| `Runtime.fork_run(overrides=dict)` | ✅ 签名不变 (嵌套 dict 语义见 Fixed) |
| `replay --patch patch.py` | ✅ unchanged |

### Breaking changes

无 (behavior fix 见 Fixed; 顶层标量覆盖不变).

### Stdlib

保持 zero runtime deps. YAML 是 `[yaml]` optional extra.

### Motivation

ai-research (consumer) 做 composer 4-model A/B 测试时, 当前痛点:
- 直接 DB INSERT 绕 API (侵入性)
- writer_chain 列存 model id (ai_writer-specific)
- 4 task 并发无 isolation (race condition)

pavoz v0.6 已有 `fork_run(overrides=dict)` (LangGraph fork 范式), 但 CLI UX 差
(JSON string 嵌套痛苦). 本 PR 加 declarative 层, 让 1 行命令 = 切 model 重跑
stage + diff 输出, 把 2 小时 hack 缩到 15 分钟.

## [0.2.0] — 2026-09-08

### Changed (stageflow → pavoz 品牌重命名)

- 包名: `stageflow` → **`pavoz`** (PyPI 包 + import 路径 + CLI 命令全统一)
- 仓库: `ebziw/stageflow` → **`liyong-labs/pavoz`** (含 pyproject URLs / docs / CI)
- 内部目录: `stageflow/` → `pavoz/` (12 文件 git mv)
- Author: `ebziw` → `liyong-labs`

### Added (协作式取消 + 生命周期事件 — 0.8.0 → 0.2.0 之间)

- 协作式取消: `Runtime(cancel_check=...)` — stage 间/重试间检查点拦截, `RunResult.status`
  新增 `"cancelled"`; 已完成 stage 照常落 checkpoint, `resume=True` 无缝续跑.
  stage 内长循环用 `ctx.cancelled()` 轮询自行退出 (循环留业务层).
  cancel_check 异常视为未取消 (fail-open).
- 生命周期事件钩子: `Runtime(on_event=...)` — run_start / stage_start / stage_end /
  stage_retry / run_end 五种结构化事件, observer 异常隔离. `RunResult.stage_timings`
  记录每 stage 墙钟耗时 (含 retry 退避). 单 stage 重放 (run_stage) 不发事件.
- 重试退避 full-jitter: `uniform(0, min(2^(attempt-1), 30))` — 多 task 同步重试防雷群.

## [0.8.0] — 2026-09-06

### Added

- 协作式取消: `Runtime(cancel_check=...)` — stage 间/重试间检查点拦截, `RunResult.status`
  新增 `"cancelled"`; 已完成 stage 照常落 checkpoint, `resume=True` 无缝续跑.
  stage 内长循环用 `ctx.cancelled()` 轮询自行退出 (循环留业务层).
  cancel_check 异常视为未取消 (fail-open).
- 生命周期事件钩子: `Runtime(on_event=...)` — run_start / stage_start / stage_end /
  stage_retry / run_end 五种结构化事件, observer 异常隔离. `RunResult.stage_timings`
  记录每 stage 墙钟耗时 (含 retry 退避). 单 stage 重放 (run_stage) 不发事件.
- 重试退避 full-jitter: `uniform(0, min(2^(attempt-1), 30))` — 多 task 同步重试防雷群.


## [0.8.0] — 2026-09-06

### Changed (checkpoint 格式: state 不落盘 — 旧格式 cp 兼容读但不再写)

- `Checkpoint.state` 字段 → property (从 `initial_state` + `stage_deltas` 惰性
  rebuild) — 落盘 2x 冗余消除. 实测 ai_writer 真实 cp 2.82MB → 2.05MB (-28%),
  rebuild 0ms 等价. 旧存量 cp (带 state) 照常读 (rebuild 覆盖 state 键).
- 2026-09-06 user 拍板: 发布前向前看, 不做向后兼容 — 序列化层不再写 state,
  无兼容分支
- `fork_overrides` 新字段: fork_run 的 overrides 持久化 (曾靠全量 state 落盘),
  随每次 checkpoint save 携带 — fork 分支的 overrides 活到分支终点

### Fixed

- fork resume 丢 overrides: fork_cp 保存后 resume 续跑, 每轮 save 的新 cp 若
  不带 overrides → 分支输入丢失 (从 from_stage 重跑用旧输入)
- run_stage/replay_from 的 legacy-cp 检查依赖 cp.state truthy — state 变
  property 后恒有值, 检查短路失效 → 防静默空输入重放的防护恢复 (只看 deltas)

### Chore

- 审计清理 (project-doctor): cli `importlib.sys.modules` → `sys.modules`,
  Ctx.caller sync-lambda default (await dict 隐患) → 单 async `_noop_caller`,
  删死 API `validate_state`, ruff UP034, `.gitignore` + data/

## [0.7.0] — 2026-09-06

### Added

- `Checkpoint.stage_ts`: stage 完成 epoch 时刻 (每 node 落盘, resume/fork 保留前缀) —
  恢复侧内容过期 (TTL) gate 的判定依据 (ai_writer CP 迁移 Step 1 用)

## [0.6.0] — 2026-09-06

### Added (time-travel fork: 任意历史节点取输入 → 改 → 装回)

- `Runtime.fork_run(dag, task_id, *, from_stage, overrides=None, run_id=None)`:
  LangGraph fork 范式 — truncate 到 from_stage 前, initial + 保留 deltas +
  overrides 重建 state, 新 run_id 落盘且 latest 指向 fork, 原 run cp 不动,
  委托 `run(resume=True)` 从 from_stage 续跑 (前序 stage 结果复用不重跑)
- `Checkpoint.state_stats()`: state 体积观测 (key top N + 构成), CLI `state --stats`
- CLI: `export-input --task-id X --stage Y` (打印 stage 执行前输入 JSON, 可编辑)
  + `fork-run ... --input file | --overrides JSON` (装回续跑)
- 失败/中断的 stage 也可 fork (依赖已全完成) — 修输入重跑失败点

## [0.5.1] — 2026-09-05

### Added (M2 stage replay + M3 single-stage replay)

- **Checkpoint 存每 stage 原始 return** (`stage_deltas`, 按完成序) + run 的
  `initial_state` (M2) — 两者合并可精确重建任意 stage 执行前 state
  (cp.state 是最终合并态, 不能当 stage 输入逆向拆)
- `Checkpoint.rebuild_state()` / `rebuild_state_before(stage_name)`: 重建
  已完成 stage merge 后的 state / 某 stage 执行前的 state (链式覆盖由
  完成序天然处理)
- `TestPipe.replay_from(cp, dag)`: 已完成 stage 用历史 delta 全 mock —
  真实 run 的 cp 直接当回归 fixture (改 stage 实现后对比终态);
  hash mismatch / v0.5.0 旧 cp → 明确报错
- `Runtime.run_stage(dag, task_id, stage_name)`: 从 cp 重建输入单 stage
  重放 (调 prompt/参数秒级迭代) — 不落 cp 不动 latest 指针 (A6/A7 防
  replay 污染 resume 目标); 失败/中断的 stage (依赖已全完成) 也可重放
- CLI `replay <dag.py> --task-id X --stage Y [--patch P.py]`: 单 stage 重放;
  patch 文件约定 `patch(dag) -> None` 替换 stage fn (async patch 拒绝 —
  防静默 no-op)
- CLI run 恒落盘 (R5 fix)

### Changed

- CLI `run` 不再只在 `--resume` 时 attach CheckpointStore — 单跑也写
  `~/.pavoz/data` (`PAVOZ_STORAGE` 可覆盖)。行为: 单次 run 即
  落盘, replay/trace/state 对 CLI 产物直接可用

### Compatibility

- Checkpoint 新字段 (`initial_state` / `stage_deltas`) `from_dict` `.get`
  默认 `{}` — v0.5.0 旧 cp 可读可 resume
- v0.5.0 旧 cp (无 stage_deltas) 上重放 (CLI replay / run_stage /
  TestPipe.replay_from) → 明确报错提示重跑一次, 不静默错配

### Internal

- 81 tests, ruff clean, 零运行时依赖 (stdlib only)

## [0.5.0] — 2026-09-05

### Added (industry-standard ID model)

- `task_id` 参数可选: 省略 → pavoz 自动生成 UUID4。跨 retry/resume 稳定 —
  幂等键 (Temporal WorkflowId / DBOS workflow_id 模式)。入口校验: 非空、
  ≤128、字符集 `[A-Za-z0-9_.-]` (禁 `/`, 防 storage key 路径注入)
- `run_id`: 每次 `Runtime.run()` 自动生成 UUID4, 返回在 `RunResult.run_id`。
  同 task 多次 run 互不覆盖 (original / resume / 重跑)
- `Ctx.run_id` + `Ctx.attempt` (1-based, stage retry 时 +1) 自动注入
- `ctx.call` 底层 caller 签名 3 参 → 4 参 (新增 `CallMeta`) —
  caller 落 trace/llm_calls 时可直接关联 task/run/stage/attempt (A1)
- `CheckpointStore.list_runs(task_id)` + `load_latest(task_id)` (指针文件) helpers

### Changed (BREAKING)

- Checkpoint 存储 key: `runs/{task_id}/{run_id}/checkpoint` (was
  `runs/{task_id}/checkpoint`) + 指针文件 `runs/{task_id}/latest` =
  `{"run_id": ...}` (uuid4 字典序 ≠ 时间序, 不靠排序判 "最新")
- `RunResult` / `Checkpoint` 新增必填 `run_id` 字段 (was absent)
- `CheckpointStore.load(task_id, run_id)` 2 参 (was 1 参); "最新 run" 用
  `load_latest(task_id)`
- `CheckpointStore.delete(task_id, run_id)` 2 参 (was 1 参 task 级); 指针文件不随
  删除更新 (删的恰是最新 run → 后续 load_latest 返 None, 属可接受边界)
- `CheckpointStore.load_compatible(task_id, run_id, dag)` 3 参 (was 2 参);
  `run_id=""` = 加载最新
- `Runtime(caller=...)` 自定义 caller 需接第 4 参 `CallMeta` (默认 no-op 已同步)
- `Runtime.run()` 移除 `deadline` kwarg (was per-run 绝对超时, 优先于
  `default_timeout`) — run 级超时现在只走 `Runtime.default_timeout`
- `Runtime.run()` `resume` 默认值 True → False (默认起新 run_id; 续跑需显式
  `resume=True`)
- resume 语义收紧: `resume=True` 无 checkpoint → RuntimeError; run 已全部完成
  (done guard) → RuntimeError 防静默 no-op。重跑 = `resume=False` 起新 run_id
- run 完成后 checkpoint **不再自动删除** (v0.4.1 前会清) — done guard 取代

### Rationale

Adopts Temporal WorkflowId/RunId, DBOS workflow_id idempotency key, and
Airflow `dag_id + task_id + run_id + try_number` composite key patterns.
Industry-standard, no caller-invented ID schemes.

### Migration (hard cut)

No legacy loader. Old `runs/{task_id}/checkpoint` keys become orphaned
on disk — caller can clean up with their storage backend.
旧数据 (无 run_id 字段) 同样不可加载。

### Internal

- 60 tests (52 core + 8 storage_loader), ruff clean, 零运行时依赖 (stdlib only)
- CLI: resume 语义错误 (无 cp / 已全部完成) → 友好消息 + exit 1, 不炸 traceback


## [0.1.1] — 2026-09-05

### Added

- **链式覆盖 (chain overwrite)**: 下游 stage 可以覆盖**传递上游 producer** 写的
  state key。数据流水线 (如 `search → filter → compress` 逐级演进同一产物) 是
  通用模式, 不再被 one-producer-per-key 误判为平行冲突。
  - `DAG.reachable(upstream, downstream)`: 传递依赖判定
  - Runtime 追踪 `state key → producer stage`, checkpoint 持久化 `producers`
  - 平行 producer (无依赖链) 冲突仍抛 `StateConflictError` (v1 语义保留)
  - 旧 checkpoint (无 producers 字段) resume → 宽松模式 (视同单链), 向后兼容

### Changed

- `merge_state(prev, delta, stage_name, overwrite_keys=None)`: 新增可选参数
- `Checkpoint`: 新增 `producers` 字段 (序列化向后兼容, 缺省 `{}`)

## [0.1.0] — 2026-09-05

### Added

首个可用版本 (micro/in-process workflow engine):

- **DAG DSL**: `@dag.stage(depends_on, retries, timeout)` 静态声明, Kahn 拓扑
  排序, Tarjan SCC 环检测 (含自环), validate 后冻结
- **Runtime**: 顺序执行 + per-node 重试 (指数退避) + absolute deadline 传播 +
  state merge + 每 node checkpoint
- **State 契约**: json-serializable 类型白名单 (str/int/float/bool/None/
  list/dict), `ctx.state` 为只读深拷贝 (写即 raise), `return dict` 是唯一写
  路径, 平行 producer 冲突 → `StateConflictError`
- **异常契约**: `StageError` (业务失败, 不重试) / `RetryableError` (退避重试,
  耗尽 fail) / `FatalError` (程序 bug, 立即 fail)
- **Checkpoint**: 每 node 落盘, `workflow_hash` (stage 结构指纹) mismatch →
  拒 resume (只改函数体不影响 hash), run 完成自动清理
- **ctx.call(kind, op, params)**: 对外调用统一入口 (caller 由业务注入),
  默认 no-op echo (demo/TestPipe 可用)
- **TestPipe**: 第一天可用的回归 fixture — mock stage 输出跑全图
- **StorageBackend Protocol + FileStorage**: core 零依赖, 业务可注入
  DB/MinIO adapter
- **TaskTrigger Protocol**: 业务 task 表边界抽象
- **CLI**: `run / trace / state / inspect` (v0.1 面)
- 38 tests, ruff clean, 零运行时依赖 (stdlib only)

## [0.1.2] — 2026-09-05

### Removed (ponytail cleanup)

- `trigger.py` (`TaskTrigger` / `TaskRef` — zero callers)
- `Ctx.log` / `Ctx.remaining_seconds` (zero callers)
- `Stage.metadata` field + `@dag.stage` `**metadata` (zero callers)
- `DAG.entrypoints` / `DAG.depth_of` (zero callers)
- CLI `inspect` subcommand (空壳 — "v0.1 无 call 级 trace" 提示)

### Internal

- API 冻结面 zero change: `@dag.stage` / `Runtime.run` / `Ctx` / `State` / 异常 / Checkpoint 全部不动
- 38 tests 全绿, ruff 0 violation

## [0.4.0] — 2026-09-05

### Added (pavoz.contrib.storage)

- **PostgresStorage** (psycopg3 + JSONB, upsert via ON CONFLICT)
- **MySQLStorage** (PyMySQL + LONGTEXT, upsert via ON DUPLICATE KEY)
- **RedisStorage** (redis-py, prefix-scoped, SCAN-based list_keys)
- **MinioStorage** (boto3, **delete/get 容忍 NoSuchKey/404** 对齐 ai_writer `sf_storage.py`)
- **SqliteStorage** (stdlib sqlite3, 单文件 DB)
- 公共 helpers: `_validate_key` (路径安全) + `_encode_payload` / `_decode_payload` (UTF-8 JSON)

### Internal

- core 12 模块 + `StorageBackend` Protocol 零变化 (API 冻结); 0 行 core 功能修改 (fix1 仅同步 manifest 版本号)
- contrib 是独立 subpackage (`pavoz.contrib.storage`); core 不引入任何 driver
- pyproject extras 分组: `[postgres]` / `[mysql]` / `[redis]` / `[minio]` / `[sqlite]` (空) / `[contrib]` (4 个) / `[all]`
- 缺 driver 时 adapter 模块顶 try/except ImportError + 安装提示

### Tests

- 38 core tests + 44 contrib tests (10 标 `@pytest.mark.integration` 需 docker)
- MinioStorage 用 moto[s3] mock; RedisStorage 用 fakeredis mock (无 docker 依赖)

## [0.4.1] — 2026-09-05

### Removed (reversal of v0.4 contrib approach)

- `pavoz.contrib.storage` 5 个 vendor adapter (SqliteStorage / PostgresStorage / MySQLStorage / RedisStorage / MinioStorage) — over-engineering, user 自管 deps + 自写 adapter 更合适
- `pavoz.contrib` subpackage 完全移除
- pyproject.toml extras (`postgres` / `mysql` / `redis` / `minio` / `sqlite` / `contrib` / `all`) 移除 — 用户自己装自己要的
- 相关 tests (tests/contrib/) 移除

### Added

- `pavoz.storage_loader.load_storage(spec: str, **kwargs) -> StorageBackend` — 按 `"pkg.module:ClassName"` 字符串 importlib 加载
- `pavoz.PavozStorageError` — 友好错误 (module 未装 / class 名错 / kwargs 错 / 类型错 5 类)
- 用户可自己写 adapter, pavoz 不带 driver, 通过 `pip install` 自管 deps
- ai_writer 兼容: `load_storage("backend.integration.sf_storage.MinioStorage", ...)` 直接可用

### Internal

- core 仍 stdlib-only + 零第三方依赖
- 0 行 core 修改 (除新增 storage_loader.py + 2 行 export)
- reversal 是真实设计演化: v0.4 contrib 是反向教材, git history 保留以做记录

## [0.1.3] — 2026-09-05

### Fixed

- `pyproject.toml` version + `pavoz/__init__.py` `__version__` 与 tag 同步 (0.1.1 → 0.1.3)。final-review B1: 之前 `pip install -e .` / `import pavoz; pavoz.__version__` 返 0.1.1,与 v0.1.2 tag 不符。
- 不 amend v0.1.2 (keep git history honest) — 走新 patch tag v0.1.3
