# API 参考

🇬🇧 [English version](../en/api.md)

公开面 = `pavoz/__init__.py` 的 `__all__`。core 零第三方依赖。

## DAG

```python
dag = DAG(name: str)
```

- `dag.stage(*, depends_on: list[str] | None = None, retries: int = 0,
  timeout: float | None = None)` — 装饰器注册 stage 函数
  - `depends_on`: 前置 stage 名列表
  - `retries`: `RetryableError`/超时可重试次数 (指数退避 2^n, 上限 30s)
  - `timeout`: 本 stage 硬超时 (秒); 实际超时 = `min(stage.timeout,
    run 剩余 deadline)`
- `dag.validate()` — 静态校验 (未知依赖 + 环检测), run 前自动调用, 通过即冻结
- `dag.topo_order() -> list[str]` — Kahn 拓扑序
- `dag.reachable(upstream, downstream) -> bool` — 传递依赖判定 (链式覆盖用)

异常: `UnknownDepError` / `CycleError`

## Runtime

```python
rt = Runtime(
    checkpoint_store: CheckpointStore | None = None,
    caller: Callable[[str, str, dict, CallMeta], Awaitable[dict]] | None = None,
    default_timeout: float | None = None,   # 整跑 absolute deadline (秒)
    on_event: Callable[[str, dict], None] | None = None,   # 生命周期事件出口
    cancel_check: Callable[[str], bool] | None = None,     # task_id → 已取消?
)

result: RunResult = await rt.run(
    dag, task_id: str | None = None,
    *,
    initial_state: dict | None = None,
    resume: bool = False,
)
```

- `task_id` 可选: 省略 → pavoz 自动生成 UUID4 (36 字符)。显式传入时入口
  校验: 非空、≤128、字符集仅 `[A-Za-z0-9_.-]` (禁 `/` — task_id 直接进
  storage key 路径)。跨 retry/resume 稳定 — 幂等键。resume 必须有显式
  task_id (自动生成的 ID 不会有 checkpoint 可找)
- `run_id` 每次 `run()` 自动生成 (UUID4), 返回在 `RunResult.run_id`。resume
  **复用** checkpoint 的原 run_id (Temporal Continue-As-New 模式) — 同 task
  多次 run 互不覆盖
- `resume` (默认 `False`): `True` → 加载该 task 最新 checkpoint 从断点续跑
  (跳过已完成 stage, 恢复 state)。无 checkpoint → `RuntimeError`; run 已全部
  完成 (done guard) → `RuntimeError` 防静默 no-op。重跑 = `resume=False`
  (起新 run_id)
- resume 时 `workflow_hash` 不匹配 (结构变了, **或 dag 名变了**) →
  `CheckpointMismatchError`; 名字不同时消息会点名两个 dag 名
- run 级 absolute deadline 来自 `Runtime(default_timeout=...)`, 缺省回退到
  `EnginePolicy(default_timeout=...)` — `run()` 无 deadline 参数; stage 级
  `timeout` 受 run 剩余 deadline 封顶。**同一 deadline 适用于全部三个入口**
  (`run` / `run_stage` 单 stage 重放 / `fork_run`): 给 policy 设了上限, 重放也
  受约束 (曾漏过 policy 回退, 导致重放完全不受限且无任何提示)
- run 完成后 checkpoint **保留** (不删除) — 它是该 task 的最新 run, 是未来
  resume / `load_latest` 的目标
- `on_event`: 生命周期事件出口 (`fn(event: str, data: dict)`), 只观察不改变执行语义。
  事件: `run_start` / `run_end` (`task_id` / `run_id` / `status`)、`stage_start` /
  `stage_end` (`stage` / `attempt` / `status` / `duration`)、`stage_retry` (+ `error`),
  —— `stage_end` 的 `status` 除了 `done`/`failed`/`cancelled` 还可能是
  **`"skipped"`**: `skip_unchanged` 命中时复用历史结果, 此时 `RunResult.stage_statuses`
  仍记 `"done"`(该 stage 的产出确实已在 state 里), **事件流是唯一能分辨"真跑了"与
  "复用旧结果"的地方** —— 排查"我改了 prompt 却没生效"要从这里看, 别只看 status,
  `stage_progress` (stage 内 `ctx.set_progress()` 触发, best-effort 瞬态信号, **不落
  checkpoint**)。observer 抛异常被隔离 (log + 忽略), 不影响 run。
  这是**扩展面的主入口** — 埋点 / 告警 / 指标都挂这里 (见 [架构 · 扩展面](architecture.md#扩展面-一个-stage-就是扩展点))
- `cancel_check`: 协作式取消 (`task_id → 已取消?`)。runtime 只在 stage 边界与重试
  边界拦截 (status="cancelled"; 已完成 stage 照常落 cp, resume 可续); stage 内的长
  循环自行 `ctx.cancelled()` 轮询退出。检查器抛异常 → 视为未取消 (fail-open)

### `Runtime.run_stage` (单 stage 重放, v0.5.1)

```python
result: RunResult = await rt.run_stage(dag, task_id: str, stage_name: str)
```

调试 helper: 从该 task 最新 checkpoint 重建 stage 执行前 state, **只跑这一个
stage** — 调 prompt/参数秒级看效果, 不重跑前序 stage。

- 执行前 state 重建: 已完成 stage → `cp.rebuild_state_before(stage_name)`;
  从未完成 (原始 run 失败/中断) 且依赖已全完成的 stage →
  `cp.rebuild_state()`; 依赖未完成 → `RuntimeError`
- **不落 checkpoint** 也不动 `latest` 指针 (replay 产出是开发临时物 —
  不能成为 resume 目标); `ctx.run_id` 是本次调用的一次性 UUID4
- **不发生命周期事件** — 重放不是 run, 故 `on_event` 一个都收不到 (无
  `run_start`/`run_end`、无 `stage_start`/`stage_end`、无 `stage_alive`)。
  挂在这些事件上的指标与告警因此**看不到重放**; 这是有意为之, 不是事件丢了
- 但 run 级策略仍然生效: `default_timeout` (显式或来自 `EnginePolicy`) 与
  `max_concurrent_runs` 都约束重放 —— 它打的是同一批 stage、同一个下游
- 错误: 无 checkpoint → `RuntimeError`; DAG hash 不匹配 →
  `CheckpointMismatchError`; pre-M2 (v0.5.0) cp 无 `stage_deltas` →
  `RuntimeError` (重跑一次生成新 cp); stage 不存在 → `KeyError`

### `Runtime.fork_run` (从历史 run 分支续跑, v0.6)

```python
result: RunResult = await rt.fork_run(
    dag, task_id, *, from_stage: str, overrides: dict | None = None,
    run_id: str | None = None,
)
```

前序 stage 复用 (不重跑), `from_stage` 及其后继用注入的 `overrides` 重跑 —
"改一个输入, 只重跑下游"。原 run 的 checkpoint 不动; fork 产生新 run_id 并成为该
task 的 latest。

> ⚠️ **`skip_unchanged=True` 时 `from_stage` 只是"起点提示", 不是强制重跑点**。
> 引擎沿 topo 序逐 stage 比对 `stage_input_hash` (fn 源码 + 执行前 state), **命中就一直
> 跳到首个未命中处**, 那之后才全部重跑 —— 所以 `from_stage` 自身 hash 命中时会被跳过。
> 实测 (`from_stage="s_a"` + `skip_unchanged=True` + **无 override**): 三个 stage 全部
> 被跳过、一个 fn 都没被调用, 而 `status=done`。
> `overrides` 注入会改变输入 hash ⇒ **必然重跑**, 所以"改了东西"的那条路径不受影响。
> 被跳过的是纯函数时重放等价于重跑; 但若你的改动落在 **hash 看不见的地方**
> (模块级常量 / 环境变量 / 文件 / 时钟, 见 `stage_input_hash` 的覆盖边界), 这次 fork
> 就成了**静默的空跑**。
> 查有没有被跳过: `RunResult.skipped_stages` / `pavos trace` 的 `skipped_stages`
> (v0.5.8 起) —— `stage_statuses` 对它们记的是 `"done"`, 光看它分辨不出来。

- `overrides`: 顶层 state key 覆盖, 注入 `from_stage` 的执行前 state (深合并 —
  兄弟键保留)。CLI 的 `--set` / `--set-file` 走同一入口 (见 [replay-params.md](../replay-params.md))
- overrides 是**注入输入, 不是最终值**: `from_stage` 重跑后可以覆盖它们 (产物以
  stage 的返回为准)。checkpoint 重建 (`cp.state`) 复现同一顺序, 故落盘与活跑结果
  恒等 —— 此前重建把 overrides 放在**最后** merge, 与 runtime 顺序相反, 造成
  `runtime 说 a='A'` 而 `cp 说 a='A9'` 的分叉
- `run_id`: 指定则在给定 run_id 上 fork (默认新 UUID4)
- `from_stage` 不在该 task 的 checkpoint 记录里 → 明确报错, 不静默新建

##### 链式 fork 不继承祖先的 `overrides`

在一次 fork 的结果之上再 fork, 新 cp 的 `fork_overrides` **只含本次给的**,
祖先那次 fork 注入的值不会带过来。这是有意的, 不是疏漏:

    r1 = await rt.fork_run(dag, "T", from_stage="s_a", overrides={"a": "A9"})
    #   s_a 重跑后把 a 覆盖回 'A' —— overrides 是注入输入, 不是最终值
    r2 = await rt.fork_run(dag, "T", from_stage="s_c", overrides={"extra": "X"})
    #   r2.state == r1.state["a"] == 'A'  (s_a 在 keep 段, 不重跑)

祖先 override 是**注入输入**, 早已被 `from_stage` 那次重跑消费; 而新 cp 的 keep 段
**不重跑**, 那些 stage 的产出已经定死。若把祖先 override 带上, 它会在截断点注入
一次本次 run 里根本没发生过的注入, 盖掉 keep 段的真实产出 —— 上例里 `a` 会从 `'A'`
变成 `'A9'`。

注意这**不是**"落盘与活跑分叉": 实测把合并真写进去, runtime 与重建同源, 两边会
一起变成 `'A9'`, 一致性不变式仍然成立。被破坏的是**可复现性** —— 同一份
`(dag, from_stage, overrides)`, fork 之前的历史不同, 结果就不同。

需要"在祖先配置之上再改几个", 把祖先那次的 `overrides` 一起传给新的 `fork_run`。

#### 源码漂移提示 (未发布, 见 CHANGELOG [Unreleased])

checkpoint 记 `stage_fn_sources` (上次真实执行的 stage 函数源码指纹; 重放不覆盖)。
`fork_run` / `run_stage` 复用旧结果时比对当前源码, 不一致则经 `logging` 发一条
WARNING — fn 体不计 `workflow_hash` 是有意设计(改 prompt 免全链重跑), 本项把
"改了代码, 跑的是新是旧"从静默变可见。**不阻断、不拒续**, 只提示。

> 注意: 它只覆盖"复用旧结果"场景。全新一次执行里改错了文件(比如改了没人读的
> 副本), 运行时无从知晓 — 那类要靠业务侧的调用点守卫。

### RunResult

- `task_id`: caller 提供或自动生成 UUID4
- `run_id`: 本次 run 的 UUID4 (resume 时 = 复用的原 run_id)
- `status`: `"running" | "done" | "failed" | "cancelled"` (终态: `done` / `failed` / `cancelled`)
- `state`: 最终 state
- `stage_statuses`: 每 stage 的 `done | failed | cancelled`
- `error`: failed/cancelled 原因 (异常转字符串)
- `error_class`: 失败时的原始异常类名 (如 `ValueError` / 业务 `PipelineError`),
  用于分诊 — 业务失败→调参重跑, 代码 bug→找人。done / cancelled 为 `None`。
  类名可随重构变化, 不作跨版本契约
- `error_category`: 失败的业务类别 (v0.5.4) — caller `raise StageError(..., category="LLM_TIMEOUT")` 透传;
  建议值 9 类: LLM_TIMEOUT / SEARCH_NO_RESULT / CODE_BUG / NETWORK / AUTH / INFRA /
  CHECKPOINT_LOAD_FAIL / STATE_VALIDATION / PROTOCOL_BREACH (自由 string, 不强制枚举);
  未传 / done / cancelled 为 `None`

  ⚠️ **这 9 类里只有前 6 类是 caller 自己能打的; 后 3 类是引擎侧失败, 而引擎
  **不替 caller 分类** (不猜业务类别是对的), 所以它们**永远到不了这个字段**
  (2026-10-17 实测, 矩阵钉在 `tests/test_error_category.py`):

  | 引擎侧失败 | 你实际拿到的 |
  |---|---|
  | `STATE_VALIDATION` (stage 返回非有限值等) | `status=failed`, `error_class="ValueError"`, `error_category=None` |
  | `PROTOCOL_BREACH` (router 返回未声明 key) | `status=failed`, `error_class="UnmappedRouteError"`, `error_category=None` |
  | `CHECKPOINT_LOAD_FAIL` (resume 时结构不匹配) | **抛 `CheckpointMismatchError`**, 压根没有 `RunResult` |

  所以分流要**两条腿**: caller 自己打的类别看 `error_category`, 引擎侧失败看
  `error_class` (或捕获那个异常)。只 dispatch `error_category` 的话, 引擎引发的失败
  全落进 `None` 分支 —— 而那恰恰是最需要分诊的一类。
- **`error_category` 与异常类型无关 (未发布, 见 CHANGELOG [Unreleased])**: 只要异常对象
  带 `.category` 属性就会透传 —— **不要求继承 `StageError`**。自定义异常同理:

  ```python
  class MyErr(Exception):
      def __init__(self, msg, category=None):
          super().__init__(msg)
          self.category = category      # → RunResult.error_category

  @dag.stage()
  async def s_x(ctx):
      raise MyErr("空结果", category="CAT_TRUNCATION")
  ```

  非 `StageError` / `FatalError` / `RetryableError` / `TimeoutError` 的异常按
  **FatalError 语义**处理(fail、不重试、终态), `error_class` 为其类名。
  **携带 category 不改变这一点** —— category 是分流元数据, 不是"可重试"信号。
  给异常分流时优先用 category, 别去匹配错误消息文本: 文本匹配会随措辞改动静默漂移。
- `stage_timings`: `{stage: 墙钟秒}` (含 retry 退避 sleep; 只含已执行的 stage)

### 失败语义 (不抛异常, 返回 failed)

| 异常 | 含义 | 行为 |
|---|---|---|
| `StageError` | 业务失败 | fail, 不重试 |
| `RetryableError` | 网络/限流/超时 | 退避重试, 耗尽 fail |
| `FatalError` / 未知异常 | 代码 bug | fail, 不消耗 retries |

上表三个都继承 `PavozError`;条件边的 `UnmappedRouteError` /
`MaxVisitsExceeded` / `OrphanStagesError` 也挂在它下面 — 库自身抛出的异常
统一走这一个基类, 包一层 SDK / 转错误码时 `except PavozError` 一次兜住。
`CheckpointMismatchError`(cp 与 DAG 结构不匹配 = 调用方用错了)不在此树下。

注意上表说的"不抛异常"指 `run()`:引擎判的失败都转成
`RunResult(status="failed", error_class=...)`。统一捕获用于 `run()` 之外的
入口 (CLI / TestPipe 等)。

## Ctx (stage 参数)

```python
ctx.task_id: str
ctx.run_id: str          # 本次 run 的 UUID4 (resume 复用的原 run_id)
ctx.attempt: int         # 1-based; 本 stage 当前尝试次数 (retry 一次 +1)
ctx.stage_name: str
ctx.state: ReadOnlyStateView   # 只读深拷贝; 写任何属性 raise
ctx.deadline: float | None     # 本 stage absolute deadline

await ctx.call(kind: str, op: str, params: dict | None = None) -> dict
ctx.set_progress(fraction: float, note: str | None = None) -> None  # 长 stage 心跳
ctx.cancelled() -> bool                                            # 协作式取消轮询
ctx.on_event: Callable[[str, dict], None] | None                   # 本 run 的事件出口
```

- `ctx.call` 是 stage 内**唯一**外部调用入口。`kind`/`op`/返回值结构由业务
  caller 决定 — 框架只透传, 默认 caller 是 no-op echo
  (`CallResult(kind=..., op=..., params=...)`)
- 注入的 caller 收**第 4 参** `meta: CallMeta` (`task_id` / `run_id` / `stage` /
  `attempt`) — caller 落 trace/llm_calls 时可直接关联执行上下文。
  `CallMeta` 在 `pavoz.runtime`。stage 函数签名不受影响
  (`async def fn(ctx) -> dict`)
- `ctx.logger`: logging.Logger (stage 名已注入)
- `ctx.set_progress(fraction, note=None)`: 长 stage 的可选心跳 — 分钟级 LLM 调用
  期间回答"在算还是卡死"。best-effort: 只进 `on_event` 的 `stage_progress` 事件,
  不落 checkpoint, 不影响 deadline / 重试 / 取消语义; 同 fraction 自动去抖, 未启用
  `on_event` 时是 no-op
- `ctx.cancelled()`: 协作式取消轮询 (长循环自行决定退出方式); `cancel_check`
  抛异常 → 视为未取消
- `ctx.on_event`: 与 `Runtime(on_event=...)` 同一个出口 — 装饰器类扩展
  (如质量门) 用它发自定义事件, 与运行时事件共用一个流

## State

- **返回 `None` 与返回 `{}` 同义** (`_run_stage` 归一成 `{}`): 纯副作用 stage (发
  webhook / 写库 / 打点) 可以什么都不 return。注意反面 —— **忘了 `return` 也是这个
  结果**, run 照报 `done`、不写任何 state, 所以下游读不到某个 stage 的产出时, 先去
  看它是不是漏了 `return`
- 类型白名单: `str/int/float/bool/None/list/dict` (json-serializable),
  其他 (`set/datetime/Path/bytes/...`) → `TypeError`
- **`float` 还必须是有限值**: `NaN` / `±Inf` 类型对, 但落盘会写成字面量
  `NaN`/`Infinity`, 那不是 RFC 8259 的合法 JSON (严格解析器 —— 其他语言 / jq /
  PostgreSQL JSONB —— 拒收整个文件, 而 Python 自己 lenient 所以本仓内不显形)
  → `ValueError`。校验在 `deep_validate_state` 这一个 chokepoint, stage 返回值与
  `--set-file` / `--overrides` 走同一条路
- ⚠️ 本节早前写的 `StateValidationError` **在代码里不存在** (实际是上面两个内置
  异常)。照旧文档写 `except StateValidationError` 会 ImportError
- 写路径: stage `return dict` → **shallow merge**, 嵌套 dict **整体替换不深合并**:
  s_a 写 `{"cfg": {"model": "m1", "temp": 0.1}}`, 下一个 stage 返回
  `{"cfg": {"temp": 0.9}}` → state 里 `cfg` 变成 `{"temp": 0.9}`, **`model` 没了**
  (实测: 下游读 `cfg["model"]` 直接 `KeyError`, run 报 failed)。想改一个叶子就把
  **整个 cfg** 返回, 或把叶子提成顶层 key
- ⚠️ 与 overrides 的语义**不同**: `--set` / `--set-file` / `--overrides` /
  `fork_run(overrides=)` 走 `apply_overrides` → **深合并** (只打 patch leaf, 兄弟键
  保留)。stage 返回值是浅、注入值是深 —— 两条路径别互相类推
- ⚠️ **点分路径**不存在时会**静默新建**, 不报错。把 `llm.model` 拼成 `llm.modle`
  ⇒ 多出一个没人读的死 key, 真正的 `llm.model` 保持旧值, 而 run 照报 `done` ——
  配置看起来设了, 实际没生效。这是**有意**的行为: 往已有结构里加新配置项
  (`llm.temp`) 与往空 state 首次注入都是合法常态, 报错会把它们一起拒掉 (pvos 不该
  猜用户的 state schema)。可发现性靠分级日志: 覆盖**非 dict 的中间值** = WARNING
  (那是数据丢失, 几乎总是写错), 中间层新建与**叶子 key 新建** = DEBUG (叶子拼错与
  加新项无法区分, 只能这样)。所以排查这类"改了没生效"要先开 DEBUG 日志, 或看
  `fork-run` 输出里的 `overrides_applied` / `--compare-with` 的 diff
- 冲突: delta key 已存在于 state 且 producer **不是**当前 stage 的传递上游
  → `StateConflictError` (平行 producer 冲突)
- 链式覆盖: producer 是传递上游 → 合法 (v0.1.1)

## Checkpoint

```python
store = CheckpointStore(storage: StorageBackend)
store.save(cp)                                  # 写 checkpoint + 更新 latest 指针
cp: Checkpoint | None = store.load(task_id, run_id)       # 指定 run
cp: Checkpoint | None = store.load_latest(task_id)        # 按指针文件加载最新
run_ids: list[str] = store.list_runs(task_id)             # 有哪些 run? (调试/清理)
store.delete(task_id, run_id)                             # 指针不随删除更新
cp: Checkpoint | None = store.load_compatible(task_id, run_id, dag)
# hash 校验加载; run_id="" → 该 task 最新 run (load_latest 哨兵)
removed: dict = store.prune(task_id, keep_last=5, dry_run=False)  # 修剪旧 run
```

- 存储布局: 每次 run 存 `runs/{task_id}/{run_id}/checkpoint`; 每次 `save()`
  同时更新指针文件 `runs/{task_id}/latest` (`{"run_id": ...}`)。同 task 多次
  run 互不覆盖; "最新" 不靠字典序 (uuid4 字典序 ≠ 时间序)。删的恰是指针
  指向的 run → `load_latest` 返 `None` ("无 checkpoint" 明确信号, 可接受边界)
- `Checkpoint` 字段: `task_id / run_id / dag_name / workflow_hash /
  stage_statuses / state / done_stages / producers` + v0.5.1:
  `initial_state` (run 的起始 state) 和 `stage_deltas` (每个已完成 stage 的
  原始 return dict, 按完成序)。两者合起来可重建任意已记录 stage 执行前的
  state (`cp.state` 是最终合并态 — 无法从它倒推每 stage 的输入)。
  `run_id` 必填 — v0.5 之前的 checkpoint (无 `run_id`, 旧 key
  `runs/{task_id}/checkpoint`) 已 orphan (hard cut, 无 legacy loader);
  当前格式内缺可选字段走默认值 (schema 向前兼容 — v0.5.0 旧 cp 无新字段
  也可读可 resume)
- `cp.stage_attempts` (0.5.9+): 每个 stage 的**每次 attempt** 记录, 追加序 =
  执行序。每条 `{visit, attempt, status, ts, duration_ms, error}` ——
  `status ∈ done | failed | retry | cancelled`; `error` 是异常消息摘要
  (截断到 500 字符, 完整消息仍在 `RunResult.error`), 成功为 `None`; `ts` 是
  attempt **开始**时刻 (epoch 秒), `duration_ms` 不含 retry 的退避 sleep。
  循环多轮按 `(visit, attempt)` 区分; resume 会在同一 visit 上重新从
  `attempt=1` 计数, 跨 resume 的先后按 `ts` 排序。`pavoz trace` 直接输出它。
  **这是纯审计数据**: 不参与执行决策或 state 重建; 旧 cp 无此字段 → `{}`。
  它解决的是"run 结束后 / resume 后, 哪个 stage 慢、上次为什么失败没有落盘
  证据" —— 事件流不持久化, `RunResult.stage_timings` 只在内存结果里。
- `cp.rebuild_state() -> dict` — 重建所有已完成 stage merge 后的 state
  (`initial_state` + done deltas 按完成序; 链式覆盖由顺序天然处理)。
  给从未完成 (依赖已全完成) 的 stage 当执行前 state (重放用)
- `cp.rebuild_state_before(stage_name) -> dict` — 重建 stage 执行前的 state
  (`initial_state` + 它之前已完成 stage 的 deltas)。stage 不在本次 run 的
  记录里 → `KeyError`。`run_stage` 用它 — 保证 stage 自己的旧输出不会污染
  重放输入
- `store.prune(task_id, *, keep_last, dry_run=False)`: 修剪旧 run 的 checkpoint —
  按"最后活动时间" (`max(stage_ts)`) 保留最新 `keep_last` 个, 其余删除 (key 级
  `delete` + 返回释放字节数近似值)。**latest 指针指向的 run 无条件保留**, 指针
  文件本身不动 (不会让指针悬空)。`dry_run=True` 只算不删。CLI: `pavoz prune`
- `workflow_hash(dag)`: **dag 名** + stage 名+依赖+retries+timeout (+条件边) 的
  sha256 指纹 (改函数体不影响 hash)。dag 名计入是刻意的: 结构相同但名字不同的
  两条流水线曾拿到同一指纹, task_id 撞车时 resume 会拿错 cp 且毫无提示 ——
  实测 `RunResult.dag_name` 报的是新 DAG, 而 state 全是旧 DAG 的数据。
  **代价**: 改名 = 指纹变了 = 旧 cp 不能续跑 (想续就把名字改回去, 或换 task_id)
- `CheckpointMismatchError`: resume 时结构变了 → 拒续跑 (`resume=False`
  强制重跑, 或删该 run 的 checkpoint)

## StorageBackend (Protocol, 业务注入)

```python
class StorageBackend:
    def put(self, key: str, data: dict) -> None: ...
    def get(self, key: str) -> dict | None: ...
    def list_keys(self, prefix: str) -> list[str]: ...
    def delete(self, key: str) -> None: ...
```

内置: `FileStorage(dir)` — 本地文件系统实现 (测试/单机)。

`FileStorage` 写记录是**原子**的 (同目录临时文件 + `os.replace`): 崩溃或断电时
写到一半, 磁盘上留下的仍是上一份**完整**记录, 而不是半截文件。

### `CorruptRecordError`

读到的记录**不是合法 JSON** 时抛出 (最常见: 崩溃/断电留下的半截文件)。消息里
带文件路径、原始错误与处置建议; CLI 顶层兜住它并 rc=1 —— 所以 `trace` / `state` /
`export-input` / `replay` / `fork-run` / `run --resume` 在这种情况下都给出**一句能看
懂的话**, 而不是一段指向 `json/decoder.py` 的 traceback。

**它不会退化成 `None`**。`get` 返回 `None` 的含义是"没有这条记录"; 若把损坏也当成
没有, `run --resume` 会从头开始 —— 用户看到 "done", 而这个 task 已有的进度被**静默
丢掉** (磁盘上的旧数据明明还在, 只是读不出来)。

## Storage loader

pavoz core **只**带 `StorageBackend` Protocol + `FileStorage`。driver
(psycopg / redis / boto3 / ...) 由用户自己装, 自己写 (或抄) 实现 Protocol 的
adapter, 然后用 config 字符串加载。

```python
from pavoz import load_storage

storage = load_storage("pavoz.storage.FileStorage", root_dir="/data/cp")
# 或:
storage = load_storage(
    "backend.integration.sf_storage.MinioStorage",
    task_id="run-2026-09-05-001",
)
cp_store = CheckpointStore(storage)
```

### `load_storage(spec: str, **kwargs) -> StorageBackend`

- `spec` — adapter 定位字符串。两种等价形式:
  - `"pkg.module:ClassName"` (canonical, 显式 separator)
  - `"pkg.module.path.ClassName"` (dotted, 最后一段是 class 名)
- `**kwargs` — 透传给 `ClassName.__init__`
- 失败抛 `PavozStorageError` (`ImportError` 子类), 5 类错误信息友好:
  格式错 / 模块未装 / class 名错 / kwargs 错 / 非 `StorageBackend` 实例。

完整参考 + 存储互操作契约 + adapter 写法: [docs/storage.md](../storage.md).

## TestPipe

```python
pipe = TestPipe(dag, initial_state={...})                  # caller=None 默认
pipe.mock("s_search", lambda state: {"sources": [...]})   # 按需替换 stage 输出
result: RunResult = await pipe.run()

# v0.5.1: 把真实 run 的 checkpoint 当回归夹具
pipe2 = TestPipe.replay_from(cp, dag)  # 已完成 stage 用存下的 delta 喂 (mock)
result2 = await pipe2.run()
assert result2.state == cp.state       # 相同 stage 输出进 → 相同图行为出
```

- 未 mock 的 stage 原样执行 (可 mock 外部调用密集段, 其余真跑)
- `pipe.calls()` 返回调用记录, 每项 `{stage, mock, state_keys}`: `stage_keys`
  是该 stage 被调用时**看到的 state 顶层 key** (插入序), 可用来断言"下游确实拿到了
  上游的产出"。只有被 mock 的 stage 留记录
- `pipe.run(resume=True)` **不支持** (TestPipe 不落盘, 无可续的 checkpoint) —
  传了直接 `ValueError` 并指出替代路径 (`replay_from` / 真 `Runtime`)。要真续跑
  用 `Runtime(checkpoint_store=...).run(..., resume=True)`
- 回归场景: 用固定 mock 输出跑全图, 断言最终 state
- `replay_from(cp, dag)`: `cp.done_stages` 里每个已完成 stage 用存的
  `stage_deltas` 输出 mock; 从未完成的 stage 真跑。要求 cp 的
  `workflow_hash` 与 DAG 一致 (结构没变) — mismatch →
  `CheckpointMismatchError`; pre-M2 (v0.5.0) cp 无 `stage_deltas` →
  `RuntimeError` (重跑一次)。真实 run 的手写 mock 回归可被它替代
- ⚠️ **环图 (条件边回路) 上只保证终态自洽, 不保证路径一致**: `stage_deltas` 是
  last-write 视图, 环里每一轮都拿到该 stage 的**最后一次** delta, 真实的 3 轮会被
  压成 1 轮。终态多数时候仍等于 `cp.state` (终值本身就是终点), 但轮次敏感的 stage
  (计数 / 累加) 会算错 —— 那类断言请走 `Runtime` 真跑

## DAG viz export (v0.5.4)

```python
from pavoz import to_mermaid, to_graph_json

# 嵌入 README / 飞书
print(to_mermaid(dag))
# → graph LR
#     s_a["s_a"]
#     s_b["s_b"]
#     s_a --> s_b

# 前端 Vue / dagre / cytoscape 渲染
import json
graph = to_graph_json(dag)
# {"dag_name": ..., "nodes": [{id, name, depends_on, retries, timeout}, ...],
#  "edges": [{from, to}, ...], "topo_order": [...]}
```

- **只出数据**, caller 自渲染 (pavez 出 UI/drag-drop/server = 触 #4 红线)
- `to_mermaid(direction="LR"|"TD")` 第二个参数可调布局方向
- `to_graph_json()` 全 JSON-serializable, 喂 wire 协议/前端免中间层
- 零依赖, pavoz core 不动

## EnginePolicy (v0.5.3)

### `stage_input_hash(fn, before_state) -> str`

stage 执行输入指纹 (sha256 前 16 hex) = **fn 源码 + 执行前全量 state**, 就这两项。
run 期间每个 stage 执行前自动记录进 checkpoint 的 `stage_input_hashes`，fork_run
`skip_unchanged=True` 时据此跳过输入未变的下游 stage（副作用 stage 用
`Stage.skip_unchanged=False` 拒跳）。

> ⚠️ **覆盖边界: prompt 必须写在 fn 体内**。指纹**不含**闭包变量、模块级配置、
> 环境变量、文件、时钟 —— 那些改了 hash 不变，于是 `skip_unchanged` 会**静默复用旧
> 结果**（实测: 把 prompt 从模块级 `PROMPT = "v1"` 改成 `"v2"`，`fn` 源码与
> 执行前 state 都没变 → fork 照旧返回 v1 的产出，`status=done`）。要改的东西写在
> stage 函数体里（改函数体 = 改 hash），或给该 stage 标 `skip_unchanged=False`。
> 纯函数语义之外的外部依赖（LLM 输出随版本漂移等）同样由 caller 用
> `skip_unchanged=False` 兜底。

```python
from pavoz import EnginePolicy, Runtime

policy = EnginePolicy(
    default_timeout=600,        # 整 run deadline; Runtime.default_timeout 显式给值时优先
    max_steps=50,               # stage attempt 总数上限 (含重试), 超限熔断 failed
    backoff_max=30.0,           # RetryableError 退避上限秒
    max_concurrent_runs=4,      # 同一 Runtime 进程内并发 run 上限
    skip_unchanged=False,       # fork_run 未显式传时的 skip_unchanged 默认
    stage_alive_interval=30.0,  # 心跳: stage 跑满该秒数后每 interval 发一次 stage_alive 事件; None 关闭
)
rt = Runtime(policy=policy)     # additive: 不传 policy 行为与旧版完全一致
```

**字段取值在构造时校验** (`EnginePolicy.__post_init__`): 负数一律 `ValueError`;
`max_steps` / `max_concurrent_runs` 的 `0` 也拒 —— 调用点一律写 `if policy.x`, 0 是
假值会被当成"没设", 于是"上限 0"读起来像不许并发、实际是**不限**, 反过来
`max_steps=0` 会让第一个 stage 立刻熔断。**"不限 / 关闭"请显式写 `None`**。

- `max_concurrent_runs` 满了是**排队**而不是拒绝。实测 (上限=1, 两个并发 run, 各一个
  0.4s 的 stage): 第二个等第一个跑完才开跑, 整体 ~0.8s, 两个都 `done`。
- ⚠️ **排队本身没有超时**。一个卡死的 run (需要把 run 级 deadline 与
  `default_timeout` 都关掉才可能发生) 会把后面的 run **永久**堵住 —— 实测第一个
  run 睡 30s 时, 第二个等 2s 仍未开始。库内没有第二个兜底: 决定排队能等多久的, 是
  run 自身的 deadline / `default_timeout`。要给**等待本身**设上界, 在调用侧包一层 ——
  `await asyncio.wait_for(rt.run(dag, task_id), timeout=X)`, 实测它抛
  `TimeoutError`, 不会被吞成假的 `RunResult` (registry 硬杀与外部取消是分开记的)。

### `stage_alive_interval` 事件 (未发布, 见 CHANGELOG [Unreleased])

stage 执行超过该间隔后, runtime 经 `on_event` 发
`stage_alive {task_id, run_id, stage, visit, attempt, elapsed}` — 长 stage 没有
`progress` 汇报时的存活信号(LLM 调用、大模型推理等场景常见)。快 stage 零事件;
不传 policy 时走引擎默认 30.0s; `None` 关闭。run 结束心跳立即停止。

## CancelRegistry (v0.5.3)

```python
reg = CancelRegistry()
rt = Runtime(cancel_registry=reg)
task = asyncio.create_task(rt.run(dag, "t1"))
reg.cancel("t1")                  # graceful: stage 边界拦截 (同 v0.9 cancel_check)
reg.cancel("t1", mode="hard")     # 硬杀: 立即打断当前 stage (前提 stage 幂等)
# 当前 stage 标了 @dag.stage(killable=False) → hard 抛 NotKillable
await task   # → RunResult(status="cancelled", failed_stage=..., retryable=None)
```

- graceful 走 stage 间/重试间检查点; hard 对运行中 stage 注入 CancelledError,
  run() 捕获后落 checkpoint 并返回 cancelled 结果 (caller 须保证 stage 幂等)
- 非 registry 来源的外部取消 (`asyncio.wait_for` 超时 / 调用方 `t.cancel()` /
  loop 关闭) 仍照旧**上抛** —— 此前只看"有没有取消标志"就把它转成
  `RunResult(status="cancelled")`, 于是 `wait_for(rt.run(...), t)` **不抛
  `TimeoutError`**, 调用方的超时保护静默失效
- **一个 task_id 同时只能有一个活跃 run**: 第二个 `run()` 抛 `TaskIdInUse`。
  ⚠️ **仅在配置了 `cancel_registry` 时成立** —— `Runtime()` 的 `cancel_registry`
  默认是 `None`, 而占用登记 (`_register`) 就发生在那条路径上。不带 registry 的
  `Runtime` 连 `cancel()` 都不可用, 于是两个 run 可以共用 task_id 并发跑完,
  **不报任何错**; 后果是 checkpoint 的 `latest` 指针变成"最后写完的那个",
  `resume` / `trace` 随之指向它 (各 run 自己的 cp 文件独立, 不会损坏)。要这层
  保护, 让所有可能跑同一 task_id 的 Runtime **共用同一个** `CancelRegistry`
  实例 —— 两个各传各的 registry 等于没传。
  registry 按 task_id 寻址, `cancel(task_id)` 在两个 run 共用时无法定义该取消
  哪一个 (且 checkpoint 也按 task_id 存, 两者会抢 latest 指针)。此前是静默
  撞车: 先收尾的 run 会把还在跑的那个的句柄一起摘掉, 导致硬杀**根本没执行**却
  返回成功、`killable=False` 的保护被绕过、而标志仍设上导致**误伤**。
- run 被取消后**标志保留** (caller 表达过的意图): 要续跑或复用该 task_id,
  先 `reg.reset(task_id)` —— 不 reset 的新 run 会立刻再次 `cancelled`。
  run **正常**结束 (done/failed) 时 `Runtime` 会自己清掉竞态残留的标志。
  取消日志 (graceful / hard 两条) 都会点名 `registry.reset(task_id)`

### `diff_runs(cp_a, cp_b) -> dict` / `CheckpointStore.diff_runs(task, run_a, run_b="")`

两个 run 的 stage 级 structured diff (R4)。输出全 JSON-serializable, 不做 AI 摘要:
`workflow_hash_changed` / `dag_names [a, b]` / `stages` (每 stage 的 `status [a, b]`、
`input_hash_changed`、`visits [a, b]`、`output_diff` path 级差异) / `state_diff`
(最终 state path 级差异)。Store 包装的 `run_b=""` 取 latest 指针 (同
load_compatible 惯例); 任一 run 无 checkpoint → RuntimeError。消费者 (AI agent)
据此判断"改了什么、哪些 stage 受影响、要不要重跑"。

- **`visits` = 该 stage 在两个 run 里各跑了多少轮**。循环图 (R1) 上这是最该被看见
  的量, 而 `output_diff` / `state_diff` 都表达不出: 实测两个**终态逐字节相同**的
  run (3 轮 vs 5 轮), `state_diff` 是空的, `output_diff` 只比出"最后一次 visit 的
  delta 差在哪"——信号方向是反的: 该报的"多干了 67% 的活"没报, 不该当差异的
  末轮细节反倒成了唯一线索
- **`dag_names` 让"改名"能被单独判出来**。dag 名自 0.5.7 起是 `workflow_hash` 的
  一部分, 所以**改名**就会让 `workflow_hash_changed` 变 `true` —— 而改名恰恰是
  最该被判成"这不是真差异"的那种: 实测两个 run 唯一差别是 dag 名时, `stages`
  里每一项都显示"无差异", 而旧的单个 `dag_name` 字段只报 a 的名字, b 叫什么
  **完全看不见**, 消费者只能得出"结构变了但行为没变"这个错误结论
- ⚠️ `output_diff` 走 `stage_deltas` —— **last-write 视图** (每个 stage 只取最后
  一次执行的 delta)。循环图上它漏掉中间轮次。逐轮全貌在
  `cp.stage_deltas_visits`, 要自己比就取那个字段
- ⚠️ `workflow_hash_changed: true` **不说明差在哪**。指纹是一个整串, 拆不出"是
  哪个 stage 的哪个属性变了"。这是有意的边界: 该字段的作用是叫消费者"别拿这两
  份比", 而不是替它解释 —— 唯一的例外是改名, 已由 `dag_names` 单独兜住

## EventRecorder (v0.5.3)

```python
rec = EventRecorder()                  # 收集 (event, data) 事件流
rec = EventRecorder(sink_path="ev.jsonl")  # 每事件追加一行 JSONL
rt = Runtime(on_event=rec)             # 签名匹配 on_event 回调
rec.events   # [(event, data), ...]
rec.names()  # ["run_start", "stage_start", ...]
```

- 生命周期事件: `run_start` / `stage_start` / `stage_end` / `stage_retry` /
  `stage_progress` / `run_end` (与 `on_event` 同一事件流)
- 不想手写收集 lambda 时的现成观察者; 落盘留档供事后排查
- **sink 的每一行都是合法 RFC 8259 JSON**: 载荷里的非有限 float (`NaN` / `±Inf`)
  落盘时字符串化成 `"nan"` / `"inf"` / `"-inf"`, 而不是写成非法字面量 ——
  这份文件是给 jq / Splunk / Loki 读的, 一行读不了就等于少一条记录。
  只动**序列化**, 不动内存: `recorder.events` 里仍是原始 float。
  (state 侧的非有限值早在 `deep_validate_state` 就被拒了, 但事件载荷是
  caller/stage 自报的数据, 不经那个 chokepoint)

## CLI

```bash
python -m pavoz run <dag.py> [--task-id X] [--input '{"k": "v"}'] [--resume]
python -m pavoz trace --task-id X
python -m pavoz state --task-id X [--key K]
python -m pavoz replay <dag.py> --task-id X --stage Y [--patch P.py]   # v0.5.1
```

- `run`: 跑 DAG 文件。**挑选规则** —— 模块作用域里**任意** `DAG` 实例都算数 (变量名
  不要求叫 `dag`; `class MyDAG(DAG)` 的子类实例同样被认, 别的库里恰好也叫 `DAG` 的
  同名实例则**不会**被误收)。文件里有多个时用 `DEFAULT_DAG = "<DAG 名>"` 指定:

      ```python
      from pavoz import DAG
      alpha = DAG("alpha");  beta = DAG("beta")
      DEFAULT_DAG = "beta"          # ← 跑 beta
      ```

  那个名字必须与 `DAG(...)` 里的名字**完全一致** (不做前缀/模糊匹配), 且**匹配不上直接
  报错并列出实际有哪些** —— 不会静默取第一个。声明了却被忽略 = 跑起另一个 pipeline 而
  全程无提示, 那比报错糟得多 (用户会照着 alpha 的结果去调 beta)。没声明且有多个 →
  取第一个, 并在 stderr 上提示一句。`PAVOZ_STORAGE` 环境变量
  指向 FileStorage 目录 — 每次 run 都落 per-stage checkpoint (单跑也算,
  v0.5.1 R5 fix), `--resume`/replay/trace/state 对 CLI 产物可用
- `trace/state`: 读 FileStorage 里的 checkpoint。`state --key K` 在 **K 不存在时
  点名报错 + 列可用 key + 退出码 1** —— 与"K 存在但值为 null"(退出码 0)严格可区分,
  接管道的调用方不能只凭输出判断
- `replay`: 在重建的输入上重放单 stage (见 `Runtime.run_stage`); 可选
  `--patch P.py` 加载暴露 `patch(dag) -> None` 的模块先换实现 (必须 sync —
  async patch 拒绝)。输出 result JSON; 不落任何盘 (不写 cp, 不动 latest 指针)

  patch 的**可用写法**是直接换 `dag.stages[名].fn`:

  ```python
  # patch.py
  def patch(dag):                     # 必须同步 def
      async def s_b_v2(ctx):
          return {"b": ctx.state["a"] * 10}
      dag.stages["s_b"].fn = s_b_v2   # ← 换实现, 不是再加一个 stage
  ```

  ⚠️ **不要用 `@dag.stage()` 重定义** —— 已存在的 stage 名会撞
  `duplicate stage name` (这是 stage 级的保护, 不是 bug)。2026-10-17 之前文档只说
  "暴露 `patch(dag) -> None`"却没给写法, 最自然的写法正是重挂装饰器。
  patch 函数体抛出的异常现在收敛成一行 (`patch 执行失败 (<类型>): <消息> — 文件
  <路径>`) + 退出码 1, 与 CLI 其余参数路径一致, 不再甩 runpy/asyncio 的栈。

  ⚠️ 只跑**被重放的那一个** stage: patch 了别的 stage 再 replay 它, 补丁不会生效
  (前序 stage 的输入是从 checkpoint 重建的, 不会重跑)。要试改 prompt, patch 的就是
  `--stage` 指的那个。

## 参数覆盖 (fork-run 的库等价物)

```python
parse_set_args(["a.b=1", "c=true"]) -> dict    # --set 的点分路径解析 (类型自动推断)
parse_set_file("overrides.yaml") -> dict        # --set-file (JSON/YAML, safe_load, 1MB 上限)
merge_overrides(base: dict, patch: dict) -> dict   # 深合并 (兄弟键保留)
apply_overrides(state: dict, overrides: dict) -> dict
```

CLI `pavoz fork-run --set / --set-file` 走的就是这条链;`Runtime.fork_run(overrides=...)`
的内部深合并语义与之相同。完整指南: [replay-params.md](../replay-params.md)。

`pavoz.__version__`: 当前版本字符串 (与 `pyproject.toml` / release tag 同步)。

## Conditional edges (v0.5.5)

编排器持所有边 (含条件边), 节点纯数据处置 — stage 只写 state, 不决定下一步;
判断逻辑在声明期集中写在边上 (同步纯函数, 只读 state)。

### `DAG.add_conditional_edges(from_node, route_fn, mapping, *, max_visits=None)`

```python
dag.add_conditional_edges(
    "s_review",
    lambda s: "ship" if s["verdict"] == "pass" else "fix",  # 同步纯函数
    {"ship": "s_save", "fix": "s_revision"},                # key → 目标 (封闭集)
    max_visits=5,
)
```

- `route_fn(state) -> key`: 同步纯函数, 同 state 必同 key (重放/resume 可判定)。
  **形态**: plain `def`、绑定方法、带查找表的**可调用对象**都支持 (对象的指纹取
  它的 `__call__`)。但 async 一律在声明期拒 —— 包括 async 藏在 `__call__` 里的
  可调用对象 (`inspect.iscoroutinefunction(实例)` 是 False, 只查函数会漏)。
  **声明期强制**: 传 `async def` 直接 `TypeError` —— 不拦的话运行期只会拿到一个
  coroutine 对象, 查 mapping 必 miss, 报"返回未声明 key <coroutine ...>", 消息会把
  人引向错误的方向 (路由压根没被调用)。
  LLM 判断请做成 judge-stage 写 state (如 `state["route"]`), 边只读 —
  LangGraph / Temporal / SFN / BPMN 收敛的同型模式。
- `mapping`: key → 目标 stage 的封闭集; 运行时返回未声明 key →
  `UnmappedRouteError`。route_fn 自己抛的异常原样传播 (error_class 保留,
  failed_stage = router)。
- 循环静态链 (R2): 回边目标所在环若没有任何成员持有 *环外* 静态父, 首个声明的
  环成员豁免为环入口 (第一圈从它起跑)。router 回跳已执行的 stage = 显式重入:
  落点的静态下游闭包重新执行 (每个重跑 stage 记新 visit), `max_visits` 熔断照常
  生效; router 链环 (环体靠路由跳转) 行为不变。中途失败 resume 从断点续跑。
- 终止合法性 (BUG 2, fail-loud): 正常终止时若有声明 stage 未被执行、且没被任何
  *决策未过期* 的已执行 router 声明覆盖 → `OrphanStagesError`, run failed 并列出
  全部孤儿名单。"决策未过期" = 该 router 决策后只跑了所选目标可达集内的 stage;
  分支外 stage 在决策后执行 (回路没闭合, 无人重判) → 该 router 的 mapping 不再
  构成豁免。路由未选中的分支 (judge 直接 pass) 是合法豁免。
- `max_visits`: router 单次 run 最大执行次数, 超限 → `MaxVisitsExceeded`;
  条件边构成回路时 validate() 强制非 None (防 LLM 循环烧钱), 分支-only 图可不设。
- ⚠️ **`max_visits` 耗尽后 `resume` 无效, 要重跑请用 `resume=False`**。计数**跨
  resume 累计** (从 cp 的完成序恢复), 所以预算用尽后再 resume 会**一个 stage 都不
  跑**就再次 `MaxVisitsExceeded`; 而调大 `max_visits` 又会改 `workflow_hash` ⇒
  `resume` / `fork_run` 双双 `CheckpointMismatchError`。出路是 `resume=False`
  (起新 run_id, 不读旧 cp) —— 修好退出条件后重跑一次即可。实测: 退出条件写错
  (永远不满足)导致熔断后, 改对条件 + `resume=False` 正常跑完。这不是死锁, 但
  不写出来的话, 每个撞上的用户都要自己在 resume 上反复试几轮才发现。
- validate() 结构规则: 条件边目标不可静态依赖 router; router 的静态下游必须是
  mapping 目标; 回路边目标带静态父 → 参与 topo 初始触发 (循环头), 否则 route-only
  (分支/循环体)。
- `workflow_hash` 计入 route_fn 源码 + mapping + max_visits — 改路由 = 结构变,
  resume/fork 拒续。
- 条件边图的单点隔离调试 (R2): `run_stage(dag, task_id, stage)` 重放单 stage
  (输入 = 完成序逐 visit 折叠到该 stage 首次执行前; 不落 cp, latest 指针不动);
  `fork_run(dag, task_id, from_stage=..., overrides=...)` 截断到 from_stage 首次
  完成处 + overrides + 路由续跑 (与 resume 同一执行模型; from_stage 必须已执行
  过)。改 stage 函数体 (如 LLM prompt) 不变 workflow_hash — 免全链重跑。fork
  overrides 的伪 producer `<fork>` 允许被续跑链改写 (改输入重跑是 fork 的本意)。
- 事件: `route` `{from, key, to}`; `stage_start/stage_end` 增加 `visit` 序号
  (循环第几轮, 区别 attempt=重试)。viz: `to_mermaid` 条件边 = key 标签虚线,
  `to_graph_json` edges 带 `kind`/`key`/`max_visits`。
- 完整指南 (含 judge-stage 模式): [conditional-edges.md](conditional-edges.md)。

## 相关文档

- [quickstart.md](quickstart.md) — 5 分钟跑通
- [architecture.md](architecture.md) — 架构与设计决策
- [use-cases/](use-cases/) — 业务接入参考实现
