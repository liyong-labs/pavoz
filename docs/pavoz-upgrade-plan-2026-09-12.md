# pavoz 功能升级改造规划 v3 (含 per-stage 小循环理论)

**作者**: Claude (需求方 / 应用侧)
**日期**: 2026-09-13 (v3 = v2 + 评审修正)
**v2 日期**: 2026-09-12 (含 per-stage 小循环理论) · **v1**: 2026-09-12
**目标读者**: pavoz 维护者 + ai-research/ai-write 集成方
**当前 pavoz 版本**: v0.3.0rc1
**配套版本**: ai-research 某 9-stage pipeline + ai-write research_* pipeline (10+ stage) + 某政策研究课题 (8 stage)
**评审**: `docs/review-pavoz-upgrade-plan-2026-09-13.md` · **Handoff**: `docs/design/handoff-upgrade-plan-v2-2026-09-13.md`

> **v3 修订摘要**: 评审结论 = **核心 0 改动**。v2 §5 主张的 `runtime_hook` + `ctx._meta` 被否决 (核心已有等价能力)。4 个扩展保留但全部落在**独立扩展仓**; §6.2/§6.3/§6.5 契约修正; §7.3/§15 应用层内容移出。

---

## 1. 执行摘要

pavoz v0.3.0rc1 现状: DAG/checkpoint/state/事件流/单 stage 重放全部就位。**缺的只有应用层的小循环把式**, 不是核心能力。

**核心理论 (v2)**: 大循环 (全 pipeline 单次审) 容易跑偏, 改成 **per-stage 小循环** (每 stage 独立 2-3 轮 reviewer 收敛 + gate) 才能避免大循环走偏 + 控制成本 + 保证质量。

**本规划主张 (v3 修正)**:
- **核心 0 改动** — `on_event` (事件出口) + `run_stage` / `fork_run` (单 stage 重放) 已 ship, 无需新增 hook 与 `ctx._meta`
- **扩展独立仓** `pavoz-extensions` (接受) — `@gate` / `@schema` 进发行版; `@cost_cap` 改造后进; `@conditional` 降级为 example
- **每个 stage 实施 per-stage 小循环** (worker + 2-3 reviewer 不同 lens + gate 评分 + cost cap), 落在**应用层**
- **独立仓从一开始就分** — 分仓才是"扩展点够不够用"的真实验证

**投入**: 1–2 周 (核心 0 + 扩展仓 3–5 天 + 应用侧接入 2–3 天 + 文档/示例 1–2 天)
**回报**: ai-write cascade 事故排查 -80%, ai-research 课题交付周期 -50%, 新课题起项目时间 -70%

---

## 2. 背景与动机

### 2.1 触发: ai-research + ai-write + 某政策研究课题 真实痛点

| 痛点 | 来源 | 频次 |
|---|---|---|
| **大循环跑偏 (一错错到底)** | 该政策课题 v3 审题被 3 reviewer 打 3.7/10, 17 条 P0 必改 | 每次新课题 |
| cascade 静默死 | 2026-07-26 / 2026-07-28 / 2026-08-28 | ≥4 次 |
| 研究 chain 卡死 (50min+ 0 产出) | 2026-07-31 research_proofread | ≥2 次 |
| 改 prompt 重跑全套, R&D 周期失控 | 2026-09-08 某次回归调试 | 多次 |
| 多版本手工迭代 (v1→v9) | 2026-09-12 某课题 | 1 课题 9 版 |
| cascade retry 不评估"是否变好" | 5 策略重试链 (某重试链实现) | 每次 retry |
| stage 间 schema 错位, 末尾神秘失败 | 某 9-stage pipeline + 多 LLM chain | 跨 stage 错误 |
| token 跑飞, 单 stage 烧 50K+ | research_proofread 14K cap, writer 24K | 长 stage |
| 跨 LLM 假共识 (RLHF bias) | 军事/制裁话题 3 LLM 全给政治正确版本 | 高敏感话题 |

### 2.2 v2 新增洞察: 大循环 vs 小循环

| 维度 | 大循环 (v1 设计) | **小循环 (v2 核心)** |
|---|---|---|
| 风险 | 走 N 步才发现方向错, 推倒重来 | 走 1 步就 gate, 错就停 |
| 成本 | 一旦跑飞, 后期 token 浪费 | 单 stage 失控可立即 abort |
| 收敛性 | 全局评估困难 | 单 stage 收敛即过 |
| 可调试 | 错在哪步难定位 | 错在哪 stage 一目了然 |
| 方法论 | 跨 stage 假设漂移 | stage 间边界硬约束 |
| **失败 1 次成本** | **N × token (全重跑)** | **1 × token (只重该 stage)** |

**结论**: **每 stage 独立小循环** 是 driver 的核心操作模式, 不是优化项。

### 2.3 现状: pavoz v0.3.0rc1

**已有** (规划需要的能力基本都在):
- DAG + topo sort + cycle 检测
- Runtime: run / resume / fork_run / run_stage (单 stage 重放)
- Checkpoint: state + deltas + producers + stage_ts
- State schema 校验 (`deep_validate_state`)
- Storage 抽象 (FileStorage)
- Caller injection (`ctx.call`, `CallMeta` 四元组)
- **事件钩子 `on_event`** (每 Runtime 注入) + `_emit` (带 observer 异常隔离)
- 进度心跳 + 取消协作

**v3 修正**: v2 把 "Hook 机制" 与 "API 版本声明" 列为缺失 — 前者**是事实错误** (`on_event` 已 ship), 后者经评审**不采用** (见 §5)。

---

## 3. 设计原则 (6 条)

1. **核心 0 改动** — 能不做就不做; "不需要改" 本身就是最好的结果 (v3 修正: 原为 "核心 API 改 ≤ 20 行")
2. **显式扩展点** — 核心文档说清扩展面: stage fn = 任意协程 / `on_event` 观察 / Protocol 注入 / 异常契约
3. **版本独立** — 扩展锁主版本区间 `pavoz>=0.3,<0.4` (v3 修正: 原写 0.9 与实际 0.3.0rc1 矛盾)
4. **失败优雅降级** — 扩展加载失败不污染核心
5. **路径一致性** — 扩展用核心已有的异常契约, 不引入新异常根
6. **per-stage 小循环** — 每 stage 独立 worker + reviewer 收敛, 不搞大循环

---

## 4. 主功能 vs 扩展 (决策表, v3 修正)

| 能力 | 归属 | 理由 |
|---|---|---|
| ~~Hook 机制 (`runtime_hook`)~~ | **否决** | 已有 `on_event` (`runtime.py:162`); 全局注册表会同进程互相泄漏 |
| ~~`ctx._meta` dict~~ | **否决** | Ctx 在重试循环内构造 (`runtime.py:596`), 每 attempt 重建 → 记账被重试绕过; 归 caller |
| ~~`__pavoz_api_version__`~~ | **否决** | `__version__` 已存在且与 tag 同步; 第二个版本号 = 第二个漂移源 |
| Quality gate (stage 后置评分 + 收敛) | **扩展仓 (gate)** | 小循环执行机制; 纯装饰器 (`dag.py:58` 接任意 NodeFn) |
| Schema validation (跨 stage 契约) | **扩展仓 (schema)** | 通用痛点; 自带 pydantic 依赖 (符合"核心零依赖、扩展自带"哲学) |
| Cost cap (token 预算) | **扩展仓 (cost_cap)**, 改造后 | 记账只能由 caller 做; 扩展只给预算对象 + 检查函数 + 异常类型 |
| Conditional stage (运行时 skip) | **降级为 example** | 一行 `if` 即可; `return {}` 的跳过语义下游无法区分"跳过"vs"没产出" |
| Per-stage reviewer 调度 | **驱动层 (driver)** | 1 driver / use case, 不进库 |

**判据**: **核心 = 编排本质** (已齐); **扩展 = 小循环基础设施** (独立仓); **驱动 = 应用层调度** (各项目自己写)。

---

## 5. 核心改动: 0 行 (v3 评审结论)

v2 §5 主张"核心加 2 个最小钩子"。评审否决, 因为两者都已有等价能力或更差:

| v2 主张 | 实际 | 出处 |
|---|---|---|
| `runtime_hook` 全局装饰器注册表 | **已有** `on_event` (每 Runtime 注入) + `_emit` 自带异常隔离 | `runtime.py:162` / `:166-173` |
| `ctx._meta` dict | Ctx 在 `_run_stage` 重试循环**内**构造 → 每 attempt 全新 → 记账被重试绕过 | `runtime.py:596` |

**为什么全局注册表比 `on_event` 更差**:
1. 同进程跑 ai-write + ai-research 两个 Runtime 时, 全局注册表会把 A 的 hook 泄漏给 B 的 run; 注入式天然隔离
2. `fn(**data)` 把事件 payload 的字段名变成隐式 API — payload 加字段/改名即破坏所有 hook; `on_event(event, data)` 用 dict 传参, 可演进

**`ctx._meta` 的替代路径 (今天就能用)**: 需要 run 级共享状态的是 **caller** — `Runtime(caller=fn)` 注入的闭包天然持有 run 级作用域, `CallMeta` 已带 `task_id / run_id / stage / attempt` 四元组, 按 `task_id` 累计即可。

> 若将来真出现硬需求, 正确形状是 run 级 (跨 attempt 存活)、由 Runtime 创建、显式声明"非契约、不落 checkpoint"的 `ctx.scratch` — 但请先带具体用例, 不要先加字段。

---

## 6. 扩展 (独立仓 `pavoz-extensions`)

### 6.1 仓结构

```
pavoz-extensions/                  # 独立仓 (非 pavoz 单仓)
├── pyproject.toml                 # 依赖: pavoz>=0.3,<0.4
├── README.md
├── pavoz_extensions/
│   ├── __init__.py
│   ├── gate.py                    # 小循环: stage 后置评分 + 收敛
│   ├── schema.py                  # 小循环边界: 跨 stage 类型契约
│   └── cost_cap.py                # 小循环成本: 预算对象 + 检查函数
└── tests/
```

`@conditional` 不在发行版内 — 降级为 `examples/` 示例 (见 §6.3)。

### 6.2 `@gate` (小循环执行机制, v3 修正契约)

```python
@gate(reviewers=[...], revise=..., threshold=7.0, max_iter=3, on_exhaustion="raise")
async def s_review(ctx): ...
```

**契约 (v3 定死)**:

| 项 | 定义 |
|---|---|
| `reviewers` | `Sequence[Callable[[ctx, draft], Awaitable[tuple[float, str]]]]` → `(score, critique)`。**普通 callable, 不是 LLM 专属** — 核心库不知道 LLM 是什么 |
| `revise` | `Callable[[ctx, draft, critique], Awaitable[draft]]` — 把批评喂回重做 |
| 聚合 | **`min`** (最严 lens 一票否决), 不是平均 — 平均会让宽松 lens 掩盖严苛 lens |
| 收敛 | `score >= threshold` → 返回当次 (或历史最高分那次) |
| 耗尽 | `on_exhaustion="raise"` (**默认**) 抛 **`StageError`** (终态, 不可重试) / `"best_effort"` 返最高分那次 |
| 成本 | `1 + max_iter × (|reviewers| + 1)` 次 `ctx.call` (末轮不 revise) |

**v2 的三个洞 (已修)**:
1. **成本乘法** — v2 耗尽抛 `RetryableError` → pavoz `dag.retries` 再跑整轮 → 最坏 `max_iter × (retries+1)`。**修**: 耗尽抛 `StageError` (不可重试) + gate stage 用 `retries=0`; 瞬时错误 (429/5xx) 由 **caller 自己做退避重试**。
2. **耗尽语义自相矛盾** — v2 第 5 步 "raise" 与第 6 步 "`pick_best=True` 返回最高分" 互斥。**修**: 显式参数 `on_exhaustion`, 默认必须 `raise` — "静默返回不达标产物" 正是本规划要消灭的失败模式。
3. **reviewer 隐含必须是 LLM** — **修**: 泛化为普通 callable (见契约表)。

**用法铁律**: `@dag.stage()` 在上、`@gate` 在下 (反了 = gate 白写: `dag.stage` 返回原 fn); 装饰器必须 `functools.wraps` (`dag.py:70` 用 `fn.__name__` 当 stage 名, 否则 duplicate stage name)。

### 6.3 `@conditional` — 降级为 example (v3 修正)

**不进发行版**。理由:
- 功能上, stage 内一行 `if` 就能跳过 — 且那是 pavoz 声明的立场 ("循环留在 stage 函数内用普通 Python 表达")
- 机制上, 现有语义是 "跳过 = `return {}`", **下游无法区分"跳过"和"跑了但没产出"**; 而 `depends_on` 是静态声明的数据契约 — 静默跳过会重演 §2.1 自己列的 "stage 间 schema 错位" 痛点
- 要保留就必须在 **DAG 构建期**校验 (被跳过的 stage 不得被依赖, 或依赖方显式声明容忍缺失) + 把 skip 写进 delta 的显式标记 — 10 行装饰器变成一个有校验面的子系统, 收益不值

### 6.4 `@schema` (小循环边界)

```python
@schema(input=WriterDraft, output=AuditorFindings)
async def s_audit(ctx): ...
```

- 轻量泛化: 接受任意 `Callable[[Any], None]`; pydantic 只是文档里的例子 (不用 pydantic 的项目也能用)
- 失败抛 `SchemaValidationError(StageError)` — 终态不重试
- 带 `warn_only` 模式 (渐进收紧, 防一上来就频繁 fail)

### 6.5 `@cost_cap` (小循环成本, v3 修正形状)

**硬事实**: 只有 caller 知道 token 数 (它才是发 HTTP 的那层)。扩展无法对不归它管的 caller 施加约束。

**修正后的形状**: 扩展只提供 **预算对象 + 检查函数 + 异常类型**, 由应用在 caller 里接线。

```python
# 应用侧 caller 里 (不是扩展里):
budget = CostBudget(limit=50_000)
...
budget.add(usage["total_tokens"])        # 按 CallMeta.task_id 累计, 不塞 ctx._meta
if budget.exceeded():
    raise CostCapExceeded(...)            # 默认 StageError 语义: 终态
```

**v2 的两处修正**:
1. **记账位置** — 归 **caller** (按 `CallMeta.task_id`), 不是 `ctx._meta` (Ctx 每 attempt 重建, `runtime.py:596`)
2. **异常语义** — 超预算默认 **`StageError` (终态)**, 不是 `RetryableError`: 超预算再重试通常是在烧更多钱, 那正是要防的。要"给 gate 一次机会"可以做可选参数, 不能是默认
3. 删掉 v2 的 "±10% buffer" — provider 返回的 usage 是结算口径, 有实测就用实测; buffer 是掩盖口径混乱

---

## 7. per-stage 小循环模式 (核心方法论)

### 7.1 通用模板

```
每 stage:
  loop N 次 (max_iter=3):
    1. worker 产出 artifact
    2. 派 2-3 个 reviewer (不同 lens, 串行或并发)
    3. reviewer 给评分 + 严苛批评
    4. 若 score >= threshold: → return (收敛, 进下一 stage)
    5. 若 score < threshold:  → 把批评喂回 worker → 重做
    6. 若 N 次后仍不通过:     → raise StageError → 小循环 fail (stage 终止)
```

参考实现 (30 行, 纯应用侧) 见 handoff §2。**任一 stage 失败 = 整个小循环重来, 不是全 pipeline 重来。**

### 7.2 为什么每 stage 都独立小循环

| 原因 | 说明 |
|---|---|
| **风险隔离** | 1 stage 失败不污染其他 stage |
| **成本控制** | 单 stage 超预算立即 abort, 不烧光全局预算 |
| **收敛明确** | 单 stage 收敛即过, 不需全局评估 |
| **可调试** | 错在哪 stage 一目了然, 不需要全局 rerun |
| **方法论锁定** | 跨 LLM audit / 引用溯源 / 敏感度检查等不是"约定", 是小循环强制 |
| **R&D 快闭环** | 单 stage 重放 + reviewer 反馈, 不需要全跑 |

> v2 表格里的 "方法论锁定" 一项属于**应用层规范**, 不进 pavoz。

### 7.3 8 stage 规格表 → 已移出

**v3 修正**: 该表是应用层内容 (某政策研究课题的具体 stage / lens / 阈值 / token 预算), 对通用用户无意义, 与 pavoz "独立通用编排库" 定位不符。

→ 已移至: `ai-research/docs/design/stage-spec-policy-research-2026-09-13.md`

---

## 8. 类似案例对比 (6 个)

| 框架 | 核心 | 扩展 | 小循环机制 | 命名约定 |
|---|---|---|---|---|
| **Apache Airflow** | `apache-airflow` | `apache-airflow-providers-*` | 原生 `on_success/failure/retry_callback` + `airflow.listeners` | `provider` |
| **pytest** | `pytest` | `pytest-*` | hookspec (入口点发现) | `pytest-{tool}` |
| **Django** | `django` | `django.contrib.*` | `check` framework | `contrib` |
| **LangChain** | `langchain-core` | `langchain-community` / `langchain-experimental` | callback system | 三层架构 |
| **Kubernetes** | core API | Operators / Helm | admission controllers | 各家自定 |
| **敏捷 Scrum** | team | — | per-sprint review / retro | — |

**共同点 (6/6)**: 核心稳定扩展热插拔 / 显式扩展点 / 版本独立 / 有小循环机制 / 失败优雅降级。

**案例表反而支持现状 (v3 修正)**: 主流框架提供的是**"事件出口"**, 循环由插件/用户实现 — 这正是 `on_event` 的形态。且 pavoz `CLAUDE.md` 明确 "❌ 不做 plugin/entry_points 强制注册", 与 pytest 的 hookspec 路线本就不同。

**本规划对标**:
- 命名: `pavoz-extensions` (独立仓) — 类似 Airflow providers
- 扩展点: `on_event` 观察 + stage fn = 任意协程 — 类似 Airflow callback
- 小循环: gate / schema / cost_cap 3 机制 — 类似 Django check framework
- 异常契约: 继承核心异常 — 类似 Airflow operators 继承 BaseOperator

---

## 9. 优势

| # | 优势 | 说明 |
|---|---|---|
| 1 | **cascade 显式化** | gate 强制评分, 静默死变显式 |
| 2 | **stage 契约安全** | schema 防跨 stage 污染 |
| 3 | **成本可控** | cost_cap 阻断跑飞 |
| 4 | **R&D 快闭环** | 单 stage 重放 (`run_stage`) + gate 评分历史 |
| 5 | **方法论编码** | 跨 LLM audit / 引用溯源 / 成本控制从"约定"变"代码" |
| 6 | **第三方可贡献** | 任何人可发自己的 `pavoz-xxx`, 无需中心化审批 |
| 7 | **核心不胖** | pavoz 保持 zero dep; 应用层需求不污染 |
| 8 | **大循环→小循环** | 失败成本 N → 1, 收敛性 + 可调试性大幅提升 |
| 9 | **失败隔离** | 单 stage 失败不污染其他 stage, R&D 锁定 |
| 10 | **零版本风险** | 核心 0 改动 = 不用等 pavoz 发版 = 第三方可复制 |

> **诚实的归因 (v3)**: 优势 4 / 8 / 9 来自**已 ship 的 `run_stage` / `fork_run` + 事件流**, 不是本次新增需求。

---

## 10. 风险

| # | 风险 | 缓解 |
|---|---|---|
| 1 | **扩展只活在公开 API 上吗?** (同仓时能偷 import 内部符号, 验证无效) | 独立仓 + CI 结构检查: 只允许 `from pavoz import ...` 顶层导入 |
| 2 | **扩展版本漂移** (与核心不兼容) | 锁 `pavoz>=0.3,<0.4` + CI 跑 "最低支持版本 × 最新版本" 矩阵 |
| 3 | **schema 误用** (过严频繁 fail / 过松失去保护) | `warn_only` 模式 + 渐进收紧 |
| 4 | **cost_cap 记账口径** (provider usage 与实际不符) | caller 层用实测 usage; **不加 buffer** (buffer 是掩盖口径混乱) |
| 5 | **小循环 reviewer 失控** (reviewer 也跑飞, 互相 feeding 错误) | reviewer 与 worker 不同模型/不同 prompt (应用侧约束, pavoz 无法也无需保证) |
| 6 | **`max_iter × dag.retries` 乘法** (v2 未识别) | gate 耗尽抛 `StageError` + gate stage `retries=0`; 成本上界写进文档 |

---

## 11. 规范与约定

1. **异常**: 扩展用核心异常根 (`StageError` / `RetryableError` / `FatalError`), 子类继承, 不引入新根
2. **依赖**: 扩展自带依赖 (如 pydantic), 不污染核心零依赖承诺
3. **版本**: `pavoz-extensions` 锁 `pavoz>=0.3,<0.4`; **不新增 `__pavoz_api_version__` 常量** (v3 修正 — `__version__` 已存在, 第二个版本号 = 第二个漂移源)
4. **配置**: 扩展 config 走单独 namespace (`pavoz_extensions.gate.threshold`)
5. **日志**: 扩展 logger 用 `pavoz_extensions.{module}` 命名空间
6. **测试**: 每个扩展独立 test suite + 集成测试 (跑在真实 pavoz DAG 上)
7. **导入纪律**: 扩展只允许 `from pavoz import ...` (顶层公开 API); 禁止 `pavoz.runtime` / `pavoz.types` 等子模块 — CI 拦
8. **小循环 reviewer 独立性**: reviewer 与 worker 用不同 model 或不同 prompt, 避免相互 feeding 错误
9. **小循环 max_iter = 3**: 单 stage 超 3 次迭代立即 fail, 防 reviewer ↔ worker 死循环

---

## 12. 决策答复 (2026-09-13 已定, 原为"待拍板")

| # | 决策 | 答复 |
|---|---|---|
| 1 | 接受 `runtime_hook`? | **不接受**。用现有 `on_event` (每 Runtime 注入, `runtime.py:162`) |
| 2 | 接受 `ctx._meta`? | **不接受 (现阶段)**。记账归 caller (`CallMeta` 四元组); 真需硬需求时另提 run 级 `ctx.scratch` 用例 |
| 3 | 扩展包命名 / 位置? | **接受 `pavoz-extensions`**, 但**独立仓** (仓已建: `liyong-labs/pavoz-extensions`), 核心仓不放 |
| 4 | release 策略? | **0.x 期间按需 release** (半年一发 = 扩展等半年才拿修复); 兼容 = `pavoz>=0.3,<0.4` + CI 矩阵 |
| 5 | 合并入主仓 / 先单仓 3 个月? | **改: 一开始就分仓**。同仓时扩展能偷 import 内部符号, 验证结果无效; 3 个月后拆仓还要重写 git 历史 / CI / 流水线 |

---

## 13. 实施时间线 (v3 修正)

| 阶段 | 工作 | 时间 |
|---|---|---|
| D1 | pavoz 核心: **0 改动**; 写 `docs/design/extension-points.md` (扩展面: stage fn / `on_event` / Protocol 注入 / 异常契约) | 0.5 天 |
| D1 | `examples/quality_loop.py` — 纯 stdlib 可跑的小循环示例 (假 scorer, 不依赖 LLM/网络) | 0.5 天 |
| D2–4 | 独立仓 `pavoz-extensions`: `@gate` (契约修好后) + `@schema` | 3 天 |
| D5 | `@cost_cap` 按 §6.5 改造; `@conditional` 降级为 example | 1 天 |
| D6–7 | 应用侧接入 (ai-research / ai-write / 政策课题) | 2 天 |
| **合计** | **核心 0 行** | **1–2 周** |

---

## 14. 验收标准 (v3 替换)

- [ ] 扩展单测覆盖率达标 (每个扩展独立 suite)
- [ ] `examples/quality_loop.py` **纯 stdlib 可跑** (假 scorer, 不依赖任何 LLM/网络)
- [ ] CI 矩阵通过 (最低支持 pavoz 版本 × 最新版本)
- [ ] **CI 结构检查通过**: 扩展只允许 `from pavoz import ...` 顶层导入, 禁止 `pavoz.runtime` / `pavoz.types` 等子模块
- [ ] **他证**: 发布后由第三方只读公开文档写一个不在首发范围里的新扩展 (如 webhook 通知), 卡点回补文档
- [ ] 对核心现有 **38+ tests 零回归** (`git diff pavoz/` 为空即可证明)

> **v3 删除**: v2 的 "集成测试: 反向用例跑通, 对比手工 v9 产物差异 ≤20%" — **不可度量** (文章文本没有定义 distance, 没人能判定 20% 是什么)。

---

## 15. 落地映射 → 已移出

**v3 修正**: 该表 (extensions × 各项目 stage) 是应用层内容。

→ 已移至: `ai-research/docs/design/stage-spec-policy-research-2026-09-13.md`

---

## 16. 决策状态

**已答复 (2026-09-13)**, 详见 §12。

| # | 原请求 | 状态 |
|---|---|---|
| 1 | 接受 `runtime_hook`? (核心 ~30 行) | ✅ 已答复: 不接受 (已有 `on_event`) |
| 2 | 接受 `ctx._meta`? (核心 ~5 行) | ✅ 已答复: 不接受 (记账归 caller) |
| 3 | 扩展包命名 `pavoz-extensions`? | ✅ 已答复: 接受, 独立仓 |
| 4 | 版本策略? | ✅ 已答复: 0.x 按需 release, `pavoz>=0.3,<0.4` |
| 5 | 核心加 hook / 先单仓 3 个月? | ✅ 已答复: 核心 0 改动 + 一开始就分仓 |

**核心改动量: 0 行。** 不阻塞任何一方落地。

---

## 附录 A: 与现有 pavoz v0.3.0rc1 兼容性

- **核心 0 改动** — `pavoz/` 目录一行不动 (v3 修正: v2 曾写"新增 2 个字段 + 1 个新模块")
- 现有 API 不变 (DAG / Runtime / Checkpoint / State 全保留)
- 现有 4 个示例 (demo / llm_pipeline / resume_after_crash / agent_loop) 全兼容
- 零迁移成本

## 附录 B: 引用

- pavoz CLAUDE.md
- `docs/review-pavoz-upgrade-plan-2026-09-13.md` (评审)
- `docs/design/handoff-upgrade-plan-v2-2026-09-13.md` (handoff + 30 行配方)
- ai-research 某 9-stage pipeline
- ai-write research_* pipeline (10+ stage)
- 某政策研究课题 (8 stage)
- 该课题审题 v3 → v4 迭代 (17 条 P0 改动, 3 reviewer 评分 3.7/10 → 收敛)
- Apache Airflow providers / pytest plugins / Django contrib+checks / LangChain 三层+callbacks / Kubernetes operators+admission controllers / 敏捷 Scrum Sprint review
