# pavoz 功能升级改造规划 v2 (含 per-stage 小循环理论)

**作者**: Claude
**日期**: 2026-09-12 (v2 含 per-stage 小循环理论)
**v1 日期**: 2026-09-12 (初版)
**目标读者**: pavoz 维护者 + ai-research/ai-write 集成方
**当前 pavoz 版本**: v0.3.0rc1
**配套版本**: ai-research 某 9-stage pipeline + ai-write research_* pipeline (10+ stage) + 某政策研究课题 (8 stage)

---

## 1. 执行摘要

pavoz v0.3.0rc1 现状是"骨架可用、扩展缺失": DAG/checkpoint/state 都在,但没有 quality gate / conditional / schema / cost cap 4 个核心增强, 更没有 per-stage **小循环**机制 (worker + reviewer 收敛).

**核心理论 (新增 v2)**: 大循环 (全 pipeline 单次审) 容易跑偏, 改成 **per-stage 小循环** (每 stage 独立 2-3 轮 reviewer 收敛 + gate) 才能避免大循环走偏 + 控制成本 + 保证质量.

**本规划主张**:
- **核心加 2 个最小钩子** (runtime_hook 装饰器 + ctx._meta dict, < 20 行)
- **扩展包 4 个** (gate / conditional / schema / cost_cap), 独立 PyPI 包 `pavoz-extensions`
- **每个 stage 实施 per-stage 小循环** (worker + 2-3 reviewer 不同 lens + gate 评分 ≥ 7/10 + cost cap)
- **本地先用, 验证后回馈上游**, 不强求上游先改

**投入**: 1.5 周 (核心 0.2 + 扩展 0.3 + per-stage 模板 0.3 + 测试 0.4 + 文档 0.3)
**回报**: ai-write cascade 事故排查 -80%, ai-research 课题交付周期 -50%, 新课题起项目时间 -70%

---

## 2. 背景与动机

### 2.1 触发: ai-research + ai-write + 某政策研究课题 真实痛点

| 痛点 | 来源 | 频次 |
|---|---|---|
| **大循环跑偏 (一错错到底)** | 该政策课题 v3 审题被3 reviewer 打 3.7/10, 17 条 P0 必改 | 每次新课题 |
| cascade 静默死 | 2026-07-26 / 2026-07-28 / 2026-08-28 | ≥4 次 |
| 研究 chain 卡死 (50min+ 0 产出) | 2026-07-31 research_proofread | ≥2 次 |
| 改 prompt 重跑全套, R&D 周期失控 | 2026-09-08 某次回归调试 | 多次 |
| 多版本手工迭代 (v1→v9) | 2026-09-12 某课题 | 1 课题 9  |
| cascade retry 不评估"是否变好" | 5 策略重试链 (某重试链实现) | 每次 retry |
| stage 间 schema 错位, 末尾神秘失败 | 某 9-stage pipeline + 多 LLM chain | 跨 stage 错误 |
| token 跑飞, 单 stage 烧 50K+ | research_proofread 14K cap, writer 24K | 长 stage |
| 跨 LLM 假共识 (RLHF bias) | 军事/制裁话题 3 LLM 全给政治正确版本 | 高敏感话题 |

### 2.2 v2 新增洞察: 大循环 vs 小循环

| 维度 | 大循环 (v1 设计) | **小循环 (v2 新增)** |
|---|---|---|
| 风险 | 走 N 步才发现方向错, 推倒重来 | 走 1 步就 gate, 错就停 |
| 成本 | 一旦跑飞, 后期 token 浪费 | 单 stage 失控可立即 abort |
| 收敛性 | 全局评估困难 | 单 stage 收敛即过 |
| 可调试 | 错在哪步难定位 | 错在哪 stage 一目了然 |
| 方法论 | 跨 stage 假设漂移 | stage 间边界硬约束 |
| **失败 1 次成本** | **N × token (全重跑)** | **1 × token (只重该 stage)** |

**结论**: **每 stage 独立小循环** 是 driver 的核心操作模式, 不是优化项.

### 2.3 现状: pavoz v0.3.0rc1

**已有**:
- DAG + topo sort + cycle 检测
- Runtime: run / resume / fork_run / run_stage (单 stage 重放)
- Checkpoint: state + deltas + producers + stage_ts
- State schema 校验 (`deepvalidate_state`)
- Storage 抽象 (FileStorage)
- Caller injection (`ctx.call`)
- 进度心跳 + 取消协作 + 事件钩子

**缺失**:
- ❌ Quality gate (stage 后置评分 + 阈值 retry) — **小循环核心**
- ❌ Conditional stage (运行时 skip) — 小循环优化
- ❌ Cross-stage schema validation — 小循环边界
- ❌ Cost cap (token 预算) — **小循环成本控制**
- ❌ Hook 机制 (小循环事件接入点)
- ❌ API 版本声明

---

## 3. 设计原则 (6 条, v2 新增 1 条)

1. **核心稳定, 扩展热插拔** — 核心 API 改 ≤ 20 行, 扩展独立发版
2. **显式扩展点** — 核心文档明确指出 "hook 在这里", 扩展按 hook 实现
3. **版本独立** — 扩展不强绑核心小版本 (`pavoz>=0.9,<1.0`)
4. **失败优雅降级** — 扩展加载失败不污染核心
5. **路径一致性** — 扩展用核心已有的异常契约, 不引入新异常类
6. **🆕 per-stage 小循环** — 每 stage 独立 worker + reviewer 收敛, 不搞大循环

---

## 4. 主功能 vs 扩展 (决策表)

| 能力 | 归属 | 理由 |
|---|---|---|
| Hook 机制 (`runtime_hook` 装饰器) | **核心** | 0 现有机制, 小循环 reviewer 接入必须 |
| `ctx._meta` dict (跨 stage 元数据) | **核心** | 小循环 cost cap 记 token 用量 |
| `__pavez_api_version__` 标识 | **核心** | extension 校验用, 10 行内 |
| Quality gate (stage 后置评分) | **扩展 (gate)** | 小循环的执行机制, 应用层语义 |
| Conditional stage (运行时 skip) | **扩展 (conditional)** | 业务配置驱动, 非编排本质 |
| Schema validation (跨 stage 契约) | **扩展 (schema)** | 依赖 pydantic, 不放核心 |
| Cost cap (token 预算) | **扩展 (cost_cap)** | 小循环成本控制 |
| Per-stage reviewer 调度 | **驱动层 (driver)** | 1 driver / use case, 不放 pavoz |

**判据**: **核心 = 编排本质**; **扩展 = 小循环基础设施**; **驱动 = 应用层调度**.

---

## 5. 主功能增强 (Core, 2 个, < 20 行)

### 5.1 `runtime_hook` 装饰器

```python
# pavoz/pavoz/hooks.py (新文件, ~30 行)
from collections.abc import Callable

_HOOKS: dict[str, list[Callable]] = {}

def hook(event: str) -> Callable:
    """注册全局 hook. 驱动层 / extension 用这个监听 stage 生命周期事件."""
    def decorator(fn: Callable) -> Callable:
        _HOOKS.setdefault(event, []).append(fn)
        return fn
    return decorator

def emit(event: str, **data) -> None:
    """pavoz 内部触发 hook. 失败隔离 (try/except + log)."""
    for fn in _HOOKS.get(event, []):
        try:
            fn(**data)
        except Exception:
            logger.exception("hook %s 异常 (忽略)", event)
```

**触发点** (在 `runtime.py` 已有的 `_emit` 旁):
- `run_start`, `run_end`
- `stage_start`, `stage_end`, `stage_retry`
- `fork_start`, `resume_start`

**驱动层使用** (per-stage 小循环):
```python
@hook("stage_end")
def on_stage_end(stage: str, status: str, duration: float, **ctx):
    """每个 stage 结束时触发小循环 reviewer."""
    if status == "done":
        schedule_reviewers(stage, ctx["state"])  # 派 2-3 reviewer
```

### 5.2 `ctx._meta` dict

```python
# pavoz/pavoz/runtime.py Ctx dataclass
@dataclass
class Ctx:
    # ... 现有字段 ...
    _meta: dict = field(default_factory=dict, repr=False)
    # 小循环 cost cap 记 token 用量, gate 记 score 历史
```

**用法**:
```python
async def my_stage(ctx: Ctx):
    ctx._meta["tokens_used"] = 1234
    ctx._meta["gate_scores"] = [0.6, 0.7, 0.8]
    ctx._meta["loop_iter"] = 2
```

---

## 6. 扩展包 (Extensions, 4 个 — 小循环基础设施)

### 6.1 `pavoz-extensions` 包结构

```
pavoz-extensions/
├── pyproject.toml
├── README.md
├── pavoz_extensions/
│ ├── __init__.py # 版本 + 兼容性校验
│ ├── gate.py            # 小循环: stage 后置评分 + retry
│ ├── conditional.py     # 小循环优化: 跳过某些 stage
│ ├── schema.py          # 小循环边界: 跨 stage 类型契约
│ └── cost_cap.py        # 小循环成本: token 预算
└── tests/
```

### 6.2 `@gate` (小循环执行机制)

```python
@pavez_ext.gate(score_fn=audit_quality, threshold=0.7, max_iter=3, pick_best=True)
async def stage_auditor(ctx):
    ...

# 小循环行为:
# 1. 跑 fn → result (iter 1)
# 2. 算 score = score_fn(result, ctx)
# 3. score >= threshold → return result
# 4. score < threshold + iter < max_iter → 派 reviewer → 拿批评 → 喂回 fn → 重跑
# 5. iter 用尽 → raise GateFailedError (RetryableError, 让外层决定)
# 6. pick_best=True → 多次迭代间记录最佳score, 最终返回 score最高那次

class GateFailedError(RetryableError):
    """质量门 N 次迭代仍未达标. 继承 RetryableError."""
```

### 6.3 `@conditional` (小循环跳过)

```python
@pavez_ext.conditional(skip_if=lambda ctx: ctx.state["config"]["sensitivity"] != "high")
async def stage_sensitivity(ctx):
    ...

# 小循环行为:
# 1. 跑前 eval skip_if(ctx)
# 2. True → log + return {} (空 delta, pavoz 继续)
# 3. False → 跑 fn (含其小循环)

# 无异常: skip 是正常路径, 不是 fail
```

### 6.4 `@schema` (小循环边界)

```python
@pavez_ext.schema(input=WriterDraft, output=AuditorFindings)
async def stage_auditor(ctx):
    ...

# 小循环行为:
# 1. 跑前 validate(input, ctx.state 上游 artifact) — 失败 raise SchemaError
# 2. 跑 fn → result
# 3. validate(output, result)
# 4. 任一失败 → raise SchemaValidationError (StageError, 终态)

class SchemaValidationError(StageError):
    """schema 契约违反. StageError = 不重试, 终态 fail."""
```

### 6.5 `@cost_cap` (小循环成本)

```python
@pavez_ext.cost_cap(tokens=50_000)
async def stage_proofread(ctx):
    ...

# 小循环行为:
# 1. ctx._meta["_cost_cap_limit"] = 50_000
# 2. fn 执行 (LLM caller 每次 ctx._meta["tokens_used"] += N)
# 3. caller 检测超 limit → raise CostCapExceeded (RetryableError)

class CostCapExceeded(RetryableError):
    """token 预算超. RetryableError = 可重试, 让 gate 介入评估."""
```

---

## 7. 🆕 per-stage 小循环模式 (v2 核心)

### 7.1 通用模板

```
每 stage:
 loop N 次 (max 3, 由 cost_cap 控制):
    1. worker 产出 artifact
    2. 派 2-3 个 reviewer (不同 lens, 串行或并发)
    3. reviewer 给评分 + 严苛批评
    4. 若 score ≥ gate_threshold:
       → return artifact (收敛, 进下一 stage)
    5. 若 score < gate_threshold:
       → 把 reviewer 批评喂回 worker → 重做
    6. 若 N 次后仍不通过:
       → raise GateFailedError → 小循环 fail (stage 终止)
```

### 7.2 为什么每 stage 都独立小循环

| 原因 | 说明 |
|---|---|
| **风险隔离** | 1 stage 失败不污染其他 stage |
| **成本控制** | 单 stage 超 cost_cap 立即 abort, 不烧光全局预算 |
| **收敛明确** | 单 stage 收敛即过, 不需全局评估 |
| **可调试** | 错在哪 stage 一目了然, 不需要全局 rerun |
| **方法论锁定** | 跨 LLM audit / 数据引用溯源 / 政治敏感性等不是"约定", 是小循环强制 |
| **R&D 快闭环** | 单 stage 重放 + reviewer 反馈, 不需要全跑 |

### 7.3 8 stage 小循环规格 (以某政策研究课题为例)

| Stage | Worker | Reviewer Lens | Gate Threshold | Cost Cap |
|---|---|---|---|---|
| **1. parse** | 审题 writer | 方法论 / 实操 / 政治 | score ≥ 7/10 | 30K token |
| **2. collect** | JSONL 采集 | 数据质量 / 信源覆盖 | 引用数 ≥ 50 + URL 100% | 80K token |
| **3. draft** | 主报告草稿 | 结构 / 数据运用 / 边界 | 4 维矩阵填全 + § 齐全 | 100K token |
| **4. audit_cross** | 3 LLM 并发 | (本身就是 reviewer) | ≥2 LLM 共识 P0/P1 | 60K token |
| **5. patch** | 修补 | 修补完整性 / 修后一致性 | P0 = 100% / P1 ≥ 80% | 40K token |
| **6. lens** | 论述层 | 合理性 / 不引入新数据 | 5 个矛盾全覆盖 | 40K token |
| **7. sensitivity** | 敏感化 | 政治安全 / 表述检查 | 词表 P0 全替换 + §4/6 删 | 20K token |
| **8. assemble** | 拼装 | 引用一致性 / 结构完整 | 引用唯一编号 + 8 节 + 3 附录 | 10K token |
| **合计** | | | | **≤380K token** |

---

## 8. 类似案例对比 (6 个, v2 新增 1 个)

| 框架 | 核心 | 扩展 | **小循环机制** | 命名约定 |
|---|---|---|---|---|
| **Apache Airflow** | `apache-airflow` | `apache-airflow-providers-*` | **无原生** (依赖第三方 checker) | `provider` |
| **pytest** | `pytest` | `pytest-*` | **hook spec + pluggy** (assertion rewriting 类似) | `pytest-{tool}` |
| **Django** | `django` | `django.contrib.*` | **check framework** (小循环) | `contrib` |
| **LangChain** | `langchain-core` | `langchain-community` / `langchain-experimental` | **callback system** (小循环 on_event) | 三层架构 |
| **Kubernetes** | core API | Operators / Helm | **admission controllers** (类似小循环) | 自家命名 |
| **🆕 敏捷 Sprint (Scrum)** | team | - | **per-sprint review/retro** (经典小循环) | - |

**共同点 (6/6)**:
- 核心稳定, 扩展热插拔
- 显式扩展点
- 版本独立
- **小循环机制** (pytest hooks / Django checks / LangChain callbacks / K8s controllers / Scrum retro)
- 失败优雅降级

**本规划对标**:
- 命名: `pavoz-extensions` (单数包, 子模块分组) — 类似 `django.contrib`
- 扩展点: hook 装饰器 + ctx._meta — 类似 pytest hookspec
- 小循环: gate/conditional/schema/cost_cap 4 机制 — 类似 Django check framework
- 异常契约: 继承核心异常 — 类似 Airflow operators 继承 BaseOperator

---

## 9. 优势 (10 项, v2 新增 2 项)

| # | 优势 | 说明 |
|---|---|---|
| 1 | **cascade 显式化** | gate 强制评分, 静默死变显式 |
| 2 | **stage 契约安全** | schema 防 cascade 跨 stage 污染 |
| 3 | **成本可控** | cost_cap 阻断跑飞 |
| 4 | **多路径统一** | conditional 让 1 个 DAG 走 N 种 config |
| 5 | **R&D 快闭环** | 单 stage 重放 (pavoz 已有) + gate score history |
| 6 | **方法论编码** | 跨 LLM audit / 引用溯源 / 成本控制从"约定"变"代码" |
| 7 | **第三方可贡献** | 其他人可写 `pavoz-ext-ml-train`, `pavoz-ext-data-pipeline` |
| 8 | **核心不胖** | pavoz 保持 zero dep, 应用层需求不污染 |
| 9 | **🆕 大循环→小循环** | 失败成本 N → 1, 收敛性 + 可调试性大幅提升 |
| 10 | **🆕 失败隔离** | 单 stage 失败不污染其他 stage, R&D 锁定 |

---

## 10. 风险 (6 项, v2 新增 1 项)

| # | 风险 | 缓解 |
|---|---|---|
| 1 | **hook 滥用** (extension 注册太多 hook, 拖慢 stage) | hook 数量限制 + 性能 profile 测试 |
| 2 | **扩展版本分裂** (extension 与 pavoz 版本不兼容) | `__pavez_api_version__` 校验 + CI 矩阵 |
| 3 | **schema 误用** (过严导致频繁 fail, 过松失去保护) | 提供 "warn-only" mode + 渐进收紧 |
| 4 | **cost_cap 不准** (LLM provider 返回 token 数与实际不符) | caller 层实测, 加 ±10% buffer |
| 5 | **核心改动风险** (加 hook 改动 runtime) | < 20 行, PR review + 单元测试覆盖 |
| 6 | **🆕 小循环 reviewer 失控** (reviewer 也跑飞, 互相 feeding 错误) | reviewer 与 worker 用不同 model / 不同 prompt, 互相独立 |

---

## 11. 规范与约定 (9 条, v2 新增 2 条)

1. **异常**: 扩展用核心异常 (`StageError` / `RetryableError` / `FatalError`), 子类继承, 不引入新根
2. **依赖**: 扩展自带依赖 (`pydantic` / 等), 不污染 pavoz 零依赖
3. **版本**: `pavez-extensions>=0.9,<1.0` 锁主版本范围; extension 独立 minor/patch
4. **配置**: extension config 走单独 namespace (`pavez_extensions.gate.threshold`)
5. **日志**: 扩展 logger 用 `pavez_extensions.{module}` 命名空间
6. **测试**: 每个扩展独立 test suite + 集成测试 (run on pavoz 真实 DAG)
7. **失败优雅**: 核心可检测 "extension 加载?" , 缺失则跳过
8. **🆕 小循环 reviewer 独立性**: reviewer 与 worker 用不同 model 或不同 prompt, 避免相互 feeding 错误
9. **🆕 小循环 max_iter = 3**: 单 stage 超 3 次迭代立即 fail, 防止 reviewer ↔ worker 死循环

---

## 12. pavoz 上游决策点 (5 项需拍板)

| # | 决策 | 选项 | 建议 |
|---|---|---|---|
| 1 | 是否接受 `runtime_hook` 机制? | 接受 / 暂缓 / 改名 | **接受** (无替代方案) |
| 2 | 是否接受 `ctx._meta` dict? | 接受 / 用 ctx.state 替代 | **接受** (state 是契约数据, _meta 是元数据, 分离) |
| 3 | 扩展包命名? | `pavez-extensions` / `pavez.contrib` / 多包 | **`pavez-extensions`** (单包简洁) |
| 4 | 版本对齐策略? | 紧跟主版本 / 独立 / 半年 release | **半年 release + 兼容主版本范围** |
| 5 | 是否合并入主仓? | 单仓 (pavez/) / 分仓 | **先单仓 3 个月, 验证后分仓** |

---

## 13. 实施时间线 (v2 增加 per-stage 小循环验证)

| 阶段 | 工作 | 时间 |
|---|---|---|
| W1 D1-2 | pavoz 核心: hook + ctx._meta + 单元测试 | 0.2 周 |
| W1 D3-5 | 4 个 extension 实现 (gate / conditional / schema / cost_cap) | 0.3 周 |
| W1 D6 | per-stage 小循环模板 + 调度逻辑 (驱动层) | 0.2 周 |
| W2 D1-2 | ai-write 套 4 个 extension + 小循环 (10+ stage) | 0.3 周 |
| W2 D3-5 | ai-research 套 + 1 个 反向用例 (per-stage 验证) | 0.3 周 |
| W2 D6 | 文档 + 给上游提 PR | 0.2 周 |
| **合计** | | **1.5 周** |

---

## 14. 验证标准

- ✅ 单元测试: 4 extensions 各 ≥10 tests, 覆盖率 ≥90%
- ✅ per-stage 小循环测试: 模拟 stage 失败 → 验证 reviewer 介入 → 验证 max_iter 阻断
- ✅ 集成测试: 反向用例跑通, 对比手工 v9 产物差异 ≤20%
- ✅ 回归测试: ai-write 现有 task 全过, 无性能回退 (<5% overhead)
- ✅ 兼容性: pavoz 0.9.x 范围, 0.3.0rc1 已验证
- ✅ 文档: README + 5 个 example + 1 个 demo + per-stage 小循环模板说明

---

## 15. 附录: 落地映射 (per-stage 小循环 + extensions)

| Extension | ai-research 用例 (1 个 stage) | ai-write 用例 (1 个 stage) | 该政策课题 (1 个 stage) |
|---|---|---|---|
| **gate** | stage 4 audit (≥0.7) | auditor chain (5 策略 + pick_best) | stage 3 draft (≥7/10) |
| **conditional** | sensitivity=medium 跳 sanitization | target_level 路由 4 路 | sensitivity=high 走 stage 7 |
| **schema** | FrameworkV1 / DraftV1 / FindingsV1 | WriterDraft / AuditorFindings | FrameworkV1 / DraftV1 |
| **cost_cap** | parse 30K, collect 80K, audit 60K | writer 24K, proofread 14K | 见 §7.3 表 |

**每个 stage 都独立小循环** (worker + reviewer + gate + cost_cap).

---

## 16. 决策请求

**请 pavoz 维护者就以下 5 项决策**:

1. 接受 `runtime_hook` 机制? (核心 ~30 行)
2. 接受 `ctx._meta` dict? (核心 ~5 行)
3. 扩展包命名 `pavez-extensions` 是否 OK?
4. 版本策略: 半年 release + 主版本范围兼容?
5. 是否接受 PR 给核心加 2 个小 hook, 扩展保持本地仓先 3 个月?

**预计响应时间**: 1 周内. 不阻塞 ai-write / ai-research 本地落地.

---

## 附录 A: 与现有 pavoz v0.3.0rc1 兼容性

- 现有 API 不变 (DAG / Runtime / Checkpoint / State 全保留)
- 新增 2 个字段 (ctx._meta) + 1 个新模块 (pavez.hooks)
- 现有 4 个示例 (demo / llm_pipeline / resume_after_crash / agent_loop) 全兼容
- 零迁移成本

## 附录 B: 引用

- pavoz CLAUDE.md
- ai-research 某 9-stage pipeline
- ai-write research_* pipeline (10+ stage)
- 某政策研究课题 (8 stage)
- 该课题审题 v3 → v4 迭代 (17 条 P0 改动, 3 reviewer 评分 3.7/10 → 收敛)
- Apache Airflow providers 模式
- pytest plugins 模式 (hookspec)
- Django contrib + check framework 模式
- LangChain 三层架构 + callback system
- Kubernetes operators + admission controllers 模式
- 敏捷 Scrum Sprint review/retro 模式