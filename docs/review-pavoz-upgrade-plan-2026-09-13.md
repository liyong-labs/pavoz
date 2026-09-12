# 需求评审: pavoz 功能升级改造规划 v2

**评审人**: pavoz 产品经理 / 架构师 (Claude)
**日期**: 2026-09-13
**被评审文档**: `docs/pavoz-upgrade-plan-2026-09-12.md` (v2)
**评审基准**: `pavoz/CLAUDE.md` 定位条款 + `pavoz/__init__.py` 公开 API (v0.3.0rc1) + 代码实际状态

---

## 0. 结论速览

| # | 需求 | 判定 | 一句话理由 |
|---|---|---|---|
| R1 | 核心: `runtime_hook` 全局装饰器注册表 | **拒绝** | 与已有 `on_event` 重复; 全局可变状态违反注入式设计 |
| R2 | 核心: `ctx._meta` dict | **拒绝** | Ctx 每 attempt 重建, 不能承载 run 级记账; 当前有替代路径 |
| R3 | 核心: API 版本标识 | **接受 (降级)** | 改名 `__pavoz_api_version__` + 配一份兼容策略, 非 10 行代码问题 |
| R4 | 扩展: `@gate` | **接受 (需修契约)** | 最高价值扩展; 但 reviewer 契约与耗尽语义必须先定死 |
| R5 | 扩展: `@conditional` | **降级为 example** | 一行 `if` 可替代, 且会削弱 DAG 静态契约 |
| R6 | 扩展: `@schema` | **接受** | 通用痛点, 与 v0.4.1 storage_loader 的"扩展自带 dep"哲学一致 |
| R7 | 扩展: `@cost_cap` | **有条件接受 (改造)** | 记账只能由 caller 做; 扩展只提供预算对象 + 检查, 异常默认终态 |
| R8 | per-stage 小循环方法论 | **接受, 但放在应用侧** | pavoz 原语已支持 (`run_stage` / `fork_run` / stage 内普通 Python); 不改核心 |
| R9 | `pavoz-extensions` 先单仓 3 个月 | **改为分仓** | 分仓才是"扩展点够用"的真实验证; 3 个月后拆仓成本更高 |
| R10 | §7.3 八 stage 规格表 / §15 落地映射 | **移出 pavoz 文档** | 应用层内容; 与 commit 4e211a6 "移除误发布的内部文档" 同一原则 |

**对核心的总改动量: 0 行必需 + 1 行可选 (`__pavoz_api_version__`)。**
这是本次评审最重要的结论 —— 规划里"核心加 2 个最小钩子"是不必要的, 而**不必要本身就是好消息**: 4 个扩展全部可以做成纯装饰器, 零核心改动 = 零版本风险 = 第三方可复制。

---

## 1. 被评审文档的两个事实性错误 (先纠正, 否则后续推论全歪)

### 1.1 "缺失 Hook 机制" —— 不成立, `on_event` 已存在且更完整

`pavoz/runtime.py:98`:

```python
on_event: Callable[[str, dict], None] | None = None  # v0.9: 生命周期事件钩子
```

`pavoz/runtime.py:166-172` 的 `_emit` 已带异常隔离 (规划 §5.1 想实现的 `emit` 语义, 逐字已有):

```python
def _emit(self, event: str, data: dict) -> None:
    """生命周期事件 → on_event 回调. observer 异常隔离 (log + 忽略), 不影响 run."""
```

已发事件集 (规划 §5.1 "触发点" 是它的**子集**):

| 事件 | 位置 |
|---|---|
| `run_start` / `run_end` | runtime.py:287 / 321,336,353 |
| `stage_start` / `stage_end` / `stage_retry` | runtime.py:592 / 584,626,638,662,669 / 652 |
| `stage_progress` (stage 内细粒度心跳) | `Ctx.set_progress` |

规划 §2.3 把 Hook 列为"❌ 缺失", 据此推出"核心必须加 2 个钩子" —— 前提错了。
**pavoz 已有的是"每 Runtime 显式注入的观察者"; 规划要加的是"进程级全局注册表", 后者更差 (见 §2.1)。**

### 1.2 版本号自相矛盾

- 代码实际: `pavoz/__init__.py` → `__version__ = "0.3.0rc1"`
- 规划 §3.3 / §11.3: 扩展锁 `pavoz>=0.9,<1.0`
- 规划 §14: "兼容性: pavoz 0.9.x 范围, 0.3.0rc1 已验证"

0.9 与 0.3.0rc1 不能同时成立。另有命名拼写错误 (全文 `pavez` → 应为 `pavoz`): `pavez-extensions` / `pavez_ext` / `pavez.hooks` / `__pavez_api_version__` / `pavez.contrib`。
这类错误在开源仓库里会被第一批 issue 直接命中, 先修。

---

## 2. 逐项评审

### R1 `runtime_hook` 全局装饰器注册表 — **拒绝**

```python
_HOOKS: dict[str, list[Callable]] = {}      # ← 模块级进程全局可变状态
```

三条理由, 任一条成立即应拒绝:

1. **重复造轮子**。观察能力 `on_event` 已 ship (含异常隔离)。规划未说明新机制能观察什么 `on_event` 观察不到的东西。
2. **违反 pavoz 的注入式设计**。`CLAUDE.md`: "存储/外部调用/任务表全部 Protocol, 由业务侧注入"。同进程跑 ai-write + ai-research 两个 Runtime 时, 全局注册表会把 A 的 hook 泄漏给 B 的 run; 注入式天然隔离。
3. **签名更脆**。`fn(**data)` 把事件 payload 的字段名变成隐式 API —— 将来 payload 加字段/改名即破坏所有 hook; `on_event(event, data)` 用 dict 传参, payload 可演进。

补充一条事实核对: 规划 §8 对标案例里, Airflow **有**原生 `on_success_callback` / `on_failure_callback` / `on_retry_callback` + `airflow.listeners`; pytest 的 hookspec 依赖 entry_points 发现插件 —— 而 pavoz `CLAUDE.md` 明确写了"❌ 不做 plugin/entry_points 强制注册"。**案例表反而支持现状**: 主流框架提供的是"事件出口", 循环由插件/用户实现, 这正是 `on_event` 的形态。

> 若驱动层确实想要装饰器的书写便利, 15 行写在应用侧或 `pavoz_extensions` 里即可 (内部包一层 `on_event`), 不必进核心。

### R2 `ctx._meta` dict — **拒绝 (现阶段)**

`pavoz/runtime.py:596`: `Ctx` 在 `_run_stage` 的 `while True` 重试循环**内部**构造 —— 即 **每 attempt 一个全新 Ctx**。后果:

| 规划的用途 | 实际后果 |
|---|---|
| "cost cap 记 token 用量" | RetryableError 一重试, 计数归零 → 预算被重试绕过; 而"cascade retry 不评估是否变好"正是规划 §2.1 要修的头号痛点 |
| "gate 记 score 历史" | gate 的重试发生在 stage 函数**内部** (同一 attempt), 局部变量即可, 不需要核心字段 |
| 跨 resume | 新进程 + 新 attempt, `_meta` 全丢 |

另外 `ctx.state` 是只读深拷贝、`return dict` 是唯一写路径——这是 v0.2 冻结宣言里的语义。`_meta` 若进 `state` 会破坏契约; 不进 `state` 又活不过 attempt/resume, 两头不靠。

**替代路径 (今天就能用, 零核心改动)**: 需要 run 级共享状态的是 **caller** —— `Runtime(caller=fn)` 注入的闭包天然持有 run 级作用域, 且 `CallMeta` 已带 `task_id / run_id / stage / attempt` 四元组, 按 task_id 累计即可。ai-writer/ai-research 的 token 记账本就在 caller 层 (llm_calls 表)。

**如果将来真的出现硬需求**, 正确的形状是 run 级 (跨 attempt 存活)、由 Runtime 创建、显式声明"非契约、不落 checkpoint"的 `ctx.scratch` —— 但请先带具体用例来, 不要先加字段。

### R3 `__pavoz_api_version__` — **接受, 但降级为策略问题**

- 拼写修正为 `__pavoz_api_version__`。
- 单靠一个整数解决不了兼容问题, 有价值的是**策略**: 0.x 期间 minor 可破坏 (semver 允许), 所以扩展应该锁 `pavoz>=0.3,<0.4` 这种 minor 区间 + CI 跑"最低支持版本 × 最新版本"矩阵。整数 API 版本可作为补充 (扩展 import 时校验), 成本 1 行, 可以加。
- 规划 §12 #4 "半年 release" 对 0.x 阶段过早 (现在 0.3.0rc1 → 半年一发 = 扩展要等半年才能拿到修复)。建议: **0.x 期间按需 release**, 1.0 后再谈节奏。

### R4 `@gate` — **接受, 但三个契约问题必须先解决**

方向对, 价值最高 (直击"静默死"与"失败成本"), 且实现上是**纯装饰器**: `pavoz/dag.py:57` 的 `@dag.stage()` 接收任意 `NodeFn`, 装饰器在注册前包一层即可, **零核心改动**。但规划 §6.2 的伪代码有三个洞:

1. **reviewer 契约是隐含的 LLM 形状**。要通用化, reviewer 必须是普通可调用对象:
   ```python
   reviewers: Sequence[Callable[[result, ctx], Awaitable[tuple[float, str]]]]  # (score, critique)
   revise:    Callable[[result, str, ctx], Awaitable[result]]                 # 喂回批评重做
   ```
   核心库不知道 LLM 是什么; `pavoz_extensions` 里给的 pydantic/LLM 示例属于文档, 不属于 API。
2. **耗尽语义自相矛盾**。§6.2 第 5 步"raise GateFailedError"与第 6 步"pick_best=True → 返回最高分那次"互斥。必须显式化:
   ```python
   on_exhaustion: Literal["raise", "best_effort"] = "raise"
   ```
   默认必须 `raise` —— "静默返回一个不达标产物"正是本规划要消灭的失败模式。
3. **与核心 `retries` 的乘法关系要写清楚**。`@dag.stage(retries=N)` 会在 `GateFailedError(RetryableError)` 后再跑 N 次 stage, 每次内部又可迭代 `max_iter` 轮 → 最坏成本 `(max_iter × reviewer) × (N+1)`。规划 §7.3 的 "总成本 ≤380K token" 没有算这一项, 预算是假的。

    建议: gate 耗尽时抛 `StageError` (不可重试), 想重跑由外层显式声明, 成本才是可预测的。

### R5 `@conditional` — **降级为 example, 不进发行版**

- 功能上, stage 内一行 `if` 就能跳过, 且那是 pavoz 声明的立场 (`__init__.py`: "循环…留在 stage 函数内用普通 Python 表达")。
- 机制上, 现有语义是"跳过 = `return {}`", **下游无法区分"跳过"和"跑了但没产出"**; 而 DAG 的 `depends_on` 是静态声明的数据契约 —— 静默跳过会重演规划 §2.1 自己列的痛点 "stage 间 schema 错位, 末尾神秘失败"。
- 若坚持要: 必须在 **DAG 构建期**校验 (被跳过的 stage 不得被依赖, 或依赖方显式声明容忍缺失), 并把 skip 写进 delta 的显式标记。这把 10 行装饰器变成一个有校验面的子系统 —— 收益不值。

### R6 `@schema` — **接受**

- 跨 stage 类型契约是通用痛点, 值得一个参考实现; 依赖 pydantic 由扩展自带 —— 与 ROADMAP v0.4.1 已确立的哲学一致 ("core 不带任何 driver, 用户自己 pip install 自己要的 deps")。
- 建议轻量泛化: 接受任意 `Callable[[Any], None]`, pydantic 只是文档里的例子。这样不用 pydantic 的项目也能用。
- 异常继承核心 `StageError` (终态不重试) —— 规划 §11.1 已写对, 保持。加一个 `warn_only` 模式 —— 规划 §10 风险 3 已提到, 采纳。

### R7 `@cost_cap` — **有条件接受, 但要改形状**

- **硬事实**: 只有 caller 知道 token 数 (它才是发 HTTP 请求的那层)。扩展无法对不归它管的 caller 施加约束。所以"扩展强制阻断"是做不到的, 能做到的是: 提供预算对象 + 检查函数 + 异常类型, 由应用在 caller 里接线。
- 异常语义建议改: 超预算**默认终态** (`StageError`), 不是 `RetryableError` —— 超预算再重试通常是在烧更多钱, 那正是要防的。要"给 gate 一次机会"可以做可选参数, 不能是默认。
- "±10% buffer" 不必要: provider 返回的 usage 是结算口径, 有实测就用实测; buffer 是掩盖口径混乱。
- 与 R2 一致: 计数器由 caller 按 `CallMeta.task_id` 持有, 不塞 `ctx._meta`。

### R8 per-stage 小循环 — **接受为方法论, 但 pavoz 侧的工作是"文档 + 示例", 不是代码**

- 规划自己把 "Per-stage reviewer 调度" 放在**驱动层** (§4 决策表最后一行) —— 这个归属是对的, 且意味着核心零改动。
- pavoz 已有的、真正支撑小循环的原语: `Runtime.run_stage` (单 stage 重放, v0.5.1)、`fork_run` (改输入续跑, v0.6)、事件流 (观测每次迭代)。**"失败 1 次成本 1× token" 能成立, 靠的正是这两个原语**, 它们已经 ship —— 规划 §9 的 "优势 9/10" 应归功于已有能力, 而不是新增需求。
- pavoz 该做的三件事 (成本 1–2 天, 收益最高):
  1. 扩写 `docs/cn/architecture.md` + `docs/en/architecture.md` 的《循环留在业务层》一节 —— 写清扩展面: stage fn = 任意协程、`on_event` 观察、Protocol 注入、异常契约;
  2. 升级 `examples/agent_loop.py` (仓内**已有**的收敛循环示例) —— **纯 stdlib**、可跑, 多 lens + 阈值 + 分数入事件, 证明不需要核心 API;
  3. 兼容策略 (见 R3)。
- 规划 §7.2 的表格里, "方法论锁定: 跨 LLM audit / 引用溯源 / 政治敏感性" 属于**应用层规范**, 不进 pavoz。
- 一个真正的技术风险规划没写: **max_iter × dag.retries 的乘法** (见 R4.3), 以及 **reviewer 与 worker 的独立性**只能是应用侧约束 (不同模型/不同 prompt), pavoz 无法也无需保证。

### R9 `pavoz-extensions` 打包 — **改: 一开始就分仓**

规划 §12 #5 建议 "先单仓 3 个月, 验证后分仓"。反了:

- 分仓才是对"扩展点够不够用"的**真实验证** —— 同仓时扩展可以偷偷 import 核心内部符号, 验证结果无效; 分仓强制它只活在公开 API 上。
- 3 个月后拆仓要重写 git 历史 / CI / 发布流水线, 成本比现在建一个新仓高。
- 定位信号: 核心仓 `docs/` 近期刚做过 "移除误发布的内部文档" (commit 4e211a6)。往同一个仓里塞应用层内容, 与这个方向相反。
- 建议形态:
  - 仓: `pavoz-extensions` (独立仓), 发行名 `pavoz-extensions`, import 名 `pavoz_extensions`
  - 依赖: `pavoz>=0.3,<0.4` + CI 矩阵 (最低支持 × 最新)
  - 核心仓只放: 公开文档的扩展面一节 (`architecture.md` cn+en) + `examples/agent_loop.py` 升级 + 兼容策略
  - **零第三方依赖是核心的承诺, 不是扩展的承诺** —— 扩展自带 pydantic 没问题 (已有先例)
  - 长期: 第三方可以发自己的 `pavoz-xxx`, 文档要写清命名约定, 不要中心化审批

### R10 应用层规格表 — **移出 pavoz 文档**

§7.3 (八 stage 规格表 + 每 stage token 预算) 与 §15 (落地映射: ai-research / ai-write / 该课题) 是**应用内容**, 不是库文档。判据来自 `CLAUDE.md`: "改 pavoz 前先问: 这个改动对 ai_writer 之外的项目也有用吗?" —— 这些表格对通用用户无意义, 且会稀释"独立通用编排库"的定位。
去处: ai-research / 政策项目自己的 docs。

---

## 3. 对 §12/§16 五个决策点的正式答复

| # | 决策 | 答复 |
|---|---|---|
| 1 | 接受 `runtime_hook`? | **不接受**。用现有 `on_event` (每 Runtime 注入)。若嫌写法啰嗦, 在应用侧/扩展包里包一层, 不进核心 |
| 2 | 接受 `ctx._meta`? | **不接受 (现阶段)**。Ctx 每 attempt 重建, 不承载 run 级状态; 记账归 caller。需要时另提 run-scoped `ctx.scratch` 用例 |
| 3 | 扩展包命名 `pavoz-extensions`? | 接受 *(拼写: pavoz)*, 但**独立仓**, 核心仓不放 |
| 4 | 半年 release + 主版本范围兼容? | 0.x 期间按需 release; 兼容用 `pavoz>=0.3,<0.4` + CI 矩阵; API 版本常量可作为补充 |
| 5 | 合并入主仓 / 先单仓 3 个月? | **分仓, 一开始就分** |

---

## 4. 接受后的落地顺序 (按"对开源定位的价值"排序)

| 优先级 | 事项 | 成本 | 价值 |
|---|---|---|---|
| P0 | 扩写 `architecture.md` (cn+en) 扩展面 + 升级 `examples/agent_loop.py` + 兼容策略 | 1–2 天 | 让**任何人**能写扩展; 零风险 |
| P1 | `pavoz-extensions` 独立仓: `@gate` (修好契约后) + `@schema` | 3–5 天 | 首个参考实现, 证明扩展点够用 |
| P2 | `@cost_cap` 按 §R7 改造后作为第三个扩展; `@conditional` 降级为 example | 1–2 天 | 中等 |
| — | 核心改动 | **0 行** | — |

规划 §14 的验收标准里, "反向用例…对比手工 v9 产物差异 ≤20%" 不可度量 (文章文本没有定义 distance), 建议换成可验证项: 扩展单测覆盖率、示例可跑、CI 矩阵通过、对现有 38+ tests 零回归。

---

## 5. 给规划方的一句话总结

**方向接受, 落点要改**: 小循环是对的, 但它落在应用层与扩展包; pavoz 核心要做的是**把扩展面写清楚 + 提供一个可跑的示例**, 而不是新增 hook 与 `_meta`。
判断标准始终是同一句 —— *"这个改动对 ai-research 之外的项目也有用吗?"* 本规划里满足这条的, 恰好全都是扩展层的需求。
