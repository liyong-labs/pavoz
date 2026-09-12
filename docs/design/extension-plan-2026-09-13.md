# pavoz 扩展机制开发计划 (v2, 已 grill)

**起草**: pavoz PM / 架构师 (Claude)
**日期**: 2026-09-13 (v1 初稿 → v2 经维护者逐项 grill 定案)
**状态**: **实施中** — W0/W1/W2/W5 已 ship; W3 首版已 ship; W2.5 待维护者 PyPI 动作

---

## 0. 进度 (2026-09-13)

| 项 | 状态 | 产出 |
|---|---|---|
| W0 扩展面文档 | ✅ ship | `docs/{cn,en}/architecture.md` 新增《扩展面》一节 (5 扩展点 + 事件表 + 版本策略); README 事件行校正 |
| W1 示例升级 | ✅ ship | `examples/agent_loop.py` 多 lens `quality_gate` 版 (min 聚合 + 分数入 `on_event`) + `tests/test_examples_agent_loop.py` 防腐化 |
| W2 兼容策略 | ✅ ship | 文档化 (不加 API 版本常量); 扩展侧 `pavoz>=0.3,<0.4` + 导入时 stdlib 校验 |
| W2.5 pavoz 0.3.0 转正 | ⏸ **待维护者** | PyPI 建项目 `pavoz` + Trusted Publisher + GH Environment `pypi` → 然后 tag `v0.3.0` (pyproject/`__version__` 现为 `0.3.0rc1`) |
| W3 扩展包 | 🟡 首版 ship | `liyong-labs/pavoz-extensions` @ `cee9b75`: `@gate` + `@schema` + 40 tests + pyflakes + 公开 API 结构检查 + 发布 workflow。**版本矩阵待 PyPI 发布后启用**; **他证待发布后** |
| W4 `@cost_cap` / `@conditional` | ⏸ 延后 | 0.2.0 候选, 契约未定不上 PyPI |
| W5 索引 | ✅ ship | README (en/cn) "扩展" 一节 + ROADMAP + CHANGELOG `[Unreleased]` |
| 追加: API 参考补齐 | ✅ ship | `docs/{cn,en}/api.md` 补 8 项滞后能力 (`on_event` / `fork_run` / `set_progress` / `cancelled` / `error_class` / `stage_timings` / `prune` / 参数覆盖) + `tests/test_api_docs_coverage.py` 防漂移 |
| 追加: 文档工具链 | ✅ ship | `llms.txt` (agent 入口) + `tools/check_doc_links.py` + `tools/check_doc_style.py` + CI 步骤; 两仓 CONTRIBUTING/README 与 CI 实际命令对齐 |

**验证记录**: pavoz 核心 201 tests ✅ / pyflakes 0;扩展 40 tests ✅ / pyflakes 0 / 结构检查通过(且自证能抓违规);
干净 venv 按 README 安装路径实测 (`pavoz@git` + `extensions@git --no-deps`) 双包 import 正常。

**顺带修的**: `tests/test_prune_error_progress.py` 未使用 `json` import (存量 CI lint 红灯) · `pavoz/dag.py`
陈旧 stage 签名 docstring (`(req, ctx)` → `(ctx)`)。
**输入**: `docs/pavoz-upgrade-plan-2026-09-12.md` (需求方 v2) + `docs/review-pavoz-upgrade-plan-2026-09-13.md` (评审) + `docs/design/handoff-upgrade-plan-v2-2026-09-13.md` (答复)
**当前版本**: `0.3.0rc1`; 扩展仓 `liyong-labs/pavoz-extensions` 已建 (2026-09-13)

---

## 1. 目标 / 非目标

**目标**: 把"扩展 (gate / schema / 质量小循环)"变成 pavoz 生态的一等公民 —— 任何第三方能照着公开文档写出自己的 `pavoz-*` 扩展, 且扩展不需要核心为新需求改一行代码。

**非目标**:
- ❌ 核心不加 hook 注册表 / `ctx._meta` / conditional / cost cap (评审 §R1 R2 R5 R7)
- ❌ 扩展不 vendor 进核心仓 (先例: v0.4 contrib storage adapters 已被反转)
- ❌ 不把任何消费方的 stage 名 / 规格表 / token 预算写进核心文档

**成功判据**:
1. 第三方只读公开文档 + 公开示例, 能独立写出可用扩展 (以发布后"他证"验证, 见 §5)
2. 扩展仓 CI 结构检查保证它只活在公开 API 上 (禁止 `pavoz.<内部子模块>` import)

---

## 2. 硬约束

| 约束 | 来源 |
|---|---|
| 核心 `dependencies = []` 永久不变 | CLAUDE.md 开发铁律 1 |
| 核心 API 语义冻结 (v0.2 宣言); 本次**零核心改动** | ROADMAP API 冻结宣言 |
| 扩展异常继承核心 `StageError` / `RetryableError` / `FatalError` | 规划 §11.1 (采纳) |
| 扩展自带依赖 (pydantic 等), 核心不代管 | ROADMAP v0.4.1 storage_loader 先例 |
| 命名一律 `pavoz` (非 pavez) | — |
| **公开仓标准**: 提交内容按 commit `4e211a6` 的"去内部黑话/私有引用"标准脱敏 | 维护者决议 (2026-09-13) |

---

## 3. 工作项

### W0 — 扩展面文档 (P0, ~0.5 天) — **扩写, 不新建**

**落点**: `docs/cn/architecture.md` + `docs/en/architecture.md` 的《核心设计决策 1 · 循环留在业务层》一节 (该节已存在并以 `s_quality_loop` 示例声明"循环不是图节点"; 本次把它扩写为完整扩展面)。

**内容**:
1. 扩展点总表: ① stage 函数 = 任意协程 → 装饰器即扩展; ② `on_event` 生命周期事件 (run_start/stage_start/stage_end/stage_retry/run_end + `ctx.set_progress` 的 stage_progress); ③ Protocol 注入 (`StorageBackend` / caller / `cancel_check`); ④ 异常契约 (StageError 终态 / RetryableError 退避 / FatalError 立即终)
2. gate/reviewer 进阶写法 (多 lens + 阈值 + 聚合 + 分数入事件流)
3. "什么该进核心 / 什么该在扩展" 判据 (CLAUDE.md 三问)
4. 版本策略: 扩展锁 `pavoz>=0.3,<0.4`; 0.x 期间 minor 可破坏; 扩展仓 CI 跑矩阵

**验收**: 中英双语同步; 不含任何应用专有名词; 示例可复制运行。
**注**: README 的 "Event hooks" 一行写"五种事件", 实际另有 `stage_progress` (`ctx.set_progress`) —— 顺手校正。

### W1 — 质量循环示例升级 (P0, ~0.5 天) — **升级, 不新建**

**落点**: `examples/agent_loop.py`(已有 review → fix → re-review 收敛示例, 单 reviewer + bool 判定) 升级为多 lens 变体:
- reviewer 返回 `(score, critique)` 而非 bool; `aggregate=min` (最严一票否决); `on_exhaustion="raise"` → `StageError`
- 每轮分数 emit 到 `on_event` (gate 可观测)
- 保留原单 reviewer 路径作对照 (同一文件内两个 stage, 或注释说明)
- 同步 `examples/README.md` 索引

**验收**: `python examples/agent_loop.py` 直接跑通; 现有测试零回归。

### W2 — 兼容策略 (P0, ~0.2 天) — **文档, 不加常量**

- **不加 `__pavoz_api_version__`** (维护者决议): `__version__` 已存在且与 tag 同步 (曾漂移一次, 见 CHANGELOG 0.1.3), 第二个版本号 = 第二个漂移源。
- 扩展侧以 `pyproject.toml` 声明 `pavoz>=0.3,<0.4` (pip 解析即强制) + 扩展 import 时 5 行 stdlib 校验 `pavoz.__version__` (仅为友好报错)。
- CI 矩阵: pavoz 最低支持版本 × 最新版本。
- 0.x 期间**按需 release** (不设半年节奏)。

### W2.5 — pavoz 0.3.0 转正 + PyPI 首发 (P0, 0.2 天 + 维护者动作)

- 去 rc 标记 → `0.3.0`; 更新 CHANGELOG; tag `v0.3.0` (push 由维护者决定, 触发 `publish.yml` Trusted Publishing)。
- **维护者一次性动作**: PyPI 建项目 `pavoz` + 配 Trusted Publisher (`liyong-labs/pavoz`) + GitHub repo Settings → Environments 建 `pypi`。publish.yml 注释里已写明前提, 缺一不可。
- 动因: README 现写 `pip install pavoz` + PyPI 徽章, 但 PyPI 上无此项目 (实测 404) —— 空头支票先补上。

### W3 — `pavoz-extensions` 仓 bootstrap + gate + schema (P1, 3–5 天)

**仓**: `liyong-labs/pavoz-extensions` (已建) — **官方扩展包, 跟随核心维护**; 文档声明核心永不反向依赖扩展, 第三方可自由发自己的 `pavoz-*`。

```
pavoz-extensions/
├── pyproject.toml            # deps: pavoz>=0.3,<0.4 ; extras: [pydantic]
├── pavoz_extensions/
│   ├── __init__.py           # 版本 + 兼容性校验 (stdlib 比 pavoz.__version__)
│   ├── gate.py               # 小循环执行机制
│   └── schema.py             # 跨 stage 契约
├── tests/
├── .github/workflows/        # ① pavoz 版本矩阵 ② 结构检查
└── README.md                 # 含"如何写你自己的 pavoz-* 扩展"
```

**`@gate` 契约 (grill 定案)**:
```python
gate(reviewers, revise=None, threshold=..., max_iter=3,
     aggregate="min",          # "min"(默认, 最严一票否决) | "mean"
     on_exhaustion="raise",    # "raise"(默认, StageError 终态) | "best_effort"
     concurrency=1)            # 默认串行; >1 时内部 asyncio.gather
# reviewers: Sequence[Callable[[result, ctx], Awaitable[tuple[float, str]]]] — 普通可调用, 非 LLM 专用
# revise:    Callable[[result, critique, ctx], Awaitable[result]] | None
```
- 与 `@dag.stage(retries=N)` 的成本乘法写进 README (推荐 `retries=0`; 上界 `1 + max_iter × (|reviewers| + 1)` 次 `ctx.call`)
- 末轮不再调用 `revise` (避免白烧一次调用)
- reader 侧守卫: `reviewers` 非空 + `max_iter >= 1`
- reviewer 独立性 (不同模型/prompt) 只作使用建议, 不做 API

**`@schema` 契约**:
```python
schema(input=Callable[[Mapping], None] | None,
       output=Callable[[Any], None] | None, warn_only=False)
# 任意 validate callable (pydantic 只是 extras/文档示例); 失败 → StageError 子类 (终态)
```

**CI 双保险**:
- ① 版本矩阵 (pavoz 最低支持 × 最新)
- ② **结构检查** (~10 行 AST): 只允许 `from pavoz import ...` 顶层命名空间, 禁止 `pavoz.runtime` / `pavoz.types` 等子模块 import (现有 handoff 示例 `from pavoz.types import StageError` 即会被拦下 → 改 `from pavoz import StageError`)

**验收**: gate ≥12 测试, schema ≥10 测试, 覆盖 ≥90%; 一个真实 DAG 集成测试; 核心仓 `git diff` 为空。

### W4 — 0.2.0 候选 (P2, 暂不排期)

- `@cost_cap`: **不进 0.1.0**。形状待实战验证: 记账归 caller (按 `CallMeta.task_id` 累计, run 级; 不放 `ctx._meta` —— Ctx 每 attempt 重建, `runtime.py:596`); 超限默认 `StageError` 终态。
- `@conditional`: **不进发行版**, 仅示例 (一行 `if` 即可; `return {}` 无法与"跑了没产出"区分)。
- 等 0.1.0 有真实使用反馈后再定是否发。

### W5 — 索引更新 (0.2 天)

- `ROADMAP.md` → 指针到本计划; `CHANGELOG.md` `[Unreleased]` 记本次决议 (拒 runtime_hook / ctx._meta; 收 gate / schema)
- `README.md` / `README.cn.md` 加"扩展生态"一节, 链到扩展仓 (官方包)

---

## 4. 顺序与时间线

| 阶段 | 工作项 | 人日 | 阻塞 |
|---|---|---|---|
| D1 | W0 + W1 + W2 | 1.2 | 无, 可立即开工 |
| D1 | W2.5 (pavoz 0.3.0 转正 + tag) | 0.2 + 维护者动作 | 维护者做 PyPI 一次性配置 |
| D2–D4 | W3 (扩展仓 bootstrap + gate + schema + 双 CI) | 3–5 | 无技术阻塞 (仓已建) |
| D5 | W5 + 脱敏归档 (评审/handoff/原稿) | 0.5 | W3 后 |
| 合计 | | 5–7 人日 | |

**顺序约束**: 扩展 0.1.0 的发布晚于 pavoz 0.3.0 上 PyPI (依赖解析需要)。

---

## 5. 完成定义 (DoD)

- [ ] 核心 `git diff` = 0 行
- [ ] `examples/agent_loop.py` 裸跑通过; 全测试绿; pyflakes 0 (CI 现有门槛)
- [ ] 公开文档中英同步, 零应用专有名词
- [ ] 扩展仓: CI 结构检查通过 (只 import `pavoz` 顶层) + 版本矩阵绿 + 对着**PyPI 上已发布的** pavoz 安装运行
- [ ] **他证**: 发布后由第三方 (非本计划执行者) 只读公开文档写一个**不在 W3 里的**新扩展 (如 webhook 通知), 记录卡点并回补文档
- [ ] 归档文档过脱敏 (事故日期表 / 未公开项目名 / 主机路径)

---

## 6. 风险

| 风险 | 缓解 |
|---|---|
| 扩展与核心版本漂移 | CI 矩阵 + 扩展侧 `pavoz>=0.3,<0.4` pin; 官方包承诺跟随 |
| `@gate` 与 `dag.retries` 成本乘法被误用 | README 显著位置 + 示例示范 `retries=0` + 上界公式 |
| 装饰器破坏 stage 名 | 强制 `functools.wraps` (`dag.py` 用 `fn.__name__` 命名); 测试断言 |
| 结构检查误伤 (合法子模块需求) | 白名单可讨论; 首选把需要的东西提到顶层 |
| 公开仓文档含内部信息 | 提交前脱敏 checklist (§5 末项) |
| PyPI 名称被占 / 发版不可撤回 | `pavoz` 项目名先到先得, 维护者尽快完成一次性配置; 0.1.0 只发契约已定的两个扩展 |

---

## 7. 决议台账 (2026-09-13 grill 定案)

| # | 决策 | 结论 |
|---|---|---|
| ① | 扩展仓形态 | **独立仓 + 发 PyPI** (`liyong-labs/pavoz-extensions`, 已建) |
| ② | API 版本常量 | **不加**; pyproject pin + CI 矩阵 + stdlib 校验 `__version__` |
| ③ | 执行分工 | **pavoz 侧全部由我执行** (W0/W1/W2/W5 核心仓 + W3/W4 扩展仓); 需求方只做应用接入 + 改自己文档 |
| ④ | 首发范围 | `pavoz-extensions 0.1.0` = **gate + schema**; cost_cap/conditional 延后 |
| ⑤ | 文档归档 | 评审/handoff/需求方原稿 **进公开仓 docs/design/ + 脱敏** |
| ⑥ | PyPI 次序 | **先 pavoz 0.3.0 转正**, 再发扩展 0.1.0 |
| ⑦ | 扩展点验证 | **CI 结构检查 + 发布后他证** |
| ⑧ | 文档/示例落点 | **扩写已有** (architecture.md 第一决策节 + agent_loop.py), 不新建 |
| ⑨ | 评分聚合 | 默认 **min** (最严一票否决), 参数化 `aggregate` |
| ⑩ | reviewer 执行 | 默认**串行** `concurrency=1`, 可开并发 |
| ⑪ | 扩展仓定位 | **官方扩展包 + 跟随核心**; 核心永不反向依赖扩展 |

**仍需维护者的动作** (非决策):
1. PyPI 一次性配置 (项目 `pavoz` + Trusted Publisher + GH Environment `pypi`); 之后扩展仓同样配置
2. tag `v0.3.0` 的 push (pavoz 规则: push 由维护者决定)

---

## 8. 明确不做 (YAGNI 记录)

- 全局 `runtime_hook` 装饰器注册表 — 与 `on_event` 重复 + 进程级全局状态 (评审 R1)
- `ctx._meta` — Ctx 每 attempt 重建, 不承载 run 级状态 (评审 R2)
- `__pavoz_api_version__` 常量 — 第二个版本号 = 第二个漂移源 (决议 ②)
- `@conditional` / `@cost_cap` 进 0.1.0 — 契约未定不上 PyPI (决议 ④)
- reviewer 默认并发 — 不假设 caller adapter 并发安全 (决议 ⑩)
- 应用层 stage 规格表 / token 预算进核心文档 (评审 R10)
- 扩展中心化审批 / entry_points 注册 (CLAUDE.md 既有 YAGNI)
