# Handoff: pavoz 升级规划 v2 → 落地执行

**日期**: 2026-09-13
**发出方**: pavoz PM / 架构师
**接收方**: 需求方 (执行 agent)
**关联文档**: `docs/pavoz-upgrade-plan-2026-09-12.md` (规划 v2) + `docs/review-pavoz-upgrade-plan-2026-09-13.md` (评审)

---

## 0. 一句话

**方向接受, 落点改变 — pavoz 核心 0 改动, 今天就能开工。**

小循环是对的, 但它落在**应用层 + 独立扩展仓**, 不是 pavoz 核心。规划 §5 "核心加 2 个最小钩子" 不需要做, 因为核心早就有等价能力。

---

## 1. 推翻规划前提的 3 个事实 (先读这个, 否则后面全歪)

| # | 规划假设 | 实际 | 出处 |
|---|---|---|---|
| 1 | 需要给核心加 `runtime_hook` 全局注册表 | **已有** `on_event` 事件出口, 含异常隔离 | `runtime.py:98` (Ctx) / `:162` (Runtime) / `:166` `_emit` |
| 2 | 需要给核心加单 stage 重放入口 | **已有** `run_stage` (v0.5.1) + `fork_run` (v0.6) | `runtime.py:358` / `:455` |
| 3 | 4 个能力需要核心配合才做得出来 | **全是纯装饰器** — `@dag.stage()` 接任意 NodeFn | `dag.py:58` / `:68` / `:83` |

**推论**:
- `pavoz/` 目录**一行不用改**。规划 §2.3 "❌ 缺失 Hook" 是事实错误 → 修文档。
- 规划 §9 的 "优势 9/10 (小循环)" 应归功于**已 ship 的 `run_stage` / `fork_run` + 事件流**, 不是新增需求。
- 核心零改动 = 零版本风险 = 第三方可复制。**"不需要改" 本身就是最好的结果。**

> 写作便利 (装饰器语法糖) 想要就自己包一层, 15 行, 放应用侧或扩展仓, 不进核心。

---

## 2. 小循环配方 (30 行, 直接复制)

**纯应用侧代码, 零核心改动。** 两个关键细节必须照抄, 否则会踩坑。

```python
# driver/quality_gate.py
import functools
from typing import Any, Awaitable, Callable, Sequence

from pavoz import StageError                # 顶层公开 API (扩展仓 CI 会拦 pavoz.types 之类子模块 import)

Ctx = Any
Reviewer = Callable[[Ctx, dict], Awaitable[tuple[float, str]]]   # → (score, critique)
Reviser  = Callable[[Ctx, dict, str], Awaitable[dict]]


def gate(reviewers: Sequence[Reviewer], revise: Reviser,
         threshold: float, max_iter: int = 3, on_exhaustion: str = "raise"):
    """worker → reviewers → revise → 收敛; 耗尽默认抛 StageError (终态, 不可重试)."""
    assert reviewers and max_iter >= 1, "gate: reviewers 非空 + max_iter >= 1"

    def deco(fn):
        @functools.wraps(fn)                   # ① 保 stage 名 — dag.py:70 用 fn.__name__
        async def wrapper(ctx: Ctx) -> dict:
            draft, best, best_score = await fn(ctx), None, -1.0
            for i in range(max_iter):
                pairs = [await r(ctx, draft) for r in reviewers]   # ② 内部走 ctx.call()
                score = min(s for s, _ in pairs)                   # ③ min = 最严 lens 一票否决
                if ctx.on_event:
                    ctx.on_event("gate_score", {"stage": ctx.stage_name, "iter": i + 1,
                                                "score": score, "lenses": [s for s, _ in pairs]})
                if score > best_score:
                    best, best_score = draft, score
                if score >= threshold:
                    return best
                if i < max_iter - 1:                               # ④ 末轮不再 revise (不白烧调用)
                    draft = await revise(ctx, draft, "\n".join(c for _, c in pairs))
            if on_exhaustion == "best_effort":
                return best
            raise StageError(f"gate 耗尽: {best_score:.2f} < {threshold} ({max_iter} iters)")
        return wrapper
    return deco


def llm_reviewer(lens: str, model: str) -> Reviewer:
    async def _r(ctx, draft):
        r = await ctx.call("llm", "review", {"lens": lens, "model": model, "draft": draft})
        return float(r["score"]), str(r["critique"])
    return _r
```

用法:

```python
@dag.stage(depends_on=["s_draft"], retries=0)   # ← retries=0, 见下方 ③
@gate(reviewers=[llm_reviewer("方法论", "qwen-plus"), llm_reviewer("政治", "glm-5.3")],
      revise=llm_revise, threshold=7.0, max_iter=3)
async def s_review(ctx): ...
```

### 关键细节 ①: `functools.wraps` 不是可选项

`dag.py:70` 用 `fn.__name__` 当 stage 名。不包 `wraps` → 所有被装饰的 stage 全叫 `wrapper` → 第二个直接 `duplicate stage name` 崩。

### 关键细节 ②: reviewer 必须走 `ctx.call()`

`ctx.call` 是唯一的出站通道 (`runtime.py:122`), `CallMeta` 带 `task_id / run_id / stage / attempt` 四元组。**reviewer 绕过它 = trace / llm_calls 断链** —— 事后查不到"这轮审核是谁给的几分", 复盘直接瞎。

### 关键细节 ③: 装饰器顺序 + `retries=0`

- **顺序**: `@dag.stage()` 必须在上, `@gate` 在下。反了 = gate 白写 (`dag.stage` 返回原 fn, DAG 里存的是未包装的那个)。
- **`retries=0`**: 见 §3 第 3 条的成本乘法。gate 的 `max_iter` 已经承接质量重试; 瞬时错误 (429/5xx) 由 **caller 自己做退避重试** (它才是发 HTTP 的那层)。两层重试叠加 = 成本失控。

**成本上界**: `1 + max_iter × (|reviewers| + 1)` 次 `ctx.call` (worker 1 次 + 每轮 |R| 个 reviewer + 每轮 1 次 revise; 末轮不 revise 已省 1 次)。`retries=0` 时这是硬上界 —— 注意上界算的是 `ctx.call` 次数, 一次 call 在 caller 里可能扇出多次 LLM 请求。

### 关键细节 ④: 聚合与并发

- **聚合默认 `min`** (最严 lens 一票否决): 平均分会让宽松的"结构" lens 掩盖严苛的"安全" lens —— 那是掩盖而非发现。要平均自己改 `score = sum(...)/len(...)`。
- **默认串行**: 3 个 lens 想并发, 把 `pairs = [...]` 换成 `asyncio.gather(...)` 即可 —— 前提是**你的 caller 能并发** (token 记账 / llm_calls 落库要并发安全), 这是应用侧的责任, 不是 pavoz 的。

---

## 3. 规划文档要改的 6 处

| # | 位置 | 问题 | 怎么改 |
|---|---|---|---|
| 1 | 全文 | **拼写错误** `pavez` → 应为 `pavoz` (出现于 `pavez-extensions` / `pavez_ext` / `pavez.hooks` / `__pavez_api_version__` / `pavez.contrib`) | 全局替换; 开源仓第一批 issue 必命中 |
| 2 | §2.3 / §3.3 / §11.3 / §14 | **版本号自相矛盾**: 写 `pavoz>=0.9,<1.0`, 实际 `__version__ = "0.3.0rc1"` (`__init__.py:36`) | 改 `pavoz>=0.3,<0.4`; §14 的 "0.9.x 范围" 同改。另: §2.3 "缺失 Hook" 改为 "已有 `on_event`" |
| 3 | §6.2 / §7.3 | **gate 三洞**: ① `max_iter × dag.retries` 成本乘法未计 — §7.3 "总成本 ≤380K" 是假预算; ② `on_exhaustion` raise-vs-`pick_best` 自相矛盾; ③ reviewer 契约隐含 "必须是 LLM" | ① 耗尽抛 `StageError` (终态不可重试) + gate stage `retries=0`, 瞬时错误归 caller; ② 显式参数 `on_exhaustion: Literal["raise","best_effort"] = "raise"`, **默认必须 raise**; ③ reviewer 泛化为普通 callable → `(result, ctx) -> (score, critique)`, LLM 只是文档示例 |
| 4 | §6.3 | **conditional 该降级**: stage 内一行 `if` 即可; 且 `return {}` 的跳过语义**下游无法区分 "跳过" 与 "跑了没产出"**, 会重演 §2.1 自己列的 "stage 间 schema 错位" 痛点 | 从扩展降为 `examples/` 里的示例, 不进发行版。要保留就必须在 DAG 构建期校验 (被跳过的 stage 不得被依赖) — 收益不值 |
| 5 | §6.5 | **cost_cap 记账位置错**: 只有 caller 知道 token 数 (它才是发 HTTP 的那层), 扩展管不到 | 扩展只提供 **预算对象 + 检查函数 + 异常类型**, 应用在 caller 接线; 计数器由 caller 按 `CallMeta.task_id` 持有 (**不是 `ctx._meta`** — Ctx 每 attempt 重建, `runtime.py:596`); 超预算异常默认 **`StageError`** (超预算再重试 = 烧更多钱, 正是要防的) |
| 6 | §7.3 / §15 | **应用层内容混进库文档**: 八 stage 规格表 + ai-research/ai-write 落地映射 | 移到应用侧 docs。判据 `CLAUDE.md`: *"这个改动对 ai_writer 之外的项目也有用吗?"* — 这两张表对通用用户无意义 |

---

## 4. 五个决策点的答复

| # | 决策 | 答复 |
|---|---|---|
| 1 | 接受 `runtime_hook`? | **不接受**。用现有 `on_event` (每 Runtime 注入, `runtime.py:162`)。全局注册表更差: 同进程跑两个 Runtime 会互相泄漏 hook; `fn(**data)` 把 payload 字段名变成隐式 API |
| 2 | 接受 `ctx._meta`? | **不接受 (现阶段)**。Ctx 在 `_run_stage` 重试循环**内**构造 (`runtime.py:596`), 每 attempt 全新 → 记账会被重试绕过 (而"重试绕过"正是要修的痛点)。记账归 **caller** (CallMeta 四元组已够)。真出现硬需求时, 正确形状是 run 级 `ctx.scratch` — 带用例来, 别先加字段 |
| 3 | 扩展包命名 `pavoz-extensions`? | **接受** (拼写 pavoz), 但**独立仓**, 核心仓不放。仓已建: `liyong-labs/pavoz-extensions` (2026-09-13) |
| 4 | 半年 release + 主版本兼容? | **0.x 期间按需 release** (现在 0.3.0rc1 → 半年一发 = 扩展等半年才拿修复)。兼容 = `pavoz>=0.3,<0.4` + CI 跑 "最低支持版本 × 最新版本" 矩阵。**不新增 `__pavoz_api_version__` 常量** —— `__version__` 已存在且与 tag 同步过, 第二个版本号 = 第二个漂移源 |
| 5 | 先单仓 3 个月再分仓? | **反了 — 一开始就分仓**。同仓时扩展能偷 import 核心内部符号, "扩展点够不够用" 的验证是无效的; 分仓强制它只活在公开 API 上。3 个月后拆仓还要重写 git 历史 / CI / 流水线 |

---

## 5. 验收标准替换 (原 §14)

**删掉这条**:
> ~~"集成测试: 反向用例跑通, 对比手工 v9 产物差异 ≤20%"~~

**不可度量** — 文章文本没有定义 distance, 没人能判定 20% 是什么。

**换成 4 条可验证项**:

- [ ] 扩展单测覆盖率达标 (每个扩展独立 suite)
- [ ] `examples/agent_loop.py` **纯 stdlib 可跑** (假 scorer, 不依赖任何 LLM/网络)
- [ ] CI 矩阵通过 (最低支持 pavoz 版本 × 最新版本)
- [ ] **CI 结构检查通过**: 扩展只允许 `from pavoz import ...` 顶层导入, 禁止 `pavoz.runtime` / `pavoz.types` 等子模块
- [ ] **他证**: 发布后由第三方只读公开文档写一个不在首发范围里的新扩展 (如 webhook 通知), 卡点回补文档
- [ ] 对核心现有 **38+ tests 零回归** (`git diff pavoz/` 为空即可证明)

---

## 6. 开工顺序 (按价值排)

> **执行分工 (2026-09-13 定)**: pavoz 侧全部由**维护侧**执行。你只做两件 —— ① 应用侧接入 (§2 配方) ② 改规划文档 (§3)。**不要**再改 pavoz 仓或扩展仓, 两个 agent 同树编辑必撞。

| 优先级 | 事项 | 成本 | 归属 |
|---|---|---|---|
| **P0** | **扩写** `docs/cn/architecture.md` + `docs/en/architecture.md` 的《循环留在业务层》一节 → 完整扩展面 (stage fn / `on_event` 事件表 / Protocol 注入 / 异常契约 / 版本策略) | 0.5 天 | pavoz 核心仓 (维护侧) |
| **P0** | **升级** `examples/agent_loop.py` (仓内**已存在**的收敛循环示例) → 多 lens + 阈值 + `min` 聚合 + 分数入 `on_event`; 保留单 reviewer 对照 | 0.5 天 | pavoz 核心仓 (维护侧) |
| **P0** | 改规划文档 6 处 (§3) | 0.5 天 | **需求方** |
| **P0** | pavoz `0.3.0` 转正 + PyPI 首发 (README 的 `pip install pavoz` 目前是空头支票, PyPI 上查无此项目) | 0.2 天 | 维护侧 (tag push / PyPI 配置由维护者) |
| **P1** | 独立仓 `pavoz-extensions` (已建): `@gate` + `@schema` = **0.1.0 首发** | 3–5 天 | 扩展仓 (维护侧) |
| **P2** | `@cost_cap` (改造后) / `@conditional` (仅示例) — **不进 0.1.0** | 待定 | 扩展仓 |
| — | **核心改动** | **0 行** | — |

**依赖顺序**: 扩展 `0.1.0` 的发布晚于 pavoz `0.3.0` 上 PyPI (pip 解析需要); 在那之前你照旧用现有安装 / `git+` 装。

---

## 7. 三条容易忘的边界

1. **零第三方依赖是核心的承诺, 不是扩展的承诺** — 扩展自带 pydantic 没问题 (已有先例)。
2. **reviewer 与 worker 的独立性是应用侧约束** (不同模型 / 不同 prompt), pavoz 无法也无需保证。
3. **同名扩展的命名约定要写进文档, 但不做中心化审批** — 第三方可以发自己的 `pavoz-xxx`。

---

## 附录: 本次核对过的代码事实

| 事实 | 位置 |
|---|---|
| `Ctx.on_event` (stage 内 `set_progress` 出口) | `runtime.py:98` |
| `Runtime.on_event` (生命周期事件钩子) | `runtime.py:162` |
| `_emit` 异常隔离 (observer 崩不影响 run) | `runtime.py:166-173` |
| `Ctx` 在重试循环内构造 (每 attempt 全新) | `runtime.py:596` |
| `ctx.call` 出站通道 + `CallMeta` 四元组 | `runtime.py:122-134` |
| `run_stage` (单 stage 重放) | `runtime.py:358` |
| `fork_run` (改输入续跑) | `runtime.py:455` |
| `@dag.stage()` 接收任意 NodeFn, 返回原 fn | `dag.py:58, 83` |
| `fn.__name__` 决定 stage 名 | `dag.py:70` |
| `__version__ = "0.3.0rc1"` | `__init__.py:36` |