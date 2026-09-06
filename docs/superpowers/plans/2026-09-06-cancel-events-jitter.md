# stageflow 协作式取消 + 事件钩子 + full-jitter 退避 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 stageflow 补齐对标成熟编排组件的功能缺口三件套：协作式取消（Runtime.cancel_check / Ctx.cancelled）、生命周期事件钩子（Runtime.on_event + RunResult.stage_timings）、重试退避 full-jitter。

**Architecture:** 全部挂在既有 `Runtime` dataclass 属性上（可选回调，默认 None = 零行为变化）。取消 = 检查点拦截（stage 循环顶 + 重试循环顶），已完成 stage 照常落 cp → resume 天然续跑，不新增控制流原语（"循环留业务层"原则不变）。事件 = 同步回调 fire-and-forget，observer 异常隔离（fail-open）；cancel_check 异常视为未取消（fail-open）。

**Tech Stack:** Python 3.12 stdlib only（asyncio / random / time 均已在用）。pytest `asyncio_mode = "auto"`（测试直接写 `async def test_*`）。

**Spec:** 本 plan 实现本次审计（对话记录 2026-09-06）结论的 #2 取消 + #3 事件钩子 + #5 jitter；审计其余项（并行执行 / CI 三件套 / SECURITY.md / tag 对齐）明确不在本 plan 范围（并行 = YAGNI 待宽图 use case；门面类归发布 wave，task #33 / #50）。

## Global Constraints

- core 零第三方依赖：只用 stdlib（现 `runtime.py` 已 import asyncio/logging/time；本 plan 新增 `import random`）
- 不做向后兼容分支（user 2026-09-06 拍板"发布前向前看"）——新回调默认 None 即旧行为，不加任何兼容 shim
- 不破坏现有 99 tests；每 task 收尾跑全量 `python -m pytest tests/ -q` 必须 99+新增 全绿；pyflakes 0 报错
- 新异常路径一律 fail-open：on_event 回调异常 → log + 吞掉；cancel_check 异常 → 视为未取消。禁止让 observer/检查器的 bug 杀死业务 run
- 状态机扩展：`RunResult.status` 新增唯一值 `"cancelled"`（与 "running"|"done"|"failed" 并列）；`stage_statuses` 值域同步加 "cancelled"
- 仓库：stageflow repo；改前先 Read 目标区域拿精确文本（行号会漂移）；1 逻辑单元 = 1 commit；禁止 git reset
- 本 plan 不 bump `__version__`（release wave 做）；CHANGELOG 写在 `## [Unreleased]` 下

---

### Task 1: 重试退避 full-jitter

**Files:**
- Modify: `stageflow/runtime.py`（顶部 import 区 ~line 22；`_run_stage` 内 Retryable 分支 backoff 行 ~line 514）
- Create: `tests/test_retry_jitter.py`

**Interfaces:**
- Consumes: 无（独立小改）
- Produces: 退避公式 `random.uniform(0, min(2 ** (attempt - 1), 30))`（full jitter，cap 不变 30s）。Task 2 的 `stage_retry` 事件 payload 里 `backoff` 字段即此值。

- [ ] **Step 1: Write the failing test**

创建 `tests/test_retry_jitter.py`：

```python
"""v0.9 重试退避 full-jitter: uniform(0, cap), cap = min(2^(attempt-1), 30).

雷群防护 (AWS 惯例): 多 task 同时失败时退避随机化, 不再整秒对齐.
"""

from stageflow import DAG, RetryableError, Runtime


async def test_backoff_full_jitter(monkeypatch):
    """第一次重试 (attempt=1) 的退避 = uniform(0, 2^0=1)."""
    import stageflow.runtime as rt_mod

    captured: dict = {}
    monkeypatch.setattr(
        rt_mod.random, "uniform",
        lambda lo, hi: (captured.update(args=(lo, hi)), lo)[1],
    )

    dag = DAG("jitter1")

    @dag.stage(retries=2)
    async def s_flaky(ctx):
        if ctx.attempt == 1:
            raise RetryableError("boom")
        return {"ok": 1}

    rt = Runtime()
    r = await rt.run(dag, "j1")
    assert r.status == "done"
    assert captured["args"] == (0, 1), f"期望 uniform(0, 1), 实际 {captured.get('args')}"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd stageflow && python -m pytest tests/test_retry_jitter.py -q
```

Expected: FAIL — `AttributeError: module 'stageflow.runtime' has no attribute 'random'`（旧实现用 `min(2**(attempt-1), 30)` 定值，不经过 random.uniform；monkeypatch 目标不存在即报错）。

- [ ] **Step 3: Implement**

`stageflow/runtime.py` 顶部 import 区（`import asyncio` 附近）加一行：

```python
import random
```

`_run_stage` 里 Retryable 分支（找到 `backoff = min(2 ** (attempt - 1), 30)`）改为：

```python
                if attempt <= retries:
                    # v0.9: full jitter (AWS 惯例) — 多 task 同步重试防雷群
                    backoff = random.uniform(0, min(2 ** (attempt - 1), 30))
```

- [ ] **Step 4: Run test to verify it passes + 全量回归**

```bash
python -m pytest tests/test_retry_jitter.py tests/ -q
```

Expected: 新测试 PASS，全量 99 passed。`python -m pyflakes stageflow tests` 0 报错。

- [ ] **Step 5: Commit**

```bash
git add stageflow/runtime.py tests/test_retry_jitter.py
git commit -m "feat(runtime): 重试退避 full-jitter — uniform(0, cap) 防雷群"
```

---

### Task 2: 生命周期事件钩子 + RunResult.stage_timings

**Files:**
- Modify: `stageflow/runtime.py`（Runtime 属性区 ~106-110；run() 主循环 ~180-263；_run_stage ~451-528）
- Modify: `stageflow/types.py`（RunResult `__slots__` / `__init__` / docstring）
- Create: `tests/test_events.py`

**Interfaces:**
- Consumes: Task 1 的 backoff 变量（stage_retry 事件引用）
- Produces: `Runtime(on_event: Callable[[str, dict], None] | None)`；事件名固定 5 种 `run_start / stage_start / stage_end / stage_retry / run_end`，payload 通用键 `task_id / run_id`，stage 事件加 `stage / attempt`，stage_end 加 `status / duration / error`，stage_retry 加 `backoff / error`，run_start 加 `dag / resume`，run_end 加 `dag / status`。`RunResult.stage_timings: dict[str, float]`（stage → 墙钟秒，含重试）。Task 3 依赖 `_emit` 方法与 stage_end 事件（cancelled 状态复用）。

- [ ] **Step 1: Write the failing tests**

创建 `tests/test_events.py`：

```python
"""v0.9 生命周期事件钩子 (on_event) + RunResult.stage_timings.

观察者模式最小实现: 同步回调, observer 异常隔离 (fail-open), 不引依赖.
"""

from stageflow import DAG, RetryableError, Runtime


async def test_events_lifecycle_order():
    """run_start → stage_start/stage_end 交替 → run_end; payload 通用字段齐."""
    events: list[tuple[str, dict]] = []
    dag = DAG("ev1")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 2}

    rt = Runtime(on_event=lambda ev, d: events.append((ev, d)))
    r = await rt.run(dag, "ev1")
    assert r.status == "done"

    names = [e for e, _ in events]
    assert names[0] == "run_start"
    assert names[-1] == "run_end"
    assert events[-1][1]["status"] == "done"
    assert names.count("stage_start") == 2 and names.count("stage_end") == 2
    starts = [d["stage"] for e, d in events if e == "stage_start"]
    assert starts == ["s_a", "s_b"]
    ends = {d["stage"]: d["status"] for e, d in events if e == "stage_end"}
    assert ends == {"s_a": "done", "s_b": "done"}
    assert all("task_id" in d and "run_id" in d for _, d in events)
    assert events[0][1]["resume"] is False


async def test_stage_retry_event_emitted():
    """RetryableError 重试 → 恰一条 stage_retry (attempt=1, backoff>=0)."""
    events: list[tuple[str, dict]] = []
    dag = DAG("ev2")

    @dag.stage(retries=2)
    async def s_flaky(ctx):
        if ctx.attempt == 1:
            raise RetryableError("boom")
        return {"ok": 1}

    rt = Runtime(on_event=lambda ev, d: events.append((ev, d)))
    r = await rt.run(dag, "ev2")
    assert r.status == "done"
    retries = [d for e, d in events if e == "stage_retry"]
    assert len(retries) == 1
    assert retries[0]["attempt"] == 1 and retries[0]["backoff"] >= 0
    assert retries[0]["error"] == "boom"


async def test_stage_timings_populated():
    """每个执行的 stage 有墙钟耗时 (>=0)."""
    dag = DAG("ev3")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 2}

    r = await Runtime().run(dag, "ev3")
    assert set(r.stage_timings) == {"s_a", "s_b"}
    assert all(t >= 0 for t in r.stage_timings.values())


async def test_on_event_exception_swallowed():
    """observer 抛异常 → 不影响 run (fail-open)."""
    dag = DAG("ev4")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    def bad(ev, d):
        raise RuntimeError("observer bug")

    r = await Runtime(on_event=bad).run(dag, "ev4")
    assert r.status == "done"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_events.py -q
```

Expected: 4 FAIL — `Runtime.__init__` 收到未知 kwarg `on_event`（dataclass field 不存在）。

- [ ] **Step 3: Implement — types.py RunResult**

`__slots__` 加 `"stage_timings"`（保持字母序）；`__init__` 在 `self.stage_statuses` 后加：

```python
        self.stage_timings: dict[str, float] = {}  # stage → 墙钟秒 (含 retry 退避)
```

docstring Attributes 加一行：

```
        stage_timings: {stage_name: wall-clock seconds} — executed stages only,
            includes retry backoff sleeps
```

- [ ] **Step 4: Implement — runtime.py**

(a) Runtime 属性区（`default_timeout` 行后）加：

```python
    on_event: Callable[[str, dict], None] | None = None  # v0.9: 生命周期事件钩子
```

(b) Runtime 内加 `_emit` 方法（放 `run` 之前）：

```python
    def _emit(self, event: str, data: dict) -> None:
        """生命周期事件 → on_event 回调. observer 异常隔离 (log + 忽略), 不影响 run."""
        if self.on_event is None:
            return
        try:
            self.on_event(event, data)
        except Exception:
            logger.exception("on_event 回调异常 (忽略) event=%s", event)
```

(c) run() 里 `order = dag.topo_order()` 之前加：

```python
        self._emit("run_start", {"task_id": task_id, "run_id": run_id,
                                 "dag": dag.name, "resume": cp is not None})
```

(d) run() 主循环里 `status, new_state, err, ... = await self._run_stage(...)` 调用前后包计时：

```python
            _st0 = time.time()
            status, new_state, err, producers, delta = await self._run_stage(
                dag, stage.fn, name, task_id, run_id, state, stage.retries,
                stage_deadline, producers,
            )
            result.stage_timings[name] = time.time() - _st0
```

(e) run() 的**每个** `return result` 语句前加 run_end 事件（done 尾部一处；failed 分支若提前 return 也加一处——先 grep `return result` 确认全部出口）：

```python
        self._emit("run_end", {"task_id": task_id, "run_id": run_id,
                               "dag": dag.name, "status": result.status})
```

(f) _run_stage：`attempt = 0` 后加 `_t0 = time.time()`；`attempt += 1` 后加 stage_start：

```python
            self._emit("stage_start", {"task_id": task_id, "run_id": run_id,
                                       "stage": name, "attempt": attempt})
```

每个 `return "failed"` / `return "done"` 前（共 4 个出口：done / StageError+FatalError / retries 耗尽 / 未知异常）加：

```python
            self._emit("stage_end", {"task_id": task_id, "run_id": run_id, "stage": name,
                                     "attempt": attempt, "status": "done",
                                     "duration": time.time() - _t0, "error": None})
```

（status/error 按出口分别填 `"done"`/`None` 或 `"failed"`/`str(e)`。）

Retryable 分支 `await asyncio.sleep(backoff)` 前加：

```python
                    self._emit("stage_retry", {"task_id": task_id, "run_id": run_id,
                                               "stage": name, "attempt": attempt,
                                               "backoff": backoff, "error": str(e)})
```

注意：`run_stage`（单 stage 重放, 调试用）**不发事件、不记 timings** —— 重放不是正式 run，避免污染观察者。

- [ ] **Step 5: Run tests + 全量回归**

```bash
python -m pytest tests/test_events.py tests/ -q && python -m pyflakes stageflow tests
```

Expected: 4 个新测试 PASS，全量 103 passed，pyflakes 0。

- [ ] **Step 6: Commit**

```bash
git add stageflow/runtime.py stageflow/types.py tests/test_events.py
git commit -m "feat(runtime): 生命周期事件钩子 on_event + RunResult.stage_timings"
```

---

### Task 3: 协作式取消

**Files:**
- Modify: `stageflow/runtime.py`（Runtime 属性区；新 `_is_cancelled` 方法；Ctx dataclass ~71-103；Ctx 构造点 ~473（唯一）；_run_stage 循环顶；run() 主循环 failed 分支前）
- Modify: `stageflow/types.py`（RunResult docstring status 值域）
- Create: `tests/test_cancel.py`

**Interfaces:**
- Consumes: Task 2 的 `_emit`（stage_end 事件 status="cancelled" 复用）
- Produces: `Runtime(cancel_check: Callable[[str], bool] | None)`；`Ctx.cancelled() -> bool`；`RunResult.status == "cancelled"`；取消语义 = stage 间/重试间检查点拦截 + 已完成 stage 已落 cp → `resume=True` 无缝续跑。

- [ ] **Step 1: Write the failing tests**

创建 `tests/test_cancel.py`：

```python
"""v0.9 协作式取消: Runtime.cancel_check 检查点拦截 + Ctx.cancelled() 轮询.

语义: 拦截发生在 stage 之间 / 重试之间 (检查点), 已完成 stage 照常落 cp →
resume 无缝续跑. stage 内长循环用 ctx.cancelled() 自行轮询自行退出
(循环留业务层原则).
"""

from stageflow import CheckpointStore, DAG, FileStorage, Runtime


async def test_cancel_between_stages_then_resume():
    """s_a 完成后取消 → cancelled (s_b 未跑); 解除取消 → resume 续跑到 done."""
    tid = "cxl1"
    store = CheckpointStore(FileStorage(f"/tmp/stageflow-cancel-test-{tid}"))
    box = {"cancel": False}
    rt = Runtime(checkpoint_store=store, cancel_check=lambda t: box["cancel"])
    ran: list[str] = []

    dag = DAG("cxl1")

    @dag.stage()
    async def s_a(ctx):
        ran.append("a")
        box["cancel"] = True  # 模拟 run 中途外部取消
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        ran.append("b")
        return {"b": 2}

    r1 = await rt.run(dag, tid)
    assert r1.status == "cancelled"
    assert "取消" in (r1.error or "") or "cancel" in (r1.error or "")
    assert ran == ["a"], f"s_b 应被拦截, 实际 ran={ran}"
    assert r1.stage_statuses.get("s_a") == "done"

    box["cancel"] = False  # 解除取消 (模拟用户撤回)
    r2 = await rt.run(dag, tid, resume=True)
    assert r2.status == "done"
    assert ran == ["a", "b"], "resume 应只补跑 s_b"


async def test_ctx_cancelled_visible_in_stage():
    """stage 内长循环可轮询 ctx.cancelled() 自行退出 (循环留业务层)."""
    calls = {"n": 0}

    def check(task_id):
        calls["n"] += 1
        return calls["n"] >= 4  # 前几次检查 False, 之后 True

    seen: dict = {}
    dag = DAG("cxl2")

    @dag.stage()
    async def s_loop(ctx):
        for _i in range(10):
            if ctx.cancelled():
                seen["stopped"] = True
                return {"stopped": True}
        seen["finished"] = True
        return {"ok": 1}

    r = await Runtime(cancel_check=check).run(dag, "cxl2")
    assert r.status == "done"
    assert seen.get("stopped") is True, "stage 应轮询到取消并提前退出"


async def test_cancel_check_exception_treated_as_not_cancelled():
    """cancel_check 抛异常 → 视为未取消 (fail-open), run 正常完成."""
    dag = DAG("cxl3")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    def bad(task_id):
        raise RuntimeError("checker db down")

    r = await Runtime(cancel_check=bad).run(dag, "cxl3")
    assert r.status == "done"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_cancel.py -q
```

Expected: 3 FAIL — `Runtime.__init__` 收到未知 kwarg `cancel_check`。

- [ ] **Step 3: Implement — runtime.py**

(a) Runtime 属性区（Task 2 加的 `on_event` 行后）加：

```python
    cancel_check: Callable[[str], bool] | None = None  # v0.9: task_id → 已取消?
```

(b) Runtime 内加 `_is_cancelled`（放 `_emit` 旁）：

```python
    def _is_cancelled(self, task_id: str) -> bool:
        """查取消状态. checker 异常 → 视为未取消 (fail-open, 检查器 bug 不杀业务 run)."""
        if self.cancel_check is None:
            return False
        try:
            return bool(self.cancel_check(task_id))
        except Exception:
            logger.exception("cancel_check 异常 (视为未取消) task=%s", task_id)
            return False
```

(c) Ctx dataclass 加字段（`deadline` 行后）+ 方法（`call` 方法后）：

```python
    cancel_check: Callable[[], bool] | None = None  # v0.9: bound 到本 task 的取消检查
```

```python
    def cancelled(self) -> bool:
        """协作式取消轮询: True = caller 要求取消本 task.

        stage 内长循环 (批量 LLM 调用等) 自行决定轮询频率与退出方式 —
        runtime 只在 stage 边界强制拦截.
        """
        return bool(self.cancel_check and self.cancel_check())
```

(d) Ctx 构造点（全库唯一, `stageflow/runtime.py:473` 附近, `_run_stage` 内）加 kwarg：

```python
                    cancel_check=(lambda: self._is_cancelled(task_id)) if self.cancel_check else None,
```

(e) _run_stage 循环顶（`while True:` 后、`attempt += 1` 前）加拦截：

```python
            if self._is_cancelled(task_id):
                logger.warning("task=%s run=%s stage=%s 取消拦截 (attempt 前)",
                               task_id, run_id[:8], name)
                self._emit("stage_end", {"task_id": task_id, "run_id": run_id,
                                         "stage": name, "attempt": attempt,
                                         "status": "cancelled",
                                         "duration": time.time() - _t0,
                                         "error": "cancelled by caller"})
                return "cancelled", state, "stage 已取消 (cancelled by caller)", producers, None
```

(f) run() 主循环：`if status == "failed":` 分支**之前**加：

```python
            if status == "cancelled":
                result.status = "cancelled"
                result.error = err
                result.stage_statuses[name] = "cancelled"
                self._emit("run_end", {"task_id": task_id, "run_id": run_id,
                                       "dag": dag.name, "status": "cancelled"})
                logger.warning(
                    "task=%s run=%s cancelled at stage=%s (已完成 %d stages 已落 cp, resume 可续跑)",
                    task_id, run_id[:8], name, len(done_stages),
                )
                return result
```

注意必须 `return result` 而非 `break` —— for 循环尾部有 `result.status = "done"`，break 会覆盖取消态。

(g) `stageflow/types.py` RunResult docstring 的 status 行改为：

```
        status: "running" | "done" | "failed" | "cancelled"
```

（`stage_statuses` docstring 值域同步加 "cancelled"。）

- [ ] **Step 4: Run tests + 全量回归**

```bash
python -m pytest tests/test_cancel.py tests/test_events.py tests/ -q && python -m pyflakes stageflow tests
```

Expected: 3 个新测试 PASS，全量 106 passed，pyflakes 0。特别确认既有 resume 测试（test_checkpoint.py / test_fork.py）不受影响。

- [ ] **Step 5: Commit**

```bash
git add stageflow/runtime.py stageflow/types.py tests/test_cancel.py
git commit -m "feat(runtime): 协作式取消 — cancel_check 检查点拦截 + ctx.cancelled() 轮询"
```

---

### Task 4: 文档同步 (README ×2 + CHANGELOG)

**Files:**
- Modify: `stageflow/README.cn.md`（§特性 表 ~53-64；§快速开始 或 §核心概念 若有 Runtime 构造示例可补一行）
- Modify: `stageflow/README.md`（英文版同表）
- Modify: `stageflow/CHANGELOG.md`（顶部）

**Interfaces:**
- Consumes: Task 1-3 的最终行为描述
- Produces: 无代码。

- [ ] **Step 1: README.cn.md §特性 表加两行**

在 `| **零依赖** |` 行前插入：

```markdown
| **协作式取消** | `Runtime(cancel_check=...)` — stage 间/重试间检查点拦截 (status="cancelled", 已完成 stage 照常落 cp, resume 无缝续跑); stage 内长循环 `ctx.cancelled()` 轮询自退出 |
| **事件钩子** | `Runtime(on_event=...)` — run_start / stage_start / stage_end / stage_retry / run_end 五种结构化事件 (observer 异常隔离); `RunResult.stage_timings` 每 stage 墙钟耗时 |
```

- [ ] **Step 2: README.md 英文版同表加对应两行**

```markdown
| **Cooperative cancellation** | `Runtime(cancel_check=...)` — interception at stage/retry boundaries (status="cancelled"; completed stages stay checkpointed, resume picks up seamlessly); long-running stages poll `ctx.cancelled()` and exit on their own |
| **Event hooks** | `Runtime(on_event=...)` — five structured lifecycle events: run_start / stage_start / stage_end / stage_retry / run_end (observer exceptions isolated); `RunResult.stage_timings` per-stage wall clock |
```

- [ ] **Step 3: CHANGELOG.md 顶部（`## [0.8.0]` 之前）加**

```markdown
## [Unreleased]

### Added

- 协作式取消: `Runtime(cancel_check=...)` — stage 间/重试间检查点拦截, `RunResult.status`
  新增 `"cancelled"`; 已完成 stage 照常落 checkpoint, `resume=True` 无缝续跑.
  stage 内长循环用 `ctx.cancelled()` 轮询自行退出 (循环留业务层).
  cancel_check 异常视为未取消 (fail-open).
- 生命周期事件钩子: `Runtime(on_event=...)` — run_start / stage_start / stage_end /
  stage_retry / run_end 五种结构化事件, observer 异常隔离. `RunResult.stage_timings`
  记录每 stage 墙钟耗时 (含 retry 退避). 单 stage 重放 (run_stage) 不发事件.
- 重试退避 full-jitter: `uniform(0, min(2^(attempt-1), 30))` — 多 task 同步重试防雷群.
```

- [ ] **Step 4: 全量回归 + Commit**

```bash
python -m pytest tests/ -q
git add README.cn.md README.md CHANGELOG.md
git commit -m "docs: 协作式取消 + 事件钩子 + full-jitter 特性入 README/CHANGELOG"
```

---

## Self-Review 记录

- **Spec coverage**: 审计 #2 取消 → Task 3；#3 事件钩子 → Task 2；#5 jitter → Task 1；审计其余项（并行/CI/SECURITY/tag/docs site）明确 out-of-scope（Spec 节已声明）。✓
- **Placeholder scan**: 4 个 task 全部含精确代码/测试文本；无 TBD/"add appropriate"。✓
- **Type consistency**: `_emit(event, data)` 签名在 Task 2 定义、Task 3 复用一致；`on_event` / `cancel_check` 属性名跨 task/测试/文档一致；`"cancelled"` 状态值在 _run_stage 返回、run() 分支、types docstring、README 四处一致。✓
- **已知风险声明**: run() 的 failed 分支是否体内 `return result` 未逐行确认（读段截断于 line 247）——Task 2 Step 4(e) 已强制 executor 先 grep `return result` 确认全部出口再加 run_end emit；Task 3(f) 用 `return result` 规避 for 尾部 `status="done"` 覆盖。executor 每步先 Read 目标区域。
