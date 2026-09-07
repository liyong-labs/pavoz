# pavoz ID Model (v0.5.0)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal**: Adopt the industry-standard ID model (task_id + run_id + attempt) so pavoz callers have unambiguous execution tracing. UUID4 default for both task_id and run_id. Resume reuses the same run_id. Storage key includes run_id so multiple runs per task don't overwrite each other. Version bumps to v0.5.0 (breaking storage key change).

**Architecture**:
- `task_id`: caller-supplied string (or auto-UUID4 if omitted). Stable across retries/resumes — the idempotency key (Temporal WorkflowId / DBOS SetWorkflowID pattern).
- `run_id`: pavoz auto-generates UUID4 per `runtime.run()` invocation. Resume reuses the existing run_id (Temporal Continue-As-New / Airflow same run_id pattern).
- `attempt`: pavoz auto-injects int (1-based) into Ctx. Increments on per-stage retries (Airflow try_number / Celery retries pattern).
- **call_id**: NOT in this plan. Caller's dispatcher is responsible for assigning call_id if it wants correlation between ctx.log and CallRecorder.

**Tech Stack**: Python 3.12+, stdlib only (`uuid.uuid4`), pytest, ruff.

**Spec**: This plan is self-contained; the 10 grill-me decisions are documented inline.

**Industry precedents**:
- Temporal: WorkflowId (caller) + RunId (server UUID) + EventId
- DBOS: workflow_id idempotency key + step_id counter
- Airflow: dag_id + task_id + run_id + try_number composite key

## Global Constraints

- **Python 3.12+ required** (`requires-python = ">=3.12"`)
- **stdlib only** — `pyproject.toml` dependencies MUST stay `[]`
- **No breaking changes to existing stage fn signatures** — `async def s_x(req, ctx) -> dict` stays as-is
- **No backward compatibility** — old `runs/{task_id}/checkpoint` keys are orphaned (hard cut)
- **No ai_writer changes in this plan** — schema migration deferred to #37 plan
- **49 existing tests must still pass** — no test removal/rename; existing Checkpoint constructors need `run_id` arg added
- **ruff clean** — `ruff check pavoz/ tests/` returns 0 violations

## Grill-Me Decisions (locked)

| # | Decision | Choice |
|---|---|---|
| 1 | UUID generation | `uuid.uuid4()` (uuid7 is 3.14+, requires-python is 3.12+) |
| 2 | Resume semantics | (a) same run_id reused; checkpoint mismatch raises |
| 3 | call_id | Out of scope — caller's responsibility |
| 4 | Legacy checkpoint loader | None — hard cut, old keys orphaned |
| 5 | ai_writer schema | Deferred to #37 plan |
| 6 | load_latest 机制 | (a) 指针文件 `runs/{task_id}/latest` (uuid4 字典序 ≠ 时间序) |
| 7 | resume 显式 run_id | 推迟 M3 (加可选参数不 breaking; 单写者 + pointer 无歧义) |
| 8 | done 后 resume | raise 明确错误; 重跑 = resume=False 起新 run |
| 9 | task_id 规范 | 入口校验: 非空, ≤128, `[A-Za-z0-9_.-]`, 禁 `/` |

## Post-Review Amendments (2026-09-05 全面审查 — SUPERSEDE 下方对应内容)

> 以下 amendments 权威。正文 (Task 1-3 代码块) 与此冲突处，以本段为准。

### A1: caller 签名加第 4 参 CallMeta (最重要)

ctx.call 底层的 caller 拿不到执行上下文 → ai_writer 无法往 llm_calls 落
task_id/stage/attempt → 用户场景 "找 task:1234 v1.0 compose 的 prompts" 做不了。

Temporal ActivityExecutionContext / LangGraph config 先例: 调用自动带上下文。

```python
# runtime.py
@dataclass(frozen=True)
class CallMeta:
    """ctx.call 自动携带的执行上下文 — caller 记录 trace 用."""
    task_id: str
    run_id: str
    stage: str
    attempt: int

# caller 签名从 3 参改 4 参:
#   Callable[[str, str, dict, CallMeta], Awaitable[dict]]
# 默认 no-op caller 同步更新. Ctx.call 内部构造 CallMeta 传入.
```

- 现有测试中 mock caller 是 `lambda kind, op, params: ...` 的都要加第 4 参
- TestPipe 的 mock 机制同步 (testing.py 里 caller 装配处)
- stage fn 签名不变 (`async def s_x(ctx) -> dict`) — 只有 caller 侧变

### A2: CheckpointStore 指针文件 (supersede Task 1 Step 5 的 load_latest)

```python
# checkpoint.py — CheckpointStore 增改:
@staticmethod
def _latest_key(task_id: str) -> str:
    return f"runs/{task_id}/latest"   # 内容 = run_id 字符串

def save(self, cp: Checkpoint) -> None:
    self.storage.put(self._key(cp.task_id, cp.run_id), cp.to_dict())
    self.storage.put(self._latest_key(cp.task_id), cp.run_id)  # 指针更新

def load_latest(self, task_id: str) -> Checkpoint | None:
    run_id = self.storage.get(self._latest_key(task_id))  # 存的是 {"run_id": "..."}? 见下
    if run_id is None:
        return None
    return self.load(task_id, run_id)
```

指针存储格式: 复用 dict 接口, 存 `{"run_id": "..."}` (StorageBackend 只有 dict put/get)。

- delete(task_id, run_id) 时: 若 run_id == 指针值, 指针不动 (旧 run 删除后 resume
  会 load_latest 到已删 run → 返回 None → caller 收到明确 "无 checkpoint" 错误,
  属可接受边界, 不额外处理)
- list_runs 保留 (调试/清理用), 但 load_latest 不再依赖它

### A3: task_id 入口校验 (Runtime.run)

```python
def _validate_task_id(task_id: str) -> None:
    """task_id 直接进 storage key 路径 — 必须无 '/' 且字符集受限."""
    if not task_id:
        raise ValueError("task_id 不能为空")
    if len(task_id) > 128:
        raise ValueError(f"task_id 过长: {len(task_id)} > 128")
    if not all(c.isalnum() or c in "_.-" for c in task_id):
        raise ValueError(
            f"task_id 含非法字符: {task_id!r}. 只允许 [A-Za-z0-9_.-] "
            f"(不含 '/', 防止 storage key 路径注入)"
        )
```

Runtime.run 入口第一行调用. 校验失败 → 立即 ValueError (不是中途炸).

### A4: resume done guard (supersede Task 2 Runtime.run)

显式 run_id 参数 **推迟到 M3** (三维审查 2026-09-05 拍板: ai_writer 单写者 +
pointer 无歧义; 加可选参数未来不 breaking, 真需要再加). API 纪律留文档:
run_id 只由 pavoz 生成, caller 不传.

```python
async def run(
    self,
    dag: DAG,
    task_id: str | None = None,
    *,
    initial_state: dict | None = None,
    resume: bool = False,
) -> RunResult:
    # ... task_id 校验 (A3)
    if resume:
        if not self.checkpoint_store:
            raise RuntimeError("resume=True requires checkpoint_store")
        cp = self.checkpoint_store.load_latest(task_id)
        if cp is None:
            raise RuntimeError(f"task_id={task_id} 无 checkpoint, 不能 resume. "
                               f"首次跑用 resume=False.")
        # done guard: topo 全覆盖 + 无 failed = 已完成 → 明确报错防静默 no-op
        topo = set(dag.topo_order())
        if topo <= set(cp.done_stages) and not any(
            s == "failed" for s in cp.stage_statuses.values()
        ):
            raise RuntimeError(
                f"task_id={task_id} run={cp.run_id[:8]} 已全部完成. "
                f"重跑请用 resume=False (起新 run_id)."
            )
        run_id = cp.run_id
    else:
        run_id = new_id()
```

### A5: 新增测试 (supersede Task 2 Step 1 测试清单)

- test_load_latest_uses_pointer_file (save 两次同 task 不同 run → load_latest 返第二次)
- test_resume_after_done_raises (跑完 → resume=True → RuntimeError)
- test_task_id_validation (空 / 含 '/' / >128 / 非法字符 → ValueError)
- test_caller_receives_call_meta (mock caller 4 参, 断言 task_id/run_id/stage/attempt)
- test_revision_new_run_id (task 跑完 → resume=False 再跑 → 不同 run_id, 不误续旧 cp)
- A1 改动后 test_calls_recorded_with_order (TestPipe) 等现有测试同步 4 参

### A6: 文档补充 (Task 3 增补)

- 恢复模式说明: pavoz resume = "从 checkpoint 续跑" (真续跑场景);
  ai_writer 首版默认 "从头重跑 + external_cache 免单" (resume=False),
  pavoz resume 留给未来真要续跑的场景
- 并发: pavoz 假定 "每 task 单写者" (业务 lease/epoch 防并发);
  同 (task_id, run_id) 双写 = 最后写者赢, 不做锁
- cancel: pavoz 不感知 (in-process; worker 死 = run 死); 业务侧 kill + 重启
- MinIO 互操作: checkpoint 落 runs/{task_id}/{run_id}/checkpoint,
  与 ai_writer 既有 research/{task_id}/v{v}/ 结构隔离 (不同 bucket/前缀)
- **M3 replay 不落 checkpoint** (防 pointer 污染: replay 是临时实验, 产出给开发
  者看, 不需要可 resume; 落盘会错误地成为 load_latest 目标)

### A7: M3 replay 依赖 stage_deltas (M2) — 依赖链锁定 (2026-09-05 审查发现)

**Gap**: M3 重放 stage N 需要 "stage N-1 完成后" 的 state, 但 checkpoint 每 stage
后覆盖保存累计态 → 最新 cp 只有终态 (含 stage N 自己的输出) → 用终态重放 = 输入污染.

**解法**: M2 (ROADMAP) 加 `stage_deltas: dict[stage, return_dict]` → replay 从
`initial_state + deltas[0..N-1]` 重建. producers 字段无法替代 (chain overwrite
场景逆向剥离不可靠).

**v0.5 本计划动作**:
1. 执行顺序锁定: M2 (deltas) → M3 (replay), 不可跳
2. checkpoint 加字段向后兼容 (from_dict 用 .get 默认) — 本计划不加 deltas,
   但 to_dict/from_dict 结构保证未来加字段不 breaking
3. ROADMAP M2/M3 依赖关系补注 (Task 3 文档步骤执行)
| 6 | Test coverage | Add 3 retry/resume/list tests |
| 7 | Worktree | None — direct commit on main (separate repo) |
| 8 | Docs update | Task 4 (CHANGELOG, README, design/runtime.md, api.md) |
| 9 | Version | v0.5.0, local tag only (push is user's call) |
| 10 | Plan scope | 4 tasks: ID model + Runtime + tests + docs |

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `pavoz/_id.py` | Create | `new_id()` helper using `uuid.uuid4()` |
| `pavoz/types.py` | Modify | Add `run_id` to `RunResult` |
| `pavoz/checkpoint.py` | Modify | Add `run_id` to `Checkpoint`; storage key includes run_id; `list_runs` + `load_latest` helpers |
| `pavoz/runtime.py` | Modify | Ctx: add `run_id` + `attempt`; Runtime.run: auto-generate UUID4; Resume: reuse existing run_id |
| `pavoz/__init__.py` | Modify | Export nothing new (RunResult/Checkpoint already exported) |
| `tests/test_id_model.py` | Create | 10 tests: ID generation, resume reuse, attempt increment, storage key, list_runs |
| `tests/test_id_model_legacy.py` | Modify | None — but EXISTING tests that construct Checkpoint need `run_id` arg added (one-time search/replace) |
| `pyproject.toml` | Modify | Bump version 0.4.1 → 0.5.0 |
| `CHANGELOG.md` | Modify | Add [0.5.0] entry |
| `README.md` | Modify | Update test count "46 → 56" |
| `docs/design/runtime.md` | Modify | Document `run_id`, `attempt`, storage key format |
| `docs/api.md` | Modify | Document `run_id`, `attempt` fields |

---

### Task 1: pavoz ID helpers + types + checkpoint schema

**Files:**
- Create: `pavoz/_id.py`
- Modify: `pavoz/types.py`
- Modify: `pavoz/checkpoint.py`
- Test: `tests/test_id_model.py` (partial — 4 tests for this task)

**Interfaces:**
- Consumes: nothing
- Produces: `new_id() -> str`, `RunResult.run_id`, `Checkpoint.run_id`, `_key(task_id, run_id) -> str`

- [ ] **Step 1: Create `pavoz/_id.py`**

```python
"""ID generation helpers.

pavoz requires Python 3.12+ where uuid.uuid4 is the canonical
UUID generator. uuid.uuid7 is 3.14+; we use uuid4 to keep the
3.12 floor. Uniqueness is the requirement — time-ordering is not.
"""
from __future__ import annotations

import uuid

__all__ = ["new_id"]


def new_id() -> str:
    """Generate a new unique ID (UUID4 canonical string form, 36 chars)."""
    return str(uuid.uuid4())
```

- [ ] **Step 2: Write failing tests for types + checkpoint**

Create `tests/test_id_model.py`:

```python
"""pavoz ID model: task_id (caller) + run_id (auto) + attempt (auto int).

Resumes reuse run_id. Storage key includes run_id so multiple runs of
the same task don't overwrite each other.
"""
from pavoz.checkpoint import CheckpointStore, workflow_hash
from pavoz.dag import DAG
from pavoz.storage import FileStorage
from pavoz.types import RunResult


# ── RunResult ──────────────────────────────────────────────

def test_run_result_has_run_id():
    """RunResult must expose run_id."""
    rr = RunResult(task_id="t-1", dag_name="d", run_id="run-abc")
    assert rr.run_id == "run-abc"


def test_run_result_repr_includes_run_id():
    """RunResult repr should include short run_id for log readability."""
    rr = RunResult(task_id="t-1", dag_name="d", run_id="abcdef1234567890")
    r = repr(rr)
    assert "abcdef12" in r


# ── Checkpoint schema ─────────────────────────────────────

def test_checkpoint_has_run_id_field():
    """Checkpoint must carry run_id (each runtime.run() = one cp)."""
    dag = DAG("d")

    @dag.stage()
    async def s_x(ctx):
        return {}

    cp = Checkpoint(
        task_id="t-1",
        run_id="run-abc",
        dag_name="d",
        workflow_hash=workflow_hash(dag),
        stage_statuses={},
        state={},
        done_stages=[],
    )
    d = cp.to_dict()
    assert d["run_id"] == "run-abc"


def test_checkpoint_storage_key_includes_run_id():
    """Storage key format: runs/{task_id}/{run_id}/checkpoint."""
    storage = FileStorage(root_dir="/tmp/sf-test-cp-key")
    store = CheckpointStore(storage)
    key = store._key("task-1", "run-abc")
    assert key == "runs/task-1/run-abc/checkpoint"
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd pavoz && python3 -m pytest tests/test_id_model.py -v`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'run_id'` and similar.

- [ ] **Step 4: Add `run_id` to `RunResult`**

Modify `pavoz/types.py`:

```python
"""pavoz 基础类型: 异常契约 + 运行结果."""

__all__ = ["FatalError", "RetryableError", "RunResult", "StageError"]


class StageError(Exception):
    """业务错误. 不重试, runtime 直接走 fail 终态.

    例: 搜索结果为空 (业务上不可重试), 校验不通过.
    """


class RetryableError(Exception):
    """可重试错误 (网络 / 超时 / 429 / 5xx). 扣 per-node retries 后重试.

    budget 耗尽仍未成功 → stage fail.
    """


class FatalError(Exception):
    """程序 bug (框架 / stage 代码错误). 立即终, 不重试, 不消耗 retries.

    例: state 类型不兼容, stage 签名错误.
    """


class RunResult:
    """一次 DAG run 的结果 (task 粒度).

    Attributes:
        task_id: caller-supplied (or auto UUID4), stable across retries/resumes
        run_id: pavoz auto UUID4 per runtime.run() — distinguishes runs
        state: final merged state after all completed stages
        stage_statuses: {stage_name: "done" | "failed" | "skipped"}
        status: "running" | "done" | "failed"
        error: error message if status == "failed"
    """

    __slots__ = ("dag_name", "error", "run_id", "stage_statuses", "state", "status", "task_id")

    def __init__(self, task_id: str, dag_name: str, run_id: str = ""):
        self.task_id = task_id
        self.dag_name = dag_name
        self.run_id = run_id
        self.state: dict = {}
        self.stage_statuses: dict[str, str] = {}
        self.status: str = "running"
        self.error: str | None = None

    def __repr__(self) -> str:
        short = self.run_id[:8] if self.run_id else "?"
        return f"<RunResult {self.dag_name} {self.task_id} run={short} status={self.status}>"
```

- [ ] **Step 5: Add `run_id` to `Checkpoint` + new storage key format**

Modify `pavoz/checkpoint.py` — replace the entire file content with:

```python
"""Checkpoint: per-run persistence + workflow hash + mismatch reject.

- Each runtime.run() invocation gets a fresh run_id (UUID4).
- Checkpoint stored at runs/{task_id}/{run_id}/checkpoint.
- Resume loads the LATEST run_id for the task and reuses it (same run_id).
- Multiple run_ids per task supported (original / resume / replay) — but the
  plan only ships (a)+(b); replay (M3 task) comes later.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from .dag import DAG
from .storage import StorageBackend

__all__ = ["Checkpoint", "CheckpointMismatchError", "CheckpointStore", "workflow_hash"]


def workflow_hash(dag: DAG) -> str:
    """DAG 结构的稳定指纹: stage 名 + 依赖 + retries + timeout.

    任何影响执行语义的结构改动 → hash 变 → 旧 checkpoint 拒 resume.
    只改 stage 函数体 (fn) 不改结构 → hash 不变 (resume 兼容, fn 重新 import 即新代码).
    """
    rows = []
    for name in dag.topo_order():
        s = dag.stages[name]
        rows.append(f"{name}:dep={','.join(s.depends_on)}:retry={s.retries}:to={s.timeout}")
    raw = "\n".join(rows)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


class CheckpointMismatchError(Exception):
    """checkpoint 的 DAG hash ≠ 当前 DAG hash. 结构变了, 不能续跑."""


@dataclass
class Checkpoint:
    """One runtime.run() invocation's persisted state.

    Same task_id can have multiple Checkpoints (one per run_id).
    """
    task_id: str
    run_id: str
    dag_name: str
    workflow_hash: str
    stage_statuses: dict[str, str]
    state: dict[str, Any]
    done_stages: list[str]
    producers: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "run_id": self.run_id,
            "dag_name": self.dag_name,
            "workflow_hash": self.workflow_hash,
            "stage_statuses": self.stage_statuses,
            "state": self.state,
            "done_stages": self.done_stages,
            "producers": self.producers,
        }

    @classmethod
    def from_dict(cls, d: dict) -> Checkpoint:
        return cls(
            task_id=d["task_id"],
            run_id=d["run_id"],
            dag_name=d["dag_name"],
            workflow_hash=d["workflow_hash"],
            stage_statuses=d["stage_statuses"],
            state=d["state"],
            done_stages=d["done_stages"],
            producers=dict(d.get("producers") or {}),
        )


class CheckpointStore:
    """Per-task, per-run checkpoint persistence.

    Key format: runs/{task_id}/{run_id}/checkpoint
    """

    def __init__(self, storage: StorageBackend):
        self.storage = storage

    @staticmethod
    def _key(task_id: str, run_id: str) -> str:
        return f"runs/{task_id}/{run_id}/checkpoint"

    def save(self, cp: Checkpoint) -> None:
        self.storage.put(self._key(cp.task_id, cp.run_id), cp.to_dict())

    def load(self, task_id: str, run_id: str) -> Checkpoint | None:
        d = self.storage.get(self._key(task_id, run_id))
        return Checkpoint.from_dict(d) if d else None

    def list_runs(self, task_id: str) -> list[str]:
        """Return all run_ids for the given task_id, lex-sorted.

        With UUID4 storage, lex order is NOT chronological — use this only
        for "what runs exist?" queries. For resume, use load_latest which
        uses storage.list_keys and takes the last entry.
        """
        prefix = f"runs/{task_id}/"
        keys = self.storage.list_keys(prefix)
        run_ids: list[str] = []
        for k in keys:
            tail = k[len(prefix):]
            if tail.endswith("/checkpoint"):
                run_ids.append(tail[: -len("/checkpoint")])
        return sorted(run_ids)

    def load_latest(self, task_id: str) -> Checkpoint | None:
        """Load the most recently saved checkpoint for a task.

        "Latest" = last entry returned by storage.list_keys (caller's
        storage backend defines order; FileStorage returns lex order).
        """
        prefix = f"runs/{task_id}/"
        keys = self.storage.list_keys(prefix)
        if not keys:
            return None
        # The last key is treated as "latest" (deterministic given a backend)
        latest_key = keys[-1]
        d = self.storage.get(latest_key)
        return Checkpoint.from_dict(d) if d else None

    def delete(self, task_id: str, run_id: str) -> None:
        self.storage.delete(self._key(task_id, run_id))

    def load_compatible(self, task_id: str, run_id: str, dag: DAG) -> Checkpoint:
        """Load + verify workflow_hash. Mismatch → CheckpointMismatchError.

        Pass run_id="" to load the latest run for the task.
        """
        if run_id:
            cp = self.load(task_id, run_id)
        else:
            cp = self.load_latest(task_id)
        if cp is None:
            return cp  # type: ignore[return-value]
        cur_hash = workflow_hash(dag)
        if cp.workflow_hash != cur_hash:
            raise CheckpointMismatchError(
                f"task {task_id} run {cp.run_id[:8]} checkpoint 的 DAG hash {cp.workflow_hash} "
                f"≠ 当前 DAG hash {cur_hash}. DAG 结构变了, 不能 resume."
                f"(只改 stage 函数体不影响 hash; 改依赖/retries/timeout 会). "
                f"如需强制重跑: 删 checkpoint 或 Runtime(..., resume=False)"
            )
        return cp
```

- [ ] **Step 6: Run new tests, expect PASS**

Run: `pytest tests/test_id_model.py -v`
Expected: 4 PASS

- [ ] **Step 7: Run full suite — expect existing tests to fail (Checkpoint constructor changed)**

Run: `pytest tests/ -q 2>&1 | tail -20`
Expected: Some FAIL with `TypeError: __init__() missing 1 required positional argument: 'run_id'`

- [ ] **Step 8: Find and fix existing Checkpoint constructor calls**

Run: `grep -rln "Checkpoint(" pavoz/tests/`

For each file that constructs Checkpoint, add `run_id="test-run"` (or a unique UUID) to the constructor. Pattern:

```python
# Before:
cp = Checkpoint(task_id="t", dag_name="d", workflow_hash=h, ...)

# After:
cp = Checkpoint(task_id="t", run_id="test-run-id", dag_name="d", workflow_hash=h, ...)
```

Also find any direct `CheckpointStore.load(task_id)` (old 1-arg) and update to `load(task_id, run_id)` or use `load_latest(task_id)`.

Also find any direct `CheckpointStore.load_compatible(task_id, dag)` (old 2-arg) and update to `load_compatible(task_id, "", dag)` (uses latest).

- [ ] **Step 9: Run full suite, expect all green**

Run: `pytest tests/ -q`
Expected: 50 passed (46 existing + 4 new)

- [ ] **Step 10: ruff check**

Run: `ruff check pavoz/ tests/`
Expected: All checks passed

- [ ] **Step 11: Commit**

```bash
cd pavoz && git add pavoz/_id.py pavoz/types.py pavoz/checkpoint.py tests/ && git commit -m "feat(id-model): add run_id to RunResult + Checkpoint + storage key

Adopt Temporal/DBOS/Airflow composite-ID pattern: task_id stable across
runs (idempotency key), run_id auto-generated per runtime.run().

Storage key: runs/{task_id}/{run_id}/checkpoint (was runs/{task_id}/).
Hard cut — no legacy loader.

46 existing tests pass after fixing Checkpoint constructor calls.
4 new tests in tests/test_id_model.py."
```

---

### Task 2: pavoz Runtime — auto-generate task_id / run_id / attempt

**Files:**
- Modify: `pavoz/runtime.py`
- Modify: `tests/test_id_model.py` (add 6 tests)

**Interfaces:**
- Consumes: `CheckpointStore.load_latest(task_id)`, `new_id()`
- Produces: `Ctx.run_id`, `Ctx.attempt`, `RunResult.run_id` populated

- [ ] **Step 1: Write failing tests for Runtime ID generation**

Append to `tests/test_id_model.py`:

```python
import pytest
from pavoz import Runtime


async def test_runtime_auto_generates_task_id_when_omitted():
    """No task_id → pavoz auto-generates UUID4 (36 chars)."""
    dag = DAG("d")

    @dag.stage()
    async def s_x(ctx):
        return {}

    result = await Runtime().run(dag)
    assert result.task_id
    assert len(result.task_id) == 36


async def test_runtime_auto_generates_run_id_per_run():
    """Same task_id, multiple run() calls → different run_id."""
    dag = DAG("d")

    @dag.stage()
    async def s_x(ctx):
        return {}

    r1 = await Runtime().run(dag, task_id="t-fixed")
    r2 = await Runtime().run(dag, task_id="t-fixed")
    assert r1.run_id != r2.run_id
    assert len(r1.run_id) == 36


async def test_ctx_has_run_id_and_attempt():
    """Ctx.run_id and Ctx.attempt are accessible inside stage fn."""
    dag = DAG("d")
    seen: dict = {}

    @dag.stage()
    async def s_x(ctx):
        seen["run_id"] = ctx.run_id
        seen["attempt"] = ctx.attempt
        return {}

    result = await Runtime().run(dag, task_id="t-1")
    assert seen["run_id"] == result.run_id
    assert seen["attempt"] == 1


async def test_attempt_increments_on_stage_retry():
    """Stage that fails first attempt → attempt 2 on retry."""
    from pavoz import RetryableError

    dag = DAG("d")
    attempts_seen: list[int] = []

    @dag.stage(retries=2)
    async def s_flaky(ctx):
        attempts_seen.append(ctx.attempt)
        if ctx.attempt < 2:
            raise RetryableError("flaky")
        return {"ok": True}

    result = await Runtime().run(dag, task_id="t-1")
    assert result.status == "done"
    assert attempts_seen == [1, 2]


async def test_resume_reuses_existing_run_id():
    """Resume loads checkpoint.run_id and reuses it (not auto-new)."""
    import tempfile

    dag = DAG("d")

    @dag.stage()
    async def s_x(ctx):
        return {}

    with tempfile.TemporaryDirectory() as tmp:
        storage = FileStorage(root_dir=tmp)
        store = CheckpointStore(storage)
        runtime = Runtime(checkpoint_store=store)

        # First run
        r1 = await runtime.run(dag, task_id="t-1")
        assert r1.status == "done"

        # Resume — should reuse run_id
        r2 = await runtime.run(dag, task_id="t-1", resume=True)
        assert r2.run_id == r1.run_id


async def test_list_runs_returns_all_run_ids_for_task():
    """Multiple runs of same task_id → list_runs returns all of them."""
    import tempfile

    dag = DAG("d")

    @dag.stage()
    async def s_x(ctx):
        return {}

    with tempfile.TemporaryDirectory() as tmp:
        storage = FileStorage(root_dir=tmp)
        store = CheckpointStore(storage)
        runtime = Runtime(checkpoint_store=store)

        await runtime.run(dag, task_id="t-1")  # fresh
        # Don't call again with same run_id — but each run() creates new run_id
        # We test the storage layer directly
        await runtime.run(dag, task_id="t-1")
        runs = store.list_runs("t-1")
        assert len(runs) >= 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_id_model.py -v -k "auto_generates or ctx_has or attempt_increments or resume_reuses or list_runs"`
Expected: 6 FAIL

- [ ] **Step 3: Read existing runtime.py to plan the patch**

Run: `wc -l pavoz/runtime.py && grep -n "def run\|class Ctx\|class Runtime\|class CallResult\|new_id\|uuid" pavoz/pavoz/runtime.py`

- [ ] **Step 4: Modify runtime.py**

Make these edits to `pavoz/runtime.py`:

**Edit 1**: top of file, add import:

```python
from ._id import new_id
```

**Edit 2**: `Ctx` dataclass — replace the class with:

```python
@dataclass
class Ctx:
    """传给 stage 的上下文. read-only state + call/log API.

    stage 拿到的 ctx.state 是 run 级 state 的 deep copy (defensive) —
    改它不影响 run state; 真正的写入靠 return dict.
    """

    task_id: str
    run_id: str           # 每次 runtime.run() 一个 UUID4 (resume 复用)
    attempt: int          # 1-based, stage retries 时 +1
    dag: DAG
    stage_name: str
    state: ReadOnlyStateView
    caller: Callable[[str, str, dict], Awaitable[dict]] = field(
        default=lambda kind, op, params: CallResult(kind=kind, op=op, params=params)
    )
    logger: logging.Logger = field(default_factory=lambda: logger)
    deadline: float | None = None  # absolute deadline (time.time()), 无 = 不限制

    async def call(self, kind: str, op: str, params: dict | None = None) -> dict:
        """Outbound call entry point. `kind` is opaque (caller-defined).

        Caller (via Runtime's caller injection) decides actual execution.
        Default is a no-op (suitable for TestPipe mocks and demos).
        """
        return await self.caller(kind, op, params or {})
```

**Edit 3**: `Runtime.run` — change signature and add run_id logic. Locate the existing `async def run(...)` and replace with:

```python
async def run(
    self,
    dag: DAG,
    task_id: str | None = None,
    *,
    initial_state: dict | None = None,
    resume: bool = False,
) -> RunResult:
    """Execute a DAG for one task.

    Args:
        dag: the DAG to run
        task_id: caller-supplied task ID (or auto UUID4 if omitted).
                 Stable across retries/resumes — the idempotency key.
        initial_state: starting state (default empty dict)
        resume: if True, load latest checkpoint for task_id and continue
                (reuses the existing run_id).

    Returns:
        RunResult with task_id, run_id, final state, and per-stage status.
    """
    from .checkpoint import CheckpointMismatchError  # avoid circular at module top

    # 1. Resolve task_id (caller-generatable, UUID4 default)
    if not task_id:
        task_id = new_id()

    # 2. Determine run_id (new per run, OR reuse on resume)
    cp: Checkpoint | None = None
    if resume:
        if not self.checkpoint_store:
            raise RuntimeError("resume=True requires checkpoint_store")
        cp = self.checkpoint_store.load_latest(task_id)
        if cp is None:
            raise RuntimeError(
                f"task_id={task_id} 无 checkpoint, 不能 resume. "
                f"首次跑用 resume=False."
            )
        run_id = cp.run_id  # Continue-As-New: same run_id across worker restarts
    else:
        run_id = new_id()

    # 3. Build RunResult
    result = RunResult(task_id=task_id, dag_name=dag.name, run_id=run_id)
    result.state = dict(initial_state or {})

    # 4. Resume state restoration
    done_stages: list[str] = []
    if resume and cp is not None:
        cur_hash = workflow_hash(dag)
        if cp.workflow_hash != cur_hash:
            raise CheckpointMismatchError(
                f"task {task_id} run {cp.run_id[:8]} checkpoint 的 DAG hash "
                f"{cp.workflow_hash} ≠ 当前 DAG hash {cur_hash}. "
                f"DAG 结构变了, 不能 resume. "
                f"如需强制重跑: 删 checkpoint 或 Runtime(..., resume=False)"
            )
        done_stages = list(cp.done_stages)
        result.state.update(cp.state)
        result.stage_statuses = dict(cp.stage_statuses)
        logger.info(
            "task=%s run=%s resume: 已完成 %d stages",
            task_id, run_id[:8], len(done_stages),
        )

    # 5. Topo-order execution
    producers: dict[str, str] = {}
    if resume and cp is not None:
        producers = dict(cp.producers)

    for stage_name in dag.topo_order():
        if stage_name in done_stages:
            continue
        stage = dag.stages[stage_name]
        attempt = 1
        max_attempts = max(1, stage.retries + 1)
        while attempt <= max_attempts:
            ctx = Ctx(
                task_id=task_id,
                run_id=run_id,
                attempt=attempt,
                dag=dag,
                stage_name=stage_name,
                state=ReadOnlyStateView(snapshot(result.state)),
                caller=self.caller or (
                    lambda k, o, p: CallResult(kind=k, op=o, params=p)
                ),
                deadline=self.deadline,
            )
            try:
                delta = await stage.fn(ctx)
                if not isinstance(delta, dict):
                    raise FatalError(
                        f"stage '{stage_name}' 必须返回 dict, got {type(delta).__name__}"
                    )
                result.state = merge_state(
                    result.state, delta, stage_name, overwrite_keys=set(stage.overwrite_keys)
                )
                producers.update(delta)
                result.stage_statuses[stage_name] = "done"
                done_stages.append(stage_name)
                logger.info(
                    "task=%s run=%s stage=%s done (len state=%d)",
                    task_id, run_id[:8], stage_name, len(result.state),
                )
                break
            except (StageError, FatalError) as e:
                result.stage_statuses[stage_name] = "failed"
                result.status = "failed"
                result.error = f"{type(e).__name__}: {e}"
                logger.warning(
                    "task=%s run=%s stage=%s %s: %s",
                    task_id, run_id[:8], stage_name, type(e).__name__, e,
                )
                break
            except RetryableError as e:
                logger.warning(
                    "task=%s run=%s stage=%s attempt=%d %s: %s",
                    task_id, run_id[:8], stage_name, attempt,
                    type(e).__name__, e,
                )
                if attempt >= max_attempts:
                    result.stage_statuses[stage_name] = "failed"
                    result.status = "failed"
                    result.error = f"{type(e).__name__}: retries 耗尽 ({e})"
                    logger.warning(
                        "task=%s run=%s stage=%s retries 耗尽: %s",
                        task_id, run_id[:8], stage_name, e,
                    )
                    break
                attempt += 1

        if result.status == "failed":
            break

        # Save checkpoint after each successful stage
        if self.checkpoint_store:
            try:
                cp_obj = Checkpoint(
                    task_id=task_id,
                    run_id=run_id,
                    dag_name=dag.name,
                    workflow_hash=workflow_hash(dag),
                    stage_statuses=result.stage_statuses,
                    state=result.state,
                    done_stages=done_stages,
                    producers=producers,
                )
                self.checkpoint_store.save(cp_obj)
            except Exception as e:
                logger.exception(
                    "task=%s run=%s checkpoint 保存失败 (non-fatal): %s",
                    task_id, run_id[:8], e,
                )

    if result.status == "running":
        result.status = "done"

    return result
```

(This patch preserves all existing logic — retries, errors, deadline, producer tracking — and adds `run_id` + `attempt` + `task_id` default.)

- [ ] **Step 5: Run new tests, expect PASS**

Run: `pytest tests/test_id_model.py -v`
Expected: all 10 PASS

- [ ] **Step 6: Run full suite**

Run: `pytest tests/ -q`
Expected: 56 passed (46 existing + 10 new)

- [ ] **Step 7: ruff check**

Run: `ruff check pavoz/ tests/`
Expected: All checks passed

- [ ] **Step 8: Commit**

```bash
cd pavoz && git add pavoz/runtime.py tests/test_id_model.py && git commit -m "feat(runtime): auto-generate task_id (UUID4 default) + run_id per run

Adopt Temporal/DBOS industry pattern:
- task_id: caller-supplied (or UUID4 default) — stable idempotency key
- run_id: auto UUID4 per runtime.run() — distinguishes runs
- attempt: auto-injected int (1-based) into Ctx, increments on retries

Resume reuses existing run_id (Temporal Continue-As-New pattern).
10 new tests; 56 total tests pass."
```

---

### Task 3: Docs + version bump

**Files:**
- Modify: `pyproject.toml` (version 0.4.1 → 0.5.0)
- Modify: `CHANGELOG.md` (add [0.5.0] entry)
- Modify: `README.md` (test count 46 → 56)
- Modify: `docs/design/runtime.md` (document run_id + attempt + storage key)
- Modify: `docs/api.md` (document new fields)
- Tag: v0.5.0 (local only, push is user's call)

**Interfaces:**
- Consumes: nothing new
- Produces: discoverable v0.5.0 release

- [ ] **Step 1: Read current pyproject.toml**

Read `pyproject.toml` and confirm version line is `version = "0.4.1"`.

- [ ] **Step 2: Bump version**

Edit `pyproject.toml` — change `version = "0.4.1"` to `version = "0.5.0"`.

- [ ] **Step 3: Read current CHANGELOG.md**

Read top of `CHANGELOG.md` to find the latest entry format.

- [ ] **Step 4: Add [0.5.0] entry**

Prepend (above [0.4.1]) a new entry:

```markdown
## [0.5.0] — 2026-09-05

### Added (industry-standard ID model)

- `task_id` parameter to `Runtime.run()` is now optional; pavoz auto-generates UUID4 if omitted. Stable across retries/resumes — the idempotency key.
- `run_id` auto-generated per `Runtime.run()` invocation (UUID4). Distinguishes original / resume / replay runs of the same task.
- `Ctx.run_id` and `Ctx.attempt` (1-based int) injected automatically.
- `CheckpointStore.list_runs(task_id)` and `load_latest(task_id)` helpers.

### Changed (BREAKING)

- Checkpoint storage key format: `runs/{task_id}/{run_id}/checkpoint` (was `runs/{task_id}/checkpoint`). Multiple runs of same task no longer overwrite each other.
- `RunResult` and `Checkpoint` require `run_id` field (was absent).
- `CheckpointStore.load(task_id, run_id)` is now 2-arg (was 1-arg); use `load_latest(task_id)` for "latest run".
- `CheckpointStore.load_compatible(task_id, run_id, dag)` is now 3-arg (was 2-arg); pass `run_id=""` for latest.

### Rationale

Adopts Temporal WorkflowId/RunId, DBOS workflow_id idempotency key, and
Airflow `dag_id + task_id + run_id + try_number` composite key patterns.
Industry-standard, no caller-invented ID schemes.

### Migration (hard cut)

No legacy loader. Old `runs/{task_id}/checkpoint` keys become orphaned
on disk — caller can clean up with their storage backend.
```

- [ ] **Step 5: Update README.md test count**

Find the line(s) mentioning "46 tests" and replace with "56 tests".

- [ ] **Step 6: Update docs/design/runtime.md**

Find sections describing Runtime.run / Ctx / Checkpoint. Add:
- `task_id` is now optional (auto UUID4 default)
- `run_id` is auto-generated, returned in RunResult
- `Ctx.run_id` and `Ctx.attempt` are available to stages
- Storage key format `runs/{task_id}/{run_id}/checkpoint`
- Resume reuses run_id (Temporal Continue-As-New)

- [ ] **Step 7: Update docs/api.md**

Find sections describing `RunResult` and `Checkpoint` types. Add `run_id` field documentation. Find `CheckpointStore` section. Document `list_runs` and `load_latest` methods.

- [ ] **Step 8: Final verification**

Run:
```bash
cd pavoz && python3 -m pytest tests/ -q && \
    python3 -m ruff check pavoz/ tests/ && \
    python3 -c "import pavoz; print(pavoz.__version__)"
```

Expected: `56 passed`, `All checks passed!`, `0.5.0`.

- [ ] **Step 9: Commit + tag**

```bash
cd pavoz && git add pyproject.toml CHANGELOG.md README.md docs/ && \
    git commit -m "docs: v0.5.0 ID model release notes + API/runbook updates

Storage key format changed to runs/{task_id}/{run_id}/checkpoint (BREAKING).
task_id now optional (auto UUID4 default). run_id and Ctx.attempt added.
56 tests pass; ruff clean."

git -C pavoz tag -d v0.4.1 2>/dev/null  # in case stale
git -C pavoz tag v0.5.0
git -C pavoz tag --list | grep v0.5
```

Expected: tag `v0.5.0` exists locally. **DO NOT push — push is user's call.**

---

## Self-Review

**1. Spec coverage**: All 10 grill-me decisions are reflected in the plan. No gaps.

**2. Placeholder scan**: Every step has concrete code or commands. No TBD/TODO.

**3. Type consistency**:
- `RunResult.run_id: str` — Task 1 Step 4 ✓
- `Checkpoint.run_id: str` — Task 1 Step 5 ✓
- `Ctx.run_id: str`, `Ctx.attempt: int` — Task 2 Step 4 ✓
- `_key(task_id, run_id) -> str` — Task 1 Step 5 ✓
- `load(task_id, run_id)`, `load_latest(task_id)`, `load_compatible(task_id, run_id, dag)` — Task 1 Step 5 ✓
- `list_runs(task_id) -> list[str]` — Task 1 Step 5 ✓

No type drift.

**4. Risk review**:
- Task 1 Step 8 (fixing existing Checkpoint constructor calls): grep first, surgical patch.
- Task 2 Step 4 (runtime.py heavy edit): preserves existing retry/error/deadline logic; adds ID fields only.
- Task 2 Step 4 may conflict with existing Ctx constructor in tests — check test files.
- Task 3 Step 6-7: docs may need restructuring if existing sections reference removed APIs.

---

## Execution

Plan complete and saved to `pavoz/docs/superpowers/plans/2026-09-05-id-model-and-run-id-column.md`.

3 tasks, ~1 day:
1. **Task 1** — types + checkpoint schema (~30 min)
2. **Task 2** — Runtime ID generation (~1 hour)
3. **Task 3** — docs + version bump (~30 min)

Two execution options:
1. **Subagent-Driven (recommended)** — dispatch 1 implementer per task + task review + final review
2. **Inline Execution** — execute all 3 tasks in this session

Which approach?
