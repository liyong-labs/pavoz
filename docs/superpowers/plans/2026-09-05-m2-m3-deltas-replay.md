# stageflow v0.5.1: M2 stage_deltas + M3 replay 调试能力

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal**: (M2) checkpoint 存每 stage 原始输出 `stage_deltas` + `initial_state`，可重建任意 stage 前的 state；(M3) 单 stage 重放调试能力（`Runtime.run_stage()` + CLI `replay --stage --patch`），复用已保存的 state，秒级迭代 prompt/参数，不重跑前序 stage、不污染 checkpoint。

**Architecture**:
- M2: Checkpoint 加 `stage_deltas: dict[stage_name, return_delta]`（按完成顺序）+ `initial_state: dict`。`rebuild_state_before(stage_name)` = 顺序 merge initial + deltas（chain-overwrite 由完成顺序天然处理）。加字段用 from_dict .get 默认 → 向后兼容（A7 已锁定此模式）。
- M3: `Runtime.run_stage()` 加载 task 最新 cp → hash 校验 → rebuild 目标 stage 前 state → 只跑该 stage（retries 生效）→ **不落 checkpoint**（A6/A7 锁定：防 pointer 污染）。CLI `replay <dag.py> --task-id X --stage Y [--patch P.py]`，patch 文件约定 `patch(dag) -> None`。
- 顺带修 R5（CLI checkpoint 装配洞）：`run` 命令恒 attach CheckpointStore（不再只 `--resume` 时）→ CLI run 落盘，trace/state/replay 对 CLI 产物可用。

**Tech Stack**: Python 3.12+, stdlib only, pytest, ruff.

**Spec**: ROADMAP v0.2 M2/M3 + plan A7 依赖链锁定（M3 需 M2）。前置 v0.5.0 已 ship（run_id/pointer/CallMeta/done guard）。

## Global Constraints

- Python 3.12+, stdlib only（pyproject dependencies 保持 `[]`）
- stage fn 签名不变 `async def fn(ctx) -> dict`
- checkpoint 加字段向后兼容：`from_dict` 用 `.get` 默认，旧 cp 可读
- replay **不落 checkpoint**、不动 `latest` 指针（A6/A7）
- 61 existing tests 全绿 + ruff clean
- docs/superpowers/ 文件不 commit

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `stageflow/checkpoint.py` | Modify | Checkpoint + `stage_deltas` + `initial_state` + `rebuild_state_before()` |
| `stageflow/runtime.py` | Modify | `_run_stage` 返 delta；`run()` 收集 deltas；`_save_cp` 传新字段；新 `run_stage()` |
| `stageflow/cli.py` | Modify | `run` 恒 attach store（修 R5）；新 `replay` 命令 |
| `stageflow/testing.py` | Modify | `TestPipe.replay_from()` |
| `tests/test_deltas.py` | Create | M2 存储 + rebuild 测试 |
| `tests/test_replay.py` | Create | M3 run_stage + CLI 测试 |
| `tests/test_cli_replay.py` | Create | 或并入 test_replay.py |
| `stageflow/__init__.py` + `pyproject.toml` | Modify | 0.5.1（Task 4） |
| `CHANGELOG.md` / `README.md` / `docs/en+cn` | Modify | Task 4 |

---

### Task 1: Checkpoint 加 stage_deltas + initial_state + rebuild_state_before

**Files:**
- Modify: `stageflow/checkpoint.py`
- Test: `tests/test_deltas.py` (new)

**Interfaces:**
- Consumes: 现有 `Checkpoint`（v0.5.0: task_id/run_id/dag_name/workflow_hash/stage_statuses/state/done_stages/producers）
- Produces: `Checkpoint.stage_deltas`, `Checkpoint.initial_state`, `Checkpoint.rebuild_state_before(stage_name) -> dict`

- [ ] **Step 1: 写失败测试 `tests/test_deltas.py`**

```python
"""M2: stage_deltas 存储 + rebuild_state_before 重建任意 stage 前 state."""
import pytest

from stageflow.checkpoint import Checkpoint, workflow_hash
from stageflow.dag import DAG


def _dag():
    dag = DAG("d")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": ctx.state["a"] + 1}

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        return {"c": ctx.state["b"] + 1, "a": 99}  # chain-overwrite: s_c 覆盖 s_a 的 a

    return dag


def _cp(dag, initial=None, deltas=None, done=None):
    """构造一个已完成全部 3 stage 的 cp (含 deltas + initial)."""
    return Checkpoint(
        task_id="t-1",
        run_id="r-1",
        dag_name=dag.name,
        workflow_hash=workflow_hash(dag),
        stage_statuses={"s_a": "done", "s_b": "done", "s_c": "done"},
        state={"a": 99, "b": 2, "c": 3},  # 终态 (c 覆盖 a)
        done_stages=done or ["s_a", "s_b", "s_c"],
        producers={"a": "s_c", "b": "s_b", "c": "s_c"},  # a 的 producer 是最后写者 s_c
        initial_state=dict(initial or {}),
        stage_deltas=dict(deltas or {
            "s_a": {"a": 1},
            "s_b": {"b": 2},
            "s_c": {"c": 3, "a": 99},
        }),
    )


def test_checkpoint_round_trip_preserves_deltas_and_initial():
    dag = _dag()
    cp = _cp(dag, initial={"query": "x"})
    d = cp.to_dict()
    cp2 = Checkpoint.from_dict(d)
    assert cp2.initial_state == {"query": "x"}
    assert cp2.stage_deltas == {
        "s_a": {"a": 1},
        "s_b": {"b": 2},
        "s_c": {"c": 3, "a": 99},
    }


def test_from_dict_tolerates_missing_new_fields():
    """旧格式 cp (v0.5.0, 无新字段) → .get 默认 {} (A7 兼容)."""
    dag = _dag()
    d = _cp(dag).to_dict()
    del d["initial_state"]
    del d["stage_deltas"]
    cp = Checkpoint.from_dict(d)
    assert cp.initial_state == {}
    assert cp.stage_deltas == {}


def test_rebuild_state_before_first_stage_returns_initial():
    dag = _dag()
    cp = _cp(dag, initial={"query": "x"})
    rebuilt = cp.rebuild_state_before("s_a")
    assert rebuilt == {"query": "x"}


def test_rebuild_state_before_mid_stage():
    dag = _dag()
    cp = _cp(dag, initial={"query": "x"})
    rebuilt = cp.rebuild_state_before("s_c")
    # s_c 前: initial + delta(s_a) + delta(s_b) — s_c 自己的覆盖还没发生
    assert rebuilt == {"query": "x", "a": 1, "b": 2}


def test_rebuild_state_before_handles_chain_overwrite():
    dag = _dag()
    cp = _cp(dag)
    rebuilt = cp.rebuild_state_before("s_c")
    assert rebuilt == {"a": 1, "b": 2}  # a 还是 s_a 的 1, 未被 s_c 覆盖


def test_rebuild_unknown_stage_raises():
    dag = _dag()
    cp = _cp(dag)
    with pytest.raises(KeyError):
        cp.rebuild_state_before("s_nonexistent")
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd stageflow && python3 -m pytest tests/test_deltas.py -v`
Expected: FAIL（Checkpoint 无 initial_state/stage_deltas 字段 + rebuild_state_before 未定义）

- [ ] **Step 3: 实现**

修改 `stageflow/checkpoint.py`：

```python
@dataclass
class Checkpoint:
    """一次 runtime.run() (一个 run_id) 的持久化状态.

    stage_deltas + initial_state 合起来可重建任意 stage 前的 state
    (M3 replay 用). 旧 cp (v0.5.0) 无这两个字段 → from_dict .get 默认 {}.
    """

    task_id: str
    run_id: str
    dag_name: str
    workflow_hash: str
    stage_statuses: dict[str, str]
    state: dict[str, Any]
    done_stages: list[str]  # 按完成顺序
    # v0.1.1: state key → producer stage. 链式覆盖判定用.
    producers: dict[str, str] = field(default_factory=dict)
    # v0.5.1 (M2): 每 stage 的原始 return delta (按完成序) + run 的初始 state.
    initial_state: dict[str, Any] = field(default_factory=dict)
    stage_deltas: dict[str, dict[str, Any]] = field(default_factory=dict)

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
            "initial_state": self.initial_state,
            "stage_deltas": self.stage_deltas,
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
            initial_state=dict(d.get("initial_state") or {}),
            stage_deltas=dict(d.get("stage_deltas") or {}),
        )

    def rebuild_state_before(self, stage_name: str) -> dict[str, Any]:
        """重建 stage 执行前的 state = initial + 已完成且在该 stage 前的 deltas.

        顺序按 done_stages 完成序 merge (dict.update) — chain-overwrite 由
        完成顺序天然处理 (后完成者覆盖先完成者). 要求 stage_name 的依赖已全完成
        (即 stage_name in done_stages 或它的 depends_on ⊆ done_stages) —
        否则它之前的 delta 不齐, raise KeyError.

        Returns: 新 dict (不 mutate).
        """
        if stage_name not in self.done_stages and stage_name not in self.stage_deltas:
            raise KeyError(
                f"stage '{stage_name}' 不在 cp 的完成记录里 "
                f"(done_stages={self.done_stages}). 无法重建其执行前 state."
            )
        rebuilt = dict(self.initial_state)
        for done in self.done_stages:
            if done == stage_name:
                break
            delta = self.stage_deltas.get(done)
            if delta:
                rebuilt.update(delta)
        return rebuilt
```

注意 rebuild 里 `done == stage_name` 的 break 前提是 stage_name 在 done_stages —— 顺序 merge 到它之前。若 stage_name 不在 done_stages（异常场景），KeyError 已在开头拦。

- [ ] **Step 4: 跑测试确认通过**

Run: `pytest tests/test_deltas.py -v`
Expected: 6 PASS

- [ ] **Step 5: 全量回归**

Run: `pytest tests/ -q`
Expected: 67 passed (61 + 6)。若有既有测试构造 Checkpoint 报缺参——检查是否需要加默认值（本 task 已给两个新字段 default_factory → 既有 ctor 不需改）。

- [ ] **Step 6: ruff + commit**

Run: `ruff check stageflow/ tests/` → clean
Commit: `git add stageflow/checkpoint.py tests/test_deltas.py && git commit -m "feat(checkpoint): stage_deltas + initial_state + rebuild_state_before (M2)

每 stage 原始 return 按完成序存入 checkpoint; 与 initial_state 合起来
可重建任意 stage 执行前的 state (M3 replay 的地基).

加字段用 from_dict .get 默认 → v0.5.0 旧 cp 兼容 (A7 锁定模式).
6 new tests; 67 total."`

---

### Task 2: runtime 收集 deltas + run_stage()

**Files:**
- Modify: `stageflow/runtime.py`
- Test: `tests/test_replay.py` (new)

**Interfaces:**
- Consumes: Task 1 的 `Checkpoint.stage_deltas/initial_state/rebuild_state_before`
- Produces: `Runtime.run_stage(dag, task_id, stage_name, *, run_id=None) -> RunResult`；`run()` 落盘含 deltas

- [ ] **Step 1: 写失败测试 `tests/test_replay.py`**

```python
"""M3: Runtime.run_stage 单 stage 重放 (不落 cp)."""
import tempfile

import pytest

from stageflow import Runtime
from stageflow.checkpoint import CheckpointStore, workflow_hash
from stageflow.dag import DAG
from stageflow.storage import FileStorage


def _dag():
    dag = DAG("d")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": ctx.state["a"] + 1}

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        return {"c": ctx.state["b"] + 1}

    return dag


async def _run_full(dag, task_id="t-1"):
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        result = await runtime.run(dag, task_id=task_id, initial_state={"seed": 0})
        assert result.status == "done"
        return store, result


def _temp_store():
    tmp = tempfile.TemporaryDirectory()
    return CheckpointStore(FileStorage(root_dir=tmp)), tmp


async def test_full_run_saves_deltas():
    """run() 落盘的 cp 含 stage_deltas (M2 存储打通 runtime)."""
    dag = _dag()
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        await runtime.run(dag, task_id="t-1", initial_state={"seed": 0})
        cp = store.load_latest("t-1")
        assert cp.initial_state == {"seed": 0}
        assert set(cp.stage_deltas.keys()) == {"s_a", "s_b", "s_c"}


async def test_run_stage_replays_single_stage():
    """run_stage('s_b') 用重建 state, 只跑 s_b."""
    dag = _dag()
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        await runtime.run(dag, task_id="t-1", initial_state={"seed": 0})

        # 改 s_b 的实现 (模拟调 prompt/参数)
        calls = []
        original = dag.stages["s_b"].fn

        async def s_b_v2(ctx):
            calls.append(ctx.state["a"])  # 应看到 s_a 的输出 1
            return {"b": ctx.state["a"] * 10}

        dag.stages["s_b"].fn = s_b_v2
        try:
            result = await runtime.run_stage(dag, task_id="t-1", stage_name="s_b")
        finally:
            dag.stages["s_b"].fn = original

        assert result.status == "done"
        assert result.state["b"] == 10  # 新实现生效
        assert calls == [1]  # ctx.state.a = 1 (s_a 的 delta, 不是终态 3)


async def test_run_stage_does_not_touch_checkpoint():
    """run_stage 不落 cp 不动 pointer (A6/A7 防污染)."""
    dag = _dag()
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        await runtime.run(dag, task_id="t-1", initial_state={"seed": 0})
        cp_before = store.load_latest("t-1")
        runs_before = store.list_runs("t-1")

        await runtime.run_stage(dag, task_id="t-1", stage_name="s_b")

        cp_after = store.load_latest("t-1")
        assert cp_after.run_id == cp_before.run_id  # pointer 没动
        assert store.list_runs("t-1") == runs_before  # 没新增 run


async def test_run_stage_missing_cp_raises():
    dag = _dag()
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        with pytest.raises(RuntimeError, match="无 checkpoint"):
            await runtime.run_stage(dag, task_id="nobody", stage_name="s_a")


async def test_run_stage_unknown_stage_raises():
    dag = _dag()
    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        await runtime.run(dag, task_id="t-1", initial_state={"seed": 0})
        with pytest.raises(KeyError):
            await runtime.run_stage(dag, task_id="t-1", stage_name="s_nope")
```

- [ ] **Step 2: 跑测试确认失败**

Run: `pytest tests/test_replay.py -v`
Expected: FAIL（run_stage 不存在 + cp 无 stage_deltas 落盘）

- [ ] **Step 3: 实现 runtime 改动**

**3a. `_run_stage` 返回 delta** — 改签名：返回 `(status, new_state, error, producers, delta)`，成功时 delta = stage 的 return dict（非 None dict），失败时 `None`：

```python
# _run_stage 成功 return 处 (原 283 行附近):
                return "done", new_state, None, producers, delta
            except (StageError, FatalError) as e:
                return "failed", state, str(e), producers, None
            except (RetryableError, TimeoutError) as e:
                ...
                return "failed", state, str(e), producers, None
            except Exception as e:
                return "failed", state, f"FatalError: {e}", producers, None
```

注意 `delta` 变量在 try 成功路径定义（`delta = await self._with_timeout(...)` 后已存在，`delta = {}` 的 None→{} 处理也在 return 前）——return 处直接引用。

**3b. `run()` 收集 deltas + initial_state** — 在 run() 内：

```python
        state: dict[str, Any] = {}
        done_stages: list[str] = []
        stage_statuses: dict[str, str] = {}
        producers: dict[str, str] = {}
        stage_deltas: dict[str, dict] = {}          # ← 新增
        initial_state_saved: dict = {}              # ← 新增
        ...
        if cp is not None:
            ...
            stage_deltas = dict(cp.stage_deltas)    # resume 续收集
            initial_state_saved = dict(cp.initial_state)
        ...
        if not done_stages and initial_state:
            state = merge_state({}, initial_state, "<init>")
            initial_state_saved = dict(initial_state)  # ← 存初始 (rebuild 需要)
            for _k in initial_state:
                producers[_k] = "<init>"
        ...
            status, new_state, err, producers, delta = await self._run_stage(...)
            ...
            if status == "done":
                if delta:
                    stage_deltas[name] = delta          # ← 收集 (完成序)
            if status == "failed":
                ...
                self._save_cp(task_id, run_id, dag, state, done_stages, stage_statuses,
                              producers, stage_deltas, initial_state_saved)
                return result
            done_stages.append(name)
            self._save_cp(task_id, run_id, dag, state, done_stages, stage_statuses,
                          producers, stage_deltas, initial_state_saved)
```

**3c. `_save_cp` 加 2 参**（带默认 None 以免破其他调用点；实际上 run() 两处调用都更新）：

```python
    def _save_cp(self, task_id, run_id, dag, state, done_stages, statuses,
                 producers=None, stage_deltas=None, initial_state=None) -> None:
        ...
            cp = Checkpoint(
                ...
                producers=dict(producers or {}),
                initial_state=dict(initial_state or {}),
                stage_deltas=dict(stage_deltas or {}),
            )
```

**3d. 新 `Runtime.run_stage()`** — 放在 run() 之后：

```python
    # ── 单 stage 重放 (M3 调试) ─────────────────────────
    async def run_stage(
        self,
        dag: DAG,
        task_id: str,
        stage_name: str,
    ) -> RunResult:
        """重放单 stage: 用已保存 cp 重建其执行前 state, 只跑该 stage.

        调试用 (改 prompt/参数后秒级看效果, 不重跑前序 stage).
        - 不落 checkpoint / 不动 latest pointer (A6/A7: 防 replay 污染 resume 目标)
        - ctx.run_id = 新 UUID4 (临时重放 run 标识, 只活在本次调用)
        - stage 的 depends_on 未全完成 → 无法重建完整输入 → 明确报错

        Args:
            dag: DAG (须与 cp 的 workflow_hash 一致, 否则 CheckpointMismatchError)
            task_id: 已有 checkpoint 的 task
            stage_name: 要重放的 stage (必须已完成过 — 才有 delta 可重建)
        """
        if self.checkpoint_store is None:
            raise RuntimeError("run_stage requires checkpoint_store")
        _validate_task_id(task_id)
        dag.validate()

        cp = self.checkpoint_store.load_latest(task_id)
        if cp is None:
            raise RuntimeError(
                f"task_id={task_id} 无 checkpoint, 不能重放. 先 run 一次."
            )
        cur_hash = workflow_hash(dag)
        if cp.workflow_hash != cur_hash:
            raise CheckpointMismatchError(
                f"task {task_id} run {cp.run_id[:8]} checkpoint 的 DAG hash "
                f"{cp.workflow_hash} ≠ 当前 DAG hash {cur_hash}. DAG 结构变了, "
                f"不能重放 (改依赖/retries/timeout 会变 hash)."
            )
        stage = dag.stages.get(stage_name)
        if stage is None:
            raise KeyError(f"stage '{stage_name}' 不在 DAG {dag.name} 里")

        # 重建 stage 前 state; stage 必须已完成过 (否则 deltas 不齐 → KeyError)
        state = cp.rebuild_state_before(stage_name)

        run_id = new_id()  # 临时重放 run; 不落 cp
        result = RunResult(task_id=task_id, dag_name=dag.name, run_id=run_id)
        result.state = state

        run_deadline = time.time() + self.default_timeout if self.default_timeout else None
        stage_deadline: float | None = None
        if stage.timeout is not None:
            stage_deadline = time.time() + stage.timeout
        if run_deadline is not None:
            stage_deadline = (
                min(stage_deadline, run_deadline) if stage_deadline is not None
                else run_deadline
            )

        status, new_state, err, _producers, delta = await self._run_stage(
            dag, stage.fn, stage_name, task_id, run_id, state, stage.retries,
            stage_deadline, dict(cp.producers),
        )
        result.state = new_state
        result.stage_statuses = {stage_name: status}
        if status == "failed":
            result.status = "failed"
            result.error = err
        else:
            result.status = "done"
        return result
```

注意：`rebuild_state_before` 若 stage 未完成过会 KeyError —— 测试 test_run_stage_unknown_stage_raises 期待 KeyError（stage 不在 DAG）。若 stage 在 DAG 但没跑过（如 DAG 新增 stage）→ rebuild 也会 KeyError（stage_name 不在 done_stages/deltas）→ 消息可读即可。

- [ ] **Step 4: 跑测试确认通过**

Run: `pytest tests/test_replay.py -v`
Expected: 5 PASS

- [ ] **Step 5: 全量回归**

Run: `pytest tests/ -q`
Expected: 72 passed (67 + 5)

- [ ] **Step 6: ruff + commit**

Run: `ruff check stageflow/ tests/` → clean
Commit: `git add stageflow/runtime.py tests/test_replay.py && git commit -m "feat(runtime): run() 收集 stage_deltas + run_stage() 单 stage 重放 (M3)

run(): 每 stage return 按完成序存入 checkpoint (M2 打通 runtime 层).
run_stage(): 加载最新 cp → hash 校验 → rebuild_state_before → 只跑目标
stage (retries 生效) → 不落 cp 不动 pointer (防 replay 污染 resume).
ctx.run_id = 临时 UUID4.

5 new tests; 72 total."`

---

### Task 3: CLI replay 命令 + run 恒 attach store (修 R5)

**Files:**
- Modify: `stageflow/cli.py`
- Test: `tests/test_cli_replay.py` (new — 或并入 test_replay.py；若 CLI 函数是 async 可直接调)

**Interfaces:**
- Consumes: Task 2 `Runtime.run_stage`
- Produces: `python -m stageflow replay <dag.py> --task-id X --stage Y [--patch P.py]`

- [ ] **Step 1: 写失败测试**

CLI 测试直接调 `_cmd_replay`（async fn）而非 subprocess：

```python
"""CLI replay 命令测试 (直接调 async _cmd_replay)."""
import json
import tempfile

from stageflow.cli import DEFAULT_STORAGE, _cmd_replay, _cmd_run
from stageflow.checkpoint import CheckpointStore
from stageflow.dag import DAG
from stageflow.storage import FileStorage
import stageflow.cli as cli_mod
import os


def _write_dag(tmp: str) -> str:
    """写一个真实 dag 文件 (与 _load_dag 的 import 机制兼容)."""
    path = os.path.join(tmp, "demo_dag.py")
    with open(path, "w") as f:
        f.write("""
from stageflow import DAG

dag = DAG("demo")

@dag.stage()
async def s_a(ctx):
    return {"a": 1}

@dag.stage(depends_on=["s_a"])
async def s_b(ctx):
    return {"b": ctx.state["a"] + 1}
""")
    return path


class _Args:
    """argparse.Namespace 替代 (测试用)."""
    def __init__(self, **kw):
        self.__dict__.update(kw)


async def test_cli_run_saves_checkpoint_even_without_resume(monkeypatch, tmp_path):
    """R5 修复: run 不带 --resume 也落盘 → replay/trace/state 可用."""
    dag_path = _write_dag(str(tmp_path))
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "store"))
    rc = await _cmd_run(_Args(dag=dag_path, task_id="cli-1", input=None, resume=False))
    assert rc == 0
    store = CheckpointStore(FileStorage(str(tmp_path / "store")))
    cp = store.load_latest("cli-1")
    assert cp is not None
    assert set(cp.stage_deltas.keys()) == {"s_a", "s_b"}


async def test_cli_replay_reruns_single_stage(monkeypatch, tmp_path):
    dag_path = _write_dag(str(tmp_path))
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "store"))

    await _cmd_run(_Args(dag=dag_path, task_id="cli-1", input=None, resume=False))
    rc = await _cmd_replay(_Args(dag=dag_path, task_id="cli-1", stage="s_b", patch=None))
    assert rc == 0
```

- [ ] **Step 2: 跑测试确认失败**

Run: `pytest tests/test_cli_replay.py -v`
Expected: FAIL（_cmd_replay 不存在；run 不带 --resume 不落盘）

- [ ] **Step 3: 实现**

**3a. `_cmd_run` 恒 attach store**（修 R5）：

```python
async def _cmd_run(args) -> int:
    dag = _load_dag(args.dag)
    task_id = args.task_id or f"run-{os.getpid()}"
    initial = {}
    if args.input:
        initial = json.loads(args.input)
    # v0.5.1 (R5 fix): 恒 attach store → CLI run 落盘, replay/trace/state 可用
    runtime = Runtime(checkpoint_store=CheckpointStore(_storage()))
    try:
        result = await runtime.run(
            dag, task_id, initial_state=initial, resume=args.resume
        )
    ...
```

**3b. 新 `_cmd_replay`**：

```python
async def _cmd_replay(args) -> int:
    """重放单 stage: stageflow replay <dag.py> --task-id X --stage Y [--patch P.py].

    patch 文件约定: 模块顶层暴露 patch(dag) -> None (import 后调用, 可改 stage fn).
    """
    dag = _load_dag(args.dag)
    if args.patch:
        patch_path = Path(args.patch).resolve()
        if not patch_path.exists():
            print(f"patch 文件不存在: {patch_path}")
            return 1
        spec = importlib.util.spec_from_file_location("_stageflow_patch", patch_path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules["_stageflow_patch"] = mod
        spec.loader.exec_module(mod)
        patcher = getattr(mod, "patch", None)
        if patcher is None:
            print(f"patch 文件 {patch_path} 需暴露 patch(dag) -> None")
            return 1
        patcher(dag)
    runtime = Runtime(checkpoint_store=CheckpointStore(_storage()))
    try:
        result = await runtime.run_stage(dag, task_id=args.task_id, stage_name=args.stage)
    except (CheckpointMismatchError, RuntimeError, ValueError, KeyError) as e:
        print(f"replay 失败: {e}")
        return 1
    print(json.dumps({
        "task_id": result.task_id,
        "run_id": result.run_id,
        "dag": result.dag_name,
        "stage": args.stage,
        "status": result.status,
        "error": result.error,
        "state": result.state,
    }, ensure_ascii=False, indent=2))
    return 0 if result.status == "done" else 1
```

**3c. argparse 注册**：

```python
    p_replay = sub.add_parser("replay", help="重放单 stage (调试: 改 prompt/参数秒级看效果)")
    p_replay.add_argument("dag", help="dag 文件路径")
    p_replay.add_argument("--task-id", required=True)
    p_replay.add_argument("--stage", required=True, help="要重放的 stage 名")
    p_replay.add_argument("--patch", default=None,
                          help="patch 文件 (暴露 patch(dag) -> None, 改 stage fn)")
    p_replay.set_defaults(fn=_cmd_replay)
```

同时更新模块 docstring（原 "replay 推 v0.2" 注释改为 "v0.5.1 已 ship"）。

- [ ] **Step 4: 跑测试确认通过**

Run: `pytest tests/test_cli_replay.py -v`
Expected: 2 PASS

- [ ] **Step 5: 全量回归 + 手动 smoke**

Run: `pytest tests/ -q` → 74 passed

手动 smoke（验证 patch 机制端到端）:
```bash
cd stageflow
# 1. 建临时 dag + patch
mkdir -p /tmp/sf-demo && cat > /tmp/sf-demo/dag.py <<'EOF'
from stageflow import DAG
dag = DAG("demo")
@dag.stage()
async def s_a(ctx):
    return {"a": 1}
@dag.stage(depends_on=["s_a"])
async def s_b(ctx):
    return {"b": ctx.state["a"] + 1}
EOF
cat > /tmp/sf-demo/patch.py <<'EOF'
def patch(dag):
    async def s_b_v2(ctx):
        return {"b": ctx.state["a"] * 10}
    dag.stages["s_b"].fn = s_b_v2
EOF
# 2. 跑一次 (落盘) → replay 原逻辑 → replay 带 patch
STAGEFLOW_STORAGE=/tmp/sf-demo/store python3 -m stageflow run /tmp/sf-demo/dag.py --task-id smoke1
STAGEFLOW_STORAGE=/tmp/sf-demo/store python3 -m stageflow replay /tmp/sf-demo/dag.py --task-id smoke1 --stage s_b
STAGEFLOW_STORAGE=/tmp/sf-demo/store python3 -m stageflow replay /tmp/sf-demo/dag.py --task-id smoke1 --stage s_b --patch /tmp/sf-demo/patch.py
```
Expected: run → done (b=2); replay → b=2; replay+patch → b=10

- [ ] **Step 6: ruff + commit**

Run: `ruff check stageflow/ tests/` → clean
Commit: `git add stageflow/cli.py tests/test_cli_replay.py && git commit -m "feat(cli): replay 命令 (--stage --patch) + run 恒 attach store (R5 fix)

replay <dag.py> --task-id X --stage Y [--patch P.py]:
- 加载最新 cp → run_stage 单 stage 重放 → JSON 输出 (含 state)
- patch 文件约定: patch(dag) -> None (改 stage fn, 不重跑前序)

R5 fix: CLI run 不再只在 --resume 时 attach CheckpointStore — 恒 attach,
CLI run 落盘 → replay/trace/state 对 CLI 产物可用.

2 new tests; 74 total."`

---

### Task 4: TestPipe.replay_from + docs + v0.5.1

**Files:**
- Modify: `stageflow/testing.py`
- Test: `tests/test_testpipe.py` (追加)
- Modify: `pyproject.toml` + `stageflow/__init__.py` (0.5.1)
- Modify: `CHANGELOG.md` + `README.md` + `docs/en|cn/api.md` + `docs/en|cn/architecture.md` + `ROADMAP.md` (M2/M3 标记 ship)

**Interfaces:**
- Consumes: Task 1 `Checkpoint.rebuild_state_before`, Task 2 deltas 落盘
- Produces: `TestPipe.replay_from(cp, *, run_from=None)`; v0.5.1 release

- [ ] **Step 1: 写失败测试（追加到 tests/test_testpipe.py）**

```python
async def test_replay_from_reproduces_full_state():
    """从真实 cp 全 mock 重放 → 终态与 cp.state 一致 (回归: 图行为没坏)."""
    import tempfile

    from stageflow.checkpoint import CheckpointStore
    from stageflow.storage import FileStorage

    dag = _dag()  # 复用文件顶部的 2-stage DAG

    with tempfile.TemporaryDirectory() as tmp:
        store = CheckpointStore(FileStorage(root_dir=tmp))
        runtime = Runtime(checkpoint_store=store)
        await runtime.run(dag, task_id="t-replay", initial_state={"items": [1, 2, 3]})
        cp = store.load_latest("t-replay")

        pipe = TestPipe.replay_from(cp, dag)
        result = await pipe.run()
        assert result.status == "done"
        assert result.state == cp.state
```

- [ ] **Step 2: 跑测试确认失败**

Run: `pytest tests/test_testpipe.py::test_replay_from_reproduces_full_state -v`
Expected: FAIL（replay_from 不存在）

- [ ] **Step 3: 实现 TestPipe.replay_from**

`testing.py` 加 classmethod：

```python
    @classmethod
    def replay_from(cls, cp, dag: DAG) -> TestPipe:
        """从真实 Checkpoint 构造 TestPipe: 已完成的 stage 用历史 delta 喂 (mock),
        未完成的走真实现 — 图回归夹具 (M2, ROADMAP).

        前提: cp 与 dag 的 workflow_hash 一致 (DAG 结构没变).
        用法: 真跑存 cp → 改 stage 实现 → replay_from 全 mock 回归对比终态.
        """
        pipe = cls(dag, initial_state=dict(cp.initial_state))
        for stage_name in cp.done_stages:
            delta = cp.stage_deltas.get(stage_name)
            if delta is not None:
                pipe.mock(stage_name, lambda _state, _d=delta: dict(_d))
        return pipe
```

注意 lambda 闭包用默认参 `_d=delta` 防迟绑定。mock fn 签名 `lambda state -> dict` 与现有 MockFn 一致。

- [ ] **Step 4: 跑测试确认通过**

Run: `pytest tests/test_testpipe.py::test_replay_from_reproduces_full_state -v`
Expected: PASS

- [ ] **Step 5: 版本 + changelog + docs**

- `pyproject.toml`: version 0.5.0 → 0.5.1
- `stageflow/__init__.py`: `__version__ = "0.5.1"` + 若有 usage docstring 检查 run_stage/replay 是否要提（不必细列）
- `CHANGELOG.md` prepend [0.5.1]:
  - Added: Checkpoint.stage_deltas + initial_state (per-stage output replay); rebuild_state_before; Runtime.run_stage(); CLI replay --stage --patch; TestPipe.replay_from; CLI run 恒落盘 (R5)
  - Changed: CLI run 不再只在 --resume 落盘（行为：单跑也写 ~/.stageflow/data）
  - 兼容: 加字段 .get 默认, v0.5.0 cp 可读
- `README.md`: Features 加 "Stage replay" 行 + CLI 示例（replay 命令）；test count → 75
- `docs/en|cn/api.md`: Checkpoint 新字段 + rebuild_state_before + Runtime.run_stage + CLI replay
- `docs/en|cn/architecture.md`: §run identity 后补 replay 语义（不落 cp/pointer）
- `ROADMAP.md`: M2/M3 标记 **已 ship (v0.5.1)**

- [ ] **Step 6: 全量验证 + commit + tag**

Run:
```bash
cd stageflow && python3 -m pytest tests/ -q   # 75 passed
python3 -m ruff check stageflow/ tests/                # clean
python3 -c "import stageflow; print(stageflow.__version__)"  # 0.5.1
```
Commit: `git add -A stageflow tests pyproject.toml CHANGELOG.md README.md docs/ 2>/dev/null; git commit -m "docs: v0.5.1 M2+M3 (stage_deltas + replay) release"`（注意别 git add docs/superpowers/）
Tag: `git tag v0.5.1`（local only）

---

## Self-Review

**1. Spec coverage**: M2（deltas 存储 + rebuild + replay_from）✓ T1/T2/T4；M3（run_stage + CLI replay + patch）✓ T2/T3；R5 fix ✓ T3；A6/A7（replay 不落 cp）✓ T2 测试显式断言；docs ✓ T4。

**2. Placeholder scan**: 全部步骤有具体代码。Task 2 Step 3 依赖实现者读现 runtime.py 精确落点——已给行号锚点 + 语义。

**3. Type consistency**:
- `Checkpoint.stage_deltas: dict[str, dict]`（T1）→ run() 收集 `stage_deltas[name] = delta`（T2）✓
- `rebuild_state_before(stage_name) -> dict`（T1）→ run_stage 用（T2）✓
- `_run_stage` 5-tuple（T2 改）→ run() 与 run_stage() 双调用点同步 ✓（T2 步骤明确两处）
- `TestPipe.replay_from(cp, dag)`（T4）✓

**4. 既有调用点风险**：`_run_stage` 返回加 delta —— 唯一调用者是 run()（已改）。无外部消费者（私有方法）。

---

## Execution

Plan complete. 4 tasks: T1 checkpoint 存储层 → T2 runtime 层 → T3 CLI → T4 TestPipe+docs+tag。
