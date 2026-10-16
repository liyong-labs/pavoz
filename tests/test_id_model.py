"""pavoz ID model: task_id (caller) + run_id (auto) + attempt (auto int).

Resumes reuse run_id. Storage key includes run_id so multiple runs of
the same task don't overwrite each other.
"""
import pytest

from pavoz import RetryableError, Runtime, StageError
from pavoz.checkpoint import Checkpoint, CheckpointStore, workflow_hash
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


# ── Runtime: task_id / run_id / attempt 自动生成 ─────────

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


# ── resume / pointer ────────────────────────────────────

def test_load_latest_uses_pointer_file(tmp_path):
    """Store-level: save 两次同 task 不同 run → load_latest 返第二次 (指针文件)."""
    dag = DAG("d")

    @dag.stage()
    async def s_x(ctx):
        return {}

    h = workflow_hash(dag)
    store = CheckpointStore(FileStorage(str(tmp_path)))
    store.save(Checkpoint(
        task_id="t-ptr", run_id="run-1", dag_name="d", workflow_hash=h,
        stage_statuses={"s_x": "done"}, done_stages=["s_x"],
        stage_deltas_visits={"s_x": [{"v": 1}]},
    ))
    store.save(Checkpoint(
        task_id="t-ptr", run_id="run-2", dag_name="d", workflow_hash=h,
        stage_statuses={"s_x": "done"}, done_stages=["s_x"],
        stage_deltas_visits={"s_x": [{"v": 2}]},
    ))

    latest = store.load_latest("t-ptr")
    assert latest is not None
    assert latest.run_id == "run-2"
    assert latest.state == {"v": 2}
    # 指针文件内容 = {"run_id": ...}; legacy task 级单 cp key 不存在
    assert store.storage.get("runs/t-ptr/latest") == {"run_id": "run-2"}
    assert store.storage.get("runs/t-ptr/checkpoint") is None
    assert store.list_runs("t-ptr") == ["run-1", "run-2"]


async def test_resume_reuses_existing_run_id(tmp_path):
    """Resume 中途失败的 task → 复用原 run_id, 已完成的 stage 不重跑."""
    dag = DAG("d")
    calls = {"b": 0}

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        calls["b"] += 1
        if calls["b"] == 1:
            raise StageError("第一次必失败")
        return {"b": 2}

    store = CheckpointStore(FileStorage(str(tmp_path)))
    runtime = Runtime(checkpoint_store=store)

    r1 = await runtime.run(dag, task_id="t-resume")  # s_b fail → checkpoint 保留
    assert r1.status == "failed"
    assert len(r1.run_id) == 36

    r2 = await runtime.run(dag, task_id="t-resume", resume=True)
    assert r2.status == "done"
    assert r2.run_id == r1.run_id  # resume 复用 run_id
    assert calls["b"] == 2  # 只重跑 s_b


async def test_resume_after_done_raises(tmp_path):
    """跑完 → resume=True → RuntimeError (done guard, 防静默 no-op)."""
    dag = DAG("d")

    @dag.stage()
    async def s_x(ctx):
        return {}

    store = CheckpointStore(FileStorage(str(tmp_path)))
    runtime = Runtime(checkpoint_store=store)
    r1 = await runtime.run(dag, task_id="t-done")
    assert r1.status == "done"

    with pytest.raises(RuntimeError, match="已全部完成"):
        await runtime.run(dag, task_id="t-done", resume=True)


async def test_revision_new_run_id(tmp_path):
    """跑完 → resume=False 再跑 = 新 run_id, 从头重跑不误续旧 cp."""
    dag = DAG("d")
    seen: list[str] = []

    @dag.stage()
    async def s_x(ctx):
        seen.append(ctx.run_id)
        return {"n": len(seen)}

    store = CheckpointStore(FileStorage(str(tmp_path)))
    runtime = Runtime(checkpoint_store=store)

    r1 = await runtime.run(dag, task_id="t-rev")
    assert r1.status == "done"
    r2 = await runtime.run(dag, task_id="t-rev", resume=False)
    assert r2.status == "done"
    assert r2.run_id != r1.run_id
    assert len(r2.run_id) == 36
    assert len(seen) == 2  # 从头重跑 (stage 又执行一次)
    latest = store.load_latest("t-rev")
    assert latest is not None and latest.run_id == r2.run_id


# ── task_id 校验 / CallMeta ─────────────────────────────

async def test_task_id_validation():
    """空 / 含 '/' / >128 / 非法字符 → ValueError; 合法字符集放行."""
    dag = DAG("d")

    @dag.stage()
    async def s_x(ctx):
        return {}

    rt = Runtime()
    for bad in ("", "a/b", "/lead", "x" * 129, "bad id!", "a:b"):
        with pytest.raises(ValueError, match="task_id"):
            await rt.run(dag, task_id=bad)
    # None → 自动 UUID4 (不校验, 另一 test 覆盖); 合法字符集 [A-Za-z0-9_.-] 通过
    r = await rt.run(dag, task_id="t-1.abc_XYZ9")
    assert r.status == "done"


async def test_task_id_rejects_unicode():
    """Unicode task_id → ValueError. str.isalnum() 把非 ASCII 当字母 (如 "任务"),
    字符集必须显式 ASCII-only [A-Za-z0-9_.-] (A3)."""
    dag = DAG("d")

    @dag.stage()
    async def s_x(ctx):
        return {}

    rt = Runtime()
    for bad in ("任务-1", "task_任务", "café", "ｔ-1"):  # 全角 ｔ 也非 ASCII
        with pytest.raises(ValueError, match="task_id"):
            await rt.run(dag, task_id=bad)
    # ASCII 组合照常放行
    r = await rt.run(dag, task_id="t-1.abc_XYZ9")
    assert r.status == "done"


async def test_caller_receives_call_meta():
    """caller 第 4 参 CallMeta: task_id/run_id/stage/attempt, frozen."""
    from pavoz.runtime import CallMeta

    dag = DAG("d")
    metas: list = []

    async def caller(kind, op, params, meta):
        metas.append(meta)
        return {}

    @dag.stage()
    async def s_x(ctx):
        await ctx.call("llm", "compose", {"q": 1})
        return {"ok": True}

    result = await Runtime(caller=caller).run(dag, task_id="t-meta")
    assert len(metas) == 1
    m = metas[0]
    assert isinstance(m, CallMeta)
    assert m.task_id == "t-meta"
    assert m.run_id == result.run_id
    assert m.stage == "s_x"
    assert m.attempt == 1
    with pytest.raises(AttributeError):  # frozen dataclass 不可写
        m.task_id = "hack"


# ── task_id 的 '.' 段: debug_dir 快照能落出目录外 (2026-10-16) ─────────────
#
# `FileStorage` 侧的同类洞在 test_storage_path_containment.py 里修过 (key 哈希
# + realpath 断言)。但 `_validate_task_id` 的字符集 `[A-Za-z0-9_.-]` **允许 '.'**,
# 于是 `..` 与 `.` 都是合法 task_id —— 而 `_dump_debug` 把它直接当路径段用:
#
#     d = os.path.join(self.debug_dir, task_id)     # task_id=".." → debug_dir 的父目录
#     os.makedirs(d, exist_ok=True)                  # 父目录本来就存在 → 成功
#
# 实测 (修复前): `Runtime(debug_dir=D).run(dag, "..")` → status=done, 而快照
# `00_s_a_done.json` 落在 **D 的外面** (D 的父目录)。`task_id="."` 则落在 D 根,
# 与各 task 子目录混在一起。
#
# 为什么校验 task_id 就是根因: 它已经是 storage key 的唯一入口校验, 而
# `run()` / `fork_run()` / `run_stage()` 三个入口都在任何落盘之前调它 ——
# `_dump_debug` 在 run 循环深处, 拿到的一定是已校验过的值。所以补 `_validate_task_id`
# 一处, 三个入口 + 两种落盘 (checkpoint / debug) 一起封住; 不必在 `_dump_debug`
# 里平行实现一遍 realpath 断言 (那会是第二份防护, 迟早漂)。


@pytest.mark.parametrize("bad", ["..", ".", "...", "...."])
async def test_task_id_rejects_dot_segments(bad):
    """`.` 在允许字符集内, 但**单独成段**的 `.` / `..` 是路径操作, 不是名字。"""
    dag = DAG("dd")

    @dag.stage()
    async def s_x(ctx):
        return {}

    with pytest.raises(ValueError, match="task_id"):
        await Runtime().run(dag, task_id=bad)


@pytest.mark.parametrize("ok", ["a..b", "a.b", ".hidden", "x..", "..a", "a..", "v1.0-rc.1"])
async def test_task_id_allows_dots_that_are_not_path_segments(ok):
    """反向约束: **只拒整段是 `.`/`..`**, 不碰正常的点号名字。

    `a..b` / `.hidden` / `v1.0-rc.1` 都是正常 task_id —— 上一版我一度想按
    "含 '..' 就拒" 来写, 那会把这些一起杀掉。判据必须是"整段等于 . 或 .."。

    ⚠️ 这条用例**当场逮到我自己的错**: 我第一版把 `..a` / `a..` 放进了拒列表,
    而实现 (和上面 docstring) 判的是"整段等于 . 或 .." —— 于是 2 条红, 差一步
    就把正常名字也拒掉。这与上一条修复 (override key 校验) 是同一个毛病的两种
    触发方式: **收紧校验时先写"哪些必须仍合法"的反向用例**, 否则实现和判据
    会一起往严的方向滑, 而且绿得看不出问题。
    """
    dag = DAG("dd2")

    @dag.stage()
    async def s_x(ctx):
        return {}

    assert (await Runtime().run(dag, task_id=ok)).status == "done"


async def test_debug_snapshot_never_lands_outside_debug_dir(tmp_path):
    """端到端: 无论 task_id 写成什么, 快照都不许出现在 debug_dir 之外。

    判据不写成"run 要抛 ValueError" —— 那是**实现**。真正的契约是"文件不许
    落在目录外", 所以这里同时断言越界文件为空。
    """
    dag = DAG("dd3")

    @dag.stage()
    async def s_a(ctx):
        return {"v": 1}

    outside_before = {p for p in tmp_path.rglob("*") if p.is_file()}
    for bad in ("..", "."):
        with pytest.raises(ValueError):
            await Runtime(debug_dir=str(tmp_path / "debug")).run(dag, task_id=bad)
    leaked = {p for p in tmp_path.rglob("*") if p.is_file()} - outside_before
    assert not leaked, f"快照泄漏到 debug_dir 之外: {sorted(str(p) for p in leaked)}"
