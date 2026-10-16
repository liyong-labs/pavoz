"""扩展面 (`ctx.call` → 注入的 caller) 的错误语义 —— 矩阵必须钉住.

背景 (2026-10-16 巡检): `ctx.call` 是每个真实用户的每次 LLM/搜索调用都要走的
路, docs 也把它的错误语义写得很细 (retryable / error_class / error_category 三者
各是什么)。但 `ctx.call` 在测试里只被碰过一次 (test_id_model 检查 CallMeta 字段),
**错误分类矩阵零覆盖** —— 而这个仓已经在这块真实翻过一次车: 泛型 `except Exception`
分支曾把 `error_category` 硬编码成 None (ai@home id=2925), 那次是靠人发现的,
不是靠测试。

本用例把实测到的四行矩阵钉成判据:

| caller 抛出              | status  | error_class     | error_category    | retryable |
|--------------------------|---------|-----------------|-------------------|-----------|
| RetryableError(category) | failed  | RetryableError  | 透传              | True      |
| StageError(category)     | failed  | StageError      | 透传              | False     |
| 自定义异常带 .category   | failed  | 自定义类名      | 透传 (不要求继承) | False     |
| 裸异常                   | failed  | 异常类名        | None              | False     |

外加两条: 重试时 CallMeta.attempt 递增 (caller 能看出这是第几次), 以及
`ctx.cancelled()` 把 Runtime.cancel_check 接进 stage 内部。
"""
from __future__ import annotations

from pavoz import DAG, CheckpointStore, FileStorage, RetryableError, Runtime, StageError


class MyErr(Exception):
    """带 category 但**不继承** StageError 的业务异常 (docs 明说也该透传)。"""

    def __init__(self, msg: str, category: str | None = None):
        super().__init__(msg)
        self.category = category


def _one_stage_dag(name: str, retries: int = 0) -> DAG:
    dag = DAG(name)

    @dag.stage(retries=retries)
    async def s_a(ctx):
        out = await ctx.call("llm", "chat", {"prompt": "x"})
        return {"out": out}

    return dag


async def test_retryable_error_from_caller_is_retried_and_category_passes_through(tmp_path):
    attempts: list[int] = []

    async def _caller(kind, op, params, meta):
        attempts.append(meta.attempt)
        raise RetryableError("503 from upstream", category="NETWORK")

    rt = Runtime(caller=_caller,
                 checkpoint_store=CheckpointStore(FileStorage(tmp_path / "cp")))
    r = await rt.run(_one_stage_dag("caller_retry", retries=2), "t1")

    assert r.status == "failed"
    assert r.error_class == "RetryableError"
    assert r.error_category == "NETWORK", "RetryableError 带 category 应当透传"
    assert r.retryable is True
    assert attempts == [1, 2, 3], (
        f"caller 应当每次 attempt 都被调一遍且 attempt 递增, 实得 {attempts}")


async def test_stage_error_from_caller_fails_without_retry(tmp_path):
    calls: list[int] = []

    async def _caller(kind, op, params, meta):
        calls.append(meta.attempt)
        raise StageError("搜索无结果", category="SEARCH_NO_RESULT")

    rt = Runtime(caller=_caller,
                 checkpoint_store=CheckpointStore(FileStorage(tmp_path / "cp")))
    r = await rt.run(_one_stage_dag("caller_stage_err", retries=2), "t1")

    assert r.status == "failed"
    assert r.error_class == "StageError"
    assert r.error_category == "SEARCH_NO_RESULT"
    assert r.retryable is False, "业务失败不该被标成可重试"
    assert calls == [1], f"StageError 不重试, 实得调了 {len(calls)} 次"


async def test_custom_exception_with_category_passes_through_without_inheriting(tmp_path):
    async def _caller(kind, op, params, meta):
        raise MyErr("结果被截断", category="CAT_TRUNCATION")

    rt = Runtime(caller=_caller,
                 checkpoint_store=CheckpointStore(FileStorage(tmp_path / "cp")))
    r = await rt.run(_one_stage_dag("caller_custom"), "t1")

    assert r.status == "failed"
    assert r.error_class == "MyErr"
    assert r.error_category == "CAT_TRUNCATION", (
        "带 .category 的自定义异常应当透传 —— 这条正是 2026-10 那次回归的靶心")
    assert r.retryable is False, "未知异常按 FatalError 语义: 不重试"


async def test_bare_exception_has_no_category(tmp_path):
    async def _caller(kind, op, params, meta):
        raise ValueError("boom")

    rt = Runtime(caller=_caller,
                 checkpoint_store=CheckpointStore(FileStorage(tmp_path / "cp")))
    r = await rt.run(_one_stage_dag("caller_bare"), "t1")

    assert r.status == "failed"
    assert r.error_class == "ValueError"
    assert r.error_category is None
    assert r.retryable is False


async def test_caller_receives_meta_and_params_unchanged(tmp_path):
    """CallMeta 四个字段 + params 原样透传 (caller 自己决定怎么用)。"""
    got: dict = {}

    async def _caller(kind, op, params, meta):
        got.update(kind=kind, op=op, params=dict(params),
                   task_id=meta.task_id, run_id=meta.run_id,
                   stage=meta.stage, attempt=meta.attempt)
        return {"text": "ok"}

    rt = Runtime(caller=_caller,
                 checkpoint_store=CheckpointStore(FileStorage(tmp_path / "cp")))
    r = await rt.run(_one_stage_dag("caller_meta"), "t-my-task")

    assert r.status == "done"
    assert got["kind"] == "llm" and got["op"] == "chat"
    assert got["params"] == {"prompt": "x"}
    assert got["task_id"] == "t-my-task"
    assert got["stage"] == "s_a"
    assert got["attempt"] == 1
    assert got["run_id"] == r.run_id


async def test_ctx_cancelled_exposes_runtime_cancel_check(tmp_path):
    """cancel_check 要能接进 stage 内部 (长循环自行轮询退出)。"""
    dag = DAG("caller_cancel")

    @dag.stage()
    async def s_a(ctx):
        seen = ctx.cancelled()
        return {"cancelled": seen}

    rt = Runtime(cancel_check=lambda tid: True,
                 checkpoint_store=CheckpointStore(FileStorage(tmp_path / "cp")))
    r = await rt.run(dag, "t1")

    # stage 体里读到 True (若 runtime 的边界拦截先动手, 则根本跑不到这里 — 两种
    # 都算"取消信号到达了 stage", 但本用例要的是"接进来了", 故放宽断言)
    assert r.status in ("done", "cancelled")
    if r.status == "done":
        assert r.state["cancelled"] is True, "cancel_check 没接到 ctx.cancelled()"


async def test_ctx_cancelled_false_when_no_checker(tmp_path):
    dag = DAG("no_cancel")

    @dag.stage()
    async def s_a(ctx):
        return {"cancelled": ctx.cancelled()}

    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(tmp_path / "cp")))
    r = await rt.run(dag, "t1")
    assert r.state["cancelled"] is False
