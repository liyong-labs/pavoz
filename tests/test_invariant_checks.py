"""两条不变式, 从"一次性探针"升级成"每次都验".

探针跑出来都是绿的 —— 本文件不修 bug, 它防的是**以后**的静默劣化: 那两类劣化
都不会报错, 只会让人在很久以后才发现"监控数字和真实结果对不上"。

## ① Checkpoint 字段往返必须穷尽

v0.8 起 state 不落盘, checkpoint 的其余字段全靠 `to_dict` / `from_dict` 往返。
新增一个 dataclass 字段却忘了写进 `to_dict` ⇒ reload 时**静默**用默认值, 没有任何
报错: resume 拿到的 cp 与落盘时的 cp 不是同一个东西。当前 15 个字段全部覆盖
(本文件用 `dataclasses.fields` 枚举, 不是手抄清单 —— 以后加字段自动跟上)。

## ② 事件流必须与 RunResult 说的是同一件事

监控与告警挂在 `on_event` 上, 消费方读的是 `RunResult`; 两者一旦分叉, 埋点就在
撒谎而没人知道。四种形状各钉一遍: 成功 / 中途失败 / 重试耗尽 / skip_unchanged 复用。
"""
from __future__ import annotations

import dataclasses
import tempfile

from pavoz import (
    DAG,
    CheckpointStore,
    EventRecorder,
    FileStorage,
    RetryableError,
    Runtime,
    StageError,
)
from pavoz.checkpoint import Checkpoint

ALL_FIELDS = sorted(f.name for f in dataclasses.fields(Checkpoint))


def _full_cp() -> Checkpoint:
    """每个字段都给非默认值 —— 默认值恰好相等的字段, 往返错了也看不出来."""
    return Checkpoint(
        task_id="t-1", run_id="r-1", dag_name="d-1", workflow_hash="wf-1",
        stage_statuses={"s": "done"}, done_stages=["s"],
        producers={"k": "s"}, initial_state={"i": 1},
        stage_deltas_visits={"s": [{"d": 1}, {"d": 2}]},
        stage_ts={"s": 1.5},
        fork_overrides={"o": 1}, stage_input_hashes={"s": "h1"},
        fork_keep_counts={"s": 1}, reset_pending=["x"],
        stage_fn_sources={"s": "src-1"},
    )


# ── ① Checkpoint 字段往返 ────────────────────────────────────────────────

def test_to_dict_covers_every_declared_field():
    d = _full_cp().to_dict()
    missing = [f for f in ALL_FIELDS if f not in d]
    assert not missing, (
        f"新增字段没写进 to_dict ⇒ reload 时静默变默认值: {missing}")
    extra = [k for k in d if k not in ALL_FIELDS]
    assert not extra, f"to_dict 里有 to_dict/from_dict 都不认识的键: {extra}"


def test_every_field_survives_a_full_roundtrip():
    cp, back = _full_cp(), Checkpoint.from_dict(_full_cp().to_dict())
    differing = [f for f in ALL_FIELDS if getattr(cp, f) != getattr(back, f)]
    assert not differing, (
        "字段往返后不一致 (reload 拿到的不是落盘时的那个 cp):\n"
        + "\n".join(f"   {f}: {getattr(cp, f)!r} → {getattr(back, f)!r}"
                    for f in differing))


def test_roundtrip_is_idempotent_across_two_generations():
    """转两代必须与转一代相同 —— 抓"first-gen 归一化, second-gen 又变"这类."""
    once = _full_cp().to_dict()
    twice = Checkpoint.from_dict(once).to_dict()
    assert once == twice, (
        "to_dict/from_dict 不幂等: 第二代与第一代不同\n"
        f"   一代: {once}\n   二代: {twice}")


# ── ② 事件流 ↔ RunResult 一致 ────────────────────────────────────────────

def _ends(rec: EventRecorder) -> dict:
    return {d["stage"]: d["status"] for n, d in rec.events if n == "stage_end"}


def _run_end(rec: EventRecorder) -> dict:
    return [d for n, d in rec.events if n == "run_end"][-1]


def _assert_agrees(rec: EventRecorder, r, *, allow_skipped=False):
    """事件流与 RunResult 必须说同一件事."""
    ends = _ends(rec)
    assert set(ends) >= set(r.stage_statuses), (
        f"有 stage 在结果里却没有 stage_end 事件: "
        f"缺 {sorted(set(r.stage_statuses) - set(ends))}")
    for stage, status in r.stage_statuses.items():
        if allow_skipped and ends[stage] == "skipped":
            continue  # 复用: 事件有 "skipped", 结果记 "done" (已文档化)
        assert ends[stage] == status, (
            f"stage {stage}: 事件说 {ends[stage]!r}, RunResult 说 {status!r} —— "
            "挂在 on_event 上的监控会与真实结果分叉")
    assert _run_end(rec)["status"] == r.status, (
        f"run_end 事件说 {_run_end(rec)['status']!r}, RunResult 说 {r.status!r}")
    assert set(r.stage_timings) <= set(r.stage_statuses), (
        f"stage_timings 里有未执行的 stage: "
        f"{sorted(set(r.stage_timings) - set(r.stage_statuses))}")


async def test_events_agree_on_a_successful_run():
    dag = DAG("ev_ok")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 1}

    rec = EventRecorder()
    r = await Runtime(on_event=rec).run(dag, "ok")
    assert r.status == "done"
    _assert_agrees(rec, r)


async def test_events_agree_when_a_stage_fails():
    dag = DAG("ev_fail")

    @dag.stage()
    async def s_x(ctx):
        return {"x": 1}

    @dag.stage(depends_on=["s_x"])
    async def s_y(ctx):
        raise StageError("炸了")

    rec = EventRecorder()
    r = await Runtime(on_event=rec).run(dag, "fail")
    assert r.status == "failed"
    _assert_agrees(rec, r)


async def test_events_agree_when_retries_are_exhausted():
    """重试次数要与 retries=1 吻合, 且退避秒数计入 stage_timings (已文档化)."""
    dag = DAG("ev_retry")

    @dag.stage(retries=1)
    async def s_r(ctx):
        raise RetryableError("503")

    rec = EventRecorder()
    r = await Runtime(on_event=rec).run(dag, "retry")
    assert r.status == "failed"
    retries = [d for n, d in rec.events if n == "stage_retry"]
    assert len(retries) == 1, f"retries=1 应恰好 1 次 stage_retry, 实得 {len(retries)}"
    assert r.stage_timings["s_r"] > 0, "退避秒数应计入 stage_timings"
    _assert_agrees(rec, r)


async def test_events_agree_on_skip_unchanged_reuse():
    """复用: 事件记 "skipped", 结果记 "done" —— 唯一允许的分叉, 且必须是有意的."""
    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    dag = DAG("ev_skip")

    @dag.stage()
    async def s_p(ctx):
        return {"p": 1}

    @dag.stage(depends_on=["s_p"])
    async def s_q(ctx):
        return {"q": 1}

    rt = Runtime(checkpoint_store=store)
    await rt.run(dag, "skip")
    rec = EventRecorder()
    r = await Runtime(checkpoint_store=store, on_event=rec).fork_run(
        dag, "skip", from_stage="s_p", skip_unchanged=True)

    assert _ends(rec) == {"s_p": "skipped", "s_q": "skipped"}, (
        "全部复用时事件流应全标 skipped")
    assert r.stage_statuses == {"s_p": "done", "s_q": "done"}
    _assert_agrees(rec, r, allow_skipped=True)

async def test_events_agree_on_the_generic_exception_branch():
    """★ 这条用例是被"判据必须能真的失败"逼出来的.

    前两条失败用例分别抛 StageError / RetryableError, 两者各有专门的 except 分支;
    **裸异常 (ValueError) 走的是泛型 `except Exception` 分支** —— 那条分支的
    stage_end 事件此前没被任何一致性用例覆盖。验证时我把该分支的
    `"status": "failed"` 改成 `"done"`, 上面两条用例**照样全绿** (它们根本没
    走到那条分支), 绿得毫无意义。这条补上后, 同样的变异会红。
    """
    dag = DAG("ev_generic")

    @dag.stage()
    async def s_g(ctx):
        raise ValueError("裸异常")

    rec = EventRecorder()
    r = await Runtime(on_event=rec).run(dag, "generic")

    assert r.status == "failed"
    assert r.error_class == "ValueError"
    assert "FatalError" in (r.error or ""), (
        f"泛型异常按 FatalError 语义处理, 实得 error={r.error!r}")
    _assert_agrees(rec, r)
