"""被 skip_unchanged 跳过的 stage 必须**看得见** —— 否则 trace 会说"都跑了"。

起因 (2026-10-17, ai@home 生产侧报告 id=3653): 对方的现象是"我改的那一段根本没执行,
两次跑的状态与数值逐字相同"。实测把机制钉出来了:

    fork_run(from_stage="s_a", skip_unchanged=True) 且**无 override** 时
      · 真正被调用的 fn : []              ← 一个都没跑
      · stage_statuses   : 三个全 "done"
      · done_stages      : 三个全在
      · stage_timings    : {}             (跳过的不计时)
      · 事件流            : 三个全 "skipped"

也就是说**从结果上与一次完整跑完全无法区分**, 而 `pavos trace` 只看 checkpoint, 于是
它会理直气壮地报告"都跑了"。`RunResult` 的 docstring 此前还写着 "resume-skipped stages
are absent, never recorded as skipped" —— 与实现不符。

`skip_unchanged` 跳过 `from_stage` 本身是**有意设计** (runtime docstring: "`from_stage`
退化为起点提示"), 所以修的不是行为而是**可见性**: 新增 `RunResult.skipped_stages` 与
`Checkpoint.stage_skipped`。

判据钉的是"列表与**实际调用**完全一致" —— 只断言"非空"会被"把三个都填上"的实现蒙混
过去, 而那正是最坏的一种错报。
"""
from __future__ import annotations

from pavoz import DAG, CheckpointStore, FileStorage, Runtime


def _dag(ran: list):
    d = DAG("skipvis")
    exec("""
@dag.stage()
async def s_a(ctx):
    RAN.append("s_a")
    return {"a": 1}
@dag.stage(depends_on=["s_a"])
async def s_b(ctx):
    RAN.append("s_b")
    return {"b": ctx.state["a"] * 10}
@dag.stage(depends_on=["s_b"])
async def s_c(ctx):
    RAN.append("s_c")
    return {"c": ctx.state["b"] + 1}
""", {"dag": d, "RAN": ran})
    return d


async def test_skipped_stages_exactly_matches_what_did_not_run(tmp_path):
    """★ 全被跳过时: `skipped_stages` 必须**恰好**等于没被调用的那批 fn。"""
    ran: list = []
    store = CheckpointStore(FileStorage(str(tmp_path)))
    await Runtime(checkpoint_store=store).run(_dag(ran), "t")
    ran.clear()

    r = await Runtime(checkpoint_store=store).fork_run(
        _dag(ran), "t", from_stage="s_a", skip_unchanged=True)

    assert ran == [], f"前置: 这一轮不该有任何 fn 被调用, 实得 {ran}"
    assert r.skipped_stages == ["s_a", "s_b", "s_c"], (
        f"跳过的三个必须都列出来: {r.skipped_stages}")
    assert r.status == "done"
    assert r.stage_statuses == {"s_a": "done", "s_b": "done", "s_c": "done"}, (
        "前置: stage_statuses 对它们记的确实是 done (所以才需要 skipped_stages)")


async def test_skipped_stages_empty_when_nothing_skipped(tmp_path):
    """对照: 真跑过的 stage 一个都不许出现在 `skipped_stages` 里 (别误报)。"""
    ran: list = []
    store = CheckpointStore(FileStorage(str(tmp_path)))
    r = await Runtime(checkpoint_store=store).run(_dag(ran), "t")

    assert ran == ["s_a", "s_b", "s_c"], f"前置: 全都该真跑, 实得 {ran}"
    assert r.skipped_stages == [], f"没跳过就不该有记录: {r.skipped_stages}"


async def test_changed_input_means_ran_not_skipped(tmp_path):
    """★ 改了输入 ⇒ 真跑 ⇒ 不许出现在 `skipped_stages` 里 (与上两条构成反向判据)。"""
    ran: list = []
    store = CheckpointStore(FileStorage(str(tmp_path)))
    await Runtime(checkpoint_store=store).run(_dag(ran), "t")
    ran.clear()

    r = await Runtime(checkpoint_store=store).fork_run(
        _dag(ran), "t", from_stage="s_b", overrides={"a": 999}, skip_unchanged=True)

    assert ran == ["s_b", "s_c"], f"改了输入就该真跑 s_b 起的那段, 实得 {ran}"
    assert r.skipped_stages == [], f"真跑过的 stage 不该被记成跳过: {r.skipped_stages}"
    assert r.state["b"] == 9990


async def test_skipped_stages_survives_checkpoint_roundtrip(tmp_path):
    """★ 落盘往返: `pavos trace` 只读 checkpoint, 不落盘它就等于没加这个字段。"""
    ran: list = []
    store = CheckpointStore(FileStorage(str(tmp_path)))
    await Runtime(checkpoint_store=store).run(_dag(ran), "t")
    ran.clear()
    await Runtime(checkpoint_store=store).fork_run(
        _dag(ran), "t", from_stage="s_a", skip_unchanged=True)

    cp = store.load_latest("t")
    assert cp.stage_skipped == ["s_a", "s_b", "s_c"], (
        f"checkpoint 里必须留着跳过记录: {cp.stage_skipped}")
    # 序列化往返
    from pavoz.checkpoint import Checkpoint
    again = Checkpoint.from_dict(cp.to_dict())
    assert again.stage_skipped == cp.stage_skipped


async def test_skipped_stages_empty_for_old_checkpoint_without_the_field():
    """旧 cp 没有这个键 → 空列表 (A7 默认), 不能炸也不能编出记录来。"""
    from pavoz.checkpoint import Checkpoint
    base = {"task_id": "t", "run_id": "r", "dag_name": "d", "workflow_hash": "h",
            "stage_statuses": {"s_a": "done"}, "done_stages": ["s_a"]}
    assert Checkpoint.from_dict(base).stage_skipped == []
