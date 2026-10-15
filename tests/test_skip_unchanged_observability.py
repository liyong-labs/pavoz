"""skip_unchanged 的复用必须**可被分辨** —— 且指纹的覆盖边界必须写在脸上.

背景 (2026-10-16 巡检): `skip_unchanged` 命中时引擎复用历史 delta, 不重跑 stage。
实测发现两处缺口, 都不是代码逻辑错, 而是"事实没写在任何地方":

1. **指纹只含 fn 源码 + 执行前 state** —— 不含闭包变量/模块级配置/环境变量/文件/
   时钟。而文档原文只说"改 fn (如改 prompt) → hash 变", 读起来像"改 prompt 就会
   让它重跑"。实测: 模块级 `PROMPT` 从 v1 改到 v2, fn 源码与 state 都没变 →
   fork 静默返回 **v1** 的产出, `status=done`, 事件流外无处可查。

2. **复用 vs 真跑, 只有事件流能分辨**: 命中时引擎发
   `stage_end {status: "skipped"}` 并打一条 INFO 日志 (实现里本来就有), 但
   `RunResult.stage_statuses` 仍记 `"done"` —— 因为该 stage 的产出确实已在 state
   里, 语义没错。问题在于文档从未写过 `"skipped"` 这个 status 值, 于是
   "唯一的区分信号"没人知道去看。

本测试把这两条**事实**钉成判据 (characterization, 不是改行为): 哪天引擎真改了
stage_statuses 的口径或指纹的覆盖面, 这些用例会红, 那时文档与实现一起更新即可。
"""
from __future__ import annotations

import tempfile

import pytest

from pavoz import (
    DAG,
    CheckpointStore,
    EventRecorder,
    FileStorage,
    Runtime,
    stage_input_hash,
)

# 模块级配置 —— 用户改 prompt 的常见写法 (刻意放在 fn 体外)
PROMPT = {"v": "v1"}


def _dag() -> DAG:
    dag = DAG("skip_obs")

    @dag.stage()
    async def s_plan(ctx):
        return {"topic": "chickens"}

    @dag.stage(depends_on=["s_plan"])
    async def s_compose(ctx):
        # f 字符串里引用的是 PROMPT["v"] —— 闭包外的全局量
        return {"text": f"[{PROMPT['v']}] {ctx.state['topic']}"}

    return dag


@pytest.fixture
def store():
    return CheckpointStore(FileStorage(tempfile.mkdtemp()))


@pytest.fixture(autouse=True)
def _reset_prompt():
    PROMPT["v"] = "v1"
    yield
    PROMPT["v"] = "v1"


async def test_closure_config_is_not_covered_by_the_fingerprint():
    """指纹只认 fn 源码 + 执行前 state —— 改模块级配置, hash 纹丝不动。

    这条用例的存在意义是**钉住边界**: 它是静默复用旧结果的根因。若将来引擎把
    闭包/配置纳入指纹, 本用例会红, 那时文档要一起改。
    """
    dag = _dag()
    state = {"topic": "chickens"}
    h1 = stage_input_hash(dag.stages["s_compose"].fn, state)

    PROMPT["v"] = "v2"                     # 用户"改了 prompt"
    h2 = stage_input_hash(dag.stages["s_compose"].fn, state)

    assert h1 == h2, "模块级配置不在指纹覆盖内 —— 正是文档里必须写明的边界"


async def test_changing_fn_body_does_change_the_fingerprint():
    """反向约束: 写在 fn 体内的东西改了, 指纹必须变 (否则 skip 会误跳)。"""

    def _fn_v1(ctx):
        return {"text": "v1"}

    def _fn_v2(ctx):
        return {"text": "v2"}

    assert (stage_input_hash(_fn_v1, {}) != stage_input_hash(_fn_v2, {}))


async def test_changing_before_state_does_change_the_fingerprint():
    dag = _dag()
    fn = dag.stages["s_compose"].fn
    assert (stage_input_hash(fn, {"topic": "a"}) != stage_input_hash(fn, {"topic": "b"}))


async def test_reuse_is_visible_in_events_but_statuses_say_done(store):
    """命中复用时: stage_statuses 记 "done", 事件流记 "skipped" —— 两者都要钉住。

    若哪天引擎把 stage_statuses 改成 "skipped" (那是 RunResult 的契约变更), 本用例
    会红; 那时 docs/api.md 与 RunResult docstring 必须同步改, 否则又是一处
    文档与能力不符。
    """
    dag = _dag()
    assert (await Runtime(checkpoint_store=store).run(dag, "t1")).status == "done"

    PROMPT["v"] = "v2"                     # 改配置 → hash 不变 → 命中复用
    rec = EventRecorder()
    r = await Runtime(checkpoint_store=store, on_event=rec).fork_run(
        dag, "t1", from_stage="s_compose", skip_unchanged=True)

    assert r.stage_statuses["s_compose"] == "done", (
        "stage_statuses 口径: 复用也记 done (产出确实在 state 里)")
    skipped = [d for name, d in rec.events
               if name == "stage_end" and d.get("status") == "skipped"]
    assert [d["stage"] for d in skipped] == ["s_compose"], (
        "事件流必须带 stage_end{status:'skipped'} —— 这是复用唯一的可观测信号, "
        "docs 依赖它做排查指引")

    # 而它复用的正是旧结果: 改了配置却拿到 v1 的产出
    assert r.state["text"] == "[v1] chickens"


async def test_skip_unchanged_off_reruns_and_sees_the_new_config(store):
    """反向约束: 关掉 skip 就该真跑 (上面的静默复用是 skip 开启时的预期行为)。"""
    dag = _dag()
    await Runtime(checkpoint_store=store).run(dag, "t1")

    PROMPT["v"] = "v2"
    rec = EventRecorder()
    r = await Runtime(checkpoint_store=store, on_event=rec).fork_run(
        dag, "t1", from_stage="s_compose", skip_unchanged=False)

    assert r.state["text"] == "[v2] chickens", "不 skip 就必须真的重跑并看到新配置"
    assert not [d for name, d in rec.events
                if name == "stage_end" and d.get("status") == "skipped"]


async def test_stage_can_opt_out_via_flag(store):
    """文档给的两条出路之一: skip_unchanged=False 拒跳 —— 真的拒吗。"""
    dag = DAG("skip_optout")

    @dag.stage()
    async def s_a(ctx):
        return {"topic": "x"}

    @dag.stage(depends_on=["s_a"], skip_unchanged=False)
    async def s_side_effect(ctx):
        return {"n": 1}

    assert (await Runtime(checkpoint_store=store).run(dag, "t2")).status == "done"

    rec = EventRecorder()
    r = await Runtime(checkpoint_store=store, on_event=rec).fork_run(
        dag, "t2", from_stage="s_side_effect", skip_unchanged=True)

    assert r.state["n"] == 1
    assert not [d for name, d in rec.events
                if name == "stage_end" and d.get("status") == "skipped"], (
        "标了 skip_unchanged=False 的 stage 不该被复用")
