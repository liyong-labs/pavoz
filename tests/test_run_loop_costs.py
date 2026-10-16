"""run 循环的热路径代价 — 判据是**访问次数**, 不是耗时.

起因 (2026-10-17, 接"结构指纹每 stage 重算"那两条): `DAG.stages` 是一个
**每次访问都 `dict(self._stages)` 建副本**的 property (有意如此: 调用方不该改到内部
状态)。而它被放在两个 O(N)/O(N²) 的位置上:

  · `_run_inner` 的 `_gated_by` 循环: 外层 N 个 stage × 内层祖先闭包
  · `_run_inner` 主执行循环 `stage = dag.stages[name]`: 每个 stage 一次

两处都提到循环外 (`dag.validate()` 已冻结 dag, run 期间结构不变; 拷贝是**浅**的,
Stage 对象共享, 所以 TestPipe 换 fn 的写法照样看得见)。实测 640 stage 的线性 DAG:
2677ms → 2173ms (17%), 且省掉的是**平方项** —— 占比随规模继续涨。

为什么判据用访问次数而不是耗时: 耗时断言在 CI 上不可靠 (机器/负载), 而且只能
间接说明。`dag.stages` 被访问几次是**确定性**的事实, 直接说明"有没有人在循环里取
它"。640 stage 修前是 O(N²) 次 (40 万+), 修后是 O(1) 次 —— 判据就把这个界钉住。
"""
from __future__ import annotations

import asyncio
import tempfile

import pytest

import pavoz.dag as dagmod
import pavoz.runtime as rt
from pavoz.checkpoint import CheckpointStore
from pavoz.storage import FileStorage


def _linear_dag(name: str, n: int):
    src = ["from pavoz import DAG", f"d = DAG({name!r})", "prev = None"]
    for i in range(n):
        src += ["@d.stage(depends_on=[prev] if prev else None)",
                f"async def f{i}(ctx):",
                "    return {'acc': ctx.state.get('acc', 0) + 1}",   # 无插值, 不必是 f-string
                f"prev = 'f{i}'"]
    ns: dict = {}
    exec("\n".join(src), ns)          # noqa: S102 — 生成测试用 DAG
    return ns["d"]


@pytest.fixture
def count_stages(monkeypatch):
    """数一次 run 里 `dag.stages` 被访问了几次。"""
    real = dagmod.DAG.stages
    box = {"n": 0}

    def counting(self):
        box["n"] += 1
        return real.fget(self)

    monkeypatch.setattr(dagmod.DAG, "stages", property(counting))
    return box


@pytest.mark.parametrize("n", [2, 6, 20])
def test_stages_property_accessed_a_constant_number_of_times(count_stages, n):
    """★ 访问次数与 stage 数**无关** —— 修前是每 stage 一次 (O(N) → 平方项)。"""
    dag = _linear_dag(f"hot_{n}", n)
    with tempfile.TemporaryDirectory() as td:
        store = CheckpointStore(FileStorage(td))
        r = asyncio.run(rt.Runtime(checkpoint_store=store).run(dag, "hot"))
    assert r.status == "done", f"前置: run 应完成, 实得 {r.status}: {str(r.error)[:80]}"
    # 界 = 3: workflow_hash 一次 + _run_inner 提出来的那一次 + 余量。
    # 修前 20 个 stage 是 21 次 —— 所以这个界能逮住"有人在循环里取它"。
    assert count_stages["n"] <= 3, (
        f"{n} 个 stage 的 run 访问了 `dag.stages` {count_stages['n']} 次 —— "
        f"它是每次都建 dict 副本的 property, 放在循环里就是平方项 (见本文件头)")


def test_stage_fn_mutation_is_still_visible_through_the_hoisted_copy():
    """提到循环外的那份拷贝不能弄坏 TestPipe 的"换 stage fn"写法。

    这是上一个优化 (把 `dag.stages` 提到循环外) 的**真实**回归面: 拷贝是浅的,
    Stage 对象共享, 所以 `dag.stages["f0"].fn = fake` 之后 run 用到的就是假的那个。

    ⚠️ 我第一版在这里写的风险假设是**错的**, 而且是被变异逮出来的: 我以为"`_all_stages`
    若在换 fn 之前取, 就会静默跑旧实现", 于是把拷贝改成深拷贝做变异 —— **全绿**。
    因为 `_all_stages` 是在 `run()` **内部**取的, 而 TestPipe 按设计一定在 `run()`
    **之前**换完 fn, 所以"取早了"根本不会发生。

    教训与本会话其他几次一样: 变异没有红, 先怀疑**我的风险假设站不住**, 而不是
    "判据太弱就再加一条"。这里真正该守的就是上面这件事, 所以留着, 只把说明改成
    事实。
    """
    dag = _linear_dag("fn_mut", 2)
    real_a = dag.stages["f0"].fn

    async def fake_a(ctx):
        return {"acc": 999, "patched": True}

    # 模拟 TestPipe 的做法: 换 dag 上某个 stage 的 fn, 然后再 run
    dag.stages["f0"].fn = fake_a

    with tempfile.TemporaryDirectory() as td:
        store = CheckpointStore(FileStorage(td))
        r = asyncio.run(rt.Runtime(checkpoint_store=store).run(dag, "fn_mut"))
    assert r.status == "done", f"前置: run 应完成, 实得 {r.status}"
    assert r.state.get("patched") is True, (
        f"换掉的 stage fn 没被执行 —— 提到循环外的那份 stages 拷贝不是浅拷贝, "
        f"或取早了 (state={r.state})")
    assert r.state["acc"] == 1000, f"f0 假实现应产出 acc=999, f1 再 +1 = 1000, 实得 {r.state}"
    assert dag.stages["f0"].fn is fake_a, "顺带确认 dag 上确实被换过 (排除夹具没生效)"
    assert real_a is not fake_a
