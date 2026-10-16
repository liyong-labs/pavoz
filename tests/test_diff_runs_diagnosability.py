"""diff_runs 的可诊断性 —— 它是给人和 AI agent 读的, 结论错比底层数据错更危险。

底层数据错, 人一眼能看出来 (state 对不上); 诊断工具的结论错, 用户会**据此
判断两次 run 的差异**, 那就是被工具带着走错。

本文件钉的是"最常见的假警报必须能被单独排除": dag 名自 d21aab8 (0.5.7) 起就是
`workflow_hash` 的一部分, 所以**改名**就会让 `workflow_hash_changed` 变 true。
而改名恰恰是最该被判成"这不是真差异"的那种 —— 它不改变任何 stage 的行为。
"""

from __future__ import annotations

import pytest

from pavoz import DAG, CheckpointStore, FileStorage, Runtime, diff_runs


def _chain(name: str) -> DAG:
    dag = DAG(name)

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 2}

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        return {"c": 3}

    return dag


async def _two_runs(tmp_path, name_a: str, name_b: str):
    """同一 task 上跑两次, dag 名分别是 name_a / name_b."""
    store = CheckpointStore(FileStorage(tmp_path / "cp"))
    rt = Runtime(checkpoint_store=store)
    await rt.run(_chain(name_a), "T")
    rid_a = store.load_latest("T").run_id
    await rt.run(_chain(name_b), "T")
    return store.load("T", rid_a), store.load_latest("T")


async def test_pure_rename_is_identifiable_from_the_diff(tmp_path):
    """★ 纯改名: 每项 stage 都"无差异", 而 `dag_names` 让这件事**能被排除**。

    修复前的实测: `dag_name` 只报 a 的名字, b 叫什么完全不可见。消费者看到
    `workflow_hash_changed: true` + 满屏"无差异", 只能得出"结构变了但行为没变"
    —— 而真相是**什么实质变化都没有**。那个错误结论会直接导致一次无谓的全量
    重跑。
    """
    cp_a, cp_b = await _two_runs(tmp_path, "名字A", "名字B")
    d = diff_runs(cp_a, cp_b)

    assert d["workflow_hash_changed"] is True, (
        "前置条件: 改名会改指纹 (d21aab8 起 dag 名入 hash)")
    assert d["dag_names"] == ["名字A", "名字B"], (
        "两个名字都要给 —— 单给 a 的那个字段看不见 b 叫什么, 于是最常见的假警报"
        f"无法排除; 实得 {d.get('dag_names')}")
    # 排除假警报所需的全部依据都在 diff 里: 每项 stage 都无差异
    for name, info in d["stages"].items():
        assert info["status"] == ["done", "done"]
        assert info["output_diff"] == {}, f"{name} 应无产出差异"
    assert d["state_diff"] == {}, "纯改名不该有任何 state 差异"


async def test_same_name_reports_equal_dag_names(tmp_path):
    """反向约束: 名字相同 → 两个位置都给同一个值, 不是只有一边有值。"""
    cp_a, cp_b = await _two_runs(tmp_path, "同名", "同名")
    d = diff_runs(cp_a, cp_b)

    assert d["workflow_hash_changed"] is False
    assert d["dag_names"] == ["同名", "同名"]


async def test_diff_output_stays_json_serializable(tmp_path):
    """R5 闭合: 新字段不许引入不可序列化的东西 (它是给 agent 直接吃的)。"""
    import json

    cp_a, cp_b = await _two_runs(tmp_path, "甲", "乙")
    d = diff_runs(cp_a, cp_b)
    assert json.loads(json.dumps(d, ensure_ascii=False)) == d


async def test_legacy_dag_name_field_still_present(tmp_path):
    """`dag_name` 单值字段保留 —— 只关心 a 的调用方不必改。

    新字段是**加**不是**换**: diff_runs 在 0.5.4 已发布, 直接把 `dag_name`
    改成 `[a, b]` 会打断现有消费者按字符串读的代码。
    """
    cp_a, cp_b = await _two_runs(tmp_path, "甲", "乙")
    d = diff_runs(cp_a, cp_b)

    assert d["dag_name"] == "甲" == cp_a.dag_name
    assert isinstance(d["dag_name"], str)


async def test_looping_graph_visits_still_reported(tmp_path):
    """反向约束: 环图轮数不同的 run, `visits` 仍如实给出 (别把新字段加坏了)。"""
    def loop_dag(thresh: int) -> DAG:
        dag = DAG("L")

        @dag.stage()
        async def s_w(ctx):
            return {"n": ctx.state.get("n", 0) + 1}

        @dag.stage(depends_on=["s_w"])
        async def s_r(ctx):
            return {"shared": f"R{ctx.state['n']}"}

        dag.add_conditional_edges(
            "s_r", lambda s: "more" if s["n"] < thresh else "stop",
            {"more": "s_w", "stop": "s_end"}, max_visits=12)

        @dag.stage()
        async def s_end(ctx):
            return {"rounds": ctx.state["n"]}

        return dag

    store = CheckpointStore(FileStorage(tmp_path / "cp"))
    rt = Runtime(checkpoint_store=store)
    await rt.run(loop_dag(3), "U")
    rid_a = store.load_latest("U").run_id
    await rt.run(loop_dag(5), "U")
    d = diff_runs(store.load("U", rid_a), store.load_latest("U"))

    assert d["stages"]["s_w"]["visits"] == [3, 5]
    assert d["stages"]["s_r"]["visits"] == [3, 5]
    assert d["stages"]["s_end"]["output_diff"] == {"rounds": [3, 5]}
