"""viz 的节点 ID 必须是 Mermaid 能解析的, 而显示名恒为 stage 原名。

问题现场: `to_mermaid` 把 **stage 名直接当节点 ID**。而 stage 名来自
`fn.__name__`, Python 侧完全合法 —— 实测 `async def end/graph/subgraph/style/
click/flowchart/direction/classDef/linkStyle` 全都造得出来, 它们是 Mermaid 的
保留字, 当 ID 用会让整张图渲染不出来。名字含连字符 (`load-data`) 同理。

⚠️ 保留字清单来自我们对该语法的了解, **无法在本仓离线核对** (Mermaid 语法表不
在仓内, 且查证工具在本环境不可用)。所以机制不只靠清单: `_safe_id` 同时用正则
兜住"不合法的 ID 形状", 保留字命中则替换。**漏掉某个保留字的后果 = 修复前的
行为**, 不会更糟 —— 这也是为什么清单可以留在这里而不必等查证。

标签不受影响: 图上看到的永远是 stage 原名, 替换只发生在 Mermaid 内部 ID。
"""

from __future__ import annotations

import pytest

from pavoz import DAG, to_mermaid, to_graph_json
from pavoz.viz import _MERMAID_RESERVED, _safe_id


def _dag_with(*names: str) -> DAG:
    dag = DAG("viz_ids")
    prev: str | None = None
    for nm in names:
        ns: dict = {}
        exec("async def _f(ctx): return {}", ns)      # noqa: S102 — 测试里造带指定名的 stage
        fn = ns["_f"]
        fn.__name__ = nm
        if prev is None:
            dag.stage()(fn)
        else:
            dag.stage(depends_on=[prev])(fn)
        prev = nm
    return dag


# ── 合法名字一个都不许变 (这决定"不改变现有输出") ──────────────────────────

@pytest.mark.parametrize("name", ["s_a", "load", "_x", "a1", "S_load_data", "x2"])
def test_ordinary_names_are_untouched(name):
    """正常名字的输出必须**逐字节不变** —— 否则这次修复就是无谓的破坏。

    这条是本次改动的兼容性底线: 绝大多数 stage 名是合法标识符, 它们不该因为
    一个只影响边角名字的修复而改变渲染 ID。
    """
    assert _safe_id(name) == name
    assert f'    {name}["{name}"]' in to_mermaid(_dag_with(name))


# ── 保留字 / 非法字符: ID 被替换, 标签保持原名 ─────────────────────────────

@pytest.mark.parametrize("name", sorted(_MERMAID_RESERVED) + ["load-data", "2fast"])
def test_dangerous_names_get_safe_id_but_keep_their_label(name):
    dag = _dag_with(name)
    out = to_mermaid(dag)
    sid = _safe_id(name)

    assert sid != name, f"{name!r} 必须是 Mermaid 保留字或非法 ID, 不该原样放行"
    assert sid[0].isalpha() or sid[0] == "_", f"ID {sid!r} 不能以数字开头"
    assert all(c.isalnum() or c == "_" for c in sid), f"ID {sid!r} 含非标识符字符"
    assert f'{sid}["{name}"]' in out, (
        f"节点行必须是 <安全ID>[\"<原名>\"] —— 显示名不得被替换, 实得:\n{out}")


def test_every_python_constructible_reserved_word_is_handled():
    """清单里的每个词都必须真的能从 Python 造出来 —— 否则是凭空的条目。

    反过来也成立: 若某个词 Python 侧造不出 (如 `class` 是 Python 关键字), 它就不
    该在清单里占位。这条让清单既不会虚胖, 也不会在改坏后静默。
    """
    for word in _MERMAID_RESERVED:
        ns: dict = {}
        try:
            exec(f"async def {word}(ctx): return {{}}", ns)   # noqa: S102
        except SyntaxError:
            pytest.fail(
                f"{word!r} 是 Python 关键字, 造不出这个 stage 名 —— 留在保留字"
                f"清单里是凭空的条目, 删掉它")
        assert callable(ns[word])


# ── 唯一性: 退化路径会让不同名字归一化成同一个串 ──────────────────────────

def test_ids_are_unique_even_when_names_normalize_to_the_same_string():
    """★ 唯一性必须由 to_mermaid 保证, 不能指望 _safe_id。

    退化路径 (名字含非 ASCII) 会把不同名字归一化成同一个串 —— 实测
    `"中文名"` / `"别的字"` / `"第三个"` 全都变成 `___`。两个节点共用一个 ID 就是
    渲染错: 后一个把前一个覆盖掉, 图少一个节点而**没有任何提示**。
    `_safe_id` 是纯函数看不到全图, 所以冲突只可能由调用方消掉。
    """
    names = ("中文名", "别的字", "第三个")
    dag = _dag_with(*names)
    out = to_mermaid(dag)

    id_lines = [l.strip() for l in out.splitlines() if '["' in l]
    assert len(id_lines) == len(names), (
        f"每个 stage 都要有自己的节点行, 实得 {len(id_lines)} 行:\n{out}")

    ids = [l.split("[")[0] for l in id_lines]
    assert len(set(ids)) == len(names), f"节点 ID 撞车了: {ids}"

    for nm in names:
        assert f'["{nm}"]' in out, f"标签必须保留原名 {nm!r}"


def test_id_assignment_is_deterministic():
    """同样内容的 DAG 每次生成同样的 ID —— 否则 diff 会一直变。"""
    assert to_mermaid(_dag_with("中文名", "别的字")) == \
           to_mermaid(_dag_with("中文名", "别的字"))


# ── 环图: 条件边的两端也走同一套映射 ───────────────────────────────────────

def test_loop_graph_edges_use_the_same_id_mapping():
    dag = DAG("viz_loop")
    ns: dict = {}
    exec("async def _w(ctx): return {}", ns)
    w = ns["_w"]; w.__name__ = "s_w"
    exec("async def _r(ctx): return {}", ns)
    r = ns["_r"]; r.__name__ = "end"          # 保留字当 router
    exec("async def _e(ctx): return {}", ns)
    e = ns["_e"]; e.__name__ = "s_end"
    dag.stage()(w)
    dag.stage(depends_on=["s_w"])(r)
    dag.stage()(e)
    dag.add_conditional_edges("end", lambda s: "more" if s["n"] < 3 else "stop",
                              {"more": "s_w", "stop": "s_end"}, max_visits=8)

    out = to_mermaid(dag)
    rid = _safe_id("end")
    assert rid != "end"
    assert f'{rid}["end"]' in out
    # 条件边的两个端点必须用映射后的 ID, 否则 Mermaid 看到的是不存在的节点
    assert f"{rid} -.more.-> s_w" in out
    assert f"{rid} -.stop.-> s_end" in out

    # graph_json 侧不受影响: 它的 id 是给 dagre/cytoscape 消费的, 那些库没有
    # Mermaid 的保留字限制, 保持 stage 原名才是稳定标识
    g = to_graph_json(dag)
    assert {n["id"] for n in g["nodes"]} == set(dag.stages)
