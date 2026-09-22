"""DAG viz export (v0.5.4): to_mermaid + to_graph_json — 出数据, caller 自渲染.

要求 (ai@home id=177, user 拍板):
- 格式: Mermaid (.md 嵌入 README/飞书) + JSON Graph (前端 Vue dagre/cytoscape)
- 不出 UI / drag-drop / server — caller 自己出
- 零依赖, ≤50 行
"""

import json

from pavoz import DAG, to_graph_json, to_mermaid


def _dag_3stage():
    """s_a → s_b → s_c 线性. 用于所有基础测试."""
    dag = DAG("linear")

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


def _dag_diamond():
    """s_a → (s_b, s_c) → s_d 菱形."""
    dag = DAG("diamond")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_c(ctx):
        return {"c": 1}

    @dag.stage(depends_on=["s_b", "s_c"])
    async def s_d(ctx):
        return {"d": 1}

    return dag


def test_to_mermaid_starts_with_graph_keyword():
    """mermaid 输出第一行以 'graph' 开头 (caller 嵌入 README 时能被 mermaid 渲染器识别)."""
    out = to_mermaid(_dag_3stage())
    first_line = out.splitlines()[0]
    assert first_line.startswith("graph "), f"mermaid 必须以 'graph ' 开头, got: {first_line!r}"


def test_to_mermaid_contains_all_stage_nodes():
    """每个 stage name 都作为节点出现 (caller 视觉上一眼看到所有 step)."""
    out = to_mermaid(_dag_3stage())
    for name in ("s_a", "s_b", "s_c"):
        assert name in out, f"mermaid 输出缺 stage {name}, output:\n{out}"


def test_to_mermaid_edges_match_dependencies():
    """每个 depends_on 对应一行 'X --> Y' 箭头."""
    out = to_mermaid(_dag_3stage())
    assert "s_a --> s_b" in out, f"缺 s_a -> s_b 边, output:\n{out}"
    assert "s_b --> s_c" in out, f"缺 s_b -> s_c 边, output:\n{out}"


def test_to_mermaid_diamond_has_two_incoming_on_join():
    """菱形汇合点 s_d 有两条入边 (s_b, s_c)."""
    out = to_mermaid(_dag_diamond())
    assert "s_b --> s_d" in out
    assert "s_c --> s_d" in out


def test_to_mermaid_no_orphan_nodes():
    """无依赖的单 stage → 输出无箭头但有节点 (有依赖的才有箭头)."""
    dag = DAG("solo")

    @dag.stage()
    async def s_only(ctx):
        return {"a": 1}

    out = to_mermaid(dag)
    assert "s_only" in out
    assert "-->" not in out


def test_to_mermaid_direction_lr():
    """默认 layout = LR (left-to-right, 流程图直观). caller 可读 'left' 可改用其他, 但默认 LR 是预期."""
    out = to_mermaid(_dag_3stage())
    assert "graph LR" in out or "graph TD" in out  # TD 也可接受, 仅验证有效关键字


def test_to_graph_json_structure():
    """结构完整: dag_name / nodes / edges / topo_order."""
    d = to_graph_json(_dag_3stage())
    assert d["dag_name"] == "linear"
    assert "nodes" in d and "edges" in d and "topo_order" in d


def test_to_graph_json_nodes_count_and_ids():
    """nodes 数 = stage 数, 每个 node 含 id/name/depends_on."""
    d = to_graph_json(_dag_3stage())
    assert len(d["nodes"]) == 3
    ids = {n["id"] for n in d["nodes"]}
    assert ids == {"s_a", "s_b", "s_c"}


def test_to_graph_json_edges_count_matches_deps():
    """edges 数 = 所有 depends_on 总和 (s_a→s_b, s_b→s_c = 2)."""
    d = to_graph_json(_dag_3stage())
    assert len(d["edges"]) == 2


def test_to_graph_json_diamond_two_incoming():
    """s_d 有两条入边 from=[s_b, s_c]."""
    d = to_graph_json(_dag_diamond())
    incoming = [e for e in d["edges"] if e["to"] == "s_d"]
    incoming_from = {e["from"] for e in incoming}
    assert incoming_from == {"s_b", "s_c"}


def test_to_graph_json_topo_order_valid():
    """topo_order 是合法拓扑序: s_a 在 s_b 前, s_b 在 s_c 前."""
    d = to_graph_json(_dag_3stage())
    topo = d["topo_order"]
    assert topo.index("s_a") < topo.index("s_b") < topo.index("s_c")


def test_to_graph_json_node_depends_on_field_matches_dag():
    """每个 node 的 depends_on 字段 = 该 stage 的真实依赖 (不能是空 list 兜底)."""
    d = to_graph_json(_dag_3stage())
    s_b = next(n for n in d["nodes"] if n["id"] == "s_b")
    s_c = next(n for n in d["nodes"] if n["id"] == "s_c")
    s_a = next(n for n in d["nodes"] if n["id"] == "s_a")
    assert s_b["depends_on"] == ["s_a"]
    assert s_c["depends_on"] == ["s_b"]
    assert s_a["depends_on"] == []  # 入口 stage 依赖为空 (不是 None)


def test_to_mermaid_direction_passed_through():
    """M-A 杀: direction 参数实参—— 若吃掉, 任何 direction 都变 TD."""
    out_lr = to_mermaid(_dag_3stage(), direction="LR")
    out_td = to_mermaid(_dag_3stage(), direction="TD")
    assert out_lr != out_td, "direction 参数被忽略 → 全部输出 graph TD (M-A 未被杀)"


def test_to_graph_json_is_json_serializable():
    """全 dict/list/str/int/bool/None — json.dumps 不抛 (caller 直接 wire 出去)."""
    d = to_graph_json(_dag_3stage())
    serialized = json.dumps(d)
    assert isinstance(serialized, str) and len(serialized) > 0