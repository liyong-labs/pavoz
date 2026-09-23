"""DAG viz export (v0.5.4): Mermaid + JSON Graph 数据.

只出数据, caller 自渲染 (嵌入 README / 飞书 Mermaid, 或 Vue dagre/cytoscape 接 JSON).
不出 UI / drag-drop / server (原则 #4 '保持独立性'). 零依赖, ≤50 行.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .dag import DAG


def _safe_id(name: str) -> str:
    """Mermaid 节点 ID — 字母数字 + _ -. (Python identifier 已是子集, 兜底防 hyphen)."""
    return name.replace(" ", "_")


def to_mermaid(dag: DAG, *, direction: str = "LR") -> str:
    """DAG → Mermaid 图 (默认 LR 布局; 嵌 README / 飞书).

    R1 (0.5.5): 条件边渲染为带 key 标签的虚线 — 逻辑分叉/回路在图上一等可见.
    """
    lines = [f"graph {direction}"]
    for name in dag.topo_order():
        node = _safe_id(name)
        # node_id["显示名"]: python identifier 是 mermaid 安全 ID
        lines.append(f'    {node}["{name}"]')
    for name in dag.stages:
        for dep in dag.stages[name].depends_on:
            lines.append(f"    {_safe_id(dep)} --> {_safe_id(name)}")
    for fname, edge in dag.conditional_edges.items():
        for key, tgt in sorted(edge.mapping.items()):
            lines.append(f'    {_safe_id(fname)} -.{key}.-> {_safe_id(tgt)}')
    return "\n".join(lines) + "\n"


def to_graph_json(dag: DAG) -> dict:
    """DAG → JSON-serializable dict (nodes + edges + topo_order + dag_name).

    R1 (0.5.5): edges 增加 kind ("static" | "conditional"); 条件边另带 key
    (分支标签) 与 max_visits — admin UI 可见逻辑分叉, 叠加 route 事件显示实际走向.
    """
    nodes = []
    for name in dag.topo_order():
        stage = dag.stages[name]
        nodes.append({
            "id": name,
            "name": name,
            "depends_on": list(stage.depends_on),
            "retries": stage.retries,
            "timeout": stage.timeout,
        })
    edges: list[dict[str, Any]] = [{"from": dep, "to": name, "kind": "static"}
                                   for name in dag.stages
                                   for dep in dag.stages[name].depends_on]
    for fname, edge in dag.conditional_edges.items():
        for key, tgt in sorted(edge.mapping.items()):
            edges.append({"from": fname, "to": tgt, "kind": "conditional",
                          "key": key, "max_visits": edge.max_visits})
    return {
        "dag_name": dag.name,
        "nodes": nodes,
        "edges": edges,
        "topo_order": dag.topo_order(),
    }