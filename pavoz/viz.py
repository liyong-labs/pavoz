"""DAG viz export (v0.5.4): Mermaid + JSON Graph 数据.

只出数据, caller 自渲染 (嵌入 README / 飞书 Mermaid, 或 Vue dagre/cytoscape 接 JSON).
不出 UI / drag-drop / server (原则 #4 '保持独立性'). 零依赖, ≤50 行.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .dag import DAG


def _safe_id(name: str) -> str:
    """Mermaid 节点 ID — 字母数字 + _ -. (Python identifier 已是子集, 兜底防 hyphen)."""
    return name.replace(" ", "_")


def to_mermaid(dag: DAG, *, direction: str = "LR") -> str:
    """DAG → Mermaid 图 (默认 LR 布局; 嵌 README / 飞书)."""
    lines = [f"graph {direction}"]
    for name in dag.topo_order():
        node = _safe_id(name)
        # node_id["显示名"]: python identifier 是 mermaid 安全 ID
        lines.append(f'    {node}["{name}"]')
    for name in dag.stages:
        for dep in dag.stages[name].depends_on:
            lines.append(f"    {_safe_id(dep)} --> {_safe_id(name)}")
    return "\n".join(lines) + "\n"


def to_graph_json(dag: DAG) -> dict:
    """DAG → JSON-serializable dict (nodes + edges + topo_order + dag_name)."""
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
    edges = [{"from": dep, "to": name}
             for name in dag.stages for dep in dag.stages[name].depends_on]
    return {
        "dag_name": dag.name,
        "nodes": nodes,
        "edges": edges,
        "topo_order": dag.topo_order(),
    }