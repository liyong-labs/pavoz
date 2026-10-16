"""DAG viz export (v0.5.4): Mermaid + JSON Graph 数据.

只出数据, caller 自渲染 (嵌入 README / 飞书 Mermaid, 或 Vue dagre/cytoscape 接 JSON).
不出 UI / drag-drop / server (原则 #4 '保持独立性'). 零依赖, ≤50 行.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .dag import DAG


# Mermaid 的**保留字**: 不能直接当节点 ID 用。
#
# ⚠️ 这份清单是**尽力而为, 可能不全** —— Mermaid 的语法表不在本仓, 我们也无法在
# 离线环境核对它。所以机制不能只靠清单: `_safe_id` 同时用正则兜住"不合法的 ID
# 形状"。清单的作用是把"形状合法但语义撞车"的那部分显式化, 便于发现新的就补
# 一行 —— 而漏掉一条的后果与修复前相同 (那个名字渲染坏), 不会更糟。
#
# 全部都是 Python 侧**合法**的标识符 (实测 `async def end/graph/...` 都能定义),
# 所以这不是"理论上不可能", 而是撞上了就渲染坏。
_MERMAID_RESERVED = frozenset({
    "end",        # 关闭 subgraph 的关键字
    "graph",      # `graph LR` 这个声明本身
    "flowchart",  # 新图类型关键字
    "subgraph",   # 子图声明
    "direction",  # 子图内的方向声明
    "click",      # 交互声明
    "style",      # 样式声明
    "classDef",   # 样式类定义
    "linkStyle",  # 边的样式声明
    "o", "x",     # `--o-->` / `--x-->` 这两种箭头的名字, 单独作 ID 会歧义
})

# 合法的 Mermaid 节点 ID: 字母/数字/下划线, 且不以数字开头。
_SAFE_ID_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_id(name: str) -> str:
    """Mermaid 节点 ID — 合法标识符形状, 且避开 Mermaid 保留字。

    **标签不受影响**: 调用方写成 `_safe_id(x)["<原名>"]`, 图上显示的始终是
    stage 原名, 替换只发生在 Mermaid 内部的 ID 上。

    ⚠️ 本函数原先的注释写着"字母数字 + _ -. (兜底防 hyphen)", 而实现只有
    `name.replace(" ", "_")` —— **连字符根本没防**, `_safe_id("load-data")`
    原样返回 `load-data`。注释声称的边界与实现不符, 让读注释的人以为有防护。
    """
    if _SAFE_ID_RE.match(name) and name not in _MERMAID_RESERVED:
        return name
    # 退化路径: 换成肯定合法的 ID。显示名由调用方单独给出, 所以这里不必可读。
    safe = "".join(ch if ch.isalnum() and ch.isascii() else "_" for ch in name)
    if not safe or not (safe[0].isalpha() or safe[0] == "_"):
        safe = f"n_{safe}"
    if safe in _MERMAID_RESERVED:          # 归一化后仍可能撞车 (如 "e-n-d" → "e_n_d" 不会, 但 "end" 变体要防)
        safe = f"n_{safe}"
    return safe


def _id_map(dag: "DAG") -> dict[str, str]:
    """stage 名 → Mermaid ID, 保证**全图唯一**。

    唯一性必须在这里保证而不能靠 `_safe_id`: 退化路径 (名字含非 ASCII 等) 会把
    不同名字归一化成同一个串 —— 实测 `"中文名"` 与 `"别的字"` 都变成 `___`,
    两个节点共用一个 ID 就是渲染错。`_safe_id` 是纯函数看不到全图, 冲突只在这里
    才暴露得出来。

    按 sorted 建映射 (而不是 topo_order): 同样内容的 DAG 每次生成同样的 ID,
    输出可复现。
    """
    used: set[str] = set()
    out: dict[str, str] = {}
    for name in sorted(dag.stages):
        base = _safe_id(name)
        cand, k = base, 1
        while cand in used:
            cand = f"{base}_{k}"
            k += 1
        used.add(cand)
        out[name] = cand
    return out


def to_mermaid(dag: DAG, *, direction: str = "LR") -> str:
    """DAG → Mermaid 图 (默认 LR 布局; 嵌 README / 飞书).

    R1 (0.5.5): 条件边渲染为带 key 标签的虚线 — 逻辑分叉/回路在图上一等可见.

    节点 ID 经 `_safe_id` 处理 (保留字 / 非法字符), 但**标签恒为 stage 原名** ——
    替换只发生在 Mermaid 内部的 ID 上, 图上看到的永远是原名。
    """
    ids = _id_map(dag)
    lines = [f"graph {direction}"]
    for name in dag.topo_order():
        lines.append(f'    {ids[name]}["{name}"]')
    for name in dag.stages:
        for dep in dag.stages[name].depends_on:
            lines.append(f"    {ids[dep]} --> {ids[name]}")
    for fname, edge in dag.conditional_edges.items():
        for key, tgt in sorted(edge.mapping.items()):
            lines.append(f'    {ids[fname]} -.{key}.-> {ids[tgt]}')
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