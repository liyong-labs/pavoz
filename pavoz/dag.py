"""DAG 声明 + 拓扑排序 + 静态 cycle 检测.

用法:
    dag = DAG("research_pipeline")

    @dag.stage()
    async def s_plan(ctx): ...

    @dag.stage(depends_on=["s_plan"], retries=2, timeout=600)
    async def s_search(ctx): ...

DAG 是静态的: 定义一次, parse 一次. 不支持运行时改图 (v1 YAGNI).
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

__all__ = ["DAG", "CycleError", "NodeFn", "Stage", "UnknownDepError"]

logger = logging.getLogger(__name__)

NodeFn = Callable[..., Coroutine[Any, Any, dict]]


class CycleError(Exception):
    """DAG 含环 (静态检测, parse 时抛)."""


class UnknownDepError(Exception):
    """stage depends_on 引用了不存在的 stage."""


@dataclass
class Stage:
    """单个 stage 节点 (被 @dag.stage 装饰后产生)."""

    name: str
    fn: NodeFn
    depends_on: tuple[str, ...]
    retries: int = 0            # RetryableError 可重试次数
    timeout: float | None = None  # 本 stage 硬超时 (秒); None = 继承 run 的 absolute deadline
    # R2 (0.5.3): False = 即使输入 hash 未变也强制重跑 (副作用 stage 标记).
    skip_unchanged: bool = True
    # R1a (0.5.3, id=137): False = 拒绝硬杀 (CancelRegistry.cancel(mode="hard") 抛 NotKillable).
    killable: bool = True


@dataclass
class ConditionalEdge:
    """条件出边 (R1, 0.5.5): router stage 完成后由编排器求值 route_fn 选下一跳.

    route_fn: 同步纯函数 (ReadOnlyStateView) -> key; 同 state 必同 key —
    replay/resume 可判定性前提. mapping: key → 目标 stage (封闭集声明,
    运行时返回未声明 key → UnmappedRouteError). max_visits: 该边组触发上限;
    条件边构成回路时 validate() 强制非 None.
    """

    from_node: str
    route_fn: Callable[[Any], str]
    mapping: dict[str, str]
    max_visits: int | None = None


class DAG:
    """静态 DAG. stage 之间只依赖前置 state key 语义 (顺序执行)."""

    def __init__(self, name: str):
        if not name or not name.strip():
            raise ValueError("DAG name 不能为空")
        self.name = name
        self._stages: dict[str, Stage] = {}
        self._definition_order: list[str] = []
        self._conditional: dict[str, ConditionalEdge] = {}
        self._frozen = False

    # ── 注册 ────────────────────────────────────────────
    def stage(
        self,
        *,
        depends_on: list[str] | tuple[str, ...] | None = None,
        retries: int = 0,
        timeout: float | None = None,
        skip_unchanged: bool = True,
        killable: bool = True,
    ) -> Callable[[NodeFn], NodeFn]:
        """@dag.stage() 装饰器. stage fn 签名: async def fn(ctx) -> dict (v1 统一 ctx-only)."""

        def _wrap(fn: NodeFn) -> NodeFn:
            if not inspect.iscoroutinefunction(fn):
                raise TypeError(f"stage {fn.__name__} 必须是 async def (got {type(fn).__name__})")
            name = fn.__name__
            if name in self._stages:
                raise ValueError(f"duplicate stage name: {name}")
            if self._frozen:
                raise RuntimeError(f"DAG {self.name} 已冻结, 不能加 stage {name}")
            self._stages[name] = Stage(
                name=name,
                fn=fn,
                depends_on=tuple(depends_on or ()),
                retries=retries,
                timeout=timeout,
                skip_unchanged=skip_unchanged,
                killable=killable,
            )
            self._definition_order.append(name)
            return fn

        return _wrap

    # ── 条件边 (R1, 0.5.5) ──────────────────────────────
    def add_conditional_edges(
        self,
        from_node: str,
        route_fn: Callable[[Any], str],
        mapping: dict[str, str],
        *,
        max_visits: int | None = None,
    ) -> None:
        """声明条件出边: from_node 完成后, 编排器求值 route_fn(state) → key →
        下一个执行 mapping[key] (节点纯数据处置, 边决策归编排器).

        route_fn 必须是同步纯函数 (同 state 必同 key — replay/resume 可判定前提;
        LLM 判断请做成 judge-stage 写 state, 边只读 — LangGraph 官方同型).
        mapping 即 key 封闭集声明, 运行时返回未声明 key → UnmappedRouteError.
        max_visits: 该边组触发上限; 条件边构成回路时 validate() 强制非 None.
        结构校验 (目标存在 / 无混入 / 回路) 在 validate() 统一做 — 声明顺序自由.
        """
        if self._frozen:
            raise RuntimeError(f"DAG {self.name} 已冻结, 不能加条件边 (from {from_node})")
        if not callable(route_fn):
            raise TypeError(f"route_fn 必须可调用 (got {type(route_fn).__name__})")
        if not isinstance(mapping, dict) or not mapping:
            raise ValueError(f"mapping 必须是非空 dict (router '{from_node}')")
        if any(not isinstance(k, str) or not isinstance(v, str) for k, v in mapping.items()):
            raise ValueError(f"mapping 的 key/value 必须都是 str (router '{from_node}')")
        if from_node in self._conditional:
            raise ValueError(f"router '{from_node}' 已声明条件边 (每 stage 至多 1 条, R1)")
        if max_visits is not None and (not isinstance(max_visits, int) or max_visits < 1):
            raise ValueError(f"max_visits 须为正整数或 None (got {max_visits!r})")
        self._conditional[from_node] = ConditionalEdge(
            from_node=from_node, route_fn=route_fn,
            mapping=dict(mapping), max_visits=max_visits,
        )

    @property
    def conditional_edges(self) -> dict[str, ConditionalEdge]:
        """from_node → ConditionalEdge (只读副本)."""
        return dict(self._conditional)

    # ── 只读访问 ────────────────────────────────────────
    @property
    def stages(self) -> dict[str, Stage]:
        return dict(self._stages)

    # ── 校验 ────────────────────────────────────────────
    def validate(self) -> None:
        """静态校验: 未知依赖 + 环检测 + 条件边结构. run 前自动调; 也可手动提前调."""
        for s in self._stages.values():
            for dep in s.depends_on:
                if dep not in self._stages:
                    raise UnknownDepError(
                        f"stage '{s.name}' depends_on '{dep}' 不存在 (DAG {self.name})"
                    )
        self._detect_cycle()
        self._validate_conditional()
        self._frozen = True  # 校验通过即冻结, 防运行中改图

    def _validate_conditional(self) -> None:
        """条件边结构校验 (R1). 规则编号对齐设计文档 2026-09-23 v4 (实现期修订)."""
        if not self._conditional:
            return
        routers = set(self._conditional)
        # V1: from_node 与 mapping 目标必须已声明
        for fname, edge in self._conditional.items():
            if fname not in self._stages:
                raise UnknownDepError(
                    f"add_conditional_edges from_node '{fname}' 不存在 (DAG {self.name})"
                )
            for key, tgt in edge.mapping.items():
                if tgt not in self._stages:
                    raise UnknownDepError(
                        f"router '{fname}' mapping[{key!r}] 目标 '{tgt}' 不存在 (DAG {self.name})"
                    )
        # V2': router 的静态下游必须是其 mapping target — 否则静态旁路会绕过路由
        for s in self._stages.values():
            for dep in s.depends_on:
                if dep in routers and s.name not in self._conditional[dep].mapping.values():
                    raise ValueError(
                        f"stage '{s.name}' depends_on router '{dep}' 但不在其 "
                        f"mapping 目标里 — 静态旁路会绕过路由 (请改走条件边)"
                    )
        # V3': 条件边目标不可静态依赖 router — 目标与 router 的关系只走路由;
        # 初始触发走非 router 的静态父 (循环头) 或纯路由 (分支/循环体)
        for fname, edge in self._conditional.items():
            for t in edge.mapping.values():
                for dep in self._stages[t].depends_on:
                    if dep in routers:
                        raise ValueError(
                            f"条件边目标 '{t}' 不可静态依赖 router '{dep}' — "
                            f"初始触发请静态依赖非 router 前置, 重入走路由"
                        )
        # 回路: 条件边参与环 → 环上 router 必须显式 max_visits (LLM 循环烧钱护栏);
        # 非环条件边不强制 (分支-only 图零摩擦, id=265/266 收敛)
        reach = self._execution_reach()
        for fname, edge in self._conditional.items():
            on_cycle = any(fname in reach.get(t, set()) for t in edge.mapping.values())
            if on_cycle and edge.max_visits is None:
                raise ValueError(
                    f"router '{fname}' 的条件边构成回路, 必须显式 max_visits "
                    f"(防死循环; EnginePolicy.max_steps 仍全局兜底)"
                )
            if on_cycle:
                logger.warning(
                    "DAG %s: 条件边 '%s' 构成回路 (受 max_visits=%s 约束)",
                    self.name, fname, edge.max_visits,
                )
    def _execution_scc(self) -> dict[str, frozenset[str]]:
        """合成图 (静态 + 条件边) 的 Tarjan SCC: stage → 所在强连通分量."""
        adj: dict[str, list[str]] = {n: [] for n in self._stages}
        for s in self._stages.values():
            for dep in s.depends_on:
                adj[dep].append(s.name)
        for fname, edge in self._conditional.items():
            adj[fname].extend(edge.mapping.values())

        index = 0
        stack: list[str] = []
        on_stack: set[str] = set()
        indices: dict[str, int] = {}
        low: dict[str, int] = {}
        scc_of: dict[str, frozenset[str]] = {}

        def _strongconnect(v: str) -> None:
            nonlocal index
            indices[v] = low[v] = index
            index += 1
            stack.append(v)
            on_stack.add(v)
            for w in adj[v]:
                if w not in indices:
                    _strongconnect(w)
                    low[v] = min(low[v], low[w])
                elif w in on_stack:
                    low[v] = min(low[v], indices[w])
            if low[v] == indices[v]:
                comp: list[str] = []
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    comp.append(w)
                    if w == v:
                        break
                frozen = frozenset(comp)
                for m in comp:
                    scc_of[m] = frozen

        for node in self._stages:
            if node not in indices:
                _strongconnect(node)
        return scc_of

    def route_only_targets(self) -> set[str]:
        """条件边目标里 "只经路由触发" 的子集 (R1).

        边 F→T 分型 (自动推导, zeroflow is_loopback 的自动版):
        - 前向边 (T 不可达 F — 分支): T 只经路由. 否则未命中的分支也会 topo 触发.
        - 回路边 (T 可达 F — 回路): T 有静态父 → 参与 topo 初始触发 (循环头);
          无静态父 → 只经路由 (循环体), 除非其所在环**完全没有**带静态父的成员 —
          那种纯路由环豁免首个声明成员当入口 (否则环不可达).
        """
        reach = self._execution_reach()
        scc_of = self._execution_scc()
        route_only: set[str] = set()
        exempted: set[str] = set()  # 纯路由环的候选入口
        for fname, edge in self._conditional.items():
            for t in edge.mapping.values():
                if fname in reach.get(t, set()):  # 回路边
                    if not self._stages[t].depends_on:
                        scc = scc_of.get(t, frozenset({t}))
                        if any(self._stages[m].depends_on for m in scc):
                            route_only.add(t)  # 环有入口, 循环体只经路由
                        else:
                            exempted.add(t)  # 纯路由环 → 稍后豁免一个入口
                else:  # 前向边 (分支)
                    route_only.add(t)
        if exempted:
            entry = min(exempted, key=lambda n: self._definition_order.index(n))
            route_only.discard(entry)
            route_only |= (exempted - {entry})
        return route_only

    def _execution_reach(self) -> dict[str, set[str]]:
        """执行方向可达集: n → 静态下游 ∪ 条件边目标. 回路判定用 (含条件边,
        与 _detect_cycle 只看静态 depends_on 互补)."""
        adj: dict[str, set[str]] = {n: set() for n in self._stages}
        for s in self._stages.values():
            for dep in s.depends_on:
                adj[dep].add(s.name)
        for fname, edge in self._conditional.items():
            adj[fname].update(edge.mapping.values())
        reach: dict[str, set[str]] = {}
        for start in self._stages:
            seen: set[str] = set()
            queue = list(adj[start])
            while queue:
                n = queue.pop(0)
                if n in seen:
                    continue
                seen.add(n)
                queue.extend(adj[n] - seen)
            reach[start] = seen
        return reach

    # ── 拓扑排序 (Kahn) ─────────────────────────────────
    def topo_order(self) -> list[str]:
        """返回拓扑顺序的 stage 名列表. 含环则抛 CycleError."""
        self.validate()
        indeg = {n: len(self._stages[n].depends_on) for n in self._definition_order}
        ready = [n for n in self._definition_order if indeg[n] == 0]
        order: list[str] = []
        while ready:
            n = ready.pop(0)
            order.append(n)
            for m in self._definition_order:
                if n in self._stages[m].depends_on:
                    indeg[m] -= 1
                    if indeg[m] == 0:
                        ready.append(m)
        if len(order) != len(self._stages):
            cyclic = [n for n in self._definition_order if n not in order]
            raise CycleError(f"DAG {self.name} 含环, 环内节点: {cyclic}")
        return order

    def reachable(self, upstream: str, downstream: str) -> bool:
        """upstream 是否 downstream 的传递依赖 (含直接 depends_on).

        链式覆盖判定用: downstream stage 覆盖 upstream producer 写的 state key
        是数据流水线演进的明确意图 (v0.1.1), 平行 producer 才 raise 冲突.
        R1 (0.5.5): 条件边目标把 router 视为祖先 (经路由触发 = 数据血缘经 router).
        """
        if upstream == downstream:
            return True
        seen: set[str] = set()
        queue = [downstream]
        while queue:
            n = queue.pop(0)
            parents = list(self._stages[n].depends_on)
            for fname, edge in self._conditional.items():
                if n in edge.mapping.values():
                    parents.append(fname)
            for dep in parents:
                if dep == upstream:
                    return True
                if dep not in seen:
                    seen.add(dep)
                    queue.append(dep)
        return False

    # ── Cycle 检测 (Tarjan SCC) ─────────────────────────
    def _detect_cycle(self) -> None:
        """Tarjan SCC: 任一 SCC 大小 >1 或自环 → CycleError."""
        index = 0
        stack: list[str] = []
        on_stack: set[str] = set()
        indices: dict[str, int] = {}
        low: dict[str, int] = {}
        result: list[list[str]] = []

        def _strongconnect(v: str) -> None:
            nonlocal index
            indices[v] = low[v] = index
            index += 1
            stack.append(v)
            on_stack.add(v)
            for w in self._stages[v].depends_on:
                if w not in indices:
                    _strongconnect(w)
                    low[v] = min(low[v], low[w])
                elif w in on_stack:
                    low[v] = min(low[v], indices[w])
            if low[v] == indices[v]:
                scc: list[str] = []
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    scc.append(w)
                    if w == v:
                        break
                if len(scc) > 1:
                    result.append(scc)
                elif len(scc) == 1 and scc[0] in self._stages[scc[0]].depends_on:
                    result.append(scc)  # 自环

        for node in self._definition_order:
            if node not in indices:
                _strongconnect(node)
        if result:
            raise CycleError(f"DAG {self.name} 含环: {result}")

    def __repr__(self) -> str:
        return f"<DAG {self.name} stages={list(self._stages)}>"
