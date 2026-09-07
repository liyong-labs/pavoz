"""DAG 声明 + 拓扑排序 + 静态 cycle 检测.

用法:
    dag = DAG("research_pipeline")

    @dag.stage()
    async def s_plan(req, ctx): ...

    @dag.stage(depends_on=["s_plan"], retries=2, timeout=600)
    async def s_search(req, ctx): ...

DAG 是静态的: 定义一次, parse 一次. 不支持运行时改图 (v1 YAGNI).
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

__all__ = ["DAG", "CycleError", "NodeFn", "Stage", "UnknownDepError"]

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


class DAG:
    """静态 DAG. stage 之间只依赖前置 state key 语义 (顺序执行)."""

    def __init__(self, name: str):
        if not name or not name.strip():
            raise ValueError("DAG name 不能为空")
        self.name = name
        self._stages: dict[str, Stage] = {}
        self._definition_order: list[str] = []
        self._frozen = False

    # ── 注册 ────────────────────────────────────────────
    def stage(
        self,
        *,
        depends_on: list[str] | tuple[str, ...] | None = None,
        retries: int = 0,
        timeout: float | None = None,
    ) -> Callable[[NodeFn], NodeFn]:
        """@dag.stage() 装饰器. stage fn 签名: async def fn(req, ctx) -> dict."""

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
            )
            self._definition_order.append(name)
            return fn

        return _wrap

    # ── 只读访问 ────────────────────────────────────────
    @property
    def stages(self) -> dict[str, Stage]:
        return dict(self._stages)

    # ── 校验 ────────────────────────────────────────────
    def validate(self) -> None:
        """静态校验: 未知依赖 + 环检测. run 前自动调; 也可手动提前调."""
        for s in self._stages.values():
            for dep in s.depends_on:
                if dep not in self._stages:
                    raise UnknownDepError(
                        f"stage '{s.name}' depends_on '{dep}' 不存在 (DAG {self.name})"
                    )
        self._detect_cycle()
        self._frozen = True  # 校验通过即冻结, 防运行中改图

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
        """
        if upstream == downstream:
            return True
        seen: set[str] = set()
        queue = [downstream]
        while queue:
            n = queue.pop(0)
            for dep in self._stages[n].depends_on:
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
