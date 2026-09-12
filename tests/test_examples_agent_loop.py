"""示例不可腐化: examples/agent_loop.py 是公开文档引用的质量门参考实现.

架构文档 (docs/{cn,en}/architecture.md 扩展面一节) 指向它, 所以它必须一直可跑.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

from pavoz import Runtime

_EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "agent_loop.py"


def _load_example():
    spec = importlib.util.spec_from_file_location("agent_loop_example", _EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def test_inline_loop_dag_converges():
    mod = _load_example()
    result = await Runtime().run(mod.dag, task_id="ex-inline")
    assert result.status == "done"


async def test_gate_dag_converges_and_scores_are_emitted():
    mod = _load_example()
    events: list[tuple[str, dict]] = []
    rt = Runtime(on_event=lambda e, d: events.append((e, d)))

    result = await rt.run(mod.gate_dag, task_id="ex-gate")

    assert result.status == "done"
    scores = [d for e, d in events if e == "gate_score"]
    assert scores, "gate 应把每轮分数写进 on_event"
    assert scores[0]["score"] < scores[-1]["score"], "revise 之后分数应上升"
    assert set(scores[0]["lenses"]) == {"mentions-topic", "has-signoff"}, "每个 lens 都要有分"
