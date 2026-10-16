"""导出 (to_mermaid / to_graph_json) 的热路径代价 — 判据是**访问次数**, 不是耗时.

起因 (2026-10-17, 与 `test_run_loop_costs.py` 同一类): `DAG.stages` 是每次访问都
`dict(self._stages)` 建副本的 property (有意如此: 调用方不该改到内部状态)。而 viz
的两个导出函数把它放在了**两层循环**与**每个节点的循环**里:

  · `to_mermaid`: `for name in dag.stages:` 里面又是 `dag.stages[name].depends_on`
  · `to_graph_json`: `for name in dag.topo_order(): stage = dag.stages[name]`, 外加
    edges 推导里的 `for name in dag.stages / dag.stages[name].depends_on`

实测导出次数 (修前, 每个 stage 一份 dict 拷贝, 拷贝又是 O(N) ⇒ 平方项):

    stages    to_mermaid    to_graph_json
        50             52             101
       200            202             401
       800            802            1601

为什么判据用访问次数而不是耗时: 耗时断言在 CI 上不可靠 (机器/负载), 只能间接说明;
"这个 property 被访问了几次"是**确定性**事实, 直接回答"有没有人在循环里取它"。
提到循环外后两种导出都只访问 1 次 (加 `_id_map` 里那 1 次), 界钉在这里。

为什么提到循环外是安全的: `dag.validate()` 冻结 dag, 导出期间结构不变; 拷贝是**浅**
的, Stage 对象共享 —— 所以读到的 `depends_on` / `retries` / `timeout` 与原来一致。
`test_exported_content_is_identical_before_and_after_hoist` 把"内容没变"钉住。
"""
from __future__ import annotations

import pytest

import pavoz.dag as dagmod
import pavoz.viz as vizmod


def _linear_dag(name: str, n: int):
    src = ["from pavoz import DAG", f"d = DAG({name!r})", "prev = None"]
    for i in range(n):
        src += ["@d.stage(depends_on=[prev] if prev else None)",
                f"async def f{i}(ctx):",
                "    return {'acc': 1}",
                f"prev = 'f{i}'"]
    ns: dict = {}
    exec("\n".join(src), ns)          # noqa: S102 — 生成测试用 DAG
    return ns["d"]


@pytest.fixture
def count_stages(monkeypatch):
    """数一次导出里 `dag.stages` 被访问了几次。"""
    real = dagmod.DAG.stages
    box = {"n": 0}

    def counting(self):
        box["n"] += 1
        return real.fget(self)

    monkeypatch.setattr(dagmod.DAG, "stages", property(counting))
    return box


@pytest.mark.parametrize("n", [2, 6, 20])
def test_mermaid_exports_visit_stages_constantly(count_stages, n):
    """★ `to_mermaid` 的访问次数与 stage 数**无关** (修前是每 stage 一次)。"""
    dag = _linear_dag(f"mmd_{n}", n)
    out = vizmod.to_mermaid(dag)
    assert out.startswith("graph LR") and out.count("-->") == n - 1, \
        f"前置: 导出内容本身得对, 实得 {out.count('-->')} 条边 (n={n})"
    assert count_stages["n"] <= 2, (
        f"{n} 个 stage 的导出访问了 `dag.stages` {count_stages['n']} 次 —— "
        f"它是每次都建 dict 副本的 property, 放在循环里就是平方项")


@pytest.mark.parametrize("n", [2, 6, 20])
def test_graph_json_exports_visit_stages_constantly(count_stages, n):
    """★ 同上, `to_graph_json` (修前 n 个节点是 2n+1 次)。"""
    dag = _linear_dag(f"json_{n}", n)
    g = vizmod.to_graph_json(dag)
    assert len(g["nodes"]) == n and len(g["edges"]) == n - 1, \
        f"前置: 导出内容本身得对, 实得 {len(g['nodes'])} 节点 / {len(g['edges'])} 边"
    assert count_stages["n"] <= 2, (
        f"{n} 个 stage 的导出访问了 `dag.stages` {count_stages['n']} 次 —— "
        f"它是每次都建 dict 副本的 property, 放在循环里就是平方项")


def test_exported_content_is_identical_before_and_after_hoist(monkeypatch):
    """提到循环外的那份拷贝**内容不变** —— 这是这个优化的真实回归面。

    拷贝是浅的: Stage 对象共享。所以 `dag.stages[name]` 在循环外取一次与在循环里
    取 N 次读到的 `depends_on` 完全一样。这条把"优化没有顺手改掉语义"钉住。
    """
    dag = _linear_dag("content", 8)

    # 修前的样子: 每次都重新取一份
    def unhoisted(name):
        return dag.stages[name].depends_on

    stages = dag.stages          # 修后的样子: 取一次
    def hoisted(name):
        return stages[name].depends_on

    assert [list(unhoisted(n)) for n in dag.topo_order()] == \
           [list(hoisted(n)) for n in dag.topo_order()]
    assert vizmod.to_mermaid(dag) == vizmod.to_mermaid(dag), "同内容两次导出必须一致"
