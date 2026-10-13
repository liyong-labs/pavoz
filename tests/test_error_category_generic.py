"""未知异常的 category 透传 — 不要求继承 StageError 才能携带 category.

背景 (ai@home id=2925 提出): `_run_stage` 的泛型 `except Exception` 分支把
error_category 硬编码成 None, 于是:

- 自定义异常即使带 .category 也拿不到分流信息, 消费方只能退回"错误消息里有没有
  触发词"这种字符串匹配来判断 —— 哪天触发词表加个词, 行为就静默变了
- **UnmappedRouteError / MaxVisitsExceeded / OrphanStagesError 已经在 __init__
  接受 category (它们继承 PavozError), 但走泛型分支时同样丢失** ——
  此前它们没被观察到, 是因为没有调用方给它们传过 category

修复: 泛型分支改 `getattr(e, "category", None)`, 与已知类分支一致。

**行为不变的边界要说清**: 没有任何调用方给上述三个异常传过 category, 所以对现有
调用点逐字节不变; 变的是"带 category 的未知异常"从此能透传。FatalError 语义
(不重试、终态 failed) 不受本改动影响 — 下面有用例钉住。
"""
from __future__ import annotations

from pavoz import DAG, CheckpointStore, FileStorage, Runtime


class _Coded(Exception):
    """不继承 StageError, 但自带 category 的自定义异常。"""

    def __init__(self, msg: str, category: str | None = None):
        super().__init__(msg)
        self.category = category


def _rt(tmp_path) -> Runtime:
    return Runtime(checkpoint_store=CheckpointStore(FileStorage(str(tmp_path / "cp"))))


async def test_custom_exception_category_passes_through(tmp_path):
    dag = DAG("coded")

    @dag.stage()
    async def s_x(ctx):
        raise _Coded("boom", category="CAT_CUSTOM")

    r = await _rt(tmp_path).run(dag, task_id="t1")
    assert r.status == "failed"
    assert r.error_class == "_Coded"
    assert r.error_category == "CAT_CUSTOM", "带 category 的未知异常应透传"


async def test_unknown_exception_without_category_still_none(tmp_path):
    """没带 category 的异常 → None (向后兼容: 绝大多数异常如此)。"""
    dag = DAG("plain")

    @dag.stage()
    async def s_x(ctx):
        raise ValueError("普通错误")

    r = await _rt(tmp_path).run(dag, task_id="t2")
    assert r.status == "failed"
    assert r.error_class == "ValueError"
    assert r.error_category is None


async def test_category_does_not_grant_retry(tmp_path):
    """★ 关键边界: 携带 category 不改变 FatalError 语义 —— 仍然不重试。

    category 是**分流元数据**, 不是"可重试"信号。若哪天有人把带 category 的异常
    纳入重试路径, 这条会红。
    """
    calls = {"n": 0}
    dag = DAG("no_retry")

    @dag.stage(retries=3)
    async def s_x(ctx):
        calls["n"] += 1
        raise _Coded("boom", category="CAT_CUSTOM")

    r = await _rt(tmp_path).run(dag, task_id="t3")
    assert r.status == "failed"
    assert calls["n"] == 1, "未知异常必须只执行一次 (FatalError 语义), 携带 category 不改变这点"


async def test_cond_edge_error_keeps_its_own_semantics(tmp_path):
    """条件边异常走泛型分支 → category 仍透传 (它们接受 category 参数)。"""
    dag = DAG("cond")
    dag.add_conditional_edges("s_a", lambda s: s["v"], {"ok": "s_b"})
    dag.add_conditional_edges("s_b", lambda s: "ok", {"ok": "s_b"}, max_visits=2)

    @dag.stage()
    async def s_a(ctx):
        return {"v": "nowhere"}          # 未声明 key → UnmappedRouteError

    @dag.stage()
    async def s_b(ctx):
        return {"b": 1}

    r = await _rt(tmp_path).run(dag, task_id="t4")
    assert r.status == "failed"
    assert r.error_class == "UnmappedRouteError"
    assert r.error_category is None      # 没人传 category → 仍是 None (兼容)
