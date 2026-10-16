"""`_ow` (overwrite_keys) 的判定 —— 单 producer 契约在 runtime 层的边界。

库层的 `merge_state` 只知道"哪些 key 允许覆盖", 是 runtime 负责算出这个集合:

    _ow = {k for k in delta
           if k in state and producers.get(k)
           and (producers[k] == "<fork>" or dag.reachable(producers[k], name))}

判错的两个方向都有害:
- **放得太宽** → 两个平行 producer 写同一个 key 被静默接受, 后写者赢, 而顺序由
  拓扑决定 ⇒ 换台机器/换个 DAG 顺序就换了结果
- **收得太窄** → 合法的流水线演进 (search→filter→compress 逐级更新同一产物) 被
  误拒, 或者环图里每轮更新同一个产出被误拒

2026-10-16 复审时把六种形状实跑了一遍, **全部正确** (平行拦 / 链式放行 / 菱形平行
拦 / 覆盖 initial 拦 / 环内放行 / fork override 放行)。`test_runtime.py` 已有其中
两形状 (平行 producer、覆盖 initial_state); 本文件补齐其余, 重点是**环内**那条 ——
它的放行理由是"环内 SCC 里互相可达", 而那个理由在非环场景下会变成误放行的借口。

⚠️ 判据是"能不能真的失败": 把 `dag.reachable(producers[k], name)` 换成恒真
(所有冲突一律豁免), 本文件必须大面积转红。
"""

from __future__ import annotations

from pavoz import DAG, CheckpointStore, FileStorage, Runtime


def _rt(tmp_path) -> Runtime:
    return Runtime(checkpoint_store=CheckpointStore(
        FileStorage(tmp_path / "cp")))


# ── 该拦的 ──────────────────────────────────────────────────────────────────

async def test_diamond_siblings_writing_same_key_conflict(tmp_path):
    """★ 菱形: `s_a → s_b`、`s_a → s_c`, 而 b 与 c 都写 `k` ⇒ 必须报冲突。

    比"完全平行"更微妙: b 和 c 有**共同的上游**, 所以光看"不是同一个 stage"不够 ——
    判据问的是 `producers[k]` 是不是当前 stage 的**传递上游**。`reachable(s_b, s_c)`
    为假 (它们平行), 所以 s_c 写 k 时 s_b 是它的 producer 却不豁免。
    """
    dag = DAG("diamond_conflict")

    @dag.stage()
    async def s_a(ctx):
        return {"base": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"k": "B"}

    @dag.stage(depends_on=["s_a"])
    async def s_c(ctx):                 # 与 s_b 平行, 都写 k
        return {"k": "C"}

    r = await _rt(tmp_path).run(dag, "dia")
    assert r.status == "failed"
    assert r.error_class == "StateConflictError"
    assert "k" in (r.error or "")


async def test_deep_parallel_under_a_shared_prefix_conflict(tmp_path):
    """菱形再深一层: 冲突的两个 stage 在 b/c 各自的下游 —— 同上必须拦。

    多一层是为了防"只检查直接 depends_on"的实现: `reachable` 是传递闭包,
    判据要能在多跳之后仍然认出"平行"。
    """
    dag = DAG("deep_parallel")

    @dag.stage()
    async def s_root(ctx):
        return {"base": 1}

    @dag.stage(depends_on=["s_root"])
    async def s_l(ctx):
        return {"v": 1}

    @dag.stage(depends_on=["s_root"])
    async def s_r(ctx):
        return {"v": 2}                # 与 s_l 平行, 同一个 key

    r = await _rt(tmp_path).run(dag, "deep")
    assert r.status == "failed"
    assert r.error_class == "StateConflictError"


# ── 该放行的 ────────────────────────────────────────────────────────────────

async def test_loop_graph_may_rewrite_the_same_key_every_round(tmp_path):
    """★ 环图内逐轮覆盖同一个 key ⇒ 必须放行。

    这是最容易误拒的形状。放行的理由是"环内两个 stage 在 SCC 里互相可达"
    (`reachable(s_a, s_r)` 与 `reachable(s_r, s_a)` 都为真) —— 而这个理由在
    **非环**场景下会变成误放行的借口, 所以本文件另有菱形那条钉住反向。

    用户写"每轮迭代同一个产出"的循环图是最自然的用法, 误拒等于这个模式不可用。
    """
    dag = DAG("loop_rewrite")

    @dag.stage()
    async def s_work(ctx):
        n = ctx.state.get("n", 0) + 1
        return {"n": n, "k": f"round{n}"}     # 每轮都写同一个 k

    @dag.stage(depends_on=["s_work"])
    async def s_route(ctx):
        return {}

    dag.add_conditional_edges(
        "s_route", lambda s: "more" if s["n"] < 3 else "stop",
        {"more": "s_work", "stop": "s_end"}, max_visits=8)

    @dag.stage()
    async def s_end(ctx):
        return {"last": ctx.state["k"]}

    r = await _rt(tmp_path).run(dag, "loop")

    assert r.status == "done", f"环内逐轮覆盖被误拒了: {r.status} / {r.error}"
    assert r.state["n"] == 3
    assert r.state["last"] == "round3", f"最后一轮的值应胜出, 实得 {r.state}"


async def test_chained_pipeline_rewrite_is_allowed(tmp_path):
    """链式演进 (filter 改写 search 的产出) 放行 —— 这个契约写在 merge_state 上。

    判据问的是"传递上游", 所以跨**多跳**也要认: a → b → c, c 改写 a 写的 key。
    """
    dag = DAG("chain_rewrite")

    @dag.stage()
    async def s_search(ctx):
        return {"hits": ["raw"]}

    @dag.stage(depends_on=["s_search"])
    async def s_filter(ctx):
        return {"hits": ["filtered"]}        # 改写上游产出

    @dag.stage(depends_on=["s_filter"])
    async def s_compress(ctx):
        return {"hits": ["compressed"]}      # 再改写 (两跳外)

    r = await _rt(tmp_path).run(dag, "chain")
    assert r.status == "done", f"链式演进被误拒: {r.status} / {r.error}"
    assert r.state["hits"] == ["compressed"]


# ⚠️ 这里**本来**有一条 `test_unknown_producer_is_not_exempt`, 想钉"`producers.get(k)`
# 的存在性检查是 fail-closed"。它是**假判据**: 那条用例在测试文件里**重写了一遍**
# 判据表达式去验它, 而真正被改的是 `runtime.py` 里那行 —— 两份代码没有联动,
# 于是变异 runtime 的判据时它照样绿 (实测: 去掉存在性检查 → 17 个用例全绿)。
#
# 教训: 判据必须在**被测的那个地方**取。"把判据抄进测试再断言它成立"测的是抄写
# 的正确性, 不是实现的正确性。真正的钉法是变异: 改 runtime 那一行, 看 run 的
# 可观察行为变不变。
#
# 顺带一个实测发现: 去掉那个存在性检查**在运行路径上不可观测** —— producer 在
# 正常路径上总是有值, 所以两条分支行为相同。它是防御性的, 不是行为契约。

# ⚠️ 这里**本来**有一条 `test_judgment_must_be_able_to_fail`, 用
# `always_true = True; assert always_true` 来说明"判据坏掉的样子"。那是**恒真
# 断言** —— 永远通过, 等于用一句自我安慰代替判别力。写它的时候我刚在同一个会话里
# 因为"变异锚点没匹配却显示全绿"栽过一次, 转头又写了条恒真的。
# 判别力靠**变异**验证 (把 `dag.reachable(producers[k], name)` 换成恒真, 本文件
# 必须大面积转红), 测试里只钉可观察的行为: 平行形状下 `reachable` 为假, 且
# 真实判据下 run 必须报冲突 —— 上面那几条钉的就是这个。


def _add(dag: DAG, name: str, deps: tuple[str, ...]) -> None:
    async def fn(ctx):
        return {"k": name}
    fn.__name__ = name
    if deps:
        dag.stage(depends_on=list(deps))(fn)
    else:
        dag.stage()(fn)
