# conditional edges: 让编排器持所有边 (R1, 0.5.5)

审计-修订这类典型 control flow (`review → fail → revision → 再 review`), 以前只能
塞进 stage 内部写 while — 节点不再纯, 循环轮次失去 checkpoint/观测/diff 边界。
conditional edge 把 control flow 还给编排器: **节点只做数据处理 + LLM 调用,
边决策归编排器, 循环每一轮都是可续跑/可观测/可 diff 的边界。**

## 最小用法 (ai-writing story 链同型)

```python
dag = DAG("story")

@dag.stage()
async def s_write(ctx): ...

@dag.stage(depends_on=["s_write"])
async def s_review(ctx):
    r = int(ctx.state.get("round", 0)) + 1
    verdict = "pass" if r >= 2 else "fail"
    return {"round": r, "verdict": verdict}          # 只写数据, 不决定下一步

@dag.stage()
async def s_revision(ctx): ...

@dag.stage()
async def s_save(ctx): ...

dag.add_conditional_edges(
    "s_review",
    lambda s: s["verdict"],                          # 同步纯函数: 只读 state
    {"pass": "s_save", "fail": "s_revision"},        # key → 目标 (封闭集)
    max_visits=5,                                    # 回路必填 (防 LLM 死循环)
)
dag.add_conditional_edges(
    "s_revision",
    lambda s: "re_review",
    {"re_review": "s_review"},
    max_visits=5,
)
```

执行序: `s_write → s_review → fix → s_revision → 回 s_review → pass → s_save`。
`max_visits` 触顶 → `MaxVisitsExceeded` (run failed, failed_stage = router)。

## LLM 判断: judge-stage 模式 (推荐)

路由需要 LLM 模糊判断时, **不要在边上调 LLM** — 把判断做成一个普通 stage:

```python
@dag.stage(depends_on=["s_audit"])
async def s_route_judge(ctx):
    """LLM 看 audit 报告 + 轮次, 产出路由决策数据."""
    decision = await judge_llm(ctx.state["audit_report"], ctx.state["round"])
    return {"route": decision, "route_reason": ..., "route_score": ...}

dag.add_conditional_edges(
    "s_route_judge",
    lambda s: s["route"],                            # 纯读 state
    {"ship": "s_save", "revise": "s_revision", "escalate": "s_save"},
    max_visits=4,
)
```

为什么 judge-stage 严格更优: 判断 LLM 调用自带 checkpoint (崩了不重跑上游)、
RetryableError 自动退避重试、进 stage_timings、可 diff — 这些边路由都给不了。
这也是 LangGraph (router 节点 + 条件边) / Temporal (LLM 必须放 Activity) /
Step Functions (决策放 Task, Choice 纯求值) 四家收敛的同一分层。

约定: judge-stage 用 `s_route_judge` / `s_decide_judge` 前缀; 输出统一写
`state["route"]` (边读) + `state["route_reason"]` / `state["route_score"]` (trace)。

## 语义速查

| 主题 | 规则 |
|---|---|
| route_fn | 同步纯函数, `(state) -> key`; 同 state 必同 key — 重放/resume 可判定的前提 |
| mapping | 封闭集声明; 未声明 key → `UnmappedRouteError` (fail-loud, 无 default) |
| max_visits | router 单次 run 执行次数上限; 回路必填, 分支图可选; `MaxVisitsExceeded` 兜底 |
| 全局兜底 | EnginePolicy.max_steps 仍生效 (双层护栏) |
| 结构校验 | 目标不可静态依赖 router; router 的静态下游必须是 mapping 目标; 回路 WARNING |
| 触发 | 回路边目标带静态父 → topo 初始触发 (循环头); 分支目标/循环体 → 只经路由 |
| workflow_hash | route_fn 源码 + mapping + max_visits 全计入 — 改路由 = 结构变, 拒续 |
| resume | 从最后完成 stage 的路由重判定 (sync 纯函数 → 同路由续跑); visit 续号 |
| fork/replay | R1 不支持 (静态续跑会截断路由目标), R2 再议 |
| 观测 | `route` 事件 `{from, key, to}`; stage 事件带 `visit`; stage_timings 累计 |
| viz | mermaid 虚线带 key 标签; graph_json edges 带 kind/key/max_visits |

## 和 fork_run 的关系

`fork_run` 是确定性重放原语, 条件边引入执行路径分叉后语义未定义 — R1 在条件边图上
直接拒绝 (RuntimeError)。分支场景的调试: 改完直接 `run` + `diff_runs` 对比两次结果。
