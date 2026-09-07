# ai_research PRD (2026-09-07)

> 配套设计 doc: [`design.md`](design.md) (5 stage 详解) / [`pavoz-requirements.md`](pavoz-requirements.md)
> 设计依据: [`handoff/ai-research/competitive-analysis.md`](handoff/ai-research/competitive-analysis.md) §3 / §8 / §9 / §10 / §11 / §12

---

## 1. 问题

用户在 `aics-platform` 工作流中常问"X 模型在 3080/10G 上能不能跑 / 哪个 prompt 模板最适合 / 某算法在某数据集上的 state-of-art"这类**有边界的研究问题**. 现有工具的不足:

- **gpt-researcher** (29.3k★): 1200 词长文, URL append 零校验 → 假引用 + 必读压力大
- **STORM** (31.2k★): Wikipedia 条目级产物, mind map 导航但**不是证据地图**, 索引一致性工程但**没有论证层**
- **ODR** (12.7k★, archived): Sources 段干净, 但**没有"挖不到"段** = 假装完备, 直接踩用户铁律
- **ADR** (1.7k★): `<solution>` 字符串, 没有产物

> user 原话 (2026-08): "衡量标准不是答案对不对, 是证据链全不全, 边界标得诚实不诚实"

## 2. 目标

**做一个 deep-research agent**, 不是 chat-bot, 是**frontier-first 而非 plan-first** 的研究助手:

1. 沿 frontier 挖证据 (饱和熔断, 不是预算熔断)
2. 产物 = **证据地图** (5 段: 已证实/社区声称/理论可跑/已知未知/矛盾仲裁), 不是长文
3. 论证层 3 件 (4 仓 + ai_writer 全员缺失): supports/contradicts 关系 + 多源升档 + 反向证伪 + 矛盾仲裁
4. 每条 insight 绑源 [ref:xxxx], 缺源 → 渲染失败 (用 ai_writer `citation_precheck`)
5. 用户可中途反问 (ODR 模式 structured clarify), 也可"挖到饱和自动停" (no_progress 熔断)

## 3. user stories

| ID | 场景 | 期望 |
|---|---|---|
| US-1 | 用户问 "适合 3080/10G 的模型有哪些" | DAG 跑: clarify → search → personas (3) → loop × N → render. 产物含 5 段, ≥3 独立模型对比, T1-T5 条目级分级 |
| US-2 | 用户中途改主意: "再加 7B Q2" | fork_run from s_personas with overrides, 续跑 |
| US-3 | 用户中途反问: "你打算怎么挖" | clarify structured 输出 `{plan, verification}`, task 置 waiting_user_input |
| US-4 | 挖 12 轮还没新发现 | RevisionLoop.no_progress 熔断, 产物含 "已知未知" 段, 显式说"这次没覆盖" |
| US-5 | 用户改 query 后再跑 | fork_run 合并 frontier (新 learnings 增量入库, 已搜的去重) |
| US-6 | 用户导出产物 | 单页 markdown 5 段, [ref:xxxx] 全量引用, 矛盾仲裁段独立 |

## 4. 范围

### M0 (本次)

- 5 stage 实现 (`s_clarify` / `s_warm_search` / `s_personas` / `s_research_loop` / `s_render_map`)
- 状态 schema (`research/state.py` TypedDict: Source/Learning/OpenQuestion/SearchedQuery/Contradiction)
- 3 LLM 档 (FAST/SMART/STRATEGIC), STRATEGIC 默认 R1 (无 fn-call, 抄 ADR 降级层)
- 搜索 2 后端 (searxng 自托管 + duckduckgo fallback), requires_scraping 协议 + ODR 风格 compress
- 产物: 单页 markdown 5 段 + Sources
- CLI `--query "..."` 跑通

### M1+

- 跨 session frontier 合并 (`fork_run merge_frontier`)
- TaskInterrupt (durable, 跨项目值得沉淀)
- KnowledgeBaseNode (pavoz 原语扩展)
- web UI (DAG 可视化 + 状态机)

### 不做 (YAGNI)

- NLI / 贝叶斯可信度 / KG embedding / mind map 节点树 / interrupt / 多 LLM provider 抽象 / bench / dashboard

## 5. 非目标

- ❌ 不是 chat-bot (不接对话历史跨 session 状态)
- ❌ 不是 plan-first (先真搜再扩, 不先列 outline 再填)
- ❌ 不是单一 LLM (强制 3 档分工)
- ❌ 不是 Wikipedia 条目 (单页 markdown ≤ 1500 词, 5 段证据地图)
- ❌ 不是 editor (用户改产物 → 重新跑 DAG, 不做 in-place edit)

## 6. 验收

| ID | 标准 |
|---|---|
| AC-1 | CLI `python -m dags.research_dag --query "适合 3080/10G 的模型"` 一次跑通到 render_map |
| AC-2 | 产物含 5 段 (已证实/社区声称/理论可跑/已知未知/矛盾仲裁) |
| AC-3 | 同 query 跑第二次, learnings cache 命中 + merge_frontier 合并去重 |
| AC-4 | frontier state 落 pavoz cp |
| AC-5 | 连续 2 轮新 learnings < 阈值, RevisionLoop 熔断 + 已知未知段显式 |
| AC-6 | 至少 1 条 learning 跑过反向证伪 (counter_searched=True) |
| AC-7 | 至少 1 对矛盾完成仲裁 (arbiter_verdict 非空) |
| AC-8 | ≥2 独立域多源印证条目, grade 已升档 |
| AC-9 | 每行产物 ≥1 个 [ref:xxxx], 占位符缺失 → 渲染失败 |

## 7. 风险

| 风险 | 缓释 |
|---|---|
| STRATEGIC 档 R1 无 fn-call 写代码 | 抄 ADR `fn_call_converter` ~150 行降级层 |
| searxng 自托管 DDG 限速 | 指数退避 + 多 backend fallback, 第 5 次 raise (LLM_NO_FALLBACK 铁律) |
| 反向证伪找不到反方 = 空产物段 | 显式记 "no counter found in N rounds", 不静默 |
| 多 persona 并发 = API rate limit | semaphore=3, persona 串行 per loop 轮次 |
| LLM 幻觉污染 frontier | 每 learning 强制 src_ids ≥1, 缺源拒绝入库 |

## 8. 关联

- ai_writer (同级项目): 复用 T1-T5 分级 + citation_precheck + 反向生成模式
- ai_mindmap (M1+): evidence_map → mind map 自动转换
- haomem: 产物 [ref:xxxx] 自动入 KB, 跨 session 召回
- pavoz: 编排框架, 不修原语, M1+ 才提 PR (TaskInterrupt)

---

(全局 work-note 铁律见 `~/.claude/common/claude-common.md`)
