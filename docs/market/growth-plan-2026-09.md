# pavoz 市场与增长计划 (2026-09-06, user 拍板: 打造成高星热门项目)

> 依据: 3 路调研 agent (README 拆解 8 头部库 / 0→1k 增长打法实证 / 市场格局 — 后补) +
> 本仓库工程现状. 目标: 公开后首年几百 star 为成功基线, 1k+ 需 ≥1 轮 HN 病毒或生态内容破圈.

## 残酷基线 (增长 agent 实证)

- 无光环 Python DAG/工作流库常态 = **几十-几百 star** (awesome-python Hidden Gem 档 100-500)
- 千级参照: Hamilton/Burr ~2.5k (数年 + Apache/公司光环); ControlFlow 1.4k 已归档;
  langgraph 40k 但有 LangChain 品牌 + 2025 agent 浪潮
- 刷星/刷下载无因果作用 (田野实验) — 只做真实分发 + 真实价值
- pavoz 可比参照 = Burr (dependency-free, low-abstraction 同族)

## 定位 (为什么是我们)

一句话: **The in-process Python workflow engine — every run resumable, replayable, forkable; zero runtime dependencies.**

4 卖点 (README v2 已落地):
1. 进程内 + 零运行时依赖 — "no server, no scheduler, no YAML" 对仗句
2. 不绑 vendor: ctx.call seam + StorageBackend adapter, core stdlib only
3. **时间旅行调试** (replay + fork) — 8 家头部 README 均未占的表述词位; DBOS fork_workflow 证明需求真实 (但绑 Postgres, 我们不绑)
4. 静态 DAG + workflow_hash + 诚实 state 模型 (冲突显式 raise)

当用/不当用边界已写进 README (诚实框架 = HN/r-Python 社区信任前提).

## Repo 就绪度清单 (已完成/待办)

✅ 完成: README v2 产品页 (en+cn) / 3 可运行示例画廊 / CI matrix (py3.12/3.13) /
   PyPI publish workflow (Trusted Publishing, tag v*) / console script / v0.7.0 版本同步 /
   CHANGELOG / LICENSE / CONTRIBUTING / docs (en+cn quickstart/architecture/api/use-cases)
⏳ 待公开后: GitHub topics + description 关键词 (见下) / GitHub Discussions 开启 /
   MkDocs Material 站 + llms.txt (2-4 周内) / PyPI 发布 (占名 pavoz, long_description=README,
   twine check, project.urls 官方键) / 徽章 (PyPI 后补 version/downloads; CI 现可加)

## Launch 计划

### 公开前 (repo 仍私有, 1-2 周内)
1. 隐喻盲测: 找 3 个 Python 圈人 10 秒理解 tagline (reaktiv 教训: 措辞即增长)
2. README 底部加 star-history 图 (公开后)
3. 写 Show HN 首条评论草稿 (见下) + r/Python Showcase 帖草稿 (不同文案, <350 词)
4. 补 docs 站 (非阻塞, 公开后 2-4 周)

### Launch 窗口 (选周二-周四, 北京 20:00-23:00 = HN 12-15 UTC 黄金窗)
- Show HN 标题草案: "Show HN: pavoz — a zero-dependency in-process workflow engine with time-travel debugging for LLM pipelines" (具体差异化, 不用 best/awesome)
- 首条评论 5 要素: ① 一句话技术版: 静态 @dag.stage DSL + per-node checkpoint/resume/replay/fork, core stdlib only ② 个人动机: 给 1500 行 LLM 管线编排 god-function 找出口, 不想要 scheduler/server ③ 独特取舍: 循环留在业务层 (无框架 loop DSL); ctx.call seam 不绑模型; 每 stage 存原始 delta → 任意历史输入可重建可 fork ④ 一条诚实局限: 单进程, 不做分布式/动态图/UI — 那是 Temporal/LangGraph 的地盘 ⑤ 请求反馈: 想听"什么场景你会需要一个不绑平台的可续跑编排层"
- 驻场 3-4 小时, 前 2 小时 15 分钟内回每条实质评论; 首条评论在 15 分钟内自回技术细节
- 次日 r/Python Showcase (四段模板: What My Project Does / Target Audience / Comparison / Disclosure), 不跨版不贴 HN 链接

### 公开后 1-4 周
- 深度技术文: "把编排层从 1500 行 god-function 里拆出来: 零依赖 DAG 引擎的设计取舍" (dev.to + 投稿 PyCoder's Weekly / Python Weekly)
- X/Bluesky 线程带真实指标; DM 3-5 个生态中小内容者评测
- PR 2-3 个 niche awesome 列表 (awesome-llm / awesome-workflow-engines 类, 门槛低立即投; awesome-python 等 100+ stars + 3-6 月真实使用再走 Hidden Gem 通道)
- 24h 内响应一切 issue/PR; 补 good-first-issue

### 规模化 (有几百 star 后)
- 每 2 周 1 篇内容复利, 打无人竞争关键词 ("python dag orchestration without airflow" 等)
- 第二/三轮大版本 Show HN (大特性才算; 时间旅行 fork 是 v0.6 叙事, stage 级 web UI 是候选 v0.8 钩子)
- pypistats/pepy 周看 downloads; downloads 涨 star 不涨 → README 顶部 10 秒区重做
- Discord 不开, GitHub Discussions 先跑 3-4 月 / ~500-1000 stars 再评估

## 里程碑 (现实)

| 时点 | 目标 | 判定 |
|---|---|---|
| 公开 + PyPI | repo public + pip install pavoz 可用 | 用户操作 (翻转 repo / PyPI 注册) |
| 首轮 launch (2 周内) | HN 首页或 r/Python 上榜 → 50-300 stars | 24h/48h 数据记录 |
| 月 1 | 100+ stars + 首个外部 issue/star | 真实使用证据 |
| 月 3 | 300-500 stars + awesome niche 收录 | pypistats 下载趋势 |
| 年 1 | 1k+ (需 1 轮 HN 病毒或生态内容破圈) | — |

## GitHub repo 元数据 (公开时设)

- description: "In-process Python DAG workflow engine — zero runtime deps, checkpoint resume, stage replay & time-travel fork for LLM pipelines"
- topics: python, workflow, dag, orchestration, llm, pipeline, durable-execution, checkpoint, machine-learning, etl, agents, asyncio
- 开启: Discussions / Issues / Wiki off
