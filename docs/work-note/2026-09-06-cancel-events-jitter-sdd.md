# cancel + events + jitter wave (SDD 执行, 2026-09-06)

## 交付 (9 commits, 起点 60b36b6)
- Task 1 full-jitter 退避: uniform(0, min(2^(n-1),30)) 防雷群
- Task 2 on_event 5 生命周期事件 + RunResult.stage_timings (observer fail-open)
- Task 3 协作式取消: cancel_check 检查点拦截 + ctx.cancelled() 轮询 +
  status="cancelled" + resume 无缝续跑 (修复轮: ctx.cancelled fail-open 对齐)
- Task 4 README×2 + CHANGELOG [Unreleased]
- Final review wave: run_stage cancelled 透传 (原来错报 done) + api.md 枚举 + 文档陈旧点

## 流程与数字
- subagent-driven: 每 task 独立实现者 (T1/T4 haiku, T2/T3/fix sonnet) + 独立审查者
- 测试 99 → 111; T3 一轮修复; 终审 2 Important + 4 Minor 全修
- 6 个 deferred (事件 schema 注记 / run_start 在 preflight 后 / ctx vs runtime
  cancel_check 签名异形等) 记录于审查报告

## 发布卫生 (user 拍板"一个不留")
- 全历史 filter-branch 重写: /home/ai/* → 相对/占位符 (bitensor/haomem/liyong/
  内网IP/私有主机名 全库本就 0 命中); 本地 refs/original + backup 已清, gc prune
- main + 7 tags force-push 对齐

## 教训
- **重写历史后必须 `git ls-remote` 对账每一个 tag** — v0.1.0-0.1.3 漏重写/漏推
  被对账抓出 (只看 rewrite 输出的 ref 列表不可靠)
- SDD: "plan 含完整代码"的 task 用 haiku 转录一次过; 集成型 task 用 sonnet
- plan 要写清"谁调用谁" — emit 埋点放共享 _run_stage 而 replay 不许发事件,
  这类矛盾应在 plan 里显式给 gate 方案
