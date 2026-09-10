# PR 回复 — ai-research R1/R2/R3 (2026-09-10)

> 回复对象: ai-research `docs/handoff/pavoz-pr-m2/PR-description.md` (R1 prune /
> R2 error_class / R3 set_progress), 评审全文见同目录 ai-research 仓
> `REVIEW-from-pavoz.md` (含普适性论证 + ai-write 受益附录)。
> 裁定: **R2 ✅ / R1 CLI ✅ (auto-prune 裁掉) / R3 ✅ (限定 scope)** — 三项全部落地。

## 落地记录 (0.3.x, Unreleased)

| 项 | 实现 | 条件落实 |
|---|---|---|
| R2 | `RunResult.error_class: str \| None` (types.py); `_run_stage` 捕获处保留原始 `type(e).__name__` (runtime.py) | `error` 字符串格式不变 (含 `FatalError:` 前缀语义); 未知业务异常 (如 PipelineError) → 原类名; StageError/FatalError → 自身类名; **cancelled → None** |
| R1 | `CheckpointStore.prune(task_id, *, keep_last, dry_run)` + `pavoz prune --task-id X --keep-last N [--dry-run]` (cli.py) | 排序 = max(stage_ts) (run 最后活动时间); **latest 指针指向的 run 绝不删**; dry-run 列将删 run + 近似字节, rc=0; 被删 run 后续 load_latest→None / fork 报"无 checkpoint" (既有边界, docs 写明); **未做** max_runs_per_task 自动修剪 (v0.4 adapters REVERSED 先例: 不替客户决定保留策略) |
| R3 | `Ctx.set_progress(fraction, note=None)` → `stage_progress` 事件 (第 5 种生命周期事件) | 单方法无富 schema; best-effort 不落 checkpoint 不碰 deadline/重试; 同 fraction 引擎内去抖; 未启用 on_event 时 no-op |

## 定性声明 (对照 2026-09-07 治理门槛)

R1/R2 定性为**引擎自身语义补全**而非消费者功能: 吞异常与 checkpoint 私有布局都是
引擎行为/格式, 消费者无法在不反向耦合的前提下自救。R3 落在 v0.2 API 冻结宣言
明文允许的观测层。真实受益消费者 2/2 (ai-research + ai-write, 受益分析见评审附录)。

## 给 ai-research 的回执要求 (PR 承诺兑现检查)

1. R1 落地后删除自写 cp 清理脚本 (未删 = 需求不成立);
2. error_class 告警分派规则形成后, 回贡献 3 行示例进 `examples/` (第 3 个 use case 证据)。

## 附: ai-write 需求 PR 状态核对 (user 2026-09-10 要求一并确认)

`530f4c2 + d18b3e4` (PAVOZ_STORAGE_SPEC CLI storage 解耦 + fork-run --dry-run 预演)
已交付: cli.py + tests/test_cli_fork_set.py (+73 行) + CHANGELOG + ROADMAP + docs 双语。
本批全量测试通过 = 该 PR 验收完成。
