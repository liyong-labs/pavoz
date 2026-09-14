# Handoff: PyPI 已上线 + 版本线对齐决定 (2026-09-14)

> **给**: pavoz@local (你, 下次上线先读这份)
> **背景**: 你最后一次提交是 stageflow 时代的 v0.5.1 (71b86a7, ID model + TestPipe.replay_from)。
> 之后项目经历了 stageflow → pavoz 改名 + 版本重起, 今天 (2026-09-14) 完成了 PyPI 首发。
> 你之前的改动全部安全 — 已包含在已发布版本里, 没丢一行。

## 发生了什么 (你离线的几天)

1. **改名定稿**: stageflow → pavoz, 包名 / 仓库 / 版本线全部独立 (CLAUDE.md §定位)。
2. **PyPI 首发 (2026-09-14, 两次成功)**:
   - `pavoz 0.3.0` — 首个 PyPI 版本 (Trusted Publishing, CI 自动发)
   - `pavoz 0.5.2` — **版本线对齐后**的当前最新版
3. **版本线决定 (user 2026-09-14 拍板, 永久生效)**:
   - **延续 0.5.x 线, 不用改名后重起的 0.3.x**。下一个版本 = **0.5.3** (semver, 0.x 期间 minor 可含破坏性变更)。
   - 你留的 stageflow 时代 tag (v0.4.1 / v0.5.0 / v0.5.1) 已从本地 + 远程**删除** (会误导"Pavoz 发过 0.5.1 后又降到 0.3.0")。
     你的提交本体仍在 main 祖先链上, 历史可查。
4. **发版管道已验证两次**: tag → CI `.github/workflows/publish.yml` → PyPI Trusted Publishing (免密, 无 API token)。
   流程文档: `RELEASING.md`。

## 你回来后必须遵守的新铁律

| # | 规则 |
|---|---|
| 1 | **版本号由主控 agent 统一管控** — 你不要自行 bump / 打 tag / 发版。需要发版时在任务说明里提, 走 RELEASING.md 流程 |
| 2 | **不要重建 v0.4.x / v0.5.0 / v0.5.1 tag** — 它们属于 stageflow 命名时代, 已有意删除 |
| 3 | 下一个版本是 **0.5.3**, 不是 0.4.0 (0.x 线延续) |
| 4 | PyPI 版本不可撤销 — 发错只能 yank 或发补丁版, 所以发版前必须过 `tests/test_version_sync.py` |
| 5 | 安装指令一律 `pip install pavoz` — 仓库里已清干净 `git+https://...` 写法, 不要加回来 |

## 仓库当前状态 (2026-09-14)

- main = 9c932aa `release: v0.5.2`, 204 tests 全绿
- tags: `v0.1.0—v0.1.3` (stageflow 最早历史, 保留) / `v0.3.0` / `v0.3.0-rc1` / `v0.5.2`
- PyPI: https://pypi.org/project/pavoz/ (0.3.0 + 0.5.2)
- CI: test (3.12/3.13 矩阵) + publish (tag 触发) 双 workflow, 全绿
- pavoz-extensions 仓: CI 依赖区间还是 `pavoz>=0.3,<0.4`, 待放宽到 `>=0.5,<0.6` (已知待办)

## 待办 (接手时从这里挑)

- [ ] pavoz-extensions CI 依赖区间放宽 `>=0.5,<0.6`
- [ ] `docs/pavoz-upgrade-plan-2026-09-12.md` 里 0.4/0.5 的版本号引用需按新版本线校对
- [ ] ROADMAP.md 若引用旧版本号, 同步校对
