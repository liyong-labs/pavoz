# 发版流程

**读者**: 有 PyPI / GitHub 权限的维护者
**目标**: 一次发版 = 推一条 tag, 其余 (构建 + 上传 PyPI) 由 CI 自动完成

## 一次性前置 (只需做一次)

| # | 在哪 | 做什么 |
|---|---|---|
| 1 | PyPI → Publishing → Add a pending publisher | 项目名 `pavoz` / owner `liyong-labs` / 仓库 `pavoz` / workflow `publish.yml` / environment `pypi` |
| 2 | GitHub 仓库 → Settings → Environments | 新建一个名为 `pypi` 的 environment |

两项都完成后, 推 tag 时 [`.github/workflows/publish.yml`](.github/workflows/publish.yml) 会用
**Trusted Publishing** 免密发布 — 不需要 API token, 也不需要把密码存进仓库。

## 每次发版 (5 步)

1. **同步两处版本号** — `pyproject.toml` 的 `version` 与 `pavoz/__init__.py` 的 `__version__`。
   `tests/test_version_sync.py` 会拦下不一致 (历史上漂移过一次)
2. **写 CHANGELOG** — `[Unreleased]` 改成 `[x.y.z] — YYYY-MM-DD`, 顶部补回新的 `[Unreleased]`
3. **提交** — `git commit -m "release: vX.Y.Z"`
4. **打 tag 并推送** — `git tag vX.Y.Z && git push origin main && git push origin vX.Y.Z`
5. **验证** — CI 绿; `pip install pavoz==X.Y.Z` 成功; `python -c "import pavoz; print(pavoz.__version__)"`
   与 tag 一致

## 版本号怎么定

- semver: 兼容性行为变更 = minor, 破坏性 = major
- **0.x 期间 minor 可能含破坏性变更** (semver 允许) — 所以扩展包锁 `pavoz>=0.3,<0.4` 这样的区间
- PyPI 的版本号**不可撤销**: 发错了只能 yank (已装的人仍能装) 或发下一个补丁版

## 发布后要跟着改的

- README (中英) 与两个 quickstart 的安装指令: 从 git 形式切回 `pip install pavoz`
  (`grep -rn "git+https://github.com/liyong-labs/pavoz@main" --include="*.md"` 找齐)
- 扩展仓 `pavoz-extensions` 的 CI 版本矩阵可以启用 (现在 PyPI 上还没有 pavoz, 回退到 git 安装)
