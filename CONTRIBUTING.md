# 贡献指南

pavoz 是通用流程编排库 — 贡献前请先读 [`docs/cn/architecture.md`](docs/cn/architecture.md)
(英文: [`docs/en/architecture.md`](docs/en/architecture.md)), 理解三个不变量:

1. **core 零依赖**: `pavoz/` 不 import 任何第三方库 (stdlib only)。
   存储/外部调用/任务表全部走 Protocol, 由业务侧注入。
2. **循环留在业务层**: 框架只有 DAG + per-node retries + checkpoint 三种能力。
   审计循环、收敛 gate 等是 stage 函数内的普通 Python — 不要给框架加
   `retry_budget` / `escalation` 之类的流程原语。
3. **API 最小化**: 每个新字段/装饰器参数必须有真实 use case, 不做"未来可能用"
   的预留。

## 开发环境

```bash
git clone git@github.com:liyong-labs/pavoz.git
cd pavoz
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## 质量门槛 (PR 前必须全过 — 与 CI 同款命令)

```bash
python -m pytest tests/ -q --cov=pavoz --cov-fail-under=80   # 全部测试 + 覆盖率门禁
python -m pyflakes pavoz tests examples tools              # lint (pyflakes, 不是 ruff)
python -m pyright                          # 类型检查 (basic)
python tools/check_doc_links.py            # markdown 链接必须解析
python tools/check_doc_style.py            # 文档风格
python tools/check_changelog.py            # 已发布 CHANGELOG 段冻结
python tools/check_text_integrity.py       # 已跟踪文本里不许有替换字符
python tools/check_business_leak.py        # 不许有厂商模型名 / 消费方 key
```

这 8 条与 CI (Python 3.12 + 3.13) 同款 —— 本清单由
`tests/test_contributor_docs_current.py` 从 `.github/workflows/test.yml` 派生核对,
CI 加一个门禁这里就会转红。别手改成不一致。

⚠️ **别只写其中一部分**: 原先这里只列了 pytest + pyflakes 两条, 而 pyflakes 的
目标列表还漏了 `tools/` —— 8 个门禁脚本全在 `tools/` 里, 照着旧清单做等于
**连门禁脚本本身都没 lint**, 而 pytest 那条也没带覆盖率门禁, 于是本地过、CI 红。

- 新行为必须有对应测试 (`tests/test_*.py`); 示例 (`examples/`) 有回归测试防腐化
- 改 merge/checkpoint 语义时, 向后兼容 (旧 checkpoint 能 resume) 是硬要求

## 提交规范

- 中文或英文 message 均可, 前缀 `feat|fix|docs|refactor|test|chore`
- 破坏性变更在 message 标注 `BREAKING` 并更新 CHANGELOG
- 语义版本: 兼容性行为变更 = minor, 破坏性 = major

## 文档

- 公开 API 变更同步 `docs/cn/api.md` + `docs/en/api.md`
- 定位/能力边界变更同步 `docs/cn/architecture.md` + `docs/en/architecture.md`
- 新版本条目追加 `CHANGELOG.md`
- 业务接入示例归 `docs/cn/use-cases/` + `docs/en/use-cases/` (参考实现, 不进主文档)

## Issue / PR

- Bug report 请附: 最小复现 DAG + 期望 vs 实际
- Feature request 请说明 use case (无真实 use case 的 feature 会被拒)
