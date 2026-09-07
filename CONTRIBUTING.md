# 贡献指南

pavoz 是通用流程编排库 — 贡献前请先读 [`docs/architecture.md`](docs/architecture.md),
理解三个不变量:

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

## 质量门槛 (PR 前必须全过)

```bash
ruff check pavoz/ tests/    # lint
pytest                           # 38+ tests
```

- 新行为必须有对应测试 (`tests/test_*.py`)
- 改 merge/checkpoint 语义时, 向后兼容 (旧 checkpoint 能 resume) 是硬要求

## 提交规范

- 中文或英文 message 均可, 前缀 `feat|fix|docs|refactor|test|chore`
- 破坏性变更在 message 标注 `BREAKING` 并更新 CHANGELOG
- 语义版本: 兼容性行为变更 = minor, 破坏性 = major

## 文档

- 公开 API 变更同步 `docs/api.md`
- 定位/能力边界变更同步 `docs/architecture.md`
- 新版本条目追加 `CHANGELOG.md`
- 业务接入示例归 `docs/use-cases/` (参考实现, 不进主文档)

## Issue / PR

- Bug report 请附: 最小复现 DAG + 期望 vs 实际
- Feature request 请说明 use case (无真实 use case 的 feature 会被拒)
