# Ponytail Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 删 pavoz v0.1.1 中零调用的死代码 (ponytail 审计识别的 YAGNI 项), 简化防御性包装, 不动 API 冻结面。

**Architecture:** 单 commit 干完。每步 ruff + 38 tests 全过作门禁。无新文件, 净减 ~160 行。

**Tech Stack:** Python 3.12+, stdlib only, ruff.

**Spec:** `ROADMAP.md` (冻结宣言, 本 plan 仅删零调用 + simplify, 不动签名/语义)

## Global Constraints

- Python 3.12+, stdlib only — 不允许 import 任何非 stdlib
- API 冻结: DAG 装饰器 / Runtime.run / Ctx / State 契约 / 异常 / Checkpoint + workflow_hash — 全部不动语义
- 每个 task 结束 ruff check 0 violation + pytest 38 全过
- 单 commit (CHORE 标签), 失败立即 revert
- 改前确认: 涉及项 `grep -r` 在 pavoz/ tests/ docs/ dags/ 全无调用点 (零调用 = 安全删)

---

### Task 1: 删 trigger.py + 移除 TaskTrigger/TaskRef export

**Files:**
- Delete: `pavoz/trigger.py` (53 行)
- Modify: `pavoz/__init__.py` (`from .trigger import TaskRef, TaskTrigger` 行 + `__all__` 列表的 `"TaskRef"`, `"TaskTrigger"`)

**Interfaces:**
- Consumes: `TaskTrigger` / `TaskRef` 全无 import (ai_writer 也没用 — 它自己 poll DB)
- Produces: 无 (公开 API 移除两项)

- [ ] **Step 1: 验证零调用**

```bash
cd pavoz && grep -rn "TaskTrigger\|TaskRef" pavoz/ tests/ docs/ dags/ --include="*.py" --include="*.md"
```
Expected: 仅 `trigger.py` (定义) + `__init__.py` (export)。ai_writer 集成层不依赖 (它自己实现 poll)。

- [ ] **Step 2: 删 trigger.py + 改 __init__.py**

```bash
cd pavoz
rm pavoz/trigger.py
```

打开 `pavoz/__init__.py`。删 `from .trigger import TaskRef, TaskTrigger` 一行; `__all__` 列表删 `"TaskRef",` 和 `"TaskTrigger",`。

- [ ] **Step 3: ruff + pytest 门禁**

```bash
cd pavoz && python -m ruff check pavoz/ tests/
cd pavoz && python -m pytest tests/ -q
```
Expected: ruff 0 violation; 38 tests PASS

---

### Task 2: 删 Ctx.log + Ctx.remaining_seconds (runtime.py:64-73)

**Files:**
- Modify: `pavoz/runtime.py` (Ctx 类 log/remaining_seconds 方法删; logger 字段保留 — runtime 自身用; deadline 字段保留 — _with_timeout 用)

**Interfaces:**
- Consumes: `ctx.log` / `ctx.remaining_seconds` 全无调用 (零调用确认)
- Produces: Ctx 类瘦 10 行

- [ ] **Step 1: 验证零调用**

```bash
cd pavoz && grep -rn "ctx\.log\|ctx\.remaining_seconds\|ctx\.logger\." pavoz/ tests/ dags/ --include="*.py"
```
Expected: 0 hit

- [ ] **Step 2: 删 Ctx.log 和 Ctx.remaining_seconds 方法**

打开 `pavoz/runtime.py`。`Ctx` 类内, 删以下两块:

```python
def log(self, msg: str, *, level: str = "INFO", **meta) -> None:
    """业务日志 (JSON trace 的一部分). level: DEBUG/INFO/WARN/ERROR."""
    lvl = getattr(logging, level.upper(), logging.INFO)
    self.logger.log(lvl, "task=%s stage=%s %s %s", self.task_id, self.stage_name, msg, meta)

def remaining_seconds(self) -> float | None:
    """absolute deadline 剩余秒数. 无 deadline 返 None."""
    if self.deadline is None:
        return None
    return max(0.0, self.deadline - time.time())
```

保留 `logger` 字段 (runtime._run_stage 内 self.logger.exception 用) 和 `deadline` 字段 (`_with_timeout` 用)。

- [ ] **Step 3: 删 Runtime.__all__ 的 'CallResult' (无外部使用) 检查**

`CallResult` 仍在 __init__ export。保留 — 它是 ctx.call 的返回值类型, 公开 API 冻结面之一。**不动**。

- [ ] **Step 4: ruff + pytest 门禁**

```bash
cd pavoz && python -m ruff check pavoz/ tests/
cd pavoz && python -m pytest tests/ -q
```
Expected: ruff 0; 38 PASS

---

### Task 3: 删 Stage.metadata + @dag.stage 的 **metadata 参数 (dag.py:44, 65, 83)

**Files:**
- Modify: `pavoz/dag.py` (Stage dataclass 删 metadata 字段; stage 装饰器删 **metadata)

**Interfaces:**
- Consumes: Stage.metadata 字段零调用 (Stage 实例外部从未读 .metadata)
- Produces: stage 装饰器签名缩, 少 1 个死路

- [ ] **Step 1: 验证零调用**

```bash
cd pavoz && grep -rn "\.metadata\|stage_metadata\|stage.*metadata" pavoz/ tests/ dags/ --include="*.py"
```
Expected: 0 hit (除定义)

- [ ] **Step 2: 删 Stage.metadata + 装饰器 **metadata**

打开 `pavoz/dag.py`:
- 删 `Stage` 的 `metadata: dict[str, Any] = field(default_factory=dict)` 一行
- `stage` 装饰器签名: `def stage(self, *, depends_on=None, retries=0, timeout=None) -> ...` (删 **metadata)
- 装饰器内部 `Stage(...)` 调用: 删 `metadata=metadata,` 一行

- [ ] **Step 3: ruff + pytest**

```bash
cd pavoz && python -m ruff check pavoz/ tests/
cd pavoz && python -m pytest tests/ -q
```
Expected: 0/38

---

### Task 4: 删 DAG.entrypoints + DAG.depth_of (dag.py:96-98, 194-208)

**Files:**
- Modify: `pavoz/dag.py` (删 entrypoints property + depth_of 方法)

- [ ] **Step 1: 验证零调用**

```bash
cd pavoz && grep -rn "\.entrypoints\|\.depth_of" pavoz/ tests/ dags/ --include="*.py"
```
Expected: 0 hit

- [ ] **Step 2: 删 entrypoints + depth_of**

打开 `pavoz/dag.py`:
- 删 `@property entrypoints` (10 行 + 1 行空白)
- 删 `def depth_of(self, name: str)` (15 行 + 1 行空白)

- [ ] **Step 3: ruff + pytest**

```bash
cd pavoz && python -m ruff check pavoz/ tests/
cd pavoz && python -m pytest tests/ -q
```
Expected: 0/38

---

### Task 5: 删 CLI inspect 子命令 (cli.py:106-112, 135-138)

**Files:**
- Modify: `pavoz/cli.py` (删 `_cmd_inspect` + subparser 注册)

- [ ] **Step 1: 验证零外部使用**

```bash
grep -rn "pavoz inspect\|pavoz.*--call-id" <local workspace> 2>/dev/null | head
```
Expected: 0 hit (claude 仓 / docs / scripts 不调)

- [ ] **Step 2: 删 _cmd_inspect + subparser**

打开 `pavoz/cli.py`:
- 删 `async def _cmd_inspect(args)` 整块 (7 行)
- 删 `p_inspect = sub.add_parser(...)` 3 行 (到 `set_defaults(fn=_cmd_inspect)`)

- [ ] **Step 3: 同步 docs/quickstart.md 移除 inspect 示例**

打开 `docs/quickstart.md`。"## CLI 命令 (v0.1)" 表删 inspect 行 (`python -m pavoz inspect ...` 一行 + 描述)。

- [ ] **Step 4: ruff + pytest**

```bash
cd pavoz && python -m ruff check pavoz/ tests/
cd pavoz && python -m pytest tests/ -q
```
Expected: 0/38

---

### Task 6: 删 __main__.py (114 字节空壳)

**Files:**
- Delete: `pavoz/__main__.py`
- Modify: `pavoz/__init__.py` (末尾加 `if __name__ == "__main__": from .cli import main; raise SystemExit(main())`)

- [ ] **Step 1: 验证 python -m pavoz 等价路径**

```bash
cd pavoz && python -m pavoz --help 2>&1 | head -5
```
Expected: usage 文本 (argparse "pavoz" + subcommands 列表)

- [ ] **Step 2: 删 __main__.py + 改 __init__.py**

```bash
cd pavoz && rm pavoz/__main__.py
```

打开 `pavoz/__init__.py`, 末尾追加:

```python
if __name__ == "__main__":
    from .cli import main
    raise SystemExit(main())
```

- [ ] **Step 3: 验证 python -m pavoz 仍工作**

```bash
cd pavoz && python -m pavoz --help 2>&1 | head -5
cd pavoz && python -m pavoz run --help 2>&1 | head -3
```
Expected: 两个都打印 usage (与 Step 1 一致 + run 子命令帮助)

- [ ] **Step 4: ruff + pytest**

```bash
cd pavoz && python -m ruff check pavoz/ tests/
cd pavoz && python -m pytest tests/ -q
```
Expected: 0/38

---

### Task 7: 单 commit + tag v0.1.2

- [ ] **Step 1: ruff 全量**

```bash
cd pavoz && python -m ruff check pavoz/ tests/ docs/
```
Expected: 0 violation (docs 不在 ruff 配置里, 这步主要是确认无遗留)

- [ ] **Step 2: pytest 全量**

```bash
cd pavoz && python -m pytest tests/ -q
```
Expected: 38 tests PASS (0 删 — 删的是非测试代码, 不动测试)

- [ ] **Step 3: CHANGELOG.md 加 v0.1.2 条目**

打开 `CHANGELOG.md`, 在 `[0.1.1]` 后 `[Unreleased]` 前插入:

```markdown
## [0.1.2] — 2026-09-05

### Removed (ponytail cleanup)

- `trigger.py` 整文件 (TaskTrigger/TaskRef 零调用, 移除 export)
- `Ctx.log` / `Ctx.remaining_seconds` (零调用)
- `Stage.metadata` 字段 + `@dag.stage` 的 `**metadata` 参数 (零调用)
- `DAG.entrypoints` / `DAG.depth_of` (零调用)
- CLI `inspect` 子命令 (空壳 "v0.1 无 call 级 trace")
- `__main__.py` (114 字节空壳, 移到 `__init__.py` 一行)

### Internal

- Ctx 类瘦 10 行, Stage 装饰器少 1 个死参数, DAG 类瘦 25 行, CLI 少 1 子命令
- 净减 ~160 行代码, 0 行测试变动
- **API 冻结面零变化**: DAG 装饰器必选位置参数 / Runtime.run / State 契约 / 异常 / Checkpoint 全部不动

```

- [ ] **Step 4: 单 commit + tag + push**

```bash
cd pavoz
git add pavoz/ docs/quickstart.md CHANGELOG.md
git commit -m "chore: ponytail cleanup v0.1.2 (-160 行死代码, 0 测试变动)

- 删 trigger.py (TaskTrigger/TaskRef 零调用)
- 删 Ctx.log / Ctx.remaining_seconds / Stage.metadata / DAG.entrypoints /
  DAG.depth_of (零调用)
- 删 CLI inspect 空壳 + __main__.py 114 字节空壳 (移到 __init__.py)
- API 冻结面零变化 (DAG/Runtime/State/异常/Checkpoint 签名+语义不动)
- CHANGELOG + docs/quickstart 同步

Co-Authored-By: Claude Code <noreply@anthropic.com>"
git tag v0.1.2
git push origin main --tags
```

- [ ] **Step 5: 验证**

```bash
cd pavoz && git tag -l && git log --oneline -3
wc -l pavoz/*.py tests/*.py | tail
```
Expected: `v0.1.2` 在列; 6 commits (Task 1-6 = 1 commit 在 Task 7) + 1 个 tag commit; 总行数从 1744 降到 ~1580