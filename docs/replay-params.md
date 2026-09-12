# pavoz fork-run: Declarative Param Override Guide (v0.3.0)

## What it solves

Before 0.3.0, changing a nested param required JSON string with deep escaping:

```bash
pavoz fork-run dag.py --task-id X --stage s_compose \
  --overrides '{"llm_config": {"research_writer": {"model": "longcat"}}}'
```

With 0.3.0:

```bash
pavoz fork-run dag.py --task-id X --stage s_compose \
  --set llm_config.research_writer.model=longcat
```

## 4 new flags

| Flag | Use case | Example |
|---|---|---|
| `--set KEY=VALUE` | Single override, repeatable | `--set llm.model=x` |
| `--set-file PATH` | Bulk JSON/YAML | `--set-file overrides.yaml` |
| `--compare-with RUN_ID` | Diff vs another run | `--compare-with <run_id>` |
| `--dry-run` | Preview apply effect (leaf diff + would_rerun), no execution | `--dry-run` |

## Priority (later overrides earlier)

```
--set > --set-file > --overrides > --input

(`--overrides` > `--input` 沿袭 v0.8 组合使用时的既有胜者 — v0.8 的合并顺序是
先载入 `--input` 再 `update` `--overrides`.)
```

## Merge semantics (0.3.0 behavior fix)

overrides 应用到 state 是**深合并**: patch 的 leaf 覆盖, 兄弟键保留。

```bash
# state 里已有 llm: {model: a, temperature: 0.5}
pavoz fork-run dag.py --task-id X --stage s_compose --set llm.model=b
# → llm: {model: b, temperature: 0.5}   # temperature 保留 (v0.8 会抹掉)
```

v0.8 行为 (顶层浅替换) 只对"标量覆盖"保持不变; 嵌套 dict 从整体替换改为深合并。
Library API 等价: `apply_overrides(state, patch)`.

## Override target rule (footgun)

`--set` 注入的是 from_stage 的**执行前输入**. 若 override 的 key 是
from_stage 自己产出的 key (stage 会重新生成它), resume 时触发
StateConflictError (单一 producer 契约: <fork> 不是该 stage 的可达上游).

```bash
# topic 由 s_a 产出 → 从 s_a fork 并 override topic = 矛盾用法, 会报错
pavoz fork-run dag.py --task-id X --stage s_a --set topic=new   # ❌ StateConflictError
# 正确: override 更上游 stage 产出 / initial_state 里的 key
pavoz fork-run dag.py --task-id X --stage s_b --set llm.model=x  # ✅
```

(是否允许 fork override 压过 stage 重新生成的值 = 产品决策, 留给 pavoz 维护者.)

## Type inference (auto)

| Raw | Type | Why |
|---|---|---|
| `42` | `int` | numeric |
| `3.14` | `float` | numeric |
| `true` / `false` | `bool` | keyword |
| `null` | `None` | keyword |
| `[1,2,3]` | `list` | JSON inline |
| `{"k":"v"}` | `dict` | JSON inline |
| `hello` | `str` | default passthrough |
| `2026-09-09` | `str` | ISO dates stay str |

无逃生门. 类型不对时用 `--dry-run` 看解析结果再执行。

## CLI storage (任意 StorageBackend)

CLI 默认写 `~/.pavoz/data` (FileStorage). 设 env 即可指向任意 backend
(全子命令生效, `{task_id}` 会替换为当前命令的 task_id):

```bash
export PAVOZ_STORAGE_SPEC="backend.integration.pavoz_storage.MinioStorage"
export PAVOZ_STORAGE_KWARGS='{"task_id": "{task_id}"}'
pavoz fork-run dag.py --task-id X --stage s_compose --set llm.model=y
```

`PAVOZ_STORAGE` (目录) 行为不变; 两者都设时 SPEC 优先。

## Safety

- ❌ dunder keys rejected (`__proto__`, `__class__`, etc.)
- ❌ path depth > 5 rejected
- ❌ file size > 1MB rejected
- ❌ YAML via `safe_load` only (no arbitrary code execution)

## Library API

```python
from pavoz import Runtime
from pavoz.state import parse_set_args, parse_set_file, merge_overrides, apply_overrides

rt = Runtime(checkpoint_store=...)

# Declarative (v0.3.0): 组合 helpers, 一行装回
overrides = merge_overrides(
    parse_set_file("overrides.yaml"),          # 可选
    parse_set_args(["llm.model=longcat", "config.target_chars=8000"]),
)
await rt.fork_run(dag, task_id, from_stage="s_compose", overrides=overrides)

# nested dict (v0.6+, 不变) — 现在也是深合并语义
await rt.fork_run(dag, task_id, from_stage="s_compose",
                  overrides={"llm": {"model": "longcat"}})

# dot-path key dict (v0.3.0) — 值应用与 nested 等价, producer 语义不同 (见下)
await rt.fork_run(dag, task_id, from_stage="s_compose",
                  overrides={"llm.model": "longcat"})

# 纯 state 变换 (不跑 pipeline)
new_state = apply_overrides(state, {"llm.model": "longcat"})
```

> ⚠️ fork resume 场景下两者的 producer 语义不同: nested patch 会让 <fork> 认领
> 顶层 key (后续 stage 链式覆盖该 key → StateConflictError); dot-path key 不改
> producer (后续 stage 重新产出该 key 时会静默覆盖你的 override).
> 值应用本身两者等价 (深合并). 按 stage 重新生成与否选格式。

## Isolation (no --in-place in 0.3.0)

fork-run 永远产生新 run_id (隔离). latest 指针随 save 移到 fork run;
原 run 仍可用 `store.load(task_id, run_id)` 寻址, 历史不丢。
覆盖原 run 的需求推迟到 v0.10 (带并发保护再上).

## Examples

See `examples/` directory:
- `01_model_switch.sh` — 4-model A/B (ai-research use case)
- `02_prompt_patch.py` — change prompt without rerunning prior stages
- `03_compare_diff.py` — show state diff between 2 fork runs
- `04_deep_merge.py` — dot-path 兄弟键保留演示 (behavior fix)
- `05_yaml_file.sh` — YAML file batch with dry-run first

## Compatibility

| 0.2.0 | 0.3.0 |
|---|---|
| `--overrides '{"k":"v"}'` | ✅ unchanged |
| `--input edited.json` | ✅ unchanged |
| `--set k=v` | ❌→ ✅ new |
| `--set-file x.yaml` | ❌→ ✅ new (needs `pip install pavoz[yaml]`) |
| `--compare-with` | ❌→ ✅ new |
| `--dry-run` | ❌→ ✅ new |
| `Runtime.fork_run(overrides=dict)` | ✅ signature unchanged (嵌套 dict 语义: 整体替换→深合并, 见上) |

## Migration from 0.2.0

No code changes required. To use new features:
1. `pip install --upgrade pavoz`
2. Optionally `pip install "pavoz[yaml] @ git+https://github.com/liyong-labs/pavoz@main"` (YAML support; once released: `pip install pavoz[yaml]`)
3. Optionally replace JSON-string `--overrides` with `--set` / `--set-file`

注意: "嵌套 dict 重置为更少 key" 的用法在 0.3.0 不可表达 (兄弟键总被保留;
仅标量覆盖整个 dict key 才整体替换) — 需要时用标量覆盖, 或先改 stage.
