# diff_runs: 两次 run 的 stage 级对比

`diff_runs` (v0.5.4) 回答一个问题:**同一个任务跑两次, 到底哪里变了** —
哪个 stage 的输入变了、输出差在哪、最终 state 差在哪。典型用途:

- 改了某 stage 的 prompt/模型后重跑, 对比两次 run 看影响面 (是只动了目标 stage,
  还是下游输出全变了)
- 回归排查: 上周还好的 task 今天结果不对, diff 找到第一个开始变化的 stage
- 评估实验: 两个参数组合各跑一次, 用输出 diff 当评估材料

输出是纯 JSON-serializable 数据, **不做 AI 摘要、不给结论** — 消费者
(AI agent / 人 / 你的评估脚本) 自行解读。pavoz 出数据, 判断归你。

## 最小用法

```python
from pavoz import DAG, Runtime, CheckpointStore, FileStorage

store = CheckpointStore(FileStorage("./cp"))   # 任意 StorageBackend
rt = Runtime(checkpoint_store=store)

# 同一 task 跑两次 (例如: 改了 s_compose 的 prompt 之后)
r1 = await rt.run(dag_v1, task_id="paper-42")
r2 = await rt.run(dag_v2, task_id="paper-42")

d = store.diff_runs("paper-42", r1.run_id, r2.run_id)
# run_id_b 传 "" (默认) = 对比 latest 指针指向的 run
```

真实输出 (上面 demo 结构: s_fetch → s_compose, 两次 run 只改了 s_compose 的代码):

```json
{
  "run_id_a": "73eb599a-18ac-44de-89b0-e989612042d0",
  "run_id_b": "d2b6e58f-f9ba-48ba-96d0-f8abd56e158b",
  "dag_name": "doc_demo",
  "workflow_hash_changed": false,
  "stages": {
    "s_compose": {
      "status": ["done", "done"],
      "input_hash_changed": true,
      "output_diff": { "doc": ["composed[data-v1]", "composed[data-v2]"] }
    },
    "s_fetch": {
      "status": ["done", "done"],
      "input_hash_changed": false,
      "output_diff": { "raw": ["data-v1", "data-v2"] }
    }
  },
  "state_diff": {
    "raw": ["data-v1", "data-v2"],
    "doc": ["composed[data-v1]", "composed[data-v2]"]
  }
}
```

## 读输出

| 字段 | 含义 |
|---|---|
| `workflow_hash_changed` | DAG **结构**指纹是否不同 (stage 增删/依赖关系变化)。true = 两次 run 的 stage 语义可能不可比, 先看这个 |
| `stages[name].status` | `[run_a 状态, run_b 状态]`; 某侧没执行过该 stage → `null` |
| `stages[name].input_hash_changed` | stage 输入指纹是否不同。指纹 = sha256(**传给 `DAG.stage()` 的 fn 的源码文本** (自 def 行起, 装饰器剥离) + **执行前全量 state**)。`true` = fn 本体或输入 state 变了, 输出变化大概率由它引起; `false` = 指纹相同但输出仍变了 (外部依赖/非确定性/委托实现变化, 见下); 一侧缺 hash → `null` |
| `stages[name].output_diff` | 该 stage return delta 的 path 级差异 (`{path: [旧值, 新值]}`) |
| `state_diff` | 两次 run 最终 merged state 的 path 级差异 |

注意区分两个指纹:

- `workflow_hash` 只锁**图结构** — 改 stage 函数体不改结构, 它不变
- `input_hash` = **fn 源码文本 + 执行前 state** — 改了 fn 本体或上游产出, 对应
  stage 的 `input_hash_changed` 变 true

**薄委托 wrapper 的陷阱**: 指纹里的"源码"是传给 `DAG.stage()` 的那个 fn 对象的
`getsource` 文本。如果 stage fn 是薄 wrapper, 真实逻辑在被委托的实现里 (如
`async def s_search(ctx): return await holder.search(ctx)`), 改 holder 方法不改变
wrapper 文本 → 指纹不变 → fork 重放会**跳过实际已变了的 stage**。要让实现变化
反映到指纹: 把逻辑内联进 stage fn, 或让 fn 源码引用一个显式的版本常量。

## 便捷形式与底层函数

```python
# CheckpointStore 包装: 按 task_id + run_id 加载后对比, "" = latest
store.diff_runs(task_id, run_id_a, run_id_b="")

# 底层函数: 直接对两个 Checkpoint 对象对比 (也可跨 task)
from pavoz import diff_runs, CheckpointStore
cp_a = store.load("paper-42", run_id_a)
cp_b = store.load_latest("paper-42")
d = diff_runs(cp_a, cp_b)
```

任一 run 无 checkpoint → `RuntimeError` (明确报错, 不静默返回空 diff)。

## 真实案例: ai-writing paper flow (9 stage → 10 stage)

第一个真实消费者是 ai-writing 的 paper flow: 同一 task (`8d24486404e3`) 两次 run
之间, DAG 从 9 stage 改成 10 stage (新增 `s_download`/`s_filter`, 若干 stage 函数体
改写)。以下为 caller 提供的脱敏摘录 (节选; `output_diff` 的值以变更 key 名代替,
`state_diff` 略):

```json
{
  "dag_name": "paper_flow",
  "workflow_hash_changed": true,
  "stages": {
    "s_plan":    { "status": ["done", "done"], "input_hash_changed": false, "output_keys_changed": [] },
    "s_search":  { "status": ["done", "done"], "input_hash_changed": false, "output_keys_changed": [] },
    "s_download":{ "status": ["done", "done"], "input_hash_changed": false, "output_keys_changed": ["download_error", "unique_sources"] },
    "s_filter":  { "status": [null, "done"],   "input_hash_changed": null,  "output_keys_changed": ["unique_sources"] },
    "s_audit":   { "status": [null, "done"],   "input_hash_changed": null,  "output_keys_changed": ["audit_report"] }
  }
}
```

三个值得注意的信号:

1. `workflow_hash_changed: true` — stage 结构变了, 两次 run 的 stage 语义不再一一
   对应。先看这个字段, 再决定 stages 层面的对比还有多少意义。
2. `status: [null, "done"]` — `null` = 该 run 没执行过这个 stage: 可能是新增 stage
   (`s_filter`), 也可能是 run_a 在中途失败没跑到 (`s_audit`)。
3. `s_plan`/`s_search`/`s_download` 指纹未变 — "改过函数体但 hash 没变"。成因即上文
   『薄委托 wrapper 的陷阱』: paper flow 的 stage fn 走闭包 wrapper 模式, fn 本体是
   稳定文本, 实际改动在被委托实现里, 指纹便一致。下游重放 skip 判定时要把这层
   计入。(若确认 fn 本体文本已改而指纹仍未变, 请报 issue — 那是引擎 bug。)

## 和 fork_run 的分工

- `fork_run` 是**执行时**原语: 重放时按输入指纹自动跳过没变的 stage (省重算)
- `diff_runs` 是**事后**原语: 告诉你两次 run 差在哪 (审计/评估/排查)

常见组合: 改 prompt → `fork_run(skip_unchanged=True)` 重跑 (上游没变就跳) →
`diff_runs` 对比重跑前后, 确认改动只影响了预期链路。API 细节另见
[api.md](api.md) 的 diff_runs 节。
