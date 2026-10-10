# diff_runs: stage-level comparison of two runs

`diff_runs` (v0.5.4) answers one question: **run the same task twice — what
actually changed?** Which stage's inputs changed, where the outputs differ, and
how the final state differs. Typical uses:

- You changed one stage's prompt or model and re-ran — diff the two runs to see
  the blast radius (did only the target stage move, or did every downstream
  output change?)
- Regression triage: a task that was fine last week is wrong today — the diff
  points at the first stage where the two runs diverge
- Experiment evaluation: two parameter combinations, one run each; use the output
  diff as evaluation material

The output is plain JSON-serializable data. **No AI summary, no conclusions** —
the consumer (an AI agent, a human, your eval script) interprets it. pavoz
produces the data; the judgement is yours.

## Minimal example

```python
from pavoz import DAG, Runtime, CheckpointStore, FileStorage

store = CheckpointStore(FileStorage("./cp"))   # any StorageBackend
rt = Runtime(checkpoint_store=store)

# same task, two runs (say, after editing s_compose's prompt)
r1 = await rt.run(dag_v1, task_id="paper-42")
r2 = await rt.run(dag_v2, task_id="paper-42")

d = store.diff_runs("paper-42", r1.run_id, r2.run_id)
# run_id_b defaults to "" — compare against the run the latest pointer points at
```

Real output (for the demo shape `s_fetch → s_compose`, where only s_compose's
code changed between runs):

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

## Reading the output

| Field | Meaning |
|---|---|
| `workflow_hash_changed` | Whether the DAG **structure** fingerprint differs (stages added/removed, dependencies changed). `true` means the two runs' stages may not be comparable — read this first |
| `stages[name].status` | `[status in run_a, status in run_b]`; `null` when that run never executed the stage |
| `stages[name].input_hash_changed` | Whether the stage's input fingerprint differs. The fingerprint is sha256 of (**the source text of the fn you passed to `DAG.stage()`**, from its `def` line with decorators stripped, **plus the full pre-run state**). `true` means the function body or the input state changed, so an output change is probably downstream of it; `false` with a changed output points at external dependencies, non-determinism, or a changed delegated implementation (see below); `null` when one side has no hash |
| `stages[name].output_diff` | Path-level differences in that stage's returned delta (`{path: [old, new]}`) |
| `state_diff` | Path-level differences between the two runs' final merged states |

Two fingerprints, don't confuse them:

- `workflow_hash` locks **graph structure only** — editing a stage's function
  body does not change it
- `input_hash` = **fn source text + pre-run state** — editing the fn body or an
  upstream output flips `input_hash_changed` for the affected stage

**The thin-delegating-wrapper trap**: the "source" in the fingerprint is the
`getsource` text of the fn object you handed to `DAG.stage()`. If the stage fn is
a thin wrapper and the real logic lives in the delegated implementation (say
`async def s_search(ctx): return await holder.search(ctx)`), editing the holder
method leaves the wrapper text — and therefore the fingerprint — unchanged, so
`fork_run` may **skip a stage whose behavior actually changed**. To make
implementation changes show up in the fingerprint, either inline the logic into
the stage fn or have the fn source reference an explicit version constant.

Note that the drift warning (see [api.md](api.md), `fork_run` section) does *not*
cover this case: it compares the same wrapper text, so a thin wrapper that hides
the change is invisible to it too.

## Convenience wrapper and the underlying function

```python
# CheckpointStore wrapper: loads by task_id + run_id, then compares; "" = latest
store.diff_runs(task_id, run_id_a, run_id_b="")

# Underlying function: compare two Checkpoint objects directly (cross-task is fine)
from pavoz import diff_runs, CheckpointStore
cp_a = store.load("paper-42", run_id_a)
cp_b = store.load_latest("paper-42")
d = diff_runs(cp_a, cp_b)
```

If either run has no checkpoint → `RuntimeError` (explicit error, never a silent
empty diff).

## Worked example: a 9-stage → 10-stage paper flow

The first real consumer was ai-writing's paper flow: between two runs of the same
task (`8d24486404e3`) the DAG went from 9 stages to 10 (`s_download` / `s_filter`
added, several stage bodies rewritten). Caller-supplied excerpt, desensitized
(`output_diff` values replaced with the changed key names; `state_diff` omitted):

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

Three signals worth reading carefully:

1. `workflow_hash_changed: true` — the stage structure changed, so the two runs'
   stages no longer correspond one-to-one. Read this before deciding how much the
   per-stage comparison still means.
2. `status: [null, "done"]` — `null` means that run never executed the stage: it
   may be a newly added stage (`s_filter`), or run_a failed before reaching it
   (`s_audit`).
3. `s_plan` / `s_search` / `s_download` fingerprints unchanged — "the body was
   edited but the hash didn't move". The cause is the thin-wrapper trap above:
   those stage fns use a closure-wrapper pattern, so the fn text is stable while
   the delegated implementation changed. Account for that when deciding what to
   skip on a downstream replay. (If you can confirm the fn body text really did
   change while the fingerprint didn't, please file an issue — that would be an
   engine bug.)

## How it divides work with fork_run

- `fork_run` is an **execution-time** primitive: on replay it skips stages whose
  input fingerprint is unchanged (saving recomputation)
- `diff_runs` is an **after-the-fact** primitive: it tells you where two runs
  diverged (audit / evaluation / triage)

Common combination: edit a prompt → `fork_run(skip_unchanged=True)` to re-run
(unchanged upstream is skipped) → `diff_runs` to compare before and after and
confirm the edit only touched the intended chain. See the `diff_runs` section of
[api.md](api.md) for API details.
