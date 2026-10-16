"""两套合并语义必须各自钉住: stage 返回是**浅**, overrides 是**深**.

同一个系统里有两个 merge, 语义相反, 文档都提了一句但没并排说, 于是容易互相类推:

  stage `return dict` → shallow merge (merge_state: `merged.update(delta)`)
    嵌套 dict **整体替换** —— 后一个 stage 返回 `{"cfg": {"temp": 0.9}}` 会把前一个
    stage 的 `cfg.model` 一并抹掉 (实测: 下游读 `cfg["model"]` 直接 KeyError)

  `--set` / `--overrides` / `fork_run(overrides=)` → `apply_overrides` → **深合并**
    只打 patch leaf, 兄弟键保留 (v0.9 起; v0.8 的浅 update 曾把兄弟键抹掉)

本文件把两侧都钉成判据。**目的不是修 bug, 是防"顺手改成深/浅"** —— 其中任何
一侧改动都是破坏性的行为变更, 不该在无人察觉时发生。
"""
from __future__ import annotations

import tempfile

from pavoz import DAG, CheckpointStore, FileStorage, Runtime
from pavoz.state import apply_overrides, merge_state


# ── 侧 1: stage 返回 = shallow ────────────────────────────────────────────

def test_merge_state_replaces_a_nested_dict_wholesale():
    """注意 overwrite_keys: 同 key 二次写先要过"单 producer 契约" (链式覆盖),
    否则 merge_state 直接抛 StateConflictError, 压根走不到合并语义那一步。"""
    prev = {"cfg": {"model": "m1", "temp": 0.1}}
    merged = merge_state(prev, {"cfg": {"temp": 0.9}}, "s_b",
                         overwrite_keys={"cfg"})
    assert merged["cfg"] == {"temp": 0.9}, (
        "stage 返回值是**浅合并** —— 嵌套 dict 整体替换。这是既有契约 (docs 写明), "
        "改成深合并是破坏性变更; 真要用深合并请走 apply_overrides")
    assert "model" not in merged["cfg"]


def test_merge_state_does_not_mutate_prev():
    prev = {"cfg": {"model": "m1"}}
    merge_state(prev, {"cfg": {"temp": 0.9}}, "s_b", overwrite_keys={"cfg"})
    assert prev == {"cfg": {"model": "m1"}}, "merge_state 返回新 dict, 不改 prev"


async def test_stage_returning_a_partial_nested_dict_silently_drops_siblings():
    """端到端钉住那条坑: 下游 KeyError, run 报 failed, 而**没有任何警告**."""
    dag = DAG("shallow_end2end")

    @dag.stage()
    async def s_a(ctx):
        return {"cfg": {"model": "m1", "temp": 0.1}}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"cfg": {"temp": 0.9}}

    @dag.stage(depends_on=["s_b"])
    async def s_c(ctx):
        return {"model": ctx.state["cfg"]["model"]}   # 兄弟键已被抹掉 → KeyError

    store = CheckpointStore(FileStorage(tempfile.mkdtemp()))
    r = await Runtime(checkpoint_store=store).run(dag, "shallow")

    assert r.status == "failed"
    assert r.error_class == "KeyError", (
        f"应当是读 cfg['model'] 的 KeyError, 实得 {r.error_class}: {r.error}")
    assert store.load_latest("shallow").state["cfg"] == {"temp": 0.9}, (
        "cfg 确实被整体替换 —— model 不在了")


# ── 侧 2: overrides = deep ────────────────────────────────────────────────

def test_apply_overrides_keeps_sibling_keys():
    base = {"cfg": {"model": "m1", "temp": 0.1}}
    out = apply_overrides(base, {"cfg": {"temp": 0.9}})
    assert out["cfg"] == {"model": "m1", "temp": 0.9}, (
        "overrides 是**深合并** —— 兄弟键必须保留 (v0.9 fix: 浅 update 会抹掉)")


def test_apply_overrides_dot_path_keeps_siblings():
    out = apply_overrides({"cfg": {"model": "m1", "temp": 0.1}}, {"cfg.temp": 0.9})
    assert out["cfg"] == {"model": "m1", "temp": 0.9}


def test_the_two_paths_actually_differ():
    """把两侧并排跑一遍 —— 本文件存在的理由就是它们**不同**且都在用."""
    prev = {"cfg": {"model": "m1", "temp": 0.1}}
    stage_way = merge_state(prev, {"cfg": {"temp": 0.9}}, "s_b",
                            overwrite_keys={"cfg"})["cfg"]
    override_way = apply_overrides(prev, {"cfg": {"temp": 0.9}})["cfg"]
    assert stage_way != override_way, (
        "两条路径若哪天变成同一种语义, 这条会红 —— 那是有意的破坏性变更, "
        "届时要同时改文档与 CHANGELOG")
