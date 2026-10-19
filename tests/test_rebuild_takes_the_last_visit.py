"""多轮 stage 的 resume 必须恢复到**末次** visit, 不是第一轮 (2026-10-19 实锤)。

## 生产现场 (ai-research `feac093bf3c8`, checkpoint 逐字)

`s_revise` 因为多次 resume 跑过 7 次:

```json
"stage_deltas_visits": {"s_revise": [ …7 条… ]}
"done_stages": ["s_brief", …, "s_revise", "s_save"]     ← s_revise 只有**一条**
```

`_rebuild_state` 按「done_stages 一个条目 → 消费 visits 一条」取, 于是取到
`visits[0]` = **第一轮**:

```
delta 末次 visit : 17424 字, skeleton_subsections = 0
rebuild_state()  : 15122 字, skeleton_subsections = 9    ← 第一轮的稿
```

⇒ resume 后 `s_save` 拿第一轮的空壳稿过闸 ⇒ **9 条 `skeleton_subsection` 硬拦, 任务死**,
而磁盘上的交付稿明明是好的。`_rebuild_state` 的注释「done_stages 完成序可含重复 (循环)」
只覆盖了**循环**触发的多轮, 没覆盖 **resume** 触发的多轮。

## 修法: 尾对齐 (与 fork 分支同一套语义)

done 条数 = n 时, 第 occ 条对应 `visits[n-1-occ]`。`len(deltas) == n` 时
`idx == occ` ⇒ **行为逐字节不变**; 只在 delta 比 done 多时 (resume 累积) 才生效。

⛔ **fork cp 的截断点前必须保持顺序对齐** —— 那一段 visits 与 done 是 1:1 的,
fork 靠 `fork_keep_counts` 切边界, 不靠尾数。改成尾对齐会让
`test_conditional_edges` / `test_shape_matrix` 的 fork 场景全红 (实测)。

## 判据钉的形状

  · 3 次 visit + 1 条 done ⇒ rebuild 取**第 3** 次
  · 1 次 visit + 1 条 done ⇒ 取第 1 次 (尾对齐 ≡ 顺序对齐)
  · 循环 stage: done 有 3 条 + visits 3 条 ⇒ 逐条对应, 不串轮
  · 反向验: 退回顺序对齐 ⇒ 变红
"""
from __future__ import annotations

import pytest

from pavoz.checkpoint import Checkpoint


def _cp(deltas_by_stage: dict[str, list[dict]], done: list[str], **kw) -> Checkpoint:
    return Checkpoint(task_id="t", run_id="r", dag_name="d", workflow_hash="w",
                      stage_statuses={}, done_stages=done,
                      stage_deltas_visits=deltas_by_stage, **kw)


def test_resume_accumulated_visits_take_the_last_one():
    """⭐ 生产现场: 3 次 visit 但 done 只有 1 条 ⇒ 必须取第 3 次。"""
    cp = _cp({"s_revise": [{"article": "R1"}, {"article": "R2"}, {"article": "R3"}]},
             ["s_revise"])
    assert cp.rebuild_state()["article"] == "R3", \
        "rebuild 折的是第一轮 ⇒ 修订成果在 resume 时全丢"


def test_single_visit_is_unchanged():
    """尾对齐在 delta 数 == done 条数时必须与旧的顺序对齐逐字节相同。"""
    cp = _cp({"s_revise": [{"article": "ONLY"}]}, ["s_revise"])
    assert cp.rebuild_state()["article"] == "ONLY"


def test_loop_stage_pairs_one_to_one():
    """循环 stage: done 3 条 + visits 3 条 ⇒ 逐条对应, 顺序不串。"""
    cp = _cp({"s_loop": [{"n": 1}, {"n": 2}, {"n": 3}]}, ["s_loop", "s_loop", "s_loop"])
    assert cp.rebuild_state()["n"] == 3, "尾对齐在等长时串轮了"


def test_fork_prefix_stays_positional():
    """⛔ fork cp 的截断点前**必须**顺序对齐 (与 kcounts 切边界配对)。"""
    cp = _cp({"s_x": [{"v": "hist0"}, {"v": "hist1"}, {"v": "hist2"}]},
             ["s_x", "s_x", "s_x"],
             fork_overrides={"m": "O"}, fork_keep_counts={"s_x": 2})
    # 截断点前 2 条是历史 (1:1, 顺序对齐), 之后才尾对齐
    assert cp.rebuild_state()["v"] == "hist2"
    assert cp.rebuild_state()["m"] == "O", "fork overrides 丢了"


@pytest.mark.parametrize("n_visits", [1, 2, 3, 5])
def test_tail_align_equals_positional_when_counts_match(n_visits: int):
    cp = _cp({"s": [{"i": i} for i in range(n_visits)]}, ["s"] * n_visits)
    assert cp.rebuild_state()["i"] == n_visits - 1


def test_the_detector_would_not_be_fooled_by_extra_visits():
    """反向验证: 把非 fork 分支退回顺序对齐, 第 1 条必须变红。"""
    import inspect

    from pavoz import checkpoint as cp_mod
    src = inspect.getsource(cp_mod.Checkpoint._rebuild_state)
    assert "if is_fork_cp:\n                    idx = occ" in src, \
        "fork 截断点前的顺序对齐被改掉了"
    assert "idx = len(deltas) - (total[done] - occ)" in src, \
        "非 fork 分支的尾对齐没了 ⇒ resume 会折回第一轮"
