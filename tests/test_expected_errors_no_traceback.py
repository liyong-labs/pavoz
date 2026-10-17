"""pavos 自己抛的**用户可处置**错误, 不该被报成"未预期异常" + 一屏 traceback.

起因 (2026-10-17 实测): 单 producer 契约被违反时, 库在 stderr 上打的是

    task=t3 run=0ef27a3f stage=s_b 未预期异常
    Traceback (most recent call last):
      File ".../runtime.py", line 1340, in _run_stage
        new_state = merge_state(state, delta, name, overwrite_keys=_ow)
      File ".../state.py", line 147, in merge_state
        raise StateConflictError(
    pavoz.state.StateConflictError: stage 's_b' 试图写已存在的 state key: ['shared'].

两个问题叠在一起:

  ① **重复**。那句话本来就带成因与建议 ("DAG 里每个 key 只允许一个 producer —
     改用不同 key (语义 key)"), 是写给读者看的, 同时还进了 JSON 的 `error` 字段。
     再配一屏栈 = 把同一句话说三遍, 而栈里没有读者需要的新信息。
  ② **标签是错的**。"未预期" 说的是程序 bug, 而单 producer 是**文档化的规则** ——
     用户看到 "未预期异常" 会先去查环境/版本, 而不是去查自己 DAG 里两个 stage
     写了同一个 key。

判据是**两侧**的, 这个形态是刻意的: 只钉"StateConflictError 不带 traceback"的话,
把 catch-all 整个删掉也能让它通过 —— 而那会让**真 bug 也失去 traceback**, 恰好是
最坏的结果。所以同一组里必须同时钉住:

  · pavos 抛的预期错误 → 无 "Traceback"、无 "未预期异常", 但消息一字不少
  · 用户代码里的真 bug (AttributeError) → **仍然**有 traceback 且仍标 "未预期异常"

第二条同时守住 CLI 的既有立场 (cli.py 顶层刻意不写 catch-all: "真 bug 的
traceback 里 [有] 文件和行号, 吞掉只会少一条线索") —— 本条不是要把栈全灭掉。
"""
from __future__ import annotations

import logging
import os
import pathlib
import subprocess
import sys

import pytest

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_PKG = [d for d in os.listdir(_ROOT) if os.path.isdir(_ROOT / d)
        and (_ROOT / d / "dag.py").is_file()][0]
sys.path.insert(0, str(_ROOT))

from importlib import import_module  # noqa: E402

_DAG = import_module(f"{_PKG}.dag")
_DAGcls = _DAG.DAG


def _dag_conflict():
    """两个 stage 写同一个 state key → StateConflictError (单 producer 契约)."""
    dag = _DAGcls("t_conflict")

    @dag.stage()
    async def s_a(ctx):
        return {"shared": "from_a"}

    @dag.stage()
    async def s_b(ctx):
        return {"shared": "from_b"}

    return dag


def _dag_real_bug():
    """用户 stage 里的 AttributeError —— 真程序 bug, 必须留 traceback."""
    dag = _DAGcls("t_bug")

    @dag.stage()
    async def s_boom(ctx):
        return {"x": None.no_such_attr}

    return dag


def _exec_run(dag_src: str, tmp_path, task_id: str):
    """经真 CLI 跑 (用户实际看到的那条路)."""
    d = tmp_path / f"dag_{task_id}.py"
    d.write_text(dag_src, encoding="utf-8")
    env = dict(os.environ, PAVOZ_STORAGE=str(tmp_path / f"data_{task_id}"))
    return subprocess.run(
        [sys.executable, "-m", _PKG, "run", str(d), "--task-id", task_id],
        capture_output=True, text=True, cwd=_ROOT, env=env,
    )


# ── 库层 ──────────────────────────────────────────────────────────────────

def test_expected_error_gets_no_traceback(caplog):
    """★ pavos 自己抛的规则违反 → 无 traceback、无"未预期异常", 消息一字不少。"""
    import asyncio
    from importlib import import_module as _im
    runtime_mod = _im(f"{_PKG}.runtime")
    state_mod = _im(f"{_PKG}.state")

    with caplog.at_level(logging.DEBUG):
        res = asyncio.run(runtime_mod.Runtime().run(_dag_conflict(), "t1"))

    assert res.status == "failed"
    assert res.error_class == state_mod.StateConflictError.__name__
    # 消息本身一字不少 (它是给读者看的, 不是给栈看的)
    assert "只允许一个 producer" in res.error
    assert "改用不同 key" in res.error

    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "未预期异常" not in text, (
        f"单 producer 契约是**文档化的规则**, 不是程序 bug —— "
        f"不该标成'未预期异常'。实测日志: {text!r}")
    assert not caplog.records or all(r.exc_info is None for r in caplog.records), (
        "预期错误不该配 traceback —— 它的消息已经带成因与建议了")


def test_real_bug_still_gets_traceback(caplog):
    """★ 反面: 用户代码里的真 bug **仍必须**有 traceback (反向门禁)."""
    import asyncio
    from importlib import import_module as _im
    runtime_mod = _im(f"{_PKG}.runtime")

    with caplog.at_level(logging.DEBUG):
        res = asyncio.run(runtime_mod.Runtime().run(_dag_real_bug(), "t2"))

    assert res.status == "failed"
    assert res.error_class == "AttributeError"
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "未预期异常" in text, (
        "真 bug 必须仍然被标成未预期并留下线索 —— "
        "本判据不是要把 traceback 全灭掉")
    traced = [r for r in caplog.records if r.exc_info]
    assert traced, "真 bug 必须留下带 traceback 的日志记录 (栈里有文件和行号)"


# ── CLI 层 (用户实际看到的那条路) ─────────────────────────────────────────

def test_cli_conflict_stderr_has_no_traceback(tmp_path):
    """★ 经真 CLI: 规则违反时 stderr 只有一行消息, 没有栈。"""
    src = (
        f"from {_PKG} import DAG\n"
        "dag = DAG('cli_conflict')\n"
        "@dag.stage()\nasync def s_a(ctx):\n    return {'shared': 'a'}\n"
        "@dag.stage()\nasync def s_b(ctx):\n    return {'shared': 'b'}\n"
    )
    p = _exec_run(src, tmp_path, "c1")
    assert p.returncode == 1
    assert "Traceback" not in p.stderr, (
        f"预期错误不该打栈。stderr:\\n{p.stderr}")
    assert "未预期异常" not in p.stderr, p.stderr
    assert "只允许一个 producer" in p.stderr, p.stderr
    # JSON 里那条消息仍在 (调用方可见字段一字未改)
    assert "只允许一个 producer" in p.stdout


def test_cli_real_bug_still_has_traceback(tmp_path):
    """★ 经真 CLI: 真 bug 仍打完整栈 (反向门禁)。"""
    src = (
        f"from {_PKG} import DAG\n"
        "dag = DAG('cli_bug')\n"
        "@dag.stage()\nasync def s_boom(ctx):\n    return {'x': None.no_such_attr}\n"
    )
    p = _exec_run(src, tmp_path, "b1")
    assert p.returncode == 1
    assert "Traceback" in p.stderr, f"真 bug 必须留栈。stderr:\\n{p.stderr}"
    assert "未预期异常" in p.stderr, p.stderr


# ── 判据自检 ──────────────────────────────────────────────────────────────

def test_judgment_is_two_sided():
    """自检: 两条断言必须指向**不同**的异常类型 —— 否则它们其实是同一条。

    只钉一侧的话, "把 catch-all 删掉"能让预期错误那条变绿, 而真 bug 也随之失去
    traceback。两侧都在, 才挡得住这种"改好了 A 却弄坏了 B"。
    """
    import asyncio
    from importlib import import_module as _im
    runtime_mod = _im(f"{_PKG}.runtime")
    state_mod = _im(f"{_PKG}.state")

    a = asyncio.run(runtime_mod.Runtime().run(_dag_conflict(), "s1"))
    b = asyncio.run(runtime_mod.Runtime().run(_dag_real_bug(), "s2"))
    assert a.error_class != b.error_class, (
        f"两条判据跑出了同一个 error_class ({a.error_class}) —— "
        f"它们不再是两侧, 判据退化成了单侧")
    assert a.error_class == state_mod.StateConflictError.__name__
    assert b.error_class == "AttributeError"


@pytest.mark.parametrize("name", ["_EXPECTED_USER_ERRORS"])
def test_expected_errors_tuple_covers_documented_types(name):
    """元组必须真的收住那些"直接继承 Exception"的用户可处置错误。

    这一半的库错误**不**继承库自己的基类, 所以没有 `except <基类>` 可以一把捞;
    靠的是这个元组。忘了往里加不会算错, 但会多一条 traceback —— 而"忘了加"
    这件事没有别的判据能抓, 所以在这里把已知的那几个钉住。
    """
    from importlib import import_module as _im
    runtime_mod = _im(f"{_PKG}.runtime")
    tup = getattr(runtime_mod, name)
    for mod, cls in (
        ("state", "StateConflictError"),
        ("dag", "CycleError"),
        ("dag", "UnknownDepError"),
        ("cancel", "NotKillable"),
        ("cancel", "TaskIdInUse"),
        ("checkpoint", "CheckpointMismatchError"),
        ("storage", "CorruptRecordError"),
    ):
        exc = getattr(_im(f"{_PKG}.{mod}"), cls)
        assert issubclass(exc, tup), (
            f"{cls} 是用户可据以行动的库错误, 却不在 {name} 里 —— "
            f"它会落到 catch-all, 被标成'未预期异常'并配一屏栈")
