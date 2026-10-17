"""磁盘上的记录损坏时, 必须**响**且**说清怎么办** —— 不能是裸 traceback, 更不能是 None.

起因 (2026-10-17 实测): `FileStorage.get` 原来是裸 `json.load`, 而 CLI 的 `main()`
没有任何顶层兜底。于是崩溃留下的半截 checkpoint 上:

    $ pavos trace --task-id X
    ...
      File "/usr/lib/python3.12/json/decoder.py", line 353, in raw_decode
    json.decoder.JSONDecodeError: Unterminated string starting at: line 1 column 898

一整段指向 stdlib 的 traceback: 既没说**哪个文件**坏了, 也没说**能怎么办** —— 而"崩溃后
回来查"恰恰是用户会跑这些命令的时刻 (trace / state / export-input / replay / fork-run /
run --resume 六条路径全部中招)。

现在抛 `CorruptRecordError` (带路径、原始错误、常见原因、处置建议), CLI 顶层兜住并
rc=1。**刻意不写 catch-all**: 真 bug 的 traceback 里的文件和行号, 吞掉只会少一条线索。

判据里最要紧的一条是**反面**: 损坏**不能**被当成"没有 checkpoint"。若 `get` 返回
None, `run --resume` 会认为"从头开始" —— 于是**静默丢掉**该 task 已有的全部进度, 而
用户以为只是重跑了一遍。这比报错糟得多, 所以单独钉住。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from pavoz import CorruptRecordError, DAG, FileStorage, Runtime
from pavoz.checkpoint import CheckpointStore

_ROOT = Path(__file__).resolve().parent.parent


async def _corrupt_store(tmp_path) -> CheckpointStore:
    """造一个"跑过一次、checkpoint 被截断"的 store。

    ⚠️ 必须是 **async**: 本仓 pytest 是 asyncio auto 模式, 而 async 测试里调
    `asyncio.run()` 会直接 "cannot be called from a running event loop"。同步的
    CLI 那条测试才用 `asyncio.run(_corrupt_store(...))` 包一层。
    """
    store = CheckpointStore(FileStorage(str(tmp_path / "store")))
    dag = DAG("corrupt")
    src = ["@dag.stage()", "async def s_a(ctx):", "    return {'a': 1}"]
    exec("\n".join(src), {"dag": dag})          # noqa: S102 — 生成测试用 DAG
    await Runtime(checkpoint_store=store).run(dag, "t")
    cp_path = next((tmp_path / "store").rglob("checkpoint.json"))
    cp_path.write_bytes(cp_path.read_bytes()[: len(cp_path.read_bytes()) // 2])
    return store


def test_get_raises_corrupt_error_not_none(tmp_path):
    """★ `get` 必须抛 —— 返回 None 等于说"没这条记录", 那会让 resume 静默重来。"""
    fs = FileStorage(str(tmp_path / "s"))
    fs.put("k", {"ok": 1})
    path = tmp_path / "s" / "k.json"
    path.write_bytes(path.read_bytes()[:5])

    with pytest.raises(CorruptRecordError):
        fs.get("k")


def test_error_is_not_a_bare_jsondecodeerror(tmp_path):
    """抛的是带上下文的 `CorruptRecordError`, 不是把 JSONDecodeError 原样放出来。"""
    fs = FileStorage(str(tmp_path / "s"))
    fs.put("k", {"ok": 1})
    path = tmp_path / "s" / "k.json"
    path.write_bytes(b'{"ok"')

    with pytest.raises(CorruptRecordError) as ei:
        fs.get("k")
    assert not isinstance(ei.value, json.JSONDecodeError), \
        "不该直接冒 JSONDecodeError —— 那正是修复前的形状"
    assert isinstance(ei.value.__cause__, json.JSONDecodeError), \
        "原始错误应当挂在 __cause__ 上 (traceback 里还得看得到)"


def test_message_names_the_file_and_says_how_to_proceed(tmp_path):
    """消息要能独立看懂: 哪个文件、能不能恢复、接下来做什么。"""
    fs = FileStorage(str(tmp_path / "s"))
    fs.put("k", {"ok": 1})
    path = tmp_path / "s" / "k.json"
    path.write_bytes(b'{"ok"')

    with pytest.raises(CorruptRecordError) as ei:
        fs.get("k")
    msg = str(ei.value)
    assert "k.json" in msg, f"消息里没有文件名: {msg}"
    assert "无法恢复" in msg, f"消息没说这个 run 恢复不了: {msg}"
    assert "处置" in msg, f"消息没给处置建议: {msg}"


def test_valid_record_still_loads(tmp_path):
    """对照: 好的记录照常读 —— 别把这条判据写成"一律报错"。"""
    fs = FileStorage(str(tmp_path / "s"))
    payload = {"stage": "f3", "blob": ["y" * 10]}
    fs.put("k", payload)
    assert fs.get("k") == payload


async def test_resume_does_not_silently_start_over(tmp_path):
    """★ 反面判据: 损坏的 checkpoint 上 `run --resume` 必须**失败**, 不能重跑一遍说 done。

    静默重来是最糟的形状: 用户看到 "done", 以为只是重跑了一次, 而这个 task 之前
    跑出来的进度已经被静默丢掉了 —— 而磁盘上的旧数据明明还在 (只是读不出来)。
    """
    store = await _corrupt_store(tmp_path)
    dag = DAG("corrupt")
    src = ["@dag.stage()", "async def s_b(ctx):", "    return {'b': 2}"]
    exec("\n".join(src), {"dag": dag})          # noqa: S102 — 生成测试用 DAG

    with pytest.raises(CorruptRecordError):
        await Runtime(checkpoint_store=store).run(dag, "t", resume=True)


@pytest.mark.parametrize("argv", [
    ["trace", "--task-id", "t"],
    ["state", "--task-id", "t"],
    ["export-input", "--task-id", "t", "--stage", "s_a"],
    ["run", "demo_dag.py", "--task-id", "t", "--resume"],
])
def test_cli_reports_cleanly_with_rc1_and_no_traceback(tmp_path, argv):
    """★ CLI: rc=1 + 一句能看懂的话, 且输出里**没有** `Traceback`。

    逐条覆盖读 checkpoint 的命令 —— 修复前它们全都打的是指向 json/decoder.py 的
    裸 traceback。
    """
    import asyncio
    asyncio.run(_corrupt_store(tmp_path))
    dag_file = tmp_path / "demo_dag.py"
    dag_file.write_text(
        "from pavoz import DAG\n"
        "dag = DAG('demo')\n"
        "@dag.stage()\n"
        "async def s_a(ctx):\n"
        "    return {'a': 1}\n",
        encoding="utf-8",
    )
    env = dict(os.environ, PAVOZ_STORAGE=str(tmp_path / "store"))
    args = [a if a != "demo_dag.py" else str(dag_file) for a in argv]

    # 包名**运行时派生**, 不手打: 这个 5 字符的名字在本会话里被错过 5 次
    # (`-m pvos` → No module named), 判据里再打错一次就是一轮假红。
    pkg = FileStorage.__module__.rsplit(".", 1)[0]
    proc = subprocess.run(
        [sys.executable, "-m", pkg, *args],
        capture_output=True, text=True, cwd=_ROOT, env=env,
    )
    out = proc.stdout + proc.stderr
    assert proc.returncode == 1, f"rc 应为 1, 实得 {proc.returncode}\n{out[:400]}"
    assert "Traceback" not in out, f"仍然在打裸 traceback:\n{out[:600]}"
    assert "记录损坏" in out, f"没给出可处置的提示:\n{out[:400]}"
