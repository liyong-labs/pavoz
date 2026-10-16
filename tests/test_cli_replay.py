"""CLI replay 命令测试 (直接调 async _cmd_replay)."""
import json
import os

import pavoz.cli as cli_mod
from pavoz.checkpoint import CheckpointStore
from pavoz.cli import _cmd_fork_run, _cmd_replay, _cmd_run
from pavoz.storage import FileStorage


def _write_dag(tmp: str) -> str:
    """写一个真实 dag 文件 (与 _load_dag 的 import 机制兼容)."""
    path = os.path.join(tmp, "demo_dag.py")
    with open(path, "w") as f:
        f.write("""
from pavoz import DAG

dag = DAG("demo")

@dag.stage()
async def s_a(ctx):
    return {"a": 1}

@dag.stage(depends_on=["s_a"])
async def s_b(ctx):
    return {"b": ctx.state["a"] + 1}
""")
    return path


def _write_patch(tmp: str, *, async_patch: bool = False) -> str:
    """写一个 patch 文件 (改 s_b 为 b*10; async_patch=True → async def patch 误用)."""
    path = os.path.join(tmp, "patch_sb.py")
    head = "async def patch(dag):" if async_patch else "def patch(dag):"
    body = """
    async def s_b_v2(ctx):
        return {"b": ctx.state["a"] * 10}
    dag.stages["s_b"].fn = s_b_v2
"""
    with open(path, "w") as f:
        f.write(head + body)
    return path


class _Args:
    """argparse.Namespace 替代 (测试用)."""
    def __init__(self, **kw):
        self.__dict__.update(kw)


async def test_cli_run_saves_checkpoint_even_without_resume(monkeypatch, tmp_path):
    """R5 修复: run 不带 --resume 也落盘 → replay/trace/state 可用."""
    dag_path = _write_dag(str(tmp_path))
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "store"))
    rc = await _cmd_run(_Args(dag=dag_path, task_id="cli-1", input=None, resume=False))
    assert rc == 0
    store = CheckpointStore(FileStorage(str(tmp_path / "store")))
    cp = store.load_latest("cli-1")
    assert cp is not None
    assert set(cp.stage_deltas.keys()) == {"s_a", "s_b"}


async def test_cli_replay_reruns_single_stage(monkeypatch, tmp_path):
    dag_path = _write_dag(str(tmp_path))
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "store"))

    await _cmd_run(_Args(dag=dag_path, task_id="cli-1", input=None, resume=False))
    rc = await _cmd_replay(_Args(dag=dag_path, task_id="cli-1", stage="s_b", patch=None))
    assert rc == 0


async def test_cli_replay_with_patch_applies_patched_fn(monkeypatch, tmp_path, capsys):
    """patch 分支: 改 s_b 实现 → replay JSON state.b == 10 (不重跑前序)."""
    dag_path = _write_dag(str(tmp_path))
    patch_path = _write_patch(str(tmp_path))
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "store"))
    await _cmd_run(_Args(dag=dag_path, task_id="cli-1", input=None, resume=False))
    capsys.readouterr()  # 丢掉 run 的 JSON, 只留 replay 的输出

    rc = await _cmd_replay(_Args(dag=dag_path, task_id="cli-1", stage="s_b", patch=patch_path))
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "done"
    assert out["stage"] == "s_b"
    assert out["state"]["b"] == 10  # patch 生效 (原逻辑 b=2)


async def test_cli_replay_async_patch_rejected(monkeypatch, tmp_path, capsys):
    """patch 误声明成 async → 明确拒绝 + exit 1 (不静默跑原 fn)."""
    dag_path = _write_dag(str(tmp_path))
    patch_path = _write_patch(str(tmp_path), async_patch=True)
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "store"))
    await _cmd_run(_Args(dag=dag_path, task_id="cli-1", input=None, resume=False))

    rc = await _cmd_replay(_Args(dag=dag_path, task_id="cli-1", stage="s_b", patch=patch_path))
    assert rc == 1
    assert "不能是 async" in capsys.readouterr().out


async def test_cli_fork_run_failed_status_returns_1(monkeypatch, tmp_path, capsys):
    """v0.9 回归: fork-run 跑出 status=failed → rc=1 (v0.8 契约, reviewer 裁定恢复).

    注: run 对 failed stage 同样 rc=1 (0 if status=="done" else 1, 实测非 reviewer
    草案里的 rc==0) — stage 异常在 runtime 管线内被捕获, 不 raise.
    """
    dag_path = os.path.join(str(tmp_path), "fail_dag.py")
    with open(dag_path, "w") as f:
        f.write("""
from pavoz import DAG

dag = DAG("fail_demo")

@dag.stage()
async def s_a(ctx):
    return {"a": 1}

@dag.stage(depends_on=["s_a"])
async def s_b(ctx):
    raise RuntimeError("boom")
""")
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "store"))
    rc = await _cmd_run(_Args(dag=dag_path, task_id="failrc", input=None, resume=False))
    assert rc == 1  # failed stage → status="failed" → rc=1 (与 _cmd_fork_run 同契约)
    capsys.readouterr()  # 丢掉 run 的 JSON, 只留 fork-run 的输出

    rc = await _cmd_fork_run(_Args(dag=dag_path, task_id="failrc", stage="s_b",
                                   overrides='{"x": 1}', input=None, set=[],
                                   set_file=None, compare_with=None, dry_run=False))
    assert rc == 1  # status != done → rc=1 (v0.8 契约)
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "failed"
    assert out["overrides_applied"] == {"x": 1}  # stdout 仍带 overrides 供诊断


# ── patch 里抛异常 = 参数错误, 不该吐 15 行 traceback ─────────────────────
#
# `--patch` 是**用户提供的代码文件** (与 `--input` 的 JSON、`--set-file` 同类), 而
# CLI 对这类输入的既有口径是"友好一行 + 退出码 1":
#   · `run --input` 的 JSON 解析错误  → "参数错误: ..."
#   · `fork-run` 四个 override 来源   → "参数错误: ..."
#   · `prune --keep-last`             → "参数错误: ..."
#   · patch 文件不存在 / 缺 patch() / patch 是 async → 各自一行
#
# 唯独**patch 函数体里抛出的异常**没有 try: `_cmd_replay` 直接 `patcher(dag)`。
# 我第一次写 patch 就撞上了 —— 用 `@dag.stage()` 重新定义一个已存在的 stage,
# `duplicate stage name: s_a` 从 cli.py 第 195 行甩出一整段 runpy/asyncio 栈,
# 而真因是 patch 文件第 3 行。用户拿到的是"引擎内部炸了"的错觉。
#
# 另一条同源的: 文档只说 patch 模块要暴露 `patch(dag) -> None`, **没给可用写法**。
# 最自然的写法就是重挂装饰器, 而那必然撞 duplicate stage name。真正能用的是直接
# 换 `dag.stages["x"].fn` (测试里一直这么写, 但从没写进文档)。


async def test_patch_raising_is_reported_as_one_line_not_traceback(monkeypatch, tmp_path, capsys):
    """★ patch 函数体抛异常 → 一行 + rc=1, 且**不吐 traceback**。"""
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "store"))
    dag_path = _write_dag(str(tmp_path))
    assert await _cmd_run(_Args(dag=dag_path, task_id="pr-1", input=None, resume=False)) == 0
    capsys.readouterr()

    path = os.path.join(str(tmp_path), "bad_patch.py")
    with open(path, "w") as f:
        f.write('''
def patch(dag):
    @dag.stage()                 # 最自然的写法, 但重定义已存在的 stage 必然撞车
    async def s_b(ctx):
        return {"b": 1}
''')
    rc = await _cmd_replay(_Args(dag=dag_path, task_id="pr-1", stage="s_b", patch=path))
    cap = capsys.readouterr()          # 只能取一次: 第二次已是空
    out, err = cap.out, cap.err
    assert rc == 1, f"patch 失败应返回 1, 实得 {rc}"
    assert "Traceback" not in err, (
        f"★ 用户 patch 的异常不该吐 traceback (它会指向 runpy/cli 而非 patch 文件), 实得:\n{err[:400]}")
    assert "duplicate stage name" in (out + err), (
        f"报错要点名真因 (duplicate stage name), 而不是只说'patch 失败'。实得: {(out+err)[:300]}")
    assert "patch" in (out + err), f"报错要点明是哪个 patch 文件, 实得: {(out+err)[:200]}"


async def test_patch_error_names_the_patch_file(monkeypatch, tmp_path, capsys):
    """异常消息里带 patch 文件路径 —— 用户可能同时有好几个 patch 文件。"""
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "store"))
    dag_path = _write_dag(str(tmp_path))
    assert await _cmd_run(_Args(dag=dag_path, task_id="pr-2", input=None, resume=False)) == 0
    capsys.readouterr()

    path = os.path.join(str(tmp_path), "raises.py")
    with open(path, "w") as f:
        f.write("def patch(dag):\n    raise RuntimeError('我自己炸的')\n")
    rc = await _cmd_replay(_Args(dag=dag_path, task_id="pr-2", stage="s_b", patch=path))
    cap = capsys.readouterr()
    out, err = cap.out, cap.err
    assert rc == 1
    blob = out + err
    assert "raises.py" in blob, f"要带 patch 文件名, 实得: {blob[:300]}"
    assert "我自己炸的" in blob, f"要保留原始异常消息 (那是用户自己写的), 实得: {blob[:300]}"
