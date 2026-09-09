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
