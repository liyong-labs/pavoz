"""CLI replay 命令测试 (直接调 async _cmd_replay)."""
import os

import stageflow.cli as cli_mod
from stageflow.checkpoint import CheckpointStore
from stageflow.cli import _cmd_replay, _cmd_run
from stageflow.storage import FileStorage


def _write_dag(tmp: str) -> str:
    """写一个真实 dag 文件 (与 _load_dag 的 import 机制兼容)."""
    path = os.path.join(tmp, "demo_dag.py")
    with open(path, "w") as f:
        f.write("""
from stageflow import DAG

dag = DAG("demo")

@dag.stage()
async def s_a(ctx):
    return {"a": 1}

@dag.stage(depends_on=["s_a"])
async def s_b(ctx):
    return {"b": ctx.state["a"] + 1}
""")
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
