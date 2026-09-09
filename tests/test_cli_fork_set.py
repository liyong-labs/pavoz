"""v0.9: fork-run 新 flag CLI 测试 (直接调 _cmd_fork_run, 模式同 test_cli_replay.py)."""
import json
import os
import subprocess
import sys

import pavoz.cli as cli_mod
from pavoz.checkpoint import CheckpointStore
from pavoz.cli import _cmd_fork_run, _cmd_run
from pavoz.storage import FileStorage


def _write_dag(tmp: str) -> str:
    """写一个真实 dag 文件 (与 _load_dag 的 import 机制兼容: 模块级 dag 变量)."""
    path = os.path.join(tmp, "demo_dag.py")
    with open(path, "w") as f:
        f.write("""
from pavoz import DAG

dag = DAG("demo")

@dag.stage()
async def s_a(ctx):
    return {"a": 1, "topic": "orig", "llm": {"model": "orig", "temperature": 0.5}}

@dag.stage(depends_on=["s_a"])
async def s_b(ctx):
    return {"b": ctx.state["a"] + 1}
""")
    return path


class _Args:
    """argparse.Namespace 替代 (测试用)."""

    def __init__(self, **kw):
        self.__dict__.update(kw)


def _fork_args(dag_path, task_id, **kw):
    # fork 点必须是 s_b (不是 s_a): override 注入的是 from_stage 的执行前 state,
    # 若在 s_a 上覆盖 topic, s_a 重跑时再 return topic → 单 producer 冲突
    # (StateConflictError, DAG 里每个 key 只允许一个 producer). 同 test_fork*.py
    # 既有约定: fork from 被覆盖 key 的下游 stage.
    base = dict(dag=dag_path, task_id=task_id, stage="s_b", overrides=None,
                input=None, set=[], set_file=None, compare_with=None, dry_run=False)
    base.update(kw)
    return _Args(**base)


def _run_initial(dag_path, task_id):
    return _cmd_run(_Args(dag=dag_path, task_id=task_id, input=None, resume=False))


async def test_help_shows_new_flags():
    result = subprocess.run(
        [sys.executable, "-m", "pavoz", "fork-run", "--help"],
        capture_output=True, text=True,
    )
    help_text = result.stdout + result.stderr
    assert "--set" in help_text
    assert "--set-file" in help_text
    assert "--compare-with" in help_text
    assert "--dry-run" in help_text


async def test_dry_run_does_not_execute(monkeypatch, tmp_path):
    dag_path = _write_dag(str(tmp_path))
    store_dir = str(tmp_path / "store")
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", store_dir)
    rc = await _cmd_fork_run(
        _fork_args(dag_path, "dry1", set=["topic=dry_run_test"], dry_run=True))
    assert rc == 0
    # dry-run 不产生任何 run
    assert CheckpointStore(FileStorage(store_dir)).list_runs("dry1") == []


async def test_set_top_level(monkeypatch, tmp_path, capsys):
    dag_path = _write_dag(str(tmp_path))
    store_dir = str(tmp_path / "store")
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", store_dir)
    await _run_initial(dag_path, "set1")
    capsys.readouterr()  # 丢掉 run 的 JSON, 只留 fork-run 的输出
    rc = await _cmd_fork_run(_fork_args(dag_path, "set1", set=["topic=cli_test"]))
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "done"
    assert out["overrides_applied"] == {"topic": "cli_test"}
    assert len(CheckpointStore(FileStorage(store_dir)).list_runs("set1")) == 2


async def test_set_nested(monkeypatch, tmp_path, capsys):
    dag_path = _write_dag(str(tmp_path))
    store_dir = str(tmp_path / "store")
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", store_dir)
    await _run_initial(dag_path, "set2")
    capsys.readouterr()  # 丢掉 run 的 JSON, 只留 fork-run 的输出
    rc = await _cmd_fork_run(
        _fork_args(dag_path, "set2", set=["llm.model=longcat", "config.x=42"]))
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["overrides_applied"]["llm"]["model"] == "longcat"
    assert out["overrides_applied"]["config"]["x"] == 42
    # CLI 路径下兄弟键保留 (mirror test_fork_overrides.py 场景):
    # fork 后新 run 的 final state 里 llm.model 被覆盖, llm.temperature 仍在
    fork_cp = CheckpointStore(FileStorage(store_dir)).load("set2", out["run_id"])
    assert fork_cp.state["llm"]["model"] == "longcat"
    assert fork_cp.state["llm"]["temperature"] == 0.5


async def test_set_file_json(monkeypatch, tmp_path, capsys):
    dag_path = _write_dag(str(tmp_path))
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "store"))
    await _run_initial(dag_path, "set3")
    capsys.readouterr()  # 丢掉 run 的 JSON, 只留 fork-run 的输出
    f = tmp_path / "ovr.json"
    f.write_text(json.dumps({"topic": "from_file"}))
    rc = await _cmd_fork_run(_fork_args(dag_path, "set3", set_file=str(f)))
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["overrides_applied"]["topic"] == "from_file"


async def test_compare_with_shows_diff(monkeypatch, tmp_path, capsys):
    dag_path = _write_dag(str(tmp_path))
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "store"))
    await _run_initial(dag_path, "cmp1")
    capsys.readouterr()  # 丢掉 run 的 JSON, 只留 fork-run 的输出
    rc1 = await _cmd_fork_run(_fork_args(dag_path, "cmp1", set=["topic=edited"]))
    out1 = json.loads(capsys.readouterr().out)
    assert rc1 == 0
    rc2 = await _cmd_fork_run(
        _fork_args(dag_path, "cmp1", set=["topic=another"],
                   compare_with=out1["run_id"]))
    assert rc2 == 0
    out2 = json.loads(capsys.readouterr().out)
    assert out2["compare_with"] == out1["run_id"]
    assert out2["diff_keys_count"] > 0


async def test_dunder_rejected(monkeypatch, tmp_path, capsys):
    dag_path = _write_dag(str(tmp_path))
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "store"))
    rc = await _cmd_fork_run(_fork_args(dag_path, "dunder1", set=["__proto__.x=1"]))
    assert rc == 1
    assert "参数错误" in capsys.readouterr().out


async def test_set_overrides_priority(monkeypatch, tmp_path, capsys):
    dag_path = _write_dag(str(tmp_path))
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "store"))
    await _run_initial(dag_path, "prio1")
    capsys.readouterr()  # 丢掉 run 的 JSON, 只留 fork-run 的输出
    rc = await _cmd_fork_run(
        _fork_args(dag_path, "prio1", overrides='{"topic": "from_overrides"}',
                   set=["topic=from_set"]))
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["overrides_applied"]["topic"] == "from_set"


async def test_dry_run_preview_leaf_diff(monkeypatch, tmp_path, capsys):
    """R2: dry-run 预演 — 对执行前 state 的 leaf 级 diff + would_rerun."""
    dag_path = _write_dag(str(tmp_path))
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "store"))
    await _run_initial(dag_path, "pv1")
    capsys.readouterr()  # 排掉 _cmd_run 的输出
    rc = await _cmd_fork_run(
        _fork_args(dag_path, "pv1", stage="s_a", set=["topic=edited"], dry_run=True))
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    pv = out["preview"]
    assert pv["diff_keys_count"] == 1
    assert "topic" in pv["diff_sample"]
    assert pv["would_rerun"] == ["s_a", "s_b"]


async def test_dry_run_preview_deep_merge_keeps_sibling(monkeypatch, tmp_path, capsys):
    """R2 + 深合并 CLI 层可见: llm.model 变, llm.temperature 不出现在 diff (兄弟键保留)."""
    dag_path = _write_dag(str(tmp_path))
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "store"))
    await _run_initial(dag_path, "pv2")
    capsys.readouterr()  # 排掉 _cmd_run 的输出
    rc = await _cmd_fork_run(
        _fork_args(dag_path, "pv2", set=["llm.model=new"], dry_run=True))
    assert rc == 0
    pv = json.loads(capsys.readouterr().out)["preview"]
    assert "llm.model" in pv["diff_sample"]
    assert "llm.temperature" not in pv["diff_sample"]
    assert pv["diff_sample"]["llm.model"] == ["orig", "new"]


async def test_dry_run_preview_stage_not_done(monkeypatch, tmp_path, capsys):
    """R2 边界: 无 cp → preview null + note, rc 0 (不阻断)."""
    dag_path = _write_dag(str(tmp_path))
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "store"))
    rc = await _cmd_fork_run(
        _fork_args(dag_path, "pv3", set=["topic=x"], dry_run=True))
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["preview"] is None
    assert "无 checkpoint" in out["preview_note"]


def test_storage_spec_env_resolution(monkeypatch, tmp_path):
    """R1: PAVOZ_STORAGE_SPEC + KWARGS (含 {task_id} 插值) → load_storage."""
    root = tmp_path / "ab-store"
    monkeypatch.setenv("PAVOZ_STORAGE_SPEC", "pavoz.storage:FileStorage")
    monkeypatch.setenv("PAVOZ_STORAGE_KWARGS",
                       json.dumps({"root_dir": f"{root}/{{task_id}}"}))
    store = cli_mod._storage("t-42")
    assert store.root.endswith("t-42")


def test_storage_spec_bad_module_friendly_error(monkeypatch):
    """R1: spec 拼错 → PavozStorageError (含安装提示), 非 traceback."""
    import pytest

    from pavoz.storage_loader import PavozStorageError

    monkeypatch.setenv("PAVOZ_STORAGE_SPEC", "nope.module:Store")
    monkeypatch.delenv("PAVOZ_STORAGE_KWARGS", raising=False)
    with pytest.raises(PavozStorageError, match="nope"):
        cli_mod._storage("t-1")


def test_storage_default_unchanged(monkeypatch, tmp_path):
    """R1 后向兼容: 不设 SPEC → FileStorage(DEFAULT_STORAGE) 现行为."""
    monkeypatch.delenv("PAVOZ_STORAGE_SPEC", raising=False)
    monkeypatch.setattr(cli_mod, "DEFAULT_STORAGE", str(tmp_path / "d"))
    store = cli_mod._storage()
    assert store.root == str(tmp_path / "d")
