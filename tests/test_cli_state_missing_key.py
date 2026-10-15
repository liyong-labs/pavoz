"""`state --key` 打错的 key 不能和"值就是 null"长得一模一样.

背景 (2026-10-16 巡检发现): `pavoz state --task-id t1 --key K` 走的是
`cp.state.get(args.key, None)` —— 打错的 key 与真值为 null 的 key 输出**逐字节
相同**, 退出码都是 0:

    state --task-id t1 --key nil   → null   rc=0     (key 存在, 值确为 null)
    state --task-id t1 --key aa    → null   rc=0     (key 不存在, 打错了)

接管道时 (`X=$(pavoz state --key aa)`) 调用方拿不到任何信号去分辨两者。这与
tools/check-inbox.sh 那次是同一类问题: 一个"静默给出错误答案"的检查, 比报错
更坏 —— 而 `state --key` 存在的全部意义就是**查某一个字段**, 打错字是最自然的
失误。

修法: key 不存在时点名报错并列出可用 key, 退出码 1; key 存在时行为一字不变
(包括显式的 null 值 —— 那是真答案, 不能动)。
"""
from __future__ import annotations

import pytest

import pavoz.cli as cli

_DAG_SRC = '''
from pavoz import DAG

dag = DAG("state_key_probe")


@dag.stage()
async def s_a(ctx):
    return {"count": 7, "explicit_null": None, "nested": {"k": "v"}}
'''


@pytest.fixture
def ran_task(tmp_path, monkeypatch, capsys):
    """跑一次 run 落盘, 返回 task_id + dag 路径 (CLI 侧全部走真实 main())."""
    monkeypatch.setenv("PAVOZ_STORAGE", str(tmp_path / "store"))
    monkeypatch.delenv("PAVOZ_STORAGE_SPEC", raising=False)
    dag_file = tmp_path / "probe_dag.py"
    dag_file.write_text(_DAG_SRC, encoding="utf-8")
    assert cli.main(["run", str(dag_file), "--task-id", "t1"]) == 0
    capsys.readouterr()          # 清掉 run 的 JSON 输出
    return str(dag_file)


def _state(capsys, *extra):
    rc = cli.main(["state", "--task-id", "t1", *extra])
    return rc, capsys.readouterr()


def test_existing_key_still_prints_value(ran_task, capsys):
    """happy path 一字不变 (防修复顺手改坏正常路径)."""
    rc, (out, err) = _state(capsys, "--key", "count")
    assert rc == 0
    assert out.strip() == "7"
    assert "Traceback" not in err


def test_explicit_null_value_is_not_treated_as_missing(ran_task, capsys):
    """值确为 null 的 key 必须照常返回 0 —— 那是真答案, 不是打错."""
    rc, (out, err) = _state(capsys, "--key", "explicit_null")
    assert rc == 0, "显式 null 值不该被判成 'key 不存在'"
    assert out.strip() == "null"
    assert "Traceback" not in err


def test_missing_key_is_distinguishable_and_fails_loud(ran_task, capsys):
    """打错的 key: 退出码非 0 + 点名 + 给出可用 key (trace 的 state_keys 同源)."""
    rc, (out, err) = _state(capsys, "--key", "cnt")
    assert rc == 1, f"key 不存在却返回成功 ({rc}), 调用方无从分辨"
    assert "cnt" in out, f"报错应点名用户敲的 key, stdout 是:\n{out}"
    # 提示里应能看到真 key, 否则用户只能回去猜
    assert "count" in out, f"提示应列出可用 key, stdout 是:\n{out}"
    assert "Traceback" not in err


def test_missing_key_output_is_not_bare_null(ran_task, capsys):
    """回归钉: 修复前的输出就是裸 'null', 逐字节等于真 null 值."""
    rc, (out, err) = _state(capsys, "--key", "definitely_absent")
    assert out.strip() != "null", "缺失 key 的输出与真 null 值撞车了"
    assert rc == 1


def test_no_key_flag_still_prints_whole_state(ran_task, capsys):
    """不带 --key 时行为不变 (全量 dump)."""
    rc, (out, err) = _state(capsys)
    assert rc == 0
    assert '"count": 7' in out
    assert "Traceback" not in err
