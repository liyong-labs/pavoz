"""CLI 参数错误必须友好退出 —— 坏 JSON 不该炸出 json/decoder 的 traceback.

背景 (2026-10-16 巡检发现): CLI 有一条明确的"友好消息"约定 —— 见 _cmd_run 里
的注释 "resume/校验错误 → 友好消息, 不炸 traceback"。但那条 try 只包住
runtime.run(); **参数解析期在 try 之外**, 于是:

    run --input 'not json'          → 15 行 json/decoder.py 内部栈, 真因埋在最后一行
    fork-run --overrides 'not json' → 参数错误: Expecting value: ...   ← 同一仓库内
                                                                       两种口径

用户手打 --input 打错一个括号, 是 CLI 最常见的失误, 也是最该被一句话说清的
那种。fork-run 那边 (parse 三个来源时统一裹了 try/except ValueError) 就是正确
口径, 本测试把两条路径钉成同一契约: 退出码 1 + "参数错误" 前缀 + stderr 无
Traceback。

判据覆盖三件事而不是只看退出码 —— 只断言 rc==1 的话, 一个真的 raise 出
JSONDecodeError 的实现也能过 (它的退出码同样是 1)。
"""
from __future__ import annotations

import pytest

import pavoz.cli as cli

_DAG_SRC = '''
from pavoz import DAG

dag = DAG("cli_probe")


@dag.stage()
async def s_a(ctx):
    return {"a": 1}
'''


@pytest.fixture
def dag_file(tmp_path):
    p = tmp_path / "probe_dag.py"
    p.write_text(_DAG_SRC, encoding="utf-8")
    return str(p)


@pytest.fixture(autouse=True)
def _isolated_storage(tmp_path, monkeypatch):
    """storage 指向 tmp —— CLI 命令默认写 ~/.pavoz, 测试不该污染用户目录."""
    monkeypatch.setenv("PAVOZ_STORAGE", str(tmp_path / "store"))
    monkeypatch.delenv("PAVOZ_STORAGE_SPEC", raising=False)
    monkeypatch.delenv("PAVOZ_STORAGE_KWARGS", raising=False)


# 逐命令列全 argv —— run 没有 --stage (早先漏了它, 试例自己先死在 argparse 上,
# 那不是被测行为, 是试例写错)。dag 路径在函数里填。
_BAD_JSON_CASES = [
    ("run --input 非法 JSON", ["run", "{dag}", "--task-id", "t1", "--input", "not json"]),
    ("fork-run --overrides 非法 JSON",
     ["fork-run", "{dag}", "--task-id", "t1", "--stage", "s_a", "--overrides", "not json"]),
]


@pytest.mark.parametrize(
    "argv_tpl",
    [c[1] for c in _BAD_JSON_CASES],
    ids=[c[0] for c in _BAD_JSON_CASES],
)
def test_bad_json_arg_exits_friendly(argv_tpl, dag_file, capsys):
    argv = [a.replace("{dag}", dag_file) for a in argv_tpl]
    desc = argv_tpl[1]

    rc = cli.main(argv)

    out, err = capsys.readouterr()
    assert rc == 1, f"{desc}: 期望退出码 1, 实得 {rc}"
    assert "参数错误" in out, f"{desc}: 缺友好前缀 '参数错误', stdout 是:\n{out}"
    # 关键判据: 没有 traceback。JSONDecodeError 是 ValueError 子类, 只断言 rc==1
    # 的话, 真 raise 的实现也能过 —— 必须显式禁止栈。
    assert "Traceback" not in err, f"{desc}: 不该吐 traceback, stderr 是:\n{err}"
    assert "JSONDecodeError" not in err, f"{desc}: stderr 漏了内部异常类型:\n{err}"


def test_prune_negative_keep_last_exits_friendly(capsys):
    """--keep-last -1 → prune() 内部 raise ValueError, 但 _cmd_prune 当时没接。

    实测 (2026-10-16): 抛出一整段 checkpoint.py:458 的栈。而 prune 是**删数据**
    的命令 —— 用户敲错一个符号拿到栈而不是"keep_last 必须 >= 0", 是最不该发生的
    那类体验。
    """
    rc = cli.main(["prune", "--task-id", "t1", "--keep-last", "-1"])

    out, err = capsys.readouterr()
    assert rc == 1, f"期望退出码 1, 实得 {rc}"
    assert "参数错误" in out, f"缺友好前缀 '参数错误', stdout 是:\n{out}"
    assert "keep_last" in out, f"报错应说清是哪个参数, stdout 是:\n{out}"
    assert "Traceback" not in err, f"不该吐 traceback, stderr 是:\n{err}"


def test_fork_run_bad_input_file_exits_friendly(tmp_path, dag_file, capsys):
    """--input 指向的**文件**内容非法 → 同契约 (fork-run 侧的另一个 JSON 解析点)."""
    bad = tmp_path / "bad.json"
    bad.write_text("{nope", encoding="utf-8")

    rc = cli.main(["fork-run", dag_file, "--task-id", "t1", "--stage", "s_a",
                   "--input", str(bad)])

    out, err = capsys.readouterr()
    assert rc == 1, f"期望退出码 1, 实得 {rc}"
    assert "参数错误" in out, f"缺友好前缀 '参数错误', stdout 是:\n{out}"
    assert "Traceback" not in err, f"不该吐 traceback, stderr 是:\n{err}"
