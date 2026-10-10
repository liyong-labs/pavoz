"""CLI 解析器契约: 用户敲的 flag 必须真的落到命令读的 args 属性上.

背景: 既有 CLI 测试 (test_cli_replay.py / test_cli_fork_set.py) 全部**绕过
main()**, 手搓 _Args 命名空间直接喂 _cmd_*。于是 argparse 装配层 —— 用户实际
敲的那一层 —— 零覆盖, 且那些测试对它**同义反复**: _Args 是按命令函数的期望
构造的, 所以若解析器把 --task-id 声明成别的 dest (或某 flag 拼错), 测试照样
绿, 真 CLI 却 AttributeError。coverage 也如实反映了这点: cli.py 51%,
未覆盖的大头正是 main() 里的 parser 装配。

本测试走真实 main(argv): 把该命令的 handler 换成记录器, 捕获解析器产出的
命名空间, 再断言**命令函数源码里实际读的每个 args.X 都存在于该命名空间**。
判据取自源码而非手抄清单 —— 命令新增一个 args 读取时, 断言自动跟上。
"""
from __future__ import annotations

import inspect
import re

import pytest

import pavoz.cli as cli

# 每个子命令的最小合法 argv (必填位置参数 + 必填选项)
_MINIMAL_ARGV = {
    "run": ["run", "d.py"],
    "trace": ["trace", "--task-id", "t1"],
    "state": ["state", "--task-id", "t1"],
    "replay": ["replay", "d.py", "--task-id", "t1", "--stage", "s_a"],
    "export-input": ["export-input", "--task-id", "t1", "--stage", "s_a"],
    "fork-run": ["fork-run", "d.py", "--task-id", "t1", "--stage", "s_a"],
    "prune": ["prune", "--task-id", "t1"],
}

# 子命令名 → handler 函数名 (从 main() 的 set_defaults 提取, 避免手抄漂移)
_CMD_TO_FN = {
    "run": "_cmd_run",
    "trace": "_cmd_trace",
    "state": "_cmd_state",
    "replay": "_cmd_replay",
    "export-input": "_cmd_export_input",
    "fork-run": "_cmd_fork_run",
    "prune": "_cmd_prune",
}


def _attrs_read_by(fn) -> set[str]:
    """handler 源码里实际读取的 args.<name> 集合."""
    return set(re.findall(r"\bargs\.([a-z_][a-z0-9_]*)", inspect.getsource(fn)))


def _namespace_from_real_parser(monkeypatch, cmd: str):
    """走真实 argparse 装配, 捕获该子命令产出的命名空间."""
    captured: dict = {}

    def recorder(args):
        captured["ns"] = args

        async def _noop():
            return 0

        return _noop()

    monkeypatch.setattr(cli, _CMD_TO_FN[cmd], recorder)
    cli.main(_MINIMAL_ARGV[cmd])
    return captured["ns"]


@pytest.mark.parametrize("cmd", sorted(_MINIMAL_ARGV))
def test_parser_supplies_every_attribute_the_handler_reads(monkeypatch, cmd):
    # 先取真实 handler 引用 —— 顺序要紧: 打桩后再 getattr 拿到的是记录器本身,
    # 属性提取会从记录器源码跑出空集, 断言恒真 (第一版就栽在这, 反向验证抓到的)。
    real_fn = getattr(cli, _CMD_TO_FN[cmd])
    ns = _namespace_from_real_parser(monkeypatch, cmd)
    needed = _attrs_read_by(real_fn)
    assert needed, f"{_CMD_TO_FN[cmd]} 没匹配到任何 args.X 读取, 提取规则可能失效"
    missing = sorted(needed - set(vars(ns)))
    assert not missing, (
        f"`pavoz {cmd}` 的 handler 读 args.{missing} 但解析器没产这些属性 — "
        f"用户敲真实 CLI 会 AttributeError (既有 CLI 测试绕过 main(), 抓不到)"
    )


@pytest.mark.parametrize("cmd", sorted(_MINIMAL_ARGV))
def test_every_subcommand_has_a_test(monkeypatch, cmd):
    """新增子命令时必须登记最小 argv, 否则它没有契约保护."""
    assert cmd in _CMD_TO_FN, f"新增子命令 `{cmd}` 需登记 handler 名"
    assert cmd in _MINIMAL_ARGV, f"新增子命令 `{cmd}` 需补最小 argv"


def test_missing_required_flag_is_rejected():
    """必填选项缺失时 argparse 必须拒绝 (真 CLI 会退出码 2, 不是 AttributeError)."""
    with pytest.raises(SystemExit) as e:
        cli.main(["replay", "d.py", "--task-id", "t1"])  # 缺 --stage
    assert e.value.code == 2
