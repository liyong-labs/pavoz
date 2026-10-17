"""examples 必须**真的能跑完** —— 而且是照它自己 docstring 写的用法跑。

起因 (2026-10-17 实测): 四个例子调的是**裸 `pavos` 命令**:

    $ bash examples/01_model_switch.sh demo s_compose
      examples/01_model_switch.sh: 行 49: pvos: 未找到命令
    $ python examples/04_deep_merge.py demo
      FileNotFoundError: [Errno 2] No such file or directory: 'pvos'

裸命令要求 **venv 的 bin 在 PATH 上**, 而"装了但 venv 没激活 / 用了别的解释器"是极常见
的处境; 失败消息既不指向原因也不给出路。

关键: 既有判据 `test_examples_runnable.py` 查的是"example 引用的路径存在"与"传给 CLI
的 flag 存在", 而它自己取 flag 面时用的是 `[sys.executable, "-m", ...]` ——
**判据验的形式和例子实际用的形式不是一回事**, 所以那两条全绿而例子照样跑不起来。

现在统一成 `sys.executable -m <pkg>` (py) / `"$PAVOZ_PY" -m <pkg>` (sh), 并用两条判据
钉住: ① 静态 —— 任何例子都不许出现裸 CLI 调用; ② 端到端 —— 真的跑一遍 docstring 里
那套流程, 且**显式把 venv 的 bin 从 PATH 里抹掉**, 否则这条判据只在"测试恰好从已激活的
venv 里跑"时才成立, 换个人跑就失效。
"""
from __future__ import annotations

import os
import pathlib
import re
import subprocess
import sys

import pytest

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_EXAMPLES = _ROOT / "examples"


def _pkg_name() -> str:
    """仓里的包名 —— **程序化派生**, 不手打 (手打这串字面量本会话错过十几次, 而错一个
    字母的表现是 ModuleNotFoundError, 极易被误读成"没装")。"""
    for d in sorted(_ROOT.iterdir()):
        if (d / "__main__.py").is_file() and (d / "types.py").is_file():
            return d.name
    raise AssertionError("在仓根找不到包目录")


_PKG = _pkg_name()


# 剥注释与 docstring 的逻辑**复用**既有判据里的 `_code_only`, 不重写一遍:
# 我第一版只剥了注释行, 于是 docstring 里"怎么用 CLI"的**散文**被判成真调用
# (6 处假阳性: agent_loop / 02 / 03 的模块 docstring 里都印着命令行示例)。
# 与其在这里重造 tokenize+ast 那套, 不如直接用已经踩过坑的那一份。
from test_examples_runnable import _code_only          # noqa: E402


# ⚠️ argv[0] 那条必须**跨行**匹配: 实际写法是
#       subprocess.run([
#           "pvos", "fork-run", str(dag),
#   argv[0] 在**下一行**。第一版按行匹配 (`subprocess.run([\s*"pvos"` 要求同行),
#   于是它对真实的裸调用**完全失明** —— 变异验证时端到端那条红了而它没红, 才发现。
#   (又一次"判据没坏, 是验证脚本/判据构造错了"的同类。)
_ARGV0 = re.compile(rf'subprocess\.run\(\s*\[\s*"{_PKG}"', re.M)


def _bare_cli_calls(path: pathlib.Path) -> list[int]:
    """裸 CLI 调用的行号 (只看**代码**): 行首直接跑命令, 或 subprocess 的 argv[0]。"""
    text = _code_only(path)
    lines = text.splitlines()
    out = []
    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if re.match(rf"^{_PKG}(\s|$)", stripped):
            out.append(i)
    for m in _ARGV0.finditer(text):
        out.append(text[: m.start()].count("\n") + 1)
    return sorted(set(out))


_EXAMPLE_FILES = sorted(
    [p for p in _EXAMPLES.glob("*.py") if p.name != "__init__.py"]
    + list(_EXAMPLES.glob("*.sh"))
)


@pytest.mark.parametrize("example", _EXAMPLE_FILES, ids=lambda p: p.name)
def test_example_never_calls_the_cli_binary_directly(example):
    """★ 例子只能经**解释器**调 CLI (`python -m`), 不能依赖 PATH 上的裸命令。"""
    bare = _bare_cli_calls(example)
    assert not bare, (
        f"{example.name} 第 {bare} 行直接调了裸 `{_PKG}` 命令 —— 它要求 venv 的 bin 在 "
        f"PATH 上, 而'装了但没激活'时失败消息毫无指向性 (实测 4 个例子中招)。"
        f"Python 例子用 `sys.executable, '-m', '{_PKG}'`, shell 用 `\"$PAVOZ_PY\" -m "
        f"{_PKG}`")


def _scrubbed_env(storage: pathlib.Path, venv_bin: pathlib.Path) -> dict[str, str]:
    """PAVOZ_STORAGE 指向临时目录, 并把 venv 的 bin 从 PATH 里抹掉 (复现失败条件)。"""
    env = dict(os.environ, PAVOZ_STORAGE=str(storage))
    parts = [p for p in env.get("PATH", "").split(os.pathsep)
             if p and pathlib.Path(p).resolve() != venv_bin.resolve()]
    env["PATH"] = os.pathsep.join(parts)
    return env


def test_example_workflow_runs_without_venv_on_path(tmp_path):
    """★ 端到端: 照 `04_deep_merge.py` docstring 的用法真跑一遍, 且 venv 不在 PATH 上。

    跑 04 而不是全部五个: 它只需一个 task、无需 yaml, 且**曾经正好是失败的那个**
    (FileNotFoundError)。判据要能真的失败 —— 这条修前红、修后绿。
    """
    venv_bin = pathlib.Path(sys.executable).parent
    env = _scrubbed_env(tmp_path / "store", venv_bin)

    # 自检: venv 的 bin 确实不在 PATH 上 (否则这条判据等于没测东西)
    on_path = [p for p in env["PATH"].split(os.pathsep) if p]
    assert str(venv_bin) not in [str(pathlib.Path(p).resolve()) for p in on_path], \
        "自检失败: venv 的 bin 仍在 PATH 里, 这条判据会恒过"

    seed = subprocess.run(
        [sys.executable, "-m", _PKG, "run", str(_EXAMPLES / "demo_dag.py"),
         "--task-id", "e2e"],
        capture_output=True, text=True, cwd=_ROOT, env=env)
    assert seed.returncode == 0, f"前置: demo_dag 跑不起来: {seed.stderr[-400:]}"

    r = subprocess.run(
        [sys.executable, str(_EXAMPLES / "04_deep_merge.py"), "e2e"],
        capture_output=True, text=True, cwd=_ROOT, env=env)
    assert r.returncode == 0, (
        f"04_deep_merge.py 在 venv 不在 PATH 的情况下跑不通 (rc={r.returncode}):\n"
        f"{r.stderr[-600:]}\n"
        f"⇒ 例子仍在依赖 PATH 上的裸 `{_PKG}` 命令")
    assert "Traceback" not in r.stderr, f"跑出了 traceback:\n{r.stderr[-600:]}"
