"""examples/ 里的每个例子都必须**对着真实存在的东西**跑。

背景 (2026-10-17 实测): 9 个例子里有 5 个照 README 跑必然失败, 而仓库里
**零条判据**看着这件事:

1. **引用了仓外路径** —— `01/02/04/05` 全部硬编码
   `backend/integration/research_pipeline_dag.py`, 那是**消费方仓库** (ai-research)
   的路径, pavoz 仓里压根没有。于是 clone 下来的人第一条命令就
   `dag 文件不存在`。它们连同 `llm_config.research_writer.*` 这个 key 与 LongCat
   模型名一起, 都是业务值漏进了开源仓 —— 既跑不通, 也不符合"pvos 不绑业务"的口径。
2. **调了 CLI 上不存在的 flag** —— `03_compare_diff.py` 调
   `pavoz state --run-id <id>`, 而 `state` 只有 `--task-id/--key/--stats`;
   CLI **没有任何命令**能读指定 run 的 state (`state` 只读 latest)。argparse 直接
   `unrecognized arguments`, 对谁都一样。

例子是用户抄的东西, 也是文档的一部分 —— 它引用的东西不存在, 与文档里的链接失效
是同一类错误。所以这里把它**变成机器判据**, 两条:

  · example 里引用的每个 `.py` 路径, 必须**能在仓里找到** (允许用变量/环境变量
    拼, 所以判据是"文件在仓内存在", 而不是"字符串长什么样")
  · example 传给 `pavoz <cmd>` 的每个 flag, 必须真属于那个子命令 (flag 面从
    `pavoz <cmd> --help` 现取, 不写死 —— 写死就会随 CLI 演进悄悄过期)
"""
from __future__ import annotations

import pathlib
import re
import shlex
import subprocess
import sys

import pytest

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_EXAMPLES = _ROOT / "examples"

# 01..05 是 fork-run 系列; 前 4 个原本全部引用仓外路径, 03 用了不存在的 flag。
# llm_pipeline / agent_loop / conditional_loop / resume_after_crash 是自包含的,
# 仍然一起查 (判据对所有例子一视同仁, 否则"新例子又漏了"没人拦)。
_ALL_EXAMPLES = sorted(p for p in _EXAMPLES.glob("*.py") if p.name != "__init__.py") + \
    sorted(_EXAMPLES.glob("*.sh"))

# 像路径的字符串字面量 / token
_PATHY = re.compile(r"[\w./-]*[\w-]\.(?:py|sh|yaml|yml)\b")
# `pavoz <cmd> ...` (python 里写成 "pavoz", "fork-run", ...) 或 shell 直接写
_SHELL_CALL = re.compile(r"\bpavoz\s+([a-z][a-z-]*)((?:[^\n\\#]|\\.)*)")


def _code_only(example: pathlib.Path) -> str:
    """只留**代码**: 剥掉注释与 docstring。

    为什么必须剥: 修好之后的 example 里会留一段 ⚠️ 说明"原先引用的是
    `backend/integration/research_pipeline_dag.py`、原先调的是 `state --run-id`"。
    那是**散文**, 不是活引用 —— 第一版判据不剥, 于是自己指着自己报错 (5 个假阳性)。
    判据扫描代码位置, 不扫叙述, 才不会既漏真问题又被自己的注释淹没。
    """
    text = example.read_text(encoding="utf-8")
    if example.suffix == ".sh":
        out = []
        for line in text.splitlines():
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            # 行尾注释 (宽松处理: 引号内的 '#' 会被切掉, 对本判据只会漏一个 token)
            cut = re.search(r"\s+#", line)
            out.append(line[:cut.start()] if cut else line)
        return "\n".join(out)

    import ast
    import io
    import tokenize

    drop: set[int] = set()
    # 注释
    for tok in tokenize.generate_tokens(io.StringIO(text).readline):
        if tok.type == tokenize.COMMENT:
            drop.update(range(tok.start[0], tok.end[0] + 1))
    # docstring (模块/类/函数的第一个字符串字面量)
    tree = ast.parse(text)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", None)
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                d = body[0].value
                drop.update(range(d.lineno, (d.end_lineno or d.lineno) + 1))
    return "\n".join("" if (i + 1) in drop else ln
                     for i, ln in enumerate(text.splitlines()))


def _repo_files() -> set[str]:
    """仓内所有文件的相对路径 (小写, 便于大小写不敏感地匹配)。"""
    out = set()
    for p in _ROOT.rglob("*"):
        if p.is_file() and ".git" not in p.parts and ".venv" not in p.parts \
                and "__pycache__" not in p.parts:
            out.add(str(p.relative_to(_ROOT)).lower())
    return out


def _cli_flags() -> dict[str, set[str]]:
    """子命令 → 它真有的 flag (从 --help 现取, 不写死)。"""
    flags: dict[str, set[str]] = {}
    for cmd in ("run", "trace", "state", "replay", "export-input", "fork-run", "prune"):
        proc = subprocess.run(
            [sys.executable, "-m", "pavoz", cmd, "--help"],
            capture_output=True, text=True, cwd=_ROOT,
        )
        assert proc.returncode == 0, f"`pavoz {cmd} --help` 失败: {proc.stderr[:200]}"
        flags[cmd] = set(re.findall(r"(--[a-z][a-z-]*)", proc.stdout))
    # 自检: 取到的 flag 面不能是空的 (空面会让下面所有断言恒过)
    assert flags["run"] and flags["fork-run"], f"flag 面提取失败: {flags}"
    return flags


def test_every_example_is_accounted_for():
    """自检: 判据本身得先确认它在扫东西 (空扫描 = 恒过)。"""
    names = {p.name for p in _ALL_EXAMPLES}
    assert len(_ALL_EXAMPLES) >= 9, f"只扫到 {len(_ALL_EXAMPLES)} 个例子, 扫漏了?"
    assert "04_deep_merge.py" in names and "03_compare_diff.py" in names
    assert _repo_files(), "仓内文件清单为空 —— 判据失明"


@pytest.mark.parametrize("example", _ALL_EXAMPLES, ids=lambda p: p.name)
def test_example_references_only_paths_that_exist(example: pathlib.Path):
    """★ example 引用的每个 .py/.sh 路径都必须能在仓里找到。

    判据是"**仓内存在**"而不是"字符串形状": 例子用 `SCRIPT_DIR / "demo_dag.py"` 或
    `${DAG:-...}` 拼路径很正当, 静态分析形状只会误报。要挡的是
    `backend/integration/research_pipeline_dag.py` 这种**仓外**引用 —— 它对任何
    clone 的人都是死路。
    """
    text = _code_only(example)
    in_repo = _repo_files()
    basenames = {r.rsplit("/", 1)[-1] for r in in_repo}
    missing = set()
    for raw in _PATHY.findall(text):
        # 变量根路径 (`$SCRIPT_DIR/x.py` / `${SCRIPT_DIR}/x.py`) 的落点由运行时决定,
        # 静态只能看**文件名**: 它在仓里存在 ⇒ 这条引用有落点 (2026-10-17 第一版判据
        # 把 `SCRIPT_DIR/demo_dag.py` 判成"仓内不存在", 而那个文件就在 examples/ 里)
        name = re.sub(r"^(\$\{?[A-Za-z_][A-Za-z0-9_]*\}?/)+", "", raw).lstrip("./")
        if name.lower() in in_repo:
            continue
        if name.rsplit("/", 1)[-1].lower() in basenames:
            continue
        # 允许"示例里生成/引用自己的输出" (临时文件、用户传进来的 prompt/yaml 文件):
        # 只对**看起来像仓内源码路径**的才判 —— 含目录分隔符的才可能是仓内引用
        if "/" not in name:
            continue
        missing.add(raw)
    assert not missing, (
        f"{example.name} 引用了仓内不存在的路径 {sorted(missing)} —— "
        f"任何 clone 这个仓的人照着跑都会失败。改用仓内文件 (examples/demo_dag.py) "
        f"或让用户用参数/环境变量传自己的路径。")


@pytest.mark.parametrize("example", _ALL_EXAMPLES, ids=lambda p: p.name)
def test_example_only_uses_flags_that_exist(example: pathlib.Path):
    """★ example 传给 `pavoz <cmd>` 的每个 flag 都必须真属于那个子命令。

    起因: `03_compare_diff.py` 调 `pavoz state --run-id` —— 而 `state` 没有这个
    flag, CLI 也没有任何命令能读指定 run 的 state。argparse 直接
    `unrecognized arguments`, 对谁都一样。而仓库里没有任何判据拦得住它。

    flag 面从 `--help` 现取 (写死一份就会随 CLI 演进悄悄过期)。
    """
    flags = _cli_flags()
    # 两种写法都要扫到 (2026-10-17 变异验证逮到的判据失明):
    #   shell:  pavoz fork-run dag.py --task-id X
    #   python: ["pavoz", "fork-run", dag, "--task-id", "X"]
    # 第一版只认第一种, 于是**所有 python 例子一个都没扫到** —— 而 "state --run-id"
    # 那个 bug 恰恰在 python 例子里。变异塞回去仍然全绿, 判据是瞎的。
    # 做法: 先把列表字面量与引号归一成空格分隔, 再用同一条正则。
    raw = _code_only(example)
    text = re.sub(r"[\"']\s*,\s*[\"']?", " ", raw)
    text = text.replace('"', " ").replace("'", " ")
    used: list[str] = []
    for m in _SHELL_CALL.finditer(text):
        cmd, rest = m.group(1), m.group(2)
        if cmd not in flags:          # 不是子命令 (例如文中举例)
            continue
        try:
            tokens = shlex.split(rest, comments=True)
        except ValueError:
            continue                   # 变量没展开, 静态分析不了 → 跳过这一个
        for tok in tokens:
            if re.fullmatch(r"--[a-z][a-z-]*", tok):
                used.append(f"{cmd} {tok}")
    bad = sorted({u for u in used if u.split()[1] not in flags[u.split()[0]]})
    assert not bad, (
        f"{example.name} 用了 CLI 上不存在的 flag: {bad} —— "
        f"照 README 跑会直接 argparse 报错。可用 flag 见 "
        f"`pavoz <cmd> --help`")
