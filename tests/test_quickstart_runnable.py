"""quickstart 的命令必须**照抄能跑** —— 且它自己说的坑必须是真的。

起因 (2026-10-17 实测): `docs/{cn,en}/quickstart.md` 里带 `--resume` 的 bash 块把两条
命令并列摆着, 照抄顺序跑**第二行必然失败**:

    PAVOZ_STORAGE=/data/sf python -m pavos run dags/my_pipeline.py \\
      --task-id task-1 --input '{"query": "..."}'
    PAVOZ_STORAGE=/data/sf python -m pavos run dags/my_pipeline.py --task-id task-1 --resume
    → run 失败: task_id=task-1 ... 已全部完成. 重跑请用 resume=False (起新 run_id).

报错讲的是"怎么重跑", 不是"你为什么不该在这儿 --resume"。同一份文档的 **Python** 块早
就写明了这个陷阱 ("run 已全部完成 → RuntimeError"), 只有 shell 块漏了。

本测试做两件事:
  ① 真跑 quickstart 的可跑前缀 (不带参数的 run + 带 --task-id/--input 的 run) —— 与
     `test_examples_runnable_e2e.py` 同一类: 文档是给人抄的, 腐烂时没有任何门禁会响。
  ② 验证文档的说法本身成立: 把那个块的两条命令连着跑, 第二条**确实**失败, 且错误里
     **确实**出现 `resume=False` —— 这样"连着跑必然报错"不是一句空话, 而是判据。
     (文档说的坑必须是真的, 否则它就成了另一条误导。)

命令里的 `python` / 包名 / `/data/sf` 都要换成测试环境里可用的值 (venv 解释器、
程序化派生的包名、临时存储目录)。这不削弱判据: 替换的只是**路径与解释器**, 被验的是
**flag 名、模块用法、参数组合**, 而那正是会悄悄腐烂的地方。
"""
from __future__ import annotations

import json
import pathlib
import re
import shlex
import subprocess
import sys

import pytest

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_DOCS = ["docs/cn/quickstart.md", "docs/en/quickstart.md"]
_IDS = ["cn", "en"]


def _pkg_name() -> str:
    """仓里的包名 —— 程序化派生, 不手打 (手打这串字面量本会话错过十几次)。"""
    for d in sorted(_ROOT.iterdir()):
        if (d / "__main__.py").is_file() and (d / "types.py").is_file():
            return d.name
    raise AssertionError("在仓根找不到包目录")


_PKG = _pkg_name()


def _blocks(doc: pathlib.Path) -> list[tuple[str, str]]:
    return re.findall(r"```(\w+)\n(.*?)```", doc.read_text(encoding="utf-8"), re.S)


def _find_block(doc: pathlib.Path, *, lang: str, must: str,
                must_not: str | None = None) -> str:
    """按**内容特征**取代码块 (不按下标 —— 文档加一段就会让下标全错位)。"""
    for bl, body in _blocks(doc):
        if bl != lang:
            continue
        if must in body and (must_not is None or must_not not in body):
            return body
    raise AssertionError(f"{doc.name}: 找不到 lang={lang} 且含 {must!r} 的代码块")


def _split_resume_block(body: str) -> tuple[str, str]:
    """把"第一条命令 / ⚠️ 说明 / 第二条命令"按**行**切开。

    ⚠️ 两次栽在"按字符串切分跨行结构"上: `partition("# ⚠️")` 与 `partition("\\n# ⚠️")`
    都会把分隔符吃掉, 于是第二段从**行中间**开始 (` 下面这条…`), 那行不再以 `#` 开头
    ⇒ 不会被"剥注释"剥掉 ⇒ 它的字样被当 argv 喂给 argparse (报 "unrecognized
    arguments: 下面这条**不是**接着上一条跑的")。行是唯一正确的切法: 注释以行为单位,
    命令也以行为单位。
    """
    lines = body.splitlines()
    idx = next((i for i, l in enumerate(lines) if l.strip().startswith("# ⚠️")), None)
    assert idx is not None, "块里找不到 `# ⚠️` 分隔行 —— 文档结构变了?"
    return "\n".join(lines[:idx]), "\n".join(lines[idx + 1:])


def _run_command(body: str, work: pathlib.Path, storage: pathlib.Path) -> subprocess.CompletedProcess:
    """把文档里的 bash 命令跑起来: 剥整行注释 → 拼续行 → shlex 分词 → 换解释器/路径。

    必须用 shlex: 按空白切词会把 `--input '{"query": "北方华创"}'` 拆成三个 argv。
    必须先剥注释再拼续行: 反过来的话, `PAVOZ_STORAGE=... \\` 会把后面的注释行接上来。
    """
    code = "\n".join(l for l in body.splitlines() if not l.strip().startswith("#"))
    joined = code.replace("\\\n", " ")
    raw = [t for t in shlex.split(joined, comments=True)
           if not t.startswith("PAVOZ_STORAGE=")]
    argv: list[str] = []
    for tok in raw:
        if tok in ("python", "python3"):
            argv.append(sys.executable)
        elif tok == "-m":
            argv.append("-m")
        elif tok.startswith("/data/sf"):
            argv.append(str(storage))
        else:
            argv.append(tok)
    assert argv[1] == "-m" and argv[2] == _PKG, f"解析出的命令形状不对: {argv[:4]}"
    return subprocess.run(argv, capture_output=True, text=True, cwd=work)


def _project(tmp_path, dag_src: str) -> tuple[pathlib.Path, pathlib.Path]:
    work = tmp_path / "proj"
    (work / "dags").mkdir(parents=True)
    (work / "dags" / "my_pipeline.py").write_text(dag_src, encoding="utf-8")
    return work, tmp_path / "store"


@pytest.mark.parametrize("rel", _DOCS, ids=_IDS)
def test_quickstart_runnable_prefix_actually_runs(tmp_path, rel):
    """★ 照抄 quickstart 的前几步必须真能跑 (换任何 flag/模块名/参数组合就会红)。"""
    doc = _ROOT / rel
    dag_src = _find_block(doc, lang="python", must='DAG("my_pipeline")')
    plain_run = _find_block(doc, lang="bash", must="run dags/my_pipeline.py",
                            must_not="--resume")
    first_cmd, _ = _split_resume_block(_find_block(doc, lang="bash", must="--resume"))
    assert "--input" in first_cmd and "--task-id" in first_cmd, \
        "前置: 找不到带 --task-id/--input 的那条命令"

    work, storage = _project(tmp_path, dag_src)

    r1 = _run_command(plain_run, work, storage)
    assert r1.returncode == 0, f"quickstart 第一条 (无参数 run) 跑不通: {r1.stderr[-400:]}"
    assert json.loads(r1.stdout)["status"] == "done"

    r2 = _run_command(first_cmd, work, storage)
    assert r2.returncode == 0, (
        f"带 --task-id/--input 的 run 跑不通: {r2.stderr[-400:]}\n"
        f"⇒ 照抄 quickstart 的用户会卡在这")
    assert json.loads(r2.stdout)["status"] == "done"


@pytest.mark.parametrize("rel", _DOCS, ids=_IDS)
def test_quickstart_resume_caveat_is_true(tmp_path, rel):
    """★ 文档说"那两行连着跑第二条必然失败、且报 resume=False" —— 判据验这句话本身。"""
    doc = _ROOT / rel
    dag_src = _find_block(doc, lang="python", must='DAG("my_pipeline")')
    first_cmd, resume_cmd = _split_resume_block(
        _find_block(doc, lang="bash", must="--resume"))
    work, storage = _project(tmp_path, dag_src)

    r1 = _run_command(first_cmd, work, storage)
    assert r1.returncode == 0, f"前置: 第一条命令应当成功: {r1.stderr[-300:]}"
    r2 = _run_command(resume_cmd, work, storage)
    assert r2.returncode != 0, (
        "文档说连着跑第二条必然失败, 实得成功 —— 那这条说明是假的, 得改文档")
    out = r1.stdout + r1.stderr + r2.stdout + r2.stderr
    assert "resume=False" in out, f"文档说报错里会指向 resume=False, 实得:\n{out[-400:]}"
