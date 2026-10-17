"""README 里**写死的示例输出**必须与实跑一致。

起因 (2026-10-17 实测): `README.cn.md` 的内联示例把输出断言成

    # done {'greeting': 'hello, pavoz', 'shout': 'HELLO, PAVOZ'}

而实跑是

    done {'name': 'pavoz', 'greeting': 'hello, pavoz', 'shout': 'HELLO, PAVOZ'}

少一个 key —— 因为 `initial_state={"name": "pavoz"}` 里的 `name` 也在 state 里。这不是
"文档不精确"的小事: 一段**输出被写死**的示例, 读者会拿它对照自己跑出来的结果, 差一个 key
就会开始怀疑是不是自己装错了。而 `README.md` 同一位置的断言 (`# → saved: 3 [...]` /
`# → done`) **全对** —— 同一件事两个版本一个对一个错, 更该有判据。

判据的形状: 从示例里把"声称的输出"抠出来, 真跑一遍, 对不上就红。
  · `# → X`      → X 必须整行出现在实跑输出里
  · `# X {...}`   → 用 `ast.literal_eval` 解析出那个 dict, 与实跑输出对应行的 dict 逐
                    key 比 (而不是比字符串, 否则 dict 的空格/顺序差异会误报)

非空自检: 每份 README 至少要抠出**一条**断言, 否则抠取逻辑坏了会变成恒过。
"""
from __future__ import annotations

import ast
import os
import pathlib
import re
import subprocess
import sys

import pytest

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_READMES = ["README.md", "README.cn.md"]


def _pkg_name() -> str:
    for d in sorted(_ROOT.iterdir()):
        if (d / "__main__.py").is_file() and (d / "types.py").is_file():
            return d.name
    raise AssertionError("在仓根找不到包目录")


_PKG = _pkg_name()


def _runnable_block(text: str) -> str:
    for lang, body in re.findall(r"```(\w+)\n(.*?)```", text, re.S):
        if lang == "python" and "asyncio.run(" in body:
            return body
    raise AssertionError("找不到含 asyncio.run 的 python 示例块")


def _claims(block: str) -> list[tuple[str, str]]:
    """抠出 (kind, claimed) —— kind 是 "line" 或 "dict"。"""
    out: list[tuple[str, str]] = []
    for line in block.splitlines():
        # ⚠️ 必须扫**整行**而不是"以 # 开头的行": README.md 的断言是**行尾**注释
        #     (print(result.status)   # → done), 而 README.cn.md 的是整行注释。
        #     第一版只认整行注释 ⇒ README.md 一条都抠不到, 判据自己报了"判据失明"。
        m = re.search(r"→\s*(\S.*?)\s*$", line)
        if m and "#" in line[:m.start()]:
            out.append(("line", m.group(1).strip()))
            continue
        m = re.search(r"#\s*(\w+)\s+(\{.*\})\s*$", line)
        if m:
            out.append(("dict", m.group(2)))
    return out


@pytest.mark.parametrize("name", _READMES)
def test_readme_claimed_output_matches_reality(tmp_path, name):
    text = (_ROOT / name).read_text(encoding="utf-8")
    block = _runnable_block(text)
    claims = _claims(block)
    assert claims, (
        f"{name}: 一个输出断言都没抠到 —— 判据失明 (示例块: {block[:60]!r})")

    script = tmp_path / "snippet.py"
    script.write_text(block, encoding="utf-8")
    env = {**os.environ, "PYTHONPATH": str(_ROOT),
           "PAVOZ_STORAGE": str(tmp_path / "store")}
    proc = subprocess.run([sys.executable, str(script)], capture_output=True,
                          text=True, cwd=tmp_path, env=env)
    assert proc.returncode == 0, f"{name} 的示例跑不起来: {proc.stderr[-400:]}"

    out_lines = [l.strip() for l in proc.stdout.splitlines() if l.strip()]
    for kind, claimed in claims:
        if kind == "line":
            assert claimed in out_lines, (
                f"{name} 声称会出现 {claimed!r}, 实跑输出里没有:\n"
                f"  {out_lines}")
        else:
            want = ast.literal_eval(claimed)
            matched = None
            for line in out_lines:
                if line.startswith("{") or " {" in line:
                    try:
                        matched = ast.literal_eval(line[line.index("{"):])
                    except (ValueError, SyntaxError):
                        matched = None
                    if matched is not None:
                        break
            assert matched is not None, f"{name}: 输出里没找到可解析的 dict: {out_lines}"
            assert matched == want, (
                f"{name} 声称的输出与实跑不一致:\n"
                f"  文档: {want}\n"
                f"  实跑: {matched}")
