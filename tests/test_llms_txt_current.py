"""`llms.txt` 是**专门写给 AI 工具读**的入口清单 —— 里面的错会被当成事实照着做.

起因 (2026-10-17 实测): 那份文件里同时有三处过期, 而**零条判据看着它**:

  1. `Install (not on PyPI yet): pip install <pkg>` —— **事实错误**。包早已在 PyPI 上
     (CHANGELOG 有 `[0.5.8]`, `git tag -l "v*"` 非空)。同一轮我刚把 README 里的
     "not on PyPI yet" 删掉, 却漏了这个文件。而 AI 读到它会告诉用户"装不上"。
  2. `ROADMAP.md` 是**死链** —— 文件在仓里根本不存在。`tools/check_doc_links.py`
     当时只扫 `*.md`, 于是 `.txt` 里的链接一条都没被验过。
  3. "Before you commit" 列了 3 个门禁, 而 CI 早就有 8 个 —— 照着做的人会少跑一半
     检查, 而且**不会知道自己漏了**。

判据从**事实源头派生**, 不信任手抄的副本:
  · 发布状态不写死: 有 tag 就说明发过版, 于是文本里不许出现 "not on PyPI"。
  · 门禁清单从 `.github/workflows/test.yml` 的 `run:` 里抽 —— CI 加一个门禁, 这里就
    立刻转红, 而不是等几个月后有人发现清单没跟上。

⚠️ 上一条 (2) 之所以没被发现, 是**扫描范围**问题: `check_doc_links` 只 glob `*.md`。
已把 `llms.txt` 加进那个工具的扫描面 —— 那属于链接, 归它管; 本文件管"内容是否与事实
一致"。
"""
from __future__ import annotations

import pathlib
import re
import subprocess
import sys
import tomllib

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_LLMS = _ROOT / "llms.txt"
_CI = _ROOT / ".github" / "workflows" / "test.yml"
_PKG = tomllib.loads((_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["name"]


def _released() -> bool:
    p = subprocess.run(["git", "tag", "-l", "v*"], cwd=_ROOT,
                       capture_output=True, text=True)
    return bool(p.stdout.split())


def _strip_comment(line: str) -> str:
    return re.sub(r"\s+#.*$", "", line).strip()


def _ci_python_commands() -> list[str]:
    """CI 里真正跑的命令 (每个 `run:` 块里的 python 行)。"""
    text = _CI.read_text(encoding="utf-8")
    out = []
    for block in re.findall(r"run:\s*\|?\s*\n((?:\s{10,}.*\n)+)", text):
        for line in block.splitlines():
            line = _strip_comment(line)
            if line.startswith("python "):
                out.append(line)
    return out


def _llms_commands() -> list[str]:
    return [_strip_comment(l) for l in _LLMS.read_text(encoding="utf-8").splitlines()
            if l.startswith("python ")]


# ── 1. 发布状态不许自相矛盾 ────────────────────────────────────────────────

def test_does_not_claim_unreleased_after_tagging():
    """★ 有 tag = 发过版 = 不许再写 "not on PyPI"。"""
    if not _released():
        return                            # 还没发版, 那句话是对的
    text = _LLMS.read_text(encoding="utf-8")
    for phrase in ("not on PyPI", "not yet on PyPI", "尚未发布"):
        assert phrase not in text, (
            f"llms.txt 写着 {phrase!r}, 但 `git tag -l \"v*\"` 非空 —— 本包早已发布。"
            f"这份文件专门给 AI 工具读, 照着它会告诉用户装不上。")


# ── 2. 门禁清单必须与 CI 一致 ─────────────────────────────────────────────

def test_gate_list_covers_everything_ci_runs():
    """★ CI 跑的门禁, llms.txt 一个都不许漏 (照着做的人不该少跑一半检查)。"""
    ci, llms = _ci_python_commands(), _llms_commands()
    missing = [c for c in ci if not any(c in l or l in c for l in llms)]
    assert not missing, (
        f"llms.txt 的 'Before you commit' 漏了 CI 里的: {missing}\n"
        f"  CI 实际跑: {ci}\n  llms.txt 列的: {llms}")


def test_gate_list_invents_nothing_ci_does_not_run():
    """★ 反面: llms.txt 不许列 CI 根本不跑的命令 (那会让人以为跑了)。"""
    ci, llms = _ci_python_commands(), _llms_commands()
    # 只对 tools/check_*.py 这一族比"不许多列": 其余命令 (pytest/pyflakes/pyright)
    # 在 CI 里可能带不同参数, 按前缀比会误报。
    ci_tools = {c.split()[1] for c in ci if c.startswith("python tools/")}
    llms_tools = {l.split()[1] for l in llms if l.startswith("python tools/")}
    extra = sorted(llms_tools - ci_tools)
    assert not extra, (
        f"llms.txt 列了 CI 不跑的工具: {extra} —— 照着做的人会以为它算门禁。"
        f"  CI 跑的工具: {sorted(ci_tools)}")


# ── 3. 自检 ──────────────────────────────────────────────────────────────

def test_judgment_is_scanning_the_file():
    """自检: 确实读到了 llms.txt 且抠出了东西 —— 否则上面两条都是恒过。"""
    assert _LLMS.is_file(), "llms.txt 不存在 —— 上面的判据全部空转"
    assert _CI.is_file(), ".github/workflows/test.yml 不存在"
    ci, llms = _ci_python_commands(), _llms_commands()
    assert len(ci) >= 5, f"只从 CI 抠到 {len(ci)} 条命令 —— 解析多半坏了: {ci}"
    assert len(llms) >= 5, f"只从 llms.txt 抠到 {len(llms)} 条 —— 同上: {llms}"


def test_llms_is_covered_by_the_link_gate():
    """★ llms.txt 的**链接**归 check_doc_links 管 —— 直接查它的**扫描结果**。

    第一版写的是 `assert "llms.txt" in <check_doc_links.py 的源码>` —— 而那个文件里
    **说明文字本身**就提到 llms.txt, 于是把扫描列表整段删掉它照样通过 (变异验证实测:
    5 条全绿)。**判据查字符串不等于查行为**: 源码里出现过这个词, 与它真的被扫到, 是
    两件事。这里改成 import 那个工具、调用它的 `markdown_files()`, 看 llms.txt 在不在
    返回值里 —— 那是它**实际**会扫的集合。

    顺带真跑一遍那个工具: 它报错的话说明本仓有死链。
    """
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "_check_doc_links", _ROOT / "tools" / "check_doc_links.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    scanned = {p.name for p in mod.markdown_files(_ROOT)}
    assert _LLMS.name in scanned, (
        f"check_doc_links 的 markdown_files() 没返回 llms.txt —— "
        f"它只 glob '*.md', 于是 .txt 里的链接一条都没人验。"
        f"死链 ROADMAP.md 当初就是这么漏掉的。实际扫到: {sorted(scanned)[:12]}")

    p = subprocess.run([sys.executable, "tools/check_doc_links.py"],
                       cwd=_ROOT, capture_output=True, text=True, timeout=90)
    assert p.returncode == 0, f"check_doc_links 报: {(p.stdout + p.stderr)[:300]}"
