"""**入口文档**里对"事实"的断言必须与仓库一致 —— llms.txt 与 CONTRIBUTING.md.

这两份文件都是**入口**: `llms.txt` 给 AI 工具读, `CONTRIBUTING.md` 给贡献者读。里面的
错会被当成事实照着做, 而**零条判据**曾经看着它们。

2026-10-17 实测, `llms.txt` 一处三错:
  1. `Install (not on PyPI yet)` —— 事实错误 (包早已发布)。同一轮我刚把 README 里的
     同一句删掉, 却漏了它。
  2. 指向 `ROADMAP.md` 的链接是死链 (文件不存在), 而 `check_doc_links` 只 glob `*.md`。
  3. "Before you commit" 列 3 个门禁, CI 有 8 个 —— 照做的人少跑一半且不知道自己漏了。

`CONTRIBUTING.md` 是**同一类、且更糟** (实测):
  · 标题写"与 CI 同款命令", 实际只列 2 条 (pytest + pyflakes), CI 有 8 条。
  · 那条 pyflakes 的目标列表**漏了 `tools`** —— 8 个门禁脚本全在 `tools/` 里, 所以
    贡献者照做**连门禁脚本本身都没 lint**。
  · pytest 没带 `--cov-fail-under=80` —— 覆盖率门禁没跑, 于是本地过、CI 红。

判据从**事实源头派生**, 不信任手抄的副本:
  · 发布状态不写死: 有 tag 就不许出现 "not on PyPI" (还没发版时判据自己跳过)。
  · 门禁清单从 `.github/workflows/test.yml` 的 `run:` 里抽, 且**精确匹配**。
    第一版用"子串包含" —— 而 CONTRIBUTING 那条 `pyflakes pavos tests examples` 正是
    CI 那条 `... examples tools` 的子串, 于是**漏掉 tools 也判通过**。对命令而言
    漏参数就是漏检查, 所以必须逐字相等。

⚠️ 上一条 "把 llms.txt 移出 check_doc_links 扫描面" 第一轮没被抓住: 我写的是
`assert "llms.txt" in <源码>`, 而那个文件的**说明文字**里同样出现这个词。
**判据查字符串不等于查行为** —— 已改成调它的 `markdown_files()` 看返回集合。
"""

from __future__ import annotations

import importlib.util
import pathlib
import re
import subprocess
import sys

import pytest

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_ENTRY_DOCS = [_ROOT / "llms.txt", _ROOT / "CONTRIBUTING.md"]
_CI = _ROOT / ".github" / "workflows" / "test.yml"


def _released() -> bool:
    p = subprocess.run(["git", "tag", "-l", "v*"], cwd=_ROOT,
                       capture_output=True, text=True)
    return bool(p.stdout.split())


def _strip_comment(line: str) -> str:
    return re.sub(r"\s+#.*$", "", line).strip()


def _ci_commands() -> list[str]:
    """CI 里真正跑的命令 (每个 `run:` 块里的 python 行)。"""
    text = _CI.read_text(encoding="utf-8")
    out = []
    for block in re.findall(r"run:\s*\|?\s*\n((?:\s{10,}.*\n)+)", text):
        for line in block.splitlines():
            line = _strip_comment(line)
            if line.startswith("python "):
                out.append(line)
    return out


def _doc_commands(path: pathlib.Path) -> list[str]:
    return [_strip_comment(l) for l in path.read_text(encoding="utf-8").splitlines()
            if l.startswith("python ")]


# ── 1. 发布状态不许自相矛盾 ────────────────────────────────────────────────

@pytest.mark.parametrize("doc", _ENTRY_DOCS, ids=lambda p: p.name)
def test_does_not_claim_unreleased_after_tagging(doc):
    """★ 有 tag = 发过版 = 不许再写 "not on PyPI"。"""
    if not _released():
        return                            # 还没发版, 那句话是对的
    text = doc.read_text(encoding="utf-8")
    for phrase in ("not on PyPI", "not yet on PyPI", "尚未发布"):
        assert phrase not in text, (
            f"{doc.name} 写着 {phrase!r}, 但 `git tag -l \"v*\"` 非空 —— 本包早已发布。"
            f"这是入口文档, 照着它会告诉读者装不上。")


# ── 2. 门禁清单必须与 CI **逐字**一致 ─────────────────────────────────────

@pytest.mark.parametrize("doc", _ENTRY_DOCS, ids=lambda p: p.name)
def test_gate_list_matches_ci_exactly(doc):
    """★ CI 跑的命令, 入口文档必须**逐字**列出 —— 漏参数就是漏检查。

    ⚠️ 必须是精确相等, 不是"子串包含"。第一版用子串, 而 `CONTRIBUTING.md` 原来那条
    `python -m pyflakes pavos tests examples` 恰好是 CI 那条
    `python -m pyflakes pavos tests examples tools` 的**子串** —— 于是**漏掉 tools
    也判通过**, 而 tools/ 里正是 8 个门禁脚本本身。
    """
    ci, listed = _ci_commands(), _doc_commands(doc)
    missing = [c for c in ci if c not in listed]
    assert not missing, (
        f"{doc.name} 的门禁清单漏了 (或与 CI 不逐字相同): {missing}\n"
        f"  CI 实际跑: {ci}\n  {doc.name} 列的: {listed}")


@pytest.mark.parametrize("doc", _ENTRY_DOCS, ids=lambda p: p.name)
def test_gate_list_invents_nothing_ci_does_not_run(doc):
    """★ 反面: 不许列 CI 根本不跑的命令 (那会让人以为跑了)。"""
    ci, listed = _ci_commands(), _doc_commands(doc)
    ci_tools = {c.split()[1] for c in ci if c.startswith("python tools/")}
    listed_tools = {l.split()[1] for l in listed if l.startswith("python tools/")}
    extra = sorted(listed_tools - ci_tools)
    assert not extra, (
        f"{doc.name} 列了 CI 不跑的工具: {extra} —— 照着做的人会以为它算门禁。"
        f"  CI 跑的工具: {sorted(ci_tools)}")


# ── 3. 自检 ──────────────────────────────────────────────────────────────

def test_judgment_is_scanning_both_files():
    """自检: 确实读到两份文件且都抠出了命令 —— 否则上面几条对某一份是恒过。"""
    assert _CI.is_file(), ".github/workflows/test.yml 不存在"
    ci = _ci_commands()
    assert len(ci) >= 5, f"只从 CI 抠到 {len(ci)} 条命令 —— 解析多半坏了: {ci}"
    for doc in _ENTRY_DOCS:
        assert doc.is_file(), f"{doc.name} 不存在"
        got = _doc_commands(doc)
        assert len(got) >= 5, f"{doc.name} 只抠到 {len(got)} 条命令 —— 解析多半坏了: {got}"


def test_llms_is_covered_by_the_link_gate():
    """★ llms.txt 的**链接**归 check_doc_links 管 —— 直接查它的**扫描结果**。

    第一版写的是 `assert "llms.txt" in <check_doc_links.py 的源码>` —— 而那个文件里
    **说明文字本身**就提到 llms.txt, 于是把扫描列表整段删掉它照样通过 (变异实测全绿)。
    **判据查字符串不等于查行为**: 源码里出现过这个词, 与它真的被扫到, 是两件事。
    """
    spec = importlib.util.spec_from_file_location(
        "_check_doc_links", _ROOT / "tools" / "check_doc_links.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    scanned = {q.name for q in mod.markdown_files(_ROOT)}
    assert "llms.txt" in scanned, (
        f"check_doc_links 的 markdown_files() 没返回 llms.txt —— 它只 glob '*.md', "
        f"于是 .txt 里的链接一条都没人验。实际扫到: {sorted(scanned)[:12]}")

    p = subprocess.run([sys.executable, "tools/check_doc_links.py"],
                       cwd=_ROOT, capture_output=True, text=True, timeout=90)
    assert p.returncode == 0, f"check_doc_links 报: {(p.stdout + p.stderr)[:300]}"
