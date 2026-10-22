"""文档与 CLI help 里指向的**仓内文件**, 必须活得进 sdist —— 被排除掉的路径,
照抄命令的人拿到的是 "dag 文件不存在".

起因 (2026-10-17, 且是**我自己刚引入的**): 为挡住消费方业务代码进发布产物, 我给
`[tool.hatch.build.targets.sdist]` 加了 `exclude = ["/research", "/dags"]` —— 结果连
`dags/demo.py` 一起排掉了。而那正是两处**面向用户的示例命令**所指向的路径:

    README.cn.md:142   python -m pavoz run dags/demo.py --task-id demo-1 ...
    pavoz/cli.py:4     (--help 的 epilog)  python -m pavoz run dags/demo.py ...

于是从 sdist 装的人照抄首页命令 → `dag 文件不存在`。而**仓里跑**的人一切正常 (排除只
作用于构建), 所以本地测试、CI、文档检查**一条都没红** —— 这是典型的"只有真实用户会
碰到"的回归。

判据的形状很克制, 因为它必须躲开两种假阳性:
  · **只断言"文件存在"**: quickstart 故意让用户自建 `dags/my_pipeline.py`, 那条路径在
    仓里**不存在** —— 断言存在会把它误判。真正的不变式是"**只要它在仓里, 它就必须发得
    出去**"。
  · **只断言"没被排除"**: 那样任何新出现的文档路径都不受约束。所以两条都要。

自检: 扫不到任何"仓内存在"的被引路径时判失败 —— 那是判据空转 (正则失配 / 文档改名)。
"""
from __future__ import annotations

import fnmatch
import os
import pathlib
import re
import tomllib

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_PKG = [d for d in os.listdir(_ROOT) if (_ROOT / d / "cli.py").is_file()][0]

# 看起来像仓内源码路径的 token (排除绝对路径 / home 展开 / 明显的占位)
_PATHY = re.compile(r"(?<![\w/.-])([\w][\w.-]*/[\w][\w./-]*\.py)\b")

# 这些地方的文字是**用户会照抄**的
SOURCES = [
    _ROOT / "README.md",
    _ROOT / "README.cn.md",
    *sorted((_ROOT / "docs").rglob("*.md")),
    _ROOT / _PKG / "cli.py",
]


def _sdist_excludes() -> list[str]:
    cfg = tomllib.loads((_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return [str(x) for x in
            cfg["tool"]["hatch"]["build"]["targets"]["sdist"].get("exclude", [])]


def _is_excluded(rel: str, excludes: list[str]) -> bool:
    """rel 本身或它的任一祖先目录命中任一 exclude 模式 → 被排除。"""
    parts = rel.split("/")
    prefixes = ["/".join(parts[:i]) for i in range(1, len(parts) + 1)]
    return any(fnmatch.fnmatch(p, pat.strip("/")) or fnmatch.fnmatch(p, "/" + pat.strip("/"))
               for p in prefixes for pat in excludes)


def _referenced_existing() -> dict[str, list[str]]:
    """被文档/help 引用、且**仓里确实存在**的路径 → 引用它的文件。"""
    excludes = _sdist_excludes()
    out: dict[str, list[str]] = {}
    for src in SOURCES:
        if not src.is_file():
            continue
        text = src.read_text(encoding="utf-8")
        for rel in set(_PATHY.findall(text)):
            if rel.startswith("~") or not (_ROOT / rel).exists():
                continue
            if _is_excluded(rel, excludes):
                out.setdefault(rel, []).append(str(src.relative_to(_ROOT)))
    return out


def test_referenced_repo_files_survive_the_sdist():
    """★ 用户照抄的命令所指向的仓内文件, 不许被 sdist exclude 挡掉。"""
    bad = _referenced_existing()
    assert not bad, (
        "这些文件被文档/CLI help 当作示例路径指向, 却被 sdist 排除了 —— "
        "从 sdist 安装的人照抄会得到 'dag 文件不存在', 而仓里跑的人一切正常 "
        "(排除只作用于构建), 所以本地测试与 CI 都不会红:\n  "
        + "\n  ".join(f"{p} <- {sorted(set(v))}" for p, v in sorted(bad.items()))
        + "\n  两条出路: 把排除收窄到消费方那几个文件, 或把这些示例改指向仓内别处。"
    )


def test_judgment_is_not_vacuous():
    """自检: 必须真扫到几个"仓内存在且被引用"的路径, 否则上面那条恒过。"""
    excludes = _sdist_excludes()
    seen = 0
    for src in SOURCES:
        if not src.is_file():
            continue
        for rel in set(_PATHY.findall(src.read_text(encoding="utf-8"))):
            if (_ROOT / rel).exists() and not _is_excluded(rel, excludes):
                seen += 1
    assert seen >= 2, (
        f"只扫到 {seen} 个'仓内存在且未被排除'的被引路径 —— 判据快空转了 "
        f"(正则失配? 文档改名?)")


def test_exclusion_matcher_covers_ancestors_but_not_siblings():
    """自检: `_is_excluded` 本身对不对 —— 主判据与"非空转"自检都用它, 它坏了两个一起坏。

    2026-10-17 实测: 把 `_is_excluded` 改成恒返回 False 时, 主判据**照样绿** —— 因为
    "没有任何东西被排除" 正是它要断言的反面。同理恒返回 True 会让它对一切过敏。辅助
    函数的失效方向不同, 主判据看不见, 所以必须单独钉。
    """
    ex = ["/research", "/dags/research_dag.py"]
    assert _is_excluded("research/llm_router.py", ex), "目录级 exclude 应覆盖其下文件"
    assert _is_excluded("dags/research_dag.py", ex), "文件级 exclude 应命中自己"
    assert not _is_excluded("dags/demo.py", ex), "只排一个文件时, 同目录的兄弟不该被连坐"
    assert not _is_excluded("dags/__init__.py", ex)
    assert not _is_excluded("pvos/cli.py", ex), "包本体永远不该被排除"
    assert not _is_excluded("research", []), "空 exclude 列表不该排除任何东西"


def test_business_dirs_are_still_excluded():
    """★ 防再引入的闸不能被顺手拆掉: 消费方业务路径的 exclude 必须一直在。

    这两个路径 2026-10-23 已从仓里删除, exclude 留着是为了"哪怕有人从历史里把
    文件捞回来 add, 也发不出去" (hatchling 默认打包全部 git 跟踪文件)。
    """
    excludes = [str(x) for x in _sdist_excludes()]
    norm = {x.strip("/") for x in excludes}
    assert "research" in norm, f"消费方业务包 research/ 的 exclude 必须留着 (防再引入): {excludes}"
    # dags/ 整体不再排除 (demo.py 要发), 但消费方那个文件仍要排除
    assert "dags/research_dag.py" in norm or "dags" in norm, (
        f"消费方的 dags/research_dag.py 必须留在 exclude 里: {excludes}")
    assert "dags" not in norm, (
        "若把整个 dags/ 排除, dags/demo.py 会被一起排掉, 而 README.cn 首页与 "
        "CLI --help 都拿它当示例命令 (2026-10-17 实测的回归)")
