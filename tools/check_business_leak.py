"""CI guard: 开源仓里不许出现**消费方的配置值** —— 厂商模型名与消费方 state key / stage 名.

这条纪律写在 CLAUDE.md 里 ("pvos 不能包含 ai_writer 业务" / "不能硬编码模型路由表"),
但**从来没有变成机器判据**, 于是它一直在被违反而不报警。2026-10-17 实测, 仓里同时
存在这四种:

  · `research/` —— **消费方的整个业务包**被 git 跟踪着进了开源仓: 4 个文件, 含
    `llm_router.py` (4 个厂商模型 + 档位 + 用户原话 + PRD 引用) 与消费方 state schema,
    还有一份给消费方本地 pyright 用的 `pavos_stub.py`。**7 个门禁一个都扫不到它**:
    pyflakes 的目标列表是 `pav*z tests examples tools`, pyright 是 `include: ["pav*z"]`。
  · `dags/research_dag.py` —— 消费方自己的 DAG, 同样被跟踪。
  · 厂商模型名当通用示例值: 出现在**库源码的 CLI help** 里 (`--set llm.model=longcat`)、
    两份 README 首页、`docs/replay-params.md` 的可复制示例、以及 3 个测试文件。
    尤其刺眼的是: **pvos 根本没有模型层** (`ctx.call` 是调用方注入的 Protocol),
    CLI help 写死一个厂商模型, 等于告诉读者这个库认得模型 —— 它不认得。
  · 消费方 state key `research_writer` / stage 名 `compose_synthesize`, 仍在文档示例里。

本判据只钉**硬泄漏**: 厂商模型名 + 消费方配置 key/stage 名。**溯源类提及不算泄漏** ——
"这个需求来自 2026-09-10 的某个内部 PR"、"这段曾引用过某业务 key, 已清理" 是在如实
记录来由, 删掉反而是篡改历史。它们逐个列在 ALLOWLIST 里并写明理由, 不是"扫到就放过"
的通配 —— 新增一处没登记的提及照样转红。

ALLOWLIST 里 `research/*` 与 `dags/research_dag.py` 两项是**明确的欠账**, 不是认可:
那是被跟踪的消费方业务代码, 删不删由 user 决定 (见 CHANGELOG 与报告)。删掉之后请把
这两条从 ALLOWLIST 一并删掉 —— 留着它们, 这个门禁就会永远为那笔债开绿灯。

Usage:  python tools/check_business_leak.py     (exit 0 = 没有未登记的业务值)
"""
from __future__ import annotations

import fnmatch
import pathlib
import subprocess
import sys

# ── 硬泄漏 token (一律大小写不敏感) ───────────────────────────────────────
# 刻意**不含** `ai-research` / `ai_writer` 这类**项目名**: 仓里正当地用它们记录需求来由
# (docstring 里的 "R1, 2026-09-10 ... PR")。项目名不是配置值, 扫它会逼着一片真话被删。
TOKENS = (
    "longcat",            # 厂商模型名, 曾被当通用示例值
    "minimax",            # 同上 (消费方路由表里)
    "deepseek",           # 同上
    "research_writer",    # 消费方 state key
    "compose_synthesize",  # 消费方 stage 名
)

# ── 逐条登记的例外 ────────────────────────────────────────────────────────
# (glob, 理由) —— 命中即跳过。**不写通配兜底**: 新增一处没登记的提及必须转红。
ALLOWLIST: tuple[tuple[str, str], ...] = (
    ("tools/check_business_leak.py",
     "判据自身 —— 它必须把 token 原样写出来才扫得到它们。"
     "与 check_text_integrity 当初的教训同源: 那里是把 U+FFFD 写成字面量, 结果**本文件"
     "自己**被自己的判据判失败; 这里用登记代替转义, 因为 token 是词不是单个码位, "
     "拆开写就失去了可读性, 也就失去了'审的人一眼看出扫的是什么'"),
    ("CHANGELOG.md",
     "变更历史 —— 且 tools/check_changelog.py 强制已发布段逐字不可改, 改它就是篡改发布记录"),
    ("examples/demo_dag.py",
     "docstring 里在讲 '这些业务值原先漏进来了、已清理' —— 那是清理记录本身"),
    ("examples/README.md", "同上 (说明 2026-10-17 那次清理)"),
    ("examples/01_model_switch.sh", "同上"),
    ("examples/02_prompt_patch.py", "同上"),
    ("examples/04_deep_merge.py", "同上"),
    ("tests/test_examples_runnable.py",
     "模块 docstring 里复述那次清理的背景; 正文代码已无这些 token"),
    # ↓↓↓ 欠账: 消费方业务代码, 是否删除待 user 决定。删掉后请连同本注释一起删 ↓↓↓
    ("research/*", "消费方 ai-research 的业务包 —— 已报给 user, 待决定是否从开源仓移除"),
    ("dags/research_dag.py", "消费方自己的 DAG —— 同上"),
)

# 这些目录**即便暂留在仓里**, 也不得进任何发布产物。2026-10-17 实测: hatchling 的
# sdist 默认打包**全部 git 跟踪的文件**, 于是 PyPI 上的 0.5.8 sdist (512KB) 里逐字含有
# research/llm_router.py 的 "用户原话 (2026-09-07)" + 四个厂商模型 + 硬编码的
# 厂商 API 端点 + **真实 token 消耗反推的人民币成本** (total_yuan: 0.474), 以及
# dags/research_dag.py。
#
# 上面的 ALLOWLIST 解决"仓里能不能留", 这条解决"**发出去**能不能带"。两者都要:
# 只做前者, 那笔债还躺在 PyPI 上; 只做后者, 仓里继续误导读代码的人。
# ⚠️ `dags` 是**文件级**, 不是目录级 —— 而这不是偏好, 是被现实逼出来的 (2026-10-17):
#   dags/research_dag.py  消费方的 DAG → 必须排除
#   dags/demo.py          通用 mock demo → **必须发出去**, README.cn 首页与 CLI --help
#                         的示例命令都指向它
# 上一版写成目录级 "/dags", 于是从 sdist 装的人照抄首页命令得到 "dag 文件不存在", 而仓里
# 跑的人一切正常 (排除只作用于构建), 本地测试与 CI 一条都没红。见
# tests/test_sdist_keeps_referenced_examples.py。
# 两条判据在这里必须一致, 否则互相打脸 —— 上一版就是 `dags` 该不该排除的两种说法并存,
# 被本门禁抓出来才定案。
MUST_NOT_SHIP: tuple[str, ...] = ("research", "dags/research_dag.py")

TEXT_SUFFIXES = frozenset({
    ".py", ".md", ".sh", ".bash", ".toml", ".yml", ".yaml",
    ".cfg", ".ini", ".txt", ".json",
})


def _repo_root() -> pathlib.Path:
    p = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True)
    if p.returncode != 0:
        print("FAIL: 不在 git 仓里 —— 判据失明, 退出而不是恒过")
        raise SystemExit(1)
    return pathlib.Path(p.stdout.strip())


def _files(root: pathlib.Path) -> list[pathlib.Path]:
    # 与 check_text_integrity 同一套发现方式 (已跟踪 + 未跟踪未忽略): 只扫已跟踪的,
    # 那道门禁已经证明它在"新文件还没 git add"那一刻是瞎的, 这里不重复那个错。
    p = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=root, capture_output=True,
    )
    if p.returncode != 0:
        print("FAIL: `git ls-files` 失败 —— 判据失明")
        raise SystemExit(1)
    names = [n for n in p.stdout.decode("utf-8", "replace").split("\0") if n]
    return [root / n for n in names if (root / n).suffix in TEXT_SUFFIXES]


def _allowed(rel: str) -> str | None:
    for pattern, reason in ALLOWLIST:
        if fnmatch.fnmatch(rel, pattern):
            return reason
    return None


def main() -> int:
    if not TOKENS:
        print("FAIL: TOKENS 是空的 —— 判据恒过, 退出")
        return 1
    root = _repo_root()
    files = _files(root)
    if len(files) < 20:
        print(f"FAIL: 只扫到 {len(files)} 个文本文件 —— 判据失明, 不该绿")
        return 1

    hits: list[str] = []
    allowed_files: list[str] = []
    scanned = 0
    for f in files:
        try:
            text = f.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue                       # 非文本/非 utf-8 归 check_text_integrity 管
        rel = str(f.relative_to(root))
        reason = _allowed(rel)
        if reason:
            # ⚠️ **这里必须真的跳过**。第一版把 `reason = _allowed(rel)` 算出来却没用它,
            # 于是白名单是个**形同虚设的摆设** —— 30 处命中里没有一处因为登记而豁免,
            # 而报告看上去像是"已逐条登记过"。判据里挂一个不生效的例外比没有例外更坏:
            # 它让人以为那几处是被审过的。**例外要么生效, 要么就别写。**
            allowed_files.append(rel)
            continue
        scanned += 1
        low = text.lower()
        for tok in TOKENS:
            if tok.lower() not in low:
                continue
            for i, line in enumerate(text.splitlines(), 1):
                if tok.lower() in line.lower():
                    snippet = line.strip()
                    if len(snippet) > 70:
                        snippet = snippet[:67] + "..."
                    hits.append(f"{rel}:{i}: [{tok}] {snippet}")

    # 自检之二: **实际扫了几个**。上面那个 `len(files) < 20` 只查"文件找全了没有", 查不
    # 出"白名单把扫描面吃空了" —— 2026-10-17 变异验证实测: 把 ALLOWLIST 里某一条改成
    # 通配 `("*", ...)`, 结果是 **0 个文件被扫、150 个被豁免, 而门禁报 OK**。这正是本
    # 会话反复栽的"空集绿灯": 一个扫不到任何东西的判据, 报的是"一切正常"。
    # 例外是逐条登记的, 不是通配 —— 一条通配就足以让整道门禁变成空转。
    # 发布产物也不许带 —— 判据与 pyproject 的 sdist exclude 必须覆盖同一批路径,
    # 两边一旦漂移就报红 (少一个 exclude = 下一版又把它发出去)。
    import tomllib
    try:
        cfg = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
        excludes = cfg["tool"]["hatch"]["build"]["targets"]["sdist"]["exclude"]
    except (OSError, KeyError, TypeError) as e:
        print(f"FAIL: 读不到 pyproject 的 sdist exclude ({e}) —— "
              f"无法确认发布产物是否干净, 不该绿")
        return 1
    norm = {str(e).strip("/") for e in excludes}
    leaking = [d for d in MUST_NOT_SHIP if d not in norm]
    if leaking:
        print(f"FAIL: {leaking} 在 MUST_NOT_SHIP 里, 但 pyproject 的 "
              f"[tool.hatch.build.targets.sdist] exclude 没覆盖它们 (当前: {sorted(norm)})。"
              f"\n  hatchling 的 sdist 默认打包**全部 git 跟踪的文件** —— 不显式排除, "
              f"这些目录就会跟着发出去 (0.5.8 实测已经发出去过一次)。")
        return 1

    if scanned < 20:
        print(f"FAIL: 只实际扫了 {scanned} 个文件 (豁免 {len(allowed_files)} 个) —— "
              f"白名单把扫描面吃空了, 本判据已空转。"
              f"逐条登记是为了让豁免可审, 不是为了通配。")
        return 1

    if hits:
        print(f"FAIL: {len(hits)} 处未登记的业务值 (厂商模型名 / 消费方 key):\n")
        print("\n".join(hits))
        print("\npvos 不绑业务 (CLAUDE.md)。这类值要么换成中性占位符, 要么把该行移进 "
              "ALLOWLIST 并写明理由 —— 后者是给人看的决定, 不是扫到就放过。")
        return 1
    print(f"OK: {scanned} 个已扫文件均无未登记的业务值; "
          f"{len(allowed_files)} 个文件按登记豁免; "
          f"sdist exclude 已覆盖 {sorted(MUST_NOT_SHIP)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
