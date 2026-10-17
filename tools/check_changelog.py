"""CI guard: 已发布的 CHANGELOG 段落一旦 tag 落地就**不许再改**.

起因 (2026-10-17 实测): 本会话往 CHANGELOG 里追加条目时, 锚点用的是"文件里第一个
`### Fixed`", 而那个小节**在 0.5.7 已发布段内**。于是 v0.5.7 tag 之后的 12 个提交,
把 214 行、10 条本该属于下一版的条目全写进了**已发布**的 0.5.7 段 —— 读 0.5.7 发布
说明的人会以为那些修复随 0.5.7 一起发布了, 而 `[Unreleased]` 一直写着"(空)"。

这个错没有任何现有门禁看得见, 因为它在 git 层面完全合法 (就是普通的一次文本编辑)。
本判据用**可判定的事实**把它挡住: 已发布段的内容不许在发布之后再变。

三档判定 (关键是别让历史遗留把门禁焊死 —— 恒红的门禁迟早被关掉, 那比没有更坏):

  1. 与自己的 tag 一致                    → OK
  2. 与自己的 tag 不一致, 但与**最新 tag** 里的同一段一致 → 历史遗留, 只提示。
     (本仓实测有 3 段如此: [0.5.3] / [0.1.3] 的段标题在发布后被改过,
      [0.3.0-rc1] 的段在发布后被删过 9 行 —— 都不是本判据引入的。)
  3. 连最新 tag 都对不上                   → **失败**: 这是发布之后又改了已发布段。

Usage:  python tools/check_changelog.py     (exit 0 = 已发布段与 tag 一致)
"""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys

CHANGELOG = pathlib.Path("CHANGELOG.md")
# `## [0.5.7] — 2026-10-17` / `## [0.5.7]` / `## [Unreleased]`
HEADING = re.compile(r"^## \[(?P<ver>[^\]]+)\]")


def sections(text: str) -> dict[str, str]:
    """版本 → 该段的完整文本 (含 `## ` 标题行, 到下一个 `## ` 之前)。

    重名时后段覆盖前段 —— 但那**不是**"重名没关系", 而是 `duplicate_versions()` 会
    先把它判失败。留这个行为是为了让 sections() 保持纯函数; 判失败在 main() 里做。
    """
    out: dict[str, str] = {}
    cur: list[str] | None = None
    for line in text.splitlines(keepends=True):
        m = HEADING.match(line)
        if m:
            cur = [line]
            out[m.group("ver")] = ""
            continue
        if cur is not None:
            cur.append(line)
            out[HEADING.match(cur[0]).group("ver")] = "".join(cur)
    return out


def duplicate_versions(text: str) -> dict[str, list[int]]:
    """版本 → 它出现过的行号 (只含出现 >1 次的)。

    起因 (2026-10-17 实测): 本仓的 CHANGELOG 里有**两个** `## [0.8.0] — 2026-09-06`
    (同日期同版本, 一次发版被误拆成两段: 一段 Added, 一段 Changed/Fixed/Chore)。而
    `sections()` 重名时"后段覆盖前段", 于是**第一段 0.8.0 从来没被本判据看过** ——
    判据对它完全失明, 而且失明得毫无征兆: 12 个段"已核对", 全绿。

    这比"CHANGELOG 排版乱"严重: 一段内容能躲开门禁, 就等于那段内容可以是任意陈旧
    的、错的、或重复的, 而门禁报告"一切正常"。所以重名必须**显式判失败**, 而不是
    悄悄挑一个赢。
    """
    seen: dict[str, list[int]] = {}
    for i, line in enumerate(text.splitlines(), 1):
        m = HEADING.match(line)
        if m:
            seen.setdefault(m.group("ver"), []).append(i)
    return {v: ls for v, ls in seen.items() if len(ls) > 1}


def newest_tag() -> str | None:
    """最新一个 tag (按版本号排, 不是按创建时间 —— tag 时间会乱)。"""
    p = subprocess.run(["git", "tag", "-l", "v*"], capture_output=True, text=True)
    tags = [t for t in p.stdout.split() if re.fullmatch(r"v\d+\.\d+\.\d+.*", t)]
    if not tags:
        return None

    def key(t: str) -> tuple:
        core = t[1:].split("-")[0]
        return tuple(int(x) for x in core.split("."))

    return sorted(tags, key=key)[-1]


def git_show(ref: str) -> str | None:
    p = subprocess.run(["git", "show", f"{ref}:{CHANGELOG}"],
                       capture_output=True, text=True)
    return p.stdout if p.returncode == 0 else None


def diff_summary(a: str, b: str, limit: int = 6) -> list[str]:
    """两段的第一处不同 + 之后若干行, 让人能直接定位。"""
    import difflib
    al, bl = a.splitlines(), b.splitlines()
    out = []
    for line in list(difflib.unified_diff(bl, al, "tag", "工作区", n=1, lineterm=""))[:limit]:
        out.append("    " + line)
    return out


def main() -> int:
    if not CHANGELOG.is_file():
        print(f"FAIL: 找不到 {CHANGELOG}")
        return 1
    raw = CHANGELOG.read_text(encoding="utf-8")

    # 重名版本段: 必须在**任何比对之前**判失败。放在后面等于没放 —— sections() 已经
    # 挑了一个赢家, 报告会像模像样地说"12 个段已核对", 而被盖掉那一段从未被看过。
    dups = duplicate_versions(raw)
    if dups:
        for ver, lines in sorted(dups.items()):
            print(f"FAIL: 版本段 [{ver}] 出现 {len(lines)} 次 (行 {', '.join(map(str, lines))})"
                  f" —— 本判据按版本号取段, 重名时**只有最后一段会被核对**, 其余的"
                  f"内容对门禁完全不存在。同一次发版不该拆成两段, 请并成一段。")
        return 1

    work = sections(raw)

    newest = newest_tag()
    newest_txt = git_show(newest) if newest else None
    newest_sec = sections(newest_txt) if newest_txt else {}

    problems: list[str] = []
    notes: list[str] = []
    compared = 0
    for ver, cur in work.items():
        if ver.lower() == "unreleased":
            continue
        tagged = git_show(f"v{ver}")
        if tagged is None:
            # 措辞要分清: "tag 没拉到" 与 "段被改名了" 是两回事, 混成一句会让人
            # 去查一个根本不存在的问题。
            notes.append(f"[{ver}] 本地没有 v{ver} 这个 tag (未发布 / 未 fetch) —— "
                         f"跳过比对")
            continue
        own = sections(tagged).get(ver)
        if own is None:
            notes.append(f"[{ver}] v{ver} 处的 CHANGELOG 里没有这一段 "
                         f"(标题发布后被改过) —— 既成事实, 不追溯")
        elif own == cur:
            pass
        elif newest_sec.get(ver) == cur:
            notes.append(f"[{ver}] 与 v{ver} 不一致, 但与最新 tag ({newest}) 一致 "
                         f"—— 历史遗留, 不追溯")
        else:
            problems.append(
                f"[{ver}] 已发布段在 {newest} 之后又被改了。已发布的段落是**历史**, "
                f"改了它等于把本次的修复说成随该版本发布了:\n"
                + "\n".join(diff_summary(newest_sec.get(ver, ""), cur))
                + "\n    → 新条目请写进 [Unreleased]")
        compared += 1

    # 自检: 一个段都没走到判定 = 本判据空转 (tag 没拉到 / 全是 Unreleased),
    # 那它就永远绿 —— 恒过的判据比没有判据更坏, 所以显式判失败。
    if compared == 0 or newest is None:
        print("FAIL: 拿不到 tag, 没法判定 —— `git fetch --tags`。"
              "否则本判据恒过, 等于没有")
        return 1

    for n in notes:
        print(f"  提示 (不判失败): {n}")
    if problems:
        print("\n".join(problems))
        return 1
    print(f"OK: {compared} 个已发布段均未被发布后修改 (最新 tag: {newest})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
