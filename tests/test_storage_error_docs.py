"""`docs/storage.md` 里那 5 条**错误输出快照**必须与实跑一致 —— 且类名错那条不许
把 import 进来的模块当候选类报出去.

起因 (2026-10-17 实测): 那一整块是手抄的实跑输出, **没有任何东西守着它**, 于是已经
烂了一条。文档写:

    # 3. class 名错
    PavozStorageError: storage 'pvos.storage:WrongClass' 类 'WrongClass' 不在模块
      'pvos.storage'. 模块导出: ['FileStorage', 'StorageBackend', 'key_sha256']

实跑是:

    模块导出: ['ABC', 'CorruptRecordError', 'FileStorage', 'StorageBackend',
               'abstractmethod', 'annotations', 'hashlib', 'json',
               'key_sha256', 'os']...

两处都不对: 文档**少了** `CorruptRecordError`, 而实跑**多出** 6 条 import 进来的
stdlib 名字 —— 消息把 `hashlib` / `os` / `json` / `ABC` / `abstractmethod` /
`annotations` 当成候选 storage 类报给用户。而这条消息唯一的作用就是**帮人挑一个正确
的类名**, 它列的 10 条里 6 条是噪声。

根因: 消息用的是 `[n for n in dir(module) if not n.startswith("_")]`, 而**模块自己
声明的公开面是 `__all__`** —— 有 `__all__` 就用它, 没有才退回 `dir()` 并滤掉模块对象
(那是噪声的主要来源)。顺带 `key_sha256` 本身是死代码: 全仓零调用、不属
`StorageBackend` 协议、不从包导出, 而它的 docstring 描述的 "内容寻址 sha256 key"
是**本库根本不用**的方案 (`FileStorage` 的 key 是 `<root>/<key>.json` 的可读层级
路径)。一条会误导人的死代码, 已删。

判据两条:
  · 文档里那 5 条快照**逐行**与实跑一致 (会烂的正是它们, 所以整块都钉, 不只烂的那条)
  · 类名错的消息**不得**出现模块对象 —— 钉住"报的是声明的公开面"这个性质, 而不是
    钉住当前那三个名字 (否则下次再加一个导出就得同步改测试, 而该改的是文档)
"""
from __future__ import annotations

import os
import pathlib
import re
import sys
from importlib import import_module

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_PKG = [d for d in os.listdir(_ROOT) if (_ROOT / d / "storage_loader.py").is_file()][0]
sys.path.insert(0, str(_ROOT))

_pavos = import_module(_PKG)
_STORAGE = f"{_PKG}.storage"
_DOC = _ROOT / "docs" / "storage.md"


def _cases():
    """(编号 → (说明, kwargs)) —— 每条都对应文档里的一个快照。"""
    return [
        (1, "spec 格式错", {}),
        (2, "module 未装", {}),
        (3, "class 名错", {}),
        (4, "kwargs 错", {}),
        (5, "类型错", {}),
    ]


def _spec_for(n: int) -> tuple[str, dict]:
    if n == 1:
        return "nocolon", {}
    if n == 2:
        return "foo.bar:Baz", {}
    if n == 3:
        return f"{_STORAGE}:WrongClass", {}
    if n == 4:
        # dotted 形式: 文档快照用的就是它, 且它与 colon 形式走的是另一条
        # 解析分支 (rpartition(".") vs rpartition(":")) —— 一并覆盖
        return f"{_STORAGE}.FileStorage", {"whatever": 1}
    return "builtins:int", {}


def _doc_samples() -> dict[int, list[str]]:
    """从 `错误信息` 标题下的那**一个**代码块里, 按 `# N. 说明` 切出各条的文本行。

    必须先定位到那个块再解析 —— docs/storage.md 上面还有另一个代码块, 它的示例注释
    也写成 `# 1. # 2. # 3.`。第一版扫全文, `setdefault` 让**前面那个块的 1/2/3 赢了**,
    于是一条快照都没真正对上过, 却报出"文档里有 5 条快照"。
    """
    lines = _DOC.read_text(encoding="utf-8").splitlines()
    start = next((i for i, l in enumerate(lines) if "错误信息" in l), None)
    assert start is not None, f"{_DOC.name} 里找不到 '错误信息' 标题 —— 文档结构变了"
    fence = next((i for i, l in enumerate(lines[start:], start) if l.lstrip().startswith("```")), None)
    assert fence is not None, f"{_DOC.name} 的 '错误信息' 段没有代码块"
    end = next((i for i, l in enumerate(lines[fence + 1:], fence + 1)
                if l.lstrip().startswith("```")), len(lines))

    out: dict[int, list[str]] = {}
    cur: int | None = None
    for line in lines[fence + 1:end]:
        m = re.match(r"^#\s*(\d+)\.\s", line)
        if m:
            cur = int(m.group(1))
            out.setdefault(cur, [])
            continue
        if cur is not None and line.strip():
            out[cur].append(line.strip())
    return out


def _real(n: int) -> str:
    spec, kw = _spec_for(n)
    try:
        _pavos.load_storage(spec, **kw)
    except Exception as e:                       # noqa: BLE001 — 要的就是异常文本
        return f"{type(e).__name__}: {e}"
    raise AssertionError(f"case {n} ({spec}) 本该报错, 却成功了 —— 文档快照与实现脱节")


def test_documented_samples_are_in_sync():
    """★ docs/storage.md 那 5 条快照逐条与实跑一致。"""
    samples = _doc_samples()
    assert set(samples) == {n for n, _, _ in _cases()}, (
        f"文档里的快照编号变了: {sorted(samples)} —— 判据与文档脱节了, "
        f"先确认文档改了哪一条")
    problems = []
    for n, desc, _ in _cases():
        want = " ".join(samples[n])
        got = _real(n)
        # 文档按宽度折行, 所以拿"拼起来后的整条"比
        if want not in got:
            problems.append(f"#{n} {desc}\n      文档: {want}\n      实跑: {got}")
    assert not problems, (
        "docs/storage.md 的错误输出快照与实跑不一致 —— 那块是手抄的, 没有东西守它, "
        "已经烂过一条。改文档 (不要改代码去迁就过期的快照):\n  "
        + "\n  ".join(problems))


def test_class_not_found_lists_declared_exports_not_modules():
    """★ 类名错的消息必须报**声明的公开面**, 不许把 import 的模块当候选类。"""
    msg = _real(3)
    listed = msg.split("模块导出: ", 1)[1]
    for noise in ("hashlib", "'os'", "'json'", "abstractmethod", "'ABC'", "annotations"):
        assert noise not in listed, (
            f"消息里出现了 {noise} —— 它是被 import 进模块的, 不是 storage 类。"
            f"这条消息唯一的作用是帮人挑类名, 噪声会把它淹掉。\n  实跑: {msg}")
    declared = getattr(import_module(_STORAGE), "__all__", [])
    for name in declared:
        assert repr(name) in listed or name in listed, (
            f"模块 __all__ 里声明了 {name}, 消息却没列 —— 该报的是声明的公开面")


def test_key_sha256_is_gone():
    """★ `key_sha256` 曾是死代码, 且它的 docstring 描述的是本库不用的 key 方案。

    留着它等于邀请用户采用一个 FileStorage 并不使用的 key 方案 (实际 key 是
    `<root>/<key>.json` 的可读层级路径)。判据钉住"别悄悄加回来"。
    """
    assert not hasattr(import_module(_STORAGE), "key_sha256"), (
        "key_sha256 已作为死代码删除 (全仓零调用 / 非协议成员 / 不从包导出)。"
        "若确有用途, 请连同调用点与文档一起加回来, 而不是只放一个孤立的函数。")
    assert "key_sha256" not in " ".join(
        l for v in _doc_samples().values() for l in v), (
        "文档快照里还留着 key_sha256 —— 它已经不导出了")


def test_sample_block_has_five_cases():
    """自检: 判据得先确认自己在扫东西 (快照块被删/改名时空扫 = 恒过)。"""
    samples = _doc_samples()
    assert len(samples) == 5, f"只解析到 {sorted(samples)} —— 判据失明"
    assert all(len(v) >= 1 for v in samples.values()), "有快照没有正文行"
