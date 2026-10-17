"""FileStorage.put 必须**原子** —— 写失败不能毁掉上一份 checkpoint.

起因 (2026-10-17 实测): `put` 原来是 `open(path, "w")` + `json.dump`, 即**先截断
再写**。而 checkpoint 正是 run 崩溃后**续跑的唯一凭据**。写到一半进程被杀时 (SIGKILL,
实测 3/3), 磁盘上留下的既不是旧的也不是新的:

    写到一半被杀 → 12-17 MB 的半截 JSON (完整应有 60 MB), `get` 直接 JSONDecodeError;

更狠的是**连崩溃前那份本来合法的 checkpoint 也一起没了** —— 实测先写好一份 825 字节
的完整 checkpoint, 再跑一个大 run 写到一半 SIGKILL, 那个文件变成 17 MB 半截。于是
"崩溃后能 resume"这件事被崩溃**本身**毁了, 而这恰是这个库最核心的承诺。

判据用**确定性的崩溃模拟**而不是真 SIGKILL: 后者依赖时序, 在 CI 上是 flaky 的来源。
做法是把 `json.dump` 换成一个"写一半就抛 KeyboardInterrupt"的函数。用
`KeyboardInterrupt` 而不是普通异常还顺带钉住一件事: 清理必须 catch
`BaseException` 而不是 `Exception` —— KeyboardInterrupt / SystemExit /
asyncio.CancelledError 都是 BaseException, 只 catch Exception 的清理会漏掉它们。
"""
from __future__ import annotations

import json
import os

import pytest

from pavoz import FileStorage


class _Crash:
    """可控的"写一半就崩溃"开关。

    ⚠️ 必须**显式上膛**而不是 fixture 一上来就换掉 `json.dump`: 那个替换是**全局**的
    (storage 里就是 `import json` 后调 `json.dump`), 于是测试自己那次"先写好一份合法
    checkpoint"的前置步骤也会跟着崩 —— 第一版就是这么写的, 结果 5 条测试全部在 setup
    阶段就 KeyboardInterrupt, 一条都没跑起来。
    """

    def __init__(self) -> None:
        self.armed = False
        self.calls = 0

    def arm(self) -> None:
        self.armed = True

    def dump(self, obj, fp, **kwargs) -> None:
        self.calls += 1
        fp.write('{"stage": "f1", "blo')       # 写了个开头就死
        raise KeyboardInterrupt("模拟崩溃")


@pytest.fixture
def crash_mid_dump(monkeypatch):
    crash = _Crash()
    real = json.dump

    def dispatch(obj, fp, **kwargs):
        if crash.armed:
            return crash.dump(obj, fp, **kwargs)
        return real(obj, fp, **kwargs)

    monkeypatch.setattr(json, "dump", dispatch)
    return crash


def _files(root):
    return sorted(p.name for p in root.rglob("*") if p.is_file())


def test_crash_mid_write_leaves_previous_checkpoint_intact(tmp_path, crash_mid_dump):
    """★ 崩溃时磁盘上必须还是**上一份** checkpoint —— 这是 resume 能成立的前提。"""
    fs = FileStorage(str(tmp_path))
    fs.put("runs/t/cp", {"stage": "f0", "blob": ["good"] * 10})
    assert fs.get("runs/t/cp")["stage"] == "f0", "前置: 先写好一份合法的"

    crash_mid_dump.arm()
    with pytest.raises(KeyboardInterrupt):
        fs.put("runs/t/cp", {"stage": "f1", "blob": ["x" * 1000]})

    got = fs.get("runs/t/cp")          # 修前: JSONDecodeError
    assert got == {"stage": "f0", "blob": ["good"] * 10}, (
        f"崩溃把上一份 checkpoint 毁了, 现在读到 {got!r}")


def test_failed_write_leaves_no_temp_file_behind(tmp_path, crash_mid_dump):
    """失败必须把临时文件收掉 —— 否则每失败一次就在数据目录里留一个垃圾。"""
    fs = FileStorage(str(tmp_path))
    fs.put("runs/t/cp", {"stage": "f0"})
    crash_mid_dump.arm()
    with pytest.raises(KeyboardInterrupt):
        fs.put("runs/t/cp", {"stage": "f1", "blob": ["x" * 100]})
    assert _files(tmp_path) == ["cp.json"], (
        f"崩溃后残留了临时文件: {_files(tmp_path)}")


def test_temp_file_is_never_visible_to_list_keys(tmp_path, monkeypatch):
    """临时文件**不能带 `.json` 后缀** —— `list_keys` 靠 `*.json` 收集 key。

    带后缀的话, 崩溃残留的临时文件会变成一个谁都读不到的假 key (出现在 trace /
    state 列表里, 但 `get` 读不出来)。
    """
    seen: dict[str, str] = {}
    real_replace = os.replace

    def spy(src, dst):
        seen["name"] = src
        return real_replace(src, dst)

    monkeypatch.setattr("os.replace", spy)
    fs = FileStorage(str(tmp_path))
    fs.put("runs/t/cp", {"stage": "f0"})

    assert seen.get("name"), (
        "整个 put 里根本没调用过 os.replace —— 现在不是原子写 "
        "(要么退回了 open(path,'w'), 要么换名那一步被拆了)")
    tmp_name = seen["name"].rsplit("/", 1)[-1]
    assert not tmp_name.endswith(".json"), (
        f"临时文件 {tmp_name!r} 带 .json 后缀 —— list_keys 会把它当成一个假 key")
    assert fs.list_keys("runs") == ["runs/t/cp"], (
        f"list_keys 看到了不该有的 key: {fs.list_keys('runs')}")


def test_successful_put_is_atomic_and_readable(tmp_path):
    """对照: 正常写仍然工作, 且不留任何临时文件。"""
    fs = FileStorage(str(tmp_path))
    payload = {"stage": "f3", "blob": ["y" * 500], "n": 7}
    fs.put("runs/t/cp", payload)
    assert fs.get("runs/t/cp") == payload
    assert fs.list_keys("runs") == ["runs/t/cp"]
    assert _files(tmp_path) == ["cp.json"], f"正常写不该留临时文件: {_files(tmp_path)}"


def test_overwrite_replaces_atomically(tmp_path):
    """覆盖写: 新值完整可见, 且旧值不会半残。"""
    fs = FileStorage(str(tmp_path))
    fs.put("k", {"v": list(range(1000))})
    fs.put("k", {"v": list(range(2000, 3000))})
    assert fs.get("k") == {"v": list(range(2000, 3000))}
    assert _files(tmp_path) == ["k.json"]
