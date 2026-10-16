"""FileStorage 必须把 key 关在 root 内 — 路径穿越防护.

背景: 2026-10 审计发现 `_path` 只有字符集过滤 (`[a-z0-9_/.-]`), 而 `.` 在允许
集内, 所以 `..` 段畅通。实测 (修复前):

    fs.get("../../xxx/secret")     → 读出 root 之外的文件
    fs.delete("../../xxx/secret")  → 删掉 root 之外的文件

`put` 当时看似被挡住, 但那是 os.makedirs 逐段创建时撞上字面量 `..` 目录而**偶然**
失败 (PermissionError), 不是防护 —— 且只挡 put, 挡不住 get/delete。

现在 `_path` 把路径解析成 realpath 后断言仍落在 root 内, 对三个操作一视同仁。
本测试用真实 FileStorage 验证越界被拒, 且不依赖"文件系统恰好报错"。
"""
from __future__ import annotations

import os

import pytest

from pavoz import DAG, CheckpointStore, FileStorage, Runtime


@pytest.fixture
def roots(tmp_path):
    """(storage_root, root 之外的受害目录, 受害文件路径)。"""
    root = tmp_path / "store"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = outside / "secret.json"
    victim.write_text('{"stolen": "top-secret"}', encoding="utf-8")
    return root, outside, victim


def _escaping_key(root, victim) -> str:
    """构造一个从 storage root 出发能走到受害文件的 key (去掉 .json 后缀)。"""
    rel = os.path.relpath(str(victim), str(root))
    return rel[: -len(".json")]


def test_get_cannot_read_outside_root(roots):
    root, _outside, victim = roots
    fs = FileStorage(str(root))
    with pytest.raises(ValueError, match="越界"):
        fs.get(_escaping_key(root, victim))


def test_delete_cannot_remove_outside_root(roots):
    root, _outside, victim = roots
    fs = FileStorage(str(root))
    with pytest.raises(ValueError, match="越界"):
        fs.delete(_escaping_key(root, victim))
    assert victim.exists(), "越界 delete 必须被拒, 受害文件仍在"


def test_put_cannot_write_outside_root(roots):
    root, outside, _victim = roots
    fs = FileStorage(str(root))
    rel = os.path.relpath(str(outside / "new.json"), str(root))[: -len(".json")]
    with pytest.raises(ValueError, match="越界"):
        fs.put(rel, {"x": 1})
    assert not (outside / "new.json").exists()


def test_list_keys_cannot_walk_outside_root(roots):
    root, _outside, victim = roots
    fs = FileStorage(str(root))
    with pytest.raises(ValueError, match="越界"):
        fs.list_keys(_escaping_key(root, victim))


def test_dotdot_task_id_cannot_escape_via_checkpoint_store(roots):
    """端到端: task_id='..' 经 CheckpointStore 落盘, 不得写到 root 之外。

    实测修复前 task_id='..' 会让 key 变成 runs/../<run_id>/checkpoint。
    """
    import asyncio

    from pavoz import DAG, CheckpointStore, Runtime

    root, outside, _victim = roots
    dag = DAG("t")

    @dag.stage()
    async def s_a(ctx):
        return {"x": 1}

    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(str(root))))
    r = asyncio.run(rt.run(dag, task_id=".."))
    assert r.status == "done"
    # 一切仍在 root 内
    assert all(str(root) in str(p) for p in root.rglob("*.json"))


def test_normal_keys_still_work(tmp_path):
    """防护不能误伤正常 key (含 runs/ 前缀的多段 key)。"""
    root = tmp_path / "store"
    fs = FileStorage(str(root))
    key = "runs/t1/abc123/checkpoint"
    fs.put(key, {"ok": True})
    assert fs.get(key) == {"ok": True}
    assert key in fs.list_keys("runs/t1")
    fs.delete(key)
    assert fs.get(key) is None


def test_illegal_character_still_rejected(tmp_path):
    root = tmp_path / "store"
    fs = FileStorage(str(root))
    with pytest.raises(ValueError, match="非法 storage key"):
        fs.get("runs/t1/evil key")


# ── 字符集过滤的**真实**边界 (2026-10-16 巡检) ──────────────────────────────
#
# `_path` 的注释曾写 "key 只允许 [a-z0-9_/.-]", 读起来像白名单; 实际
# `str.isalnum()` 对大写与全部 Unicode 字母/数字都成立。安全上无害 (真正的边界
# 是 realpath containment), 但注释误导。下面把**实测到的**边界钉住, 免得注释
# 再漂回去, 也免得有人以为 Unicode key 会被拒而绕路。

@pytest.mark.parametrize("key", [
    "plain", "UPPER", "MixedCase99",
    "任务",                      # 中文
    "café",                 # 带重音拉丁
    "٣",                      # 阿拉伯-印度数字
    "with-dash", "with_underscore", "with/slash",
])
def test_charset_filter_accepts_unicode_and_uppercase(key, tmp_path):
    """字符集过滤**不是**白名单 —— 大写与 Unicode 都放行 (实测边界)。"""
    fs = FileStorage(tmp_path)
    fs.put(key, {"v": 1})
    assert fs.get(key) == {"v": 1}


@pytest.mark.parametrize("key", [
    "with space", "tab\tkey", "C:\\windows", "star*", "quote'key",
])
def test_charset_filter_still_rejects_obvious_non_keys(key, tmp_path):
    """它确实挡什么: 空格 / 控制符 / 盘符 / 通配符 / 引号。"""
    fs = FileStorage(tmp_path)
    with pytest.raises(ValueError, match="非法 storage key"):
        fs.put(key, {"v": 1})


async def test_task_id_is_the_place_that_enforces_ascii_only(tmp_path):
    """task_id 侧才是 ASCII-only (它把键当**身份**用), storage key 侧不是 ——
    两处口径不同是有意的, 别把它们混成一处。pytest 走 asyncio auto 模式。"""
    dag = DAG("ascii_task_id")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage(tmp_path)))
    with pytest.raises(ValueError, match="非法字符"):
        await rt.run(dag, "任务-1")            # task_id 收 Unicode → 拒
    assert (await rt.run(dag, "task-1")).status == "done"


# ── symlink 逃逸: realpath 防护的**核心价值**, 此前完全没被钉住 ───────────
#
# 上面那批用例全用字面 `..` 穿越。它们只能证明"路径里有 `..` 时被拒", 证明不了
# **realpath 本身**在起作用 —— 因为把 `_path` 里的 `os.path.realpath` 换成
# `os.path.abspath`, 上面每一个都照样绿 (abspath 只是纯词法拼接, 不解析 symlink)。
#
# 而 symlink 正是"纯字符串前缀检查"和"realpath containment"的唯一分水岭: 攻击者
# 在 root 内放一个指向外部的 symlink, 词法路径 `root/escape/secret.json` 看着在
# root 内, 真实路径在 root 外。realpath 会解析它并拦下; abspath 不会。
#
# 这不是假想威胁: storage root 一般由部署方创建, 而 stage 的 task_id / 外部调用
# 传进来的 key 若能控制 root 内的某个文件名, 就能借已有的 symlink 把数据写到
# 别处。本机攻击者本来不需要这个漏洞, 但"CI 上跑测试"的场景里, 被测代码能碰
# storage 目录。

def test_symlink_inside_root_cannot_escape(roots):
    """★ root 内的 symlink 指向外部 → 读写都必须被拒。

    判别力: 把 `_path` 的 `realpath` 换成 `abspath`, 本用例转红, 上面那批全绿。
    """
    root, outside, victim = roots
    (root / "escape").symlink_to(outside, target_is_directory=True)
    fs = FileStorage(str(root))

    with pytest.raises(ValueError, match="越界"):
        fs.get("escape/secret")
    with pytest.raises(ValueError, match="越界"):
        fs.put("escape/stolen", {"x": 1})
    with pytest.raises(ValueError, match="越界"):
        fs.delete("escape/secret")

    assert victim.read_text(encoding="utf-8") == '{"stolen": "top-secret"}', (
        "受害文件必须原封不动")


def test_symlink_to_file_cannot_escape(roots):
    """指向单个文件的 symlink (非目录) 也要拦 —— 形态不同, realpath 行为一致。"""
    root, outside, victim = roots
    (root / "link.json").symlink_to(victim)
    fs = FileStorage(str(root))

    with pytest.raises(ValueError, match="越界"):
        fs.get("link")
    with pytest.raises(ValueError, match="越界"):
        fs.delete("link")
    assert victim.exists()


@pytest.mark.parametrize("key", [
    "a/../../escape/secret",      # 中间目录跳出
    "./../../outside/secret",     # 前缀 ./
    "sub/./../../outside/secret",  # 中间的 ./
    "UPPER/../../outside/secret",  # 大写段照样能跳 (字符集放行大写)
    "任务/../../outside/secret",    # Unicode 段也照样能跳 (字符集放行 Unicode)
])
def test_multi_segment_traversal_variants_are_rejected(roots, key):
    """各种拼法的 `..` 穿越 —— 字符集放行的那两类 (大写/Unicode) 同样要拦。

    字符集层放行大写与 Unicode 是**有意的** (真正的边界在 realpath), 所以必须
    有一批用例证明: 那些"看起来该被字符集挡掉"的 key, 确实是被**第二道**挡住的。
    否则日后有人收紧字符集 (看起来是加固), 会误以为第二道没被测过。
    """
    root, _outside, victim = roots
    fs = FileStorage(str(root))
    with pytest.raises(ValueError, match="越界"):
        fs.get(key)
    assert victim.exists()


def test_nul_byte_is_rejected_by_charset_layer(roots):
    """NUL 字节: 字符集第一道就该拒 (realpath 对含 NUL 的路径会抛 ValueError,
    但那时已经进了 os 层 —— 判据要明确它是被**第一道**拦的, 而不是碰巧没炸)。"""
    root, _outside, _victim = roots
    fs = FileStorage(str(root))
    with pytest.raises(ValueError, match="非法 storage key"):
        fs.get("with\x00nul")


@pytest.mark.parametrize("key,stays_inside", [
    ("..", True),                 # → root/...json (文件名是三个点), 在 root 内
    ("a/../..", True),            # → root/a/...json, 在 root 内
    ("任务/秘密", True),           # → root/任务/秘密.json, 在 root 内
    ("UPPER/../outside/secret", True),   # 归一化后 = root/outside/secret, 在 root 内
])
def test_keys_that_look_like_traversal_but_land_inside_are_allowed(
        roots, key, stays_inside):
    """★ 反向约束: **长得像穿越但实际落在 root 内**的 key 仍要**放行**。

    真实边界是 realpath containment, 不是"字符串里有没有 `..`"。若改成见 `..`
    就拒, 会误伤合法的相对路径 key (`任务/秘密` 这类 Unicode key 是
    CheckpointStore 的日常产物)。这条把"边界在哪"钉死, 免得日后有人
    "顺手收紧"。

    判别力: 变异成"含 `..` 即拒" → 本用例转红。
    """
    root, _outside, _victim = roots
    fs = FileStorage(str(root))
    fs.put(key, {"ok": True})
    assert fs.get(key) == {"ok": True}
