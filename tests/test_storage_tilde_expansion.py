"""storage 路径里的 `~` 必须展开 —— 否则数据静默落进 cwd 里的字面量 `~` 目录.

背景 (2026-10-17 实测): `FileStorage.__init__` 直接 `os.fspath(root_dir)`, 没有
expanduser。而 `examples/03_compare_diff.py` 的 docstring 正是教用户这么写
(`FileStorage('~/.pvos/data')`)。实测**两个入口**都中招:

    FileStorage("~/.pvos/data").root         → 字面量 '~/.pvos/data'
    PAVOZ_STORAGE='~/.pvos/data' pavos run   → cwd 里长出一棵字面量 `~` 目录树
    (cli.py 只 expand 了**默认值**那一支: `os.environ.get(spec, expanduser(...))`,
     env 一命中就是原样返回, 里面的 `~` 没人展开)

后果不是报错, 是**静默写到别处**: CLI 写 $HOME/.pvos/data, 读的一侧读
./~/.pvos/data。于是 `03_compare_diff.py` 对**刚跑完**的 task 报"该 task 现有的
run: (无)" —— 那个 example 的全部职责就是列 run, 它照文档用法必然读不到自己写的数据,
而且每读一次就在 cwd 留一棵垃圾目录。

现在展开落在 `FileStorage.__init__`: 路径的**唯一**汇合点 (CLI 的 `_storage` 也走
FileStorage), 放这一处两个入口一起修好。
"""
from __future__ import annotations

from pavoz import FileStorage
from pavoz import cli


def test_file_storage_expands_leading_tilde(tmp_path, monkeypatch):
    """库 API 直传 `'~/...'`: 展开到 home, 且真的建在 home 下。"""
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.chdir(tmp_path)          # cwd 里若长出 `~` 就是没展开

    fs = FileStorage("~/.pvos/data")

    assert fs.root == str(home / ".pvos" / "data"), f"未展开: {fs.root!r}"
    assert "~" not in fs.root
    assert (home / ".pvos" / "data").is_dir(), "展开后就该建在 home 下"
    assert not (tmp_path / "~").exists(), "cwd 里不该出现字面量 `~` 目录"


def test_cli_env_var_tilde_expands_and_leaves_no_litter(tmp_path, monkeypatch):
    """`PAVOZ_STORAGE='~/...'` 那条路也必须展开 (不是只修库 API 就完事)。

    `cli.DEFAULT_STORAGE` 是 **import 时**求值的 `os.environ.get("PAVOZ_STORAGE",
    expanduser(默认值))`, 所以 env 命中那一支存下来的就是**没展开**的字符串 ——
    直接把它替成 `'~/data'` 就是那个状态的忠实复现 (真跑一次 CLI 当然也行, 只是慢)。
    """
    home = tmp_path / "home"
    home.mkdir()
    work = tmp_path / "work"
    work.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setattr(cli, "DEFAULT_STORAGE", "~/data")
    monkeypatch.chdir(work)

    fs = cli._storage()

    assert fs.root == str(home / "data"), f"env 里的 ~ 没展开: {fs.root!r}"
    assert not (work / "~").exists(), (
        f"cwd 里出现了字面量 `~` 目录: "
        f"{[p.name for p in work.iterdir()]} —— 数据静默落错地方")


def test_only_leading_tilde_is_touched(tmp_path, monkeypatch):
    """只动**行首**的 ~ (与 shell 一致): 绝对/相对路径与路径中间的 ~ 一律不变。

    这一条钉住"纯修正": 展开不能顺手改掉既有解析语义 (否则 `a/~/b` 会被拉去 home,
    那是新行为而不是修 bug)。

    ⚠️ chdir 到 tmp_path: `FileStorage.__init__` 会 makedirs, 不挪开就在**仓根**造出
    `rel/` 与字面量 `~/` 两棵垃圾树 (2026-10-17 第一版判据真造出来了 —— 而那恰好
    复现了本文件要修的 bug 的现场形态)。
    """
    monkeypatch.chdir(tmp_path)
    cases = {
        str(tmp_path / "abs"): str(tmp_path / "abs"),   # 绝对路径
        "rel/dir": "rel/dir",                          # 相对路径
        str(tmp_path / "a~b"): str(tmp_path / "a~b"),   # 路径**中间**有 ~
        "./~/x": "./~/x",                              # `~` 不在行首
    }
    for given, expected in cases.items():
        fs = FileStorage(given)
        assert fs.root == expected, f"{given!r} 被改成了 {fs.root!r}"
