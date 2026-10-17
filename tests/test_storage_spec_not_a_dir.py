"""`PAVOZ_STORAGE` 收下一个 **storage spec** 时, 必须报错 —— 而不是建出那个名字的目录.

起因 (2026-10-17 实测, 而且是先咬了我自己): 两个环境变量只差一个词 ——
`PAVOZ_STORAGE` 收**目录**, `PAVOZ_STORAGE_SPEC` 收 **spec**。写错的后果实测是:

    $ PAVOZ_STORAGE='pavos.storage:FileStorage' pavos trace --task-id t1
    task t1 无 checkpoint (没跑过)          # rc=0, stderr 一个字没有
    $ ls
    pavos.storage:FileStorage/              # ← cwd 里长出来的目录

也就是说**没有任何提示**, 而之后所有 checkpoint 都静默写进那个没人会去看的地方。
根因是 `FileStorage.__init__` 里那句 `os.makedirs(self.root, exist_ok=True)` **对来者
不拒** —— 它在用户看到任何提示之前就把目录建好了。本会话自己被咬过一次:
仓根躺了好几轮才被我发现一个空的 `pavos.storage:WrongClass/`。

修法: 在 `FileStorage.__init__` 里 (路径的**唯一**汇合点, CLI 与库 API 一起修好),
凡形如 `pkg.mod:Class` 的根目录直接拒绝, 消息说清两个变量各自收什么。识别刻意很窄:
必须同时含 '.' 与 ':' 且冒号后是合法标识符 —— 相对路径 './data'、绝对路径
'/var/lib/x'、带点的目录名 'data.v2'、带冒号的 'weird:name' **都不该被误伤**, 所以
下面专门有一条判据钉住这四类。

判据形态与本会话其它几条一致: **两侧都钉**, 并且额外钉住"**什么都没发生**" ——
只断言"报错了"是不够的, 真正的危害是目录被建出来。
"""
from __future__ import annotations

import os
import pathlib
import subprocess
import sys
from importlib import import_module

import pytest

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_PKG = [d for d in os.listdir(_ROOT) if (_ROOT / d / "storage.py").is_file()][0]
sys.path.insert(0, str(_ROOT))

_storage = import_module(f"{_PKG}.storage")

# 这些**必须**被当成正常目录接受 —— 识别规则太宽就会打断真实用户。
# 分两组是因为它们走的**不是同一条代码路径**:
#   · 裸名组: 真的会去正则, 所以它测的是识别的**宽窄** (第一版的判据只传了绝对路径,
#     `"/" not in self.root` 那半先把值全放行, 于是把点号段改成可选也全绿 —— 判据
#     没覆盖它声称覆盖的范围)。
#   · 路径组: 带 '/' , 被 guard 直接放行, 永远走不到正则。
LEGIT_BARE_DIRS = [
    "./data",
    "data.v2",              # 带点, 无冒号
    "weird:name",           # 带冒号, 无点
    "plain",                # 什么都不带
]


def test_spec_shaped_root_is_rejected(tmp_path, monkeypatch):
    """★ `pkg.mod:Class` 形状的根目录 → 报错, 且**目录不许被建出来**。"""
    # 用**相对**的裸 spec —— 这是真实世界的形状 (把 PAVOZ_STORAGE_SPEC 的值照抄进
    # PAVOZ_STORAGE)。带 '/' 的路径不在拒绝范围内: 那是一个路径, 目录名叫成什么样是
    # 用户自己的事 (见 FileStorage.__init__ 里那段说明)。
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError) as ei:
        _storage.FileStorage("pvos.storage:FileStorage")
    fake = tmp_path / "pvos.storage:FileStorage"
    msg = str(ei.value)
    assert "PAVOZ_STORAGE" in msg and "load_storage" in msg, (
        f"消息要同时点出两个变量各自收什么: {msg}")
    assert not fake.exists(), (
        "目录在建出来之后才报错是不够的 —— 真正的危害是那个目录留在磁盘上, "
        "之后所有数据静默写进去")


@pytest.mark.parametrize("name", LEGIT_BARE_DIRS)
def test_legit_bare_directory_names_still_accepted(tmp_path, name, monkeypatch):
    """★ 反面: 正常**裸目录名**不许被误伤 (太宽的识别规则会打断真实用户)。

    必须在 tmp 里跑并传裸名 —— 见上面 LEGIT_BARE_DIRS 的注释: 第一版传绝对路径,
    `"/" not in self.root` 那半会先放行, 正则的宽窄根本没被测到。
    """
    monkeypatch.chdir(tmp_path)
    assert _storage.FileStorage(name).root == name
    assert (tmp_path / name.lstrip("./")).is_dir()


@pytest.mark.parametrize("sub", ["sub/data", "sub/data.v2"])
def test_legit_path_shaped_names_still_accepted(tmp_path, sub):
    """路径形 (带 '/') 直接被 guard 放行, 走不到正则。"""
    target = tmp_path / sub
    assert _storage.FileStorage(str(target)).root == str(target)
    assert target.is_dir()


def test_spec_shaped_name_inside_a_path_is_still_allowed(tmp_path):
    """★ 刻意边界: 只有**裸** spec token 被拒; 带路径分隔符的放行。

    `"/var/data/pkg.mod:Class"` 是一个**路径** —— 目录名叫成什么样是用户自己的事, 替他
    决定会打断真实部署。这条边界此前没有任何判据覆盖, 于是"去掉 '/ not in self.root'
    那半"这个变异是**全绿**的 (2026-10-17 实测)。
    """
    target = tmp_path / "pkg.mod:Class"
    assert _storage.FileStorage(str(target)).root == str(target)
    assert target.is_dir()


def test_spec_pattern_boundaries():
    """自检: 识别规则本身 —— 必须认得出 spec, 且认不出上面那四类正常路径。

    这一条是为了防两个方向的退化: 规则**太宽** (打断正常用户) 与**太窄** (spec 漏网,
    于是判据自己绿着而缺陷还在)。
    """
    import re
    pat = re.compile(
        r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+:[A-Za-z_][A-Za-z0-9_]*$")
    for spec in ("pvos.storage:FileStorage", "a.b:C", "pkg.mod.sub:MyStore"):
        assert pat.match(spec), f"规则认不出合法 spec {spec!r}"
    for legit in (*LEGIT_BARE_DIRS, "/var/lib/pvos"):
        assert not pat.match(legit), f"规则会误伤正常目录名 {legit!r}"
    # 还有两个易漏的: 带空格的 spec (load_storage 允许吗? 不允许, 但别误伤)
    assert not pat.match("my data")


def _cli_env(**extra) -> dict:
    """子进程环境 —— **必须**把 PYTHONPATH 钉到本仓。

    否则会测到**别的** pavos: 本仓的 venv 里有 `_editable_impl_*.pth`, 它把仓库根加进
    sys.path, 于是 `python -m <pkg>` 加载的是那棵树, 与 cwd 无关。实测 (2026-10-17):
    在一份临时副本里跑本文件, 把 CLI 里"预期内参数错转 SystemExit"那段撤掉后 ——
    **9 条全绿**。因为子进程加载的是真仓那份**未变异**的代码, 撤掉的那段根本没参与运行。
    也就是说那条变异是被"判据没测到它要测的东西"挡住的, 不是被行为挡住的。

    与本会话其它判据同源的一类问题: **判据必须自己保证它测的是被测对象**。
    """
    env = dict(os.environ)
    env["PYTHONPATH"] = str(_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    env.update(extra)
    return env


def test_cli_reports_it_cleanly(tmp_path):
    """★ 经真 CLI: 一行说清 + rc=1, 且 **cwd 里没有多出任何东西**。"""
    work = tmp_path / "cwd"
    work.mkdir()
    env = _cli_env(PAVOZ_STORAGE=f"{_PKG}.storage:FileStorage")
    p = subprocess.run(
        [sys.executable, "-m", _PKG, "trace", "--task-id", "t1"],
        capture_output=True, text=True, cwd=work, env=env, timeout=90,
    )
    assert p.returncode == 1, f"rc={p.returncode}\nstdout={p.stdout[:300]}"
    assert "Traceback" not in p.stderr, (
        f"这是**预期内**的参数错, 不该甩 traceback。stderr:\\n{p.stderr}")
    assert "PAVOZ_STORAGE" in p.stderr, p.stderr
    assert list(work.iterdir()) == [], (
        f"cwd 里多出了东西: {list(work.iterdir())} —— 错误提示必须早于副作用")


def test_subprocess_env_really_points_at_this_tree():
    """自检: 子进程加载的 pavos 必须来自本文件所在的仓, 不是别处那一份。"""
    code = "import importlib.util,sys;"
    code += "s=importlib.util.find_spec(sys.argv[1]);print(s.origin if s else '')"
    env = _cli_env()
    p = subprocess.run([sys.executable, "-c", code, _PKG],
                       capture_output=True, text=True, env=env, timeout=60)
    origin = p.stdout.strip()
    assert origin, f"子进程里根本 import不到 {_PKG}: {p.stderr[:200]}"
    assert str(_ROOT) in origin, (
        f"子进程加载的是 {origin}, 不是本仓 —— 判据会静默测到另一个 pavos。"
        f"(本仓 venv 里有 editable .pth 指向仓库根, 不钉 PYTHONPATH 就由它说了算。)")


def test_normal_storage_dir_still_works_end_to_end(tmp_path):
    """反面 (端到端): 正常目录一路跑通, 不是只过构造。"""
    dag = tmp_path / "d.py"
    dag.write_text(
        f"from {_PKG} import DAG\n"
        "d = DAG('probe')\n"
        "@d.stage()\n"
        "async def s_a(ctx):\n"
        "    return {'ran': 'OK'}\n", encoding="utf-8")
    data = tmp_path / "data"
    env = _cli_env(PAVOZ_STORAGE=str(data))
    p = subprocess.run(
        [sys.executable, "-m", _PKG, "run", str(dag), "--task-id", "t2"],
        capture_output=True, text=True, cwd=_ROOT, env=env, timeout=120,
    )
    assert p.returncode == 0, p.stderr[-400:]
    assert "OK" in p.stdout
    assert data.is_dir() and any(data.iterdir())
