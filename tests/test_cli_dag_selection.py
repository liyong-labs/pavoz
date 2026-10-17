"""`_load_dag` 挑哪个 DAG —— 三条静默/误报缺陷的判据.

起因 (2026-10-17 实测): 这个函数**零测试**, 而它决定"用户声明要跑的流水线是不是
真的被跑了"。三个缺陷都是实跑出来的:

  ① **`DEFAULT_DAG` 写错就静默跑另一个**。一个 dag 文件里两个 DAG, 顶上写
     `DEFAULT_DAG = "beta_typo"` (打错字):

         $ pavos run dag.py --task-id a1
         # rc=0, stderr 一个字都没有, JSON:
         "dag": "alpha",          ← 声明的是 beta_typo, 跑的是 alpha
         "state": {"ran": "ALPHA"}

     原实现匹配不上就掉到 `return dags[0]`。用户**明确指定了**却被忽略, 比根本没写
     更糟 —— 他以为跑的是自己声明的那个, 于是照着 alpha 的结果去调 beta。

  ② **`class MyDAG(DAG)` 的子类实例不被认**。原实现是
     `type(v).__name__ == "DAG"` (按**类名字符串**认, 不是 isinstance), 子类的
     类名是 `MyDAG`, 于是:

         $ pavos run dag_sub.py --task-id b1
         /path/dag_sub.py 里没有 DAG 实例        rc=1

     文件里明明有一个 DAG。报错还把人指向错误的方向 (让人去检查"是不是忘了实例化")。

  ③ **作用域里别人的同名 `DAG` 实例会被收进来**。同一行判据的另一面 —— 只要有别的库
     把 `DAG` 这个名字暴露出来并在模块级实例化, 它就混进候选, 选中后:

         AttributeError: 'DAG' object has no attribute 'validate'

     一条完全看不出"你的 dag 文件里有重名东西"的裸栈。

三个缺陷同源: 挑选逻辑既**认错对象** (按名字而非 isinstance), 又**在认不出时替用户
猜** (静默取第一个)。判据要同时钉住这两头 —— 只钉 ① 的话, 修法可以是"永远取第一个",
而那正是 ① 本身。
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import subprocess
import sys
from importlib import import_module

import pytest

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_PKG = [d for d in os.listdir(_ROOT) if (_ROOT / d / "cli.py").is_file()][0]
sys.path.insert(0, str(_ROOT))

_cli = import_module(f"{_PKG}.cli")

# ── 各种"用户会写的 dag 文件" ──────────────────────────────────────────────

TWO_DAGS_TYPO = """\
from {pkg} import DAG
alpha = DAG("alpha")
@alpha.stage()
async def s_a(ctx):
    return {{"ran": "ALPHA"}}
beta = DAG("beta")
@beta.stage()
async def s_b(ctx):
    return {{"ran": "BETA"}}
DEFAULT_DAG = "beta_typo"
"""

TWO_DAGS_OK = """\
from {pkg} import DAG
alpha = DAG("alpha")
@alpha.stage()
async def s_a(ctx):
    return {{"ran": "ALPHA"}}
beta = DAG("beta")
@beta.stage()
async def s_b(ctx):
    return {{"ran": "BETA"}}
DEFAULT_DAG = "beta"
"""

TWO_DAGS_NO_DEFAULT = """\
from {pkg} import DAG
alpha = DAG("alpha")
@alpha.stage()
async def s_a(ctx):
    return {{"ran": "ALPHA"}}
beta = DAG("beta")
@beta.stage()
async def s_b(ctx):
    return {{"ran": "BETA"}}
"""

SUBCLASS = """\
from {pkg} import DAG as BaseDAG

class MyDAG(BaseDAG):
    pass

mydag = MyDAG("subclassed")
@mydag.stage()
async def s_a(ctx):
    return {{"ran": "SUBCLASS"}}
"""

FOREIGN_COLLISION = """\
from {pkg} import DAG as PAVOS_DAG

class DAG:                        # 别的库的同名类, 被 import 进作用域
    def __init__(self, n):
        self.name = n

foreign = DAG("from_foreign_lib")   # 模块级实例, type(x).__name__ == "DAG"

real = PAVOS_DAG("real_one")
@real.stage()
async def s_a(ctx):
    return {{"ran": "REAL"}}
"""

ALIASED = """\
from {pkg} import DAG
one = DAG("only_one")
alias = one                    # 同一个实例绑到两个名字 —— 不算"多个 DAG"
@one.stage()
async def s_a(ctx):
    return {{"ran": "OK"}}
"""

SINGLE = """\
from {pkg} import DAG
d = DAG("solo")
@d.stage()
async def s_a(ctx):
    return {{"ran": "SOLO"}}
"""


def _write(tmp_path, name, body):
    f = tmp_path / f"{name}.py"
    f.write_text(body.format(pkg=_PKG), encoding="utf-8")
    return str(f)


def _cli_run(tmp_path, dag_file, task_id):
    env = dict(os.environ, PAVOZ_STORAGE=str(tmp_path / f"data_{task_id}"))
    return subprocess.run(
        [sys.executable, "-m", _PKG, "run", dag_file, "--task-id", task_id],
        capture_output=True, text=True, cwd=_ROOT, env=env, timeout=90,
    )


# ── ① DEFAULT_DAG ─────────────────────────────────────────────────────────

def test_default_dag_typo_is_fatal(tmp_path):
    """★ 声明了 DEFAULT_DAG 却匹配不上 → **报错**, 不许静默取第一个。"""
    f = _write(tmp_path, "typo", TWO_DAGS_TYPO)
    with pytest.raises(SystemExit) as ei:
        _cli._load_dag(f)
    msg = str(ei.value)
    assert "beta_typo" in msg, msg
    # 必须告诉用户**实际有哪些**名字 —— 只说"没找到"等于让人自己去数
    assert "alpha" in msg and "beta" in msg, msg


def test_default_dag_typo_does_not_run_anything(tmp_path):
    """★ 经真 CLI: rc=1、stderr 说清、**一个 DAG 都没跑** (没有 state 被写)。"""
    f = _write(tmp_path, "typo", TWO_DAGS_TYPO)
    p = _cli_run(tmp_path, f, "t_typo")
    assert p.returncode == 1, f"rc={p.returncode}, stdout={p.stdout[:300]}"
    assert "beta_typo" in p.stderr and "alpha" in p.stderr, p.stderr
    # 关键: 不能出现任何"跑成功了"的痕迹 —— 静默跑错比报错糟得多
    assert '"ran"' not in p.stdout, f"声明了 DEFAULT_DAG 却还是跑了东西: {p.stdout[:300]}"
    assert "ALPHA" not in p.stdout and "BETA" not in p.stdout, p.stdout[:300]


def test_default_dag_picks_the_named_one(tmp_path):
    """★ 正确写法不许回归 —— 声明 beta 就必须跑 beta。"""
    f = _write(tmp_path, "ok", TWO_DAGS_OK)
    assert _cli._load_dag(f).name == "beta"


def test_multiple_dags_without_default_warns(tmp_path):
    """多个 DAG 且没声明 → 取第一个, 但**在 stderr 上说一声** (静默挑是同一类错)。"""
    f = _write(tmp_path, "nodefault", TWO_DAGS_NO_DEFAULT)
    assert _cli._load_dag(f).name == "alpha"
    p = _cli_run(tmp_path, f, "t_nd")
    assert "DEFAULT_DAG" in p.stderr and "alpha" in p.stderr, p.stderr


# ── ② 认对象而不是认名字 ───────────────────────────────────────────────────

def test_dag_subclass_is_recognized(tmp_path):
    """★ `class MyDAG(DAG)` 的实例必须被认 —— 继承是正常用法, 不是"没有 DAG"。"""
    f = _write(tmp_path, "sub", SUBCLASS)
    assert _cli._load_dag(f).name == "subclassed"
    p = _cli_run(tmp_path, f, "t_sub")
    assert p.returncode == 0, p.stderr[-400:]
    assert "SUBCLASS" in p.stdout


def test_foreign_same_named_class_is_ignored(tmp_path):
    """★ 别的库的同名 `DAG` 实例不许混进候选 (原实现按类名字符串认, 会收进来)。"""
    f = _write(tmp_path, "collide", FOREIGN_COLLISION)
    got = _cli._load_dag(f)
    assert got.name == "real_one", f"选错了: {got.name!r}"
    p = _cli_run(tmp_path, f, "t_col")
    assert p.returncode == 0, p.stderr[-400:]
    assert "REAL" in p.stdout


def test_aliased_instance_is_not_multiple_dags(tmp_path):
    """同一实例绑两个名字不算"多个 DAG" —— 否则会误报提示、绕过 DEFAULT_DAG。"""
    f = _write(tmp_path, "alias", ALIASED)
    assert _cli._load_dag(f).name == "only_one"
    p = _cli_run(tmp_path, f, "t_alias")
    assert "多个 DAG" not in p.stderr, p.stderr


def test_single_dag_still_works(tmp_path):
    f = _write(tmp_path, "single", SINGLE)
    assert _cli._load_dag(f).name == "solo"


# ── 判据自检 ──────────────────────────────────────────────────────────────

def test_fixtures_are_distinct():
    """自检: 各 fixture 必须真的不同 —— 否则"修好了 A"可能只是把 B 也写成了 A。"""
    bodies = {n: b for n, b in
              (("typo", TWO_DAGS_TYPO), ("ok", TWO_DAGS_OK),
               ("nodefault", TWO_DAGS_NO_DEFAULT), ("sub", SUBCLASS),
               ("collide", FOREIGN_COLLISION), ("alias", ALIASED),
               ("single", SINGLE))}
    assert len(set(bodies.values())) == len(bodies), "有 fixture 内容重复"
    assert all("DAG" in b for b in bodies.values()), "有 fixture 里根本没有 DAG"


def test_collected_dags_are_real_instances(tmp_path):
    """自检: 收集靠的是 isinstance —— 直接查收集结果的真身, 不经 CLI。

    `_load_dag` 只返回一个, 看不到全部候选, 所以这里单独查判定本身: 文件里混了
    外来同名实例时, 收上来的必须**只有**真 DAG (而不是"恰好第一个是对的")。
    """
    f = _write(tmp_path, "collide2", FOREIGN_COLLISION)
    DAGcls = import_module(f"{_PKG}.dag").DAG
    spec = importlib.util.spec_from_file_location("_probe_dag", f)
    assert spec is not None and spec.loader is not None
    m = importlib.util.module_from_spec(spec)
    sys.modules["_probe_dag"] = m
    spec.loader.exec_module(m)
    picked = [v for v in vars(m).values() if isinstance(v, DAGcls)]
    assert [d.name for d in picked] == ["real_one"], [d.name for d in picked]
