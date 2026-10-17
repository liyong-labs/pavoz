#!/usr/bin/env bash
# 发版深度验证 — 拿 **PyPI 上装的那个包** 跑仓库的全套测试.
#
# 为什么要有这个 (2026-10-17): RELEASING.md 第 5 步只做浅验证 ——
#   "pip install pavoz==X.Y.Z 成功; python -c 'import pavoz; print(__version__)'"
# 而"装得上、import 得到、版本号对"**证不出打包有没有漏东西**: 少打一个模块、
# 少一个子包、pyproject 的 package 列表写漏了, 三样都能通过那三步, 直到用户
# `from pavoz import X` 时才炸。
#
# 这里跑的是**全套产品测试**, 对象是 site-packages 里的 pavoz。仓库自己的两个
# "检查仓库文件"的测试 (doc 链接 / 文档可达) 被排除 —— 它们要完整 checkout 才
# 有意义 (doc 链接指向 pavoz/*.py), 而把源码拷进工作目录会让 Python 优先 import
# 本地副本, 正好毁掉这次验证。
#
# 自检是**硬要求**: 先断言 pavoz 来自 site-packages 再跑。少了它, "本地源码被
# 当成装好的包测了一遍"会一路绿到结束 —— 这类空集/错对象绿灯本轮已栽过三次。
#
# 用法: tools/verify-release.sh 0.5.7 [源码目录]
set -o pipefail

VER="${1:?用法: verify-release.sh <版本号> [源码目录]}"
# 源码目录: 第二个参数, 缺省 = 本脚本所在仓库 (tools/ 的上一级)。
# ⚠️ 原先写死了维护者本机的家目录路径 —— 仓外维护者照 RELEASING.md 跑会失败。
SRC="${2:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
TD=$(mktemp -d)
trap 'rm -rf "$TD"' EXIT

echo "=== 1. 干净 venv 装 pavoz==$VER (来自 PyPI, 不带 editable) ==="
python3 -m venv "$TD/venv" || exit 1
"$TD/venv/bin/pip" install -q "pavoz==$VER" pytest pytest-asyncio pyyaml || {
    echo "❌ 装不上 pavoz==$VER —— 版本可能没发出去, 或依赖缺失"; exit 1; }
"$TD/venv/bin/python" -c "import pavoz; print('  __version__ =', pavoz.__version__)" || exit 1

echo
echo "=== 2. 拷测试 + 仓库文档 (不拷源码!) ==="
# ⚠️ pyproject.toml **必须**拷: 它带 pytest 的 asyncio_mode (缺了 = 几百条 async
#   用例全灭) 与版本号 (test_version_sync 读它)。第一版脚本漏了它, 首跑 337 红 ——
#   看着像"打包漏了 337 个东西", 实际是我自己少拷一个文件。
#   判据要能区分这两种, 所以下面先自查关键文件在不在, 缺了就早退并点名。
cp -r "$SRC/tests" "$SRC/tools" "$SRC/docs" "$SRC/dags" "$SRC/examples" "$TD/" || exit 1
for f in pyproject.toml CHANGELOG.md README.md README.cn.md CONTRIBUTING.md llms.txt MIGRATION.md; do
    if [ -f "$SRC/$f" ]; then
        cp "$SRC/$f" "$TD/"
    elif [ "$f" = pyproject.toml ]; then
        echo "❌ 源目录没有 pyproject.toml — pytest 配置与版本同步测试都要它"; exit 1
    fi
done
echo "  已拷: tests/ tools/ docs/ dags/ examples/ pyproject.toml (+ 文档)"

echo
echo "=== 3. ★ 自检: 被测的必须是装好的包, 不是源码副本 ==="
cat > "$TD/tests/test_zz_which_pavoz.py" <<'PY'
import pathlib
import pavoz


def test_under_test_is_the_installed_package():
    p = pathlib.Path(pavoz.__file__).parent.resolve()
    assert "site-packages" in p.parts, (
        f"★ 测的不是装好的包, 是 {p} —— 整轮验证白做 (多半是工作目录里有 pavoz/ 被优先 import)")
    print(f"\n  被测包: {p}")
PY
cd "$TD" || exit 1
"$TD/venv/bin/python" -m pytest tests/test_zz_which_pavoz.py -q -p no:cacheprovider -s || {
    echo "❌ 自检没过, 后面不用跑了"; exit 1; }

echo
echo "=== 4. 全套产品测试 (对装好的包) ==="
"$TD/venv/bin/python" -m pytest tests/ -q -p no:cacheprovider \
    --deselect tests/test_doc_link_checker.py::test_real_repo_docs_pass \
    --deselect tests/test_docs_reachable.py::test_every_doc_page_is_reachable
rc=$?

echo
if [ "$rc" -eq 0 ]; then
    echo "verify-release: ✅ PyPI 上的 $VER 通过仓库全套产品测试 (打包无遗漏)"
else
    echo "verify-release: ❌ PyPI 上的 $VER 有测试不过 (rc=$rc)"
    echo "  两种可能: (1) 打包漏了东西; (2) 仓库 HEAD 的新测试还没发版 ——"
    echo "  先分清: 把 tests/ 里失败的用例逐条看是不是针对**未发布**的修复。"
fi
exit "$rc"
