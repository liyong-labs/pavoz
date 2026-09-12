"""发版最容易错的一环: 两处版本号必须一致.

CHANGELOG 记过一次漂移事故 (0.1.1 装机却报 0.1.3) — 用户 `import pavoz` 看到的
版本与 tag / PyPI 不符, 排查成本高。这里钉死。
"""
from __future__ import annotations

import pathlib
import tomllib

import pavoz

_ROOT = pathlib.Path(__file__).resolve().parent.parent


def test_version_matches_pyproject() -> None:
    pyproject = tomllib.loads((_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert pavoz.__version__ == pyproject["project"]["version"], (
        "pavoz/__init__.py 的 __version__ 与 pyproject.toml 的 version 不一致 — "
        "发版前必须同步 (见 RELEASING.md)"
    )
