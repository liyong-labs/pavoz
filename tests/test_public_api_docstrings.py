"""导出的公开面 (`pavoz.__all__`) 每一处都要有 docstring.

起因: 对照竞品的接口注释密度 (justpipe 的构造器参数逐项 `Args`, Hamilton 的
`:param:`) 后, 补全了公开入口的参数/返回/异常说明。但"补过一次"不等于"以后都在"
—— 新增导出符号时没有任何门禁会提醒你写注释, 读过源码的人只能再发现一次。

判据只查 `pavoz.__all__` 里的类/函数及其**公开**方法/属性:
  · 下划线开头的成员不算 —— `_run_inner` 这类实现细节不是调用者的接口面;
  · dunder (`__repr__` / `__eq__`) 同样跳过 —— 它们的注释不是给调用者查的。

`len(__all__) >= 30` 是空集自检: 导出表被清空 / 导入失败时, 前两条会对着
空集合报"全部有注释" —— 那种绿比红更危险。
"""

from __future__ import annotations

import inspect

import pavoz


def _doc(obj) -> str:
    return inspect.getdoc(obj) or ""


def test_every_exported_symbol_has_a_docstring():
    missing = []
    for name in pavoz.__all__:
        obj = getattr(pavoz, name)
        if not (inspect.isclass(obj) or inspect.isfunction(obj)):
            continue                      # 常量 / 类型别名不在此列
        if not obj.__module__.startswith(pavoz.__name__):
            continue                      # 从 stdlib/第三方再导出的不动
        if not _doc(obj):
            missing.append(name)
    assert len(pavoz.__all__) >= 30, (
        f"__all__ 只有 {len(pavoz.__all__)} 项 —— 导出面没被正确加载, 判据在空集上会恒绿")
    assert not missing, f"导出的公开符号缺 docstring: {missing}"


def test_public_members_of_exported_classes_have_docstrings():
    missing = []
    for name in pavoz.__all__:
        cls = getattr(pavoz, name)
        if not (inspect.isclass(cls) and cls.__module__.startswith(pavoz.__name__)):
            continue
        for mname, member in vars(cls).items():
            if mname.startswith("_"):
                continue
            target = member.fget if isinstance(member, property) else member
            if callable(target) and not _doc(target):
                missing.append(f"{name}.{mname}")
    assert not missing, (
        f"导出类的公开成员缺 docstring: {missing}\n"
        f"这些是调用者会用 help() 查的接口面; 实现细节请改成下划线私有.")
