"""pavoz stub for local Pyright. 部署机用真实 pavoz, 本地占位.

stub 是接口协议占位, 参数故意不用. Pyright basic 模式无法用 pragma 关闭 unused-param.
实际解决方案: stub 文件由 pyrightconfig.json exclude 掉, 让 Pyright 不扫描.
"""
from typing import Any, Callable, Protocol


class _CallLike(Protocol):
    async def __call__(self, kind: str, op: str, params: dict) -> Any: ...


class Context:
    state: dict
    call: _CallLike


def _stage_decorator(fn: Callable) -> Callable:
    return fn


class _DAGAPI:
    def stage(self, fn: Callable) -> Callable:
        return _stage_decorator(fn)


dag = _DAGAPI()
__all__ = ["dag", "Context"]