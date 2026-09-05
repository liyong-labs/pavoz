"""StorageBackend adapter 可选实现.

每个 adapter 懒导入 driver; 缺 driver 时抛 ImportError + 安装提示.
"""

from __future__ import annotations

# 不强导入任何 adapter — 让用户按需 import:
#   from stageflow.contrib.storage import PostgresStorage
# 这才触发对应 adapter 的 driver 检查。

__all__: list[str] = []
