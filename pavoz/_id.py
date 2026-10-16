"""ID generation helpers.

pavoz requires Python 3.12+ where uuid.uuid4 is the canonical
UUID generator. uuid.uuid7 is 3.14+; we use uuid4 to keep the
3.12 floor. Uniqueness is the requirement — time-ordering is not.
"""
from __future__ import annotations

import uuid

__all__ = []   # 内部: ID 生成是实现细节, 不是公开面


def new_id() -> str:
    """Generate a new unique ID (UUID4 canonical string form, 36 chars)."""
    return str(uuid.uuid4())
