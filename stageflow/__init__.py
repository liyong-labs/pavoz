"""stageflow — 通用流程编排库 (micro/in-process workflow engine).

独立于任何业务系统与模型/存储服务: DAG (静态声明) + Runtime (顺序执行 +
per-node retry + absolute deadline) + Checkpoint (断点续跑) + TestPipe (回归).
循环 (质量收敛等) 留在 stage 函数内用普通 Python 表达. 存储/外部调用/任务表
全部 Protocol, 由业务侧注入. core 零第三方依赖.

公开 API:
    DAG / @dag.stage(depends_on, retries, timeout)
    Runtime().run(dag, task_id, initial_state=..., resume=True)
    TestPipe(dag).mock("s_x", lambda state: {...}).run()
    StorageBackend / FileStorage / CheckpointStore
"""

from .checkpoint import Checkpoint, CheckpointMismatchError, CheckpointStore, workflow_hash
from .dag import DAG, CycleError, Stage, UnknownDepError
from .runtime import CallResult, Ctx, Runtime
from .state import ReadOnlyStateView, StateConflictError
from .storage import FileStorage, StorageBackend
from .testing import TestPipe
from .types import FatalError, RetryableError, RunResult, StageError

__version__ = "0.1.3"

__all__ = [
    "DAG",
    "CallResult",
    "Checkpoint",
    "CheckpointMismatchError",
    "CheckpointStore",
    "Ctx",
    "CycleError",
    "FatalError",
    "FileStorage",
    "ReadOnlyStateView",
    "RetryableError",
    "RunResult",
    "Runtime",
    "Stage",
    "StageError",
    "StateConflictError",
    "StorageBackend",
    "TestPipe",
    "UnknownDepError",
    "__version__",
    "workflow_hash",
]
