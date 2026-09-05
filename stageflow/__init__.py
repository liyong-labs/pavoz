"""stageflow — LLM/SE/Extract 流程编排 (micro/in-process engine).

v0.1: DAG (静态) + Runtime (顺序 + retry + deadline) + Checkpoint + TestPipe.
循环 (audit cascade 等) 留在 stage 函数内用普通 Python 表达.

公开 API:
    DAG / @dag.stage
    Runtime().run(dag, task_id, ...)
    TestPipe(dag).mock("s_x", lambda state: {...}).run()
    StorageBackend / FileStorage / CheckpointStore
    TaskTrigger (业务 adapter 实现)
"""

from .checkpoint import Checkpoint, CheckpointMismatchError, CheckpointStore, workflow_hash
from .dag import DAG, CycleError, Stage, UnknownDepError
from .runtime import CallResult, Ctx, Runtime
from .state import ReadOnlyStateView, StateConflictError
from .storage import FileStorage, StorageBackend
from .testing import TestPipe
from .trigger import TaskRef, TaskTrigger
from .types import FatalError, RetryableError, RunResult, StageError

__version__ = "0.1.0"

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
    "TaskRef",
    "TaskTrigger",
    "TestPipe",
    "UnknownDepError",
    "__version__",
    "workflow_hash",
]
