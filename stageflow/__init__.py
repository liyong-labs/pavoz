"""stageflow — 通用流程编排库 (micro/in-process workflow engine).

独立于任何业务系统与模型/存储服务: DAG (静态声明) + Runtime (顺序执行 +
per-node retry + absolute deadline) + Checkpoint (断点续跑) + TestPipe (回归).
循环 (质量收敛等) 留在 stage 函数内用普通 Python 表达. 存储/外部调用/任务表
全部 Protocol, 由业务侧注入. core 零第三方依赖.

公开 API:
    DAG / @dag.stage(depends_on, retries, timeout)
    Runtime().run(dag, task_id=None, initial_state=..., resume=False)
        task_id 省略 → 自动 UUID4; run_id 每次 run 自动生成 (resume 复用)
        run_stage(dag, task_id, stage_name) — 从 cp 重建输入单 stage 重放 (v0.5.1)
        fork_run(dag, task_id, from_stage=..., overrides=...) — 历史节点取输入改装回
            续跑: 前序复用, from_stage 起用 overrides 重跑, 原 cp 不动 (v0.6)
    TestPipe(dag).mock("s_x", lambda state: {...}).run()
        TestPipe.replay_from(cp, dag) — 真实 cp 已完成 stage 用历史 delta mock 回归 (v0.5.1)
    StorageBackend / FileStorage / CheckpointStore
"""

from .checkpoint import Checkpoint, CheckpointMismatchError, CheckpointStore, workflow_hash
from .dag import DAG, CycleError, Stage, UnknownDepError
from .runtime import CallResult, Ctx, Runtime
from .state import ReadOnlyStateView, StateConflictError
from .storage import FileStorage, StorageBackend
from .storage_loader import StageflowStorageError, load_storage
from .testing import TestPipe
from .types import FatalError, RetryableError, RunResult, StageError

__version__ = "0.5.1"

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
    "StageflowStorageError",
    "StateConflictError",
    "StorageBackend",
    "TestPipe",
    "UnknownDepError",
    "__version__",
    "load_storage",
    "workflow_hash",
]
