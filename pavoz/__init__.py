"""pavoz — 通用流程编排库 (micro/in-process workflow engine).

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

from .cancel import CancelRegistry, NotKillable, TaskIdInUse
from .checkpoint import (
    Checkpoint,
    CheckpointMismatchError,
    CheckpointStore,
    diff_runs,
    stage_input_hash,
    workflow_hash,
)
from .dag import DAG, CycleError, Stage, UnknownDepError
from .events import EventRecorder
from .policy import EnginePolicy
from .runtime import CallMeta, CallResult, Ctx, Runtime

# ⚠️ CallMeta 曾漏在这里 (2026-10-16 修): `pavoz.runtime.__all__` 一直把它
# 列为公开名, 但顶层没导出 ⇒ 用户写自定义 caller 时**无法标注签名**
# `Callable[[str, str, dict, CallMeta], Awaitable[dict]]`, 只能走内部路径
# `from pavoz.runtime import ...`。而 caller 是核心扩展点 (Runtime 的注入
# 参数 / Ctx.caller 字段 / TestPipe(caller=...)), 同族的 CallResult / Ctx
# 都导出了, 只有它没有。
from .state import (
    ReadOnlyStateView,
    StateConflictError,
    apply_overrides,
    merge_overrides,
    parse_set_args,
    parse_set_file,
)
from .storage import CorruptRecordError, FileStorage, StorageBackend
from .storage_loader import PavozStorageError, load_storage
from .testing import TestPipe
from .types import (
    FatalError,
    MaxVisitsExceeded,
    OrphanStagesError,
    PavozError,
    RetryableError,
    RunResult,
    StageError,
    UnmappedRouteError,
)
from .viz import to_graph_json, to_mermaid

__version__ = "0.5.8"

__all__ = [
    "CorruptRecordError",
    "DAG",
    "CallMeta",
    "CallResult",
    "CancelRegistry",
    "Checkpoint",
    "CheckpointMismatchError",
    "CheckpointStore",
    "Ctx",
    "CycleError",
    "EnginePolicy",
    "EventRecorder",
    "FatalError",
    "FileStorage",
    "MaxVisitsExceeded",
    "NotKillable",
    "TaskIdInUse",
    "OrphanStagesError",
    "PavozError",
    "PavozStorageError",
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
    "UnmappedRouteError",
    "__version__",
    "apply_overrides",
    "diff_runs",
    "load_storage",
    "merge_overrides",
    "parse_set_args",
    "parse_set_file",
    "stage_input_hash",
    "to_graph_json",
    "to_mermaid",
    "workflow_hash",
]
