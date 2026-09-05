"""stageflow 基础类型: 异常契约 + 运行结果."""

__all__ = ["FatalError", "RetryableError", "RunResult", "StageError"]


class StageError(Exception):
    """业务错误. 不重试, runtime 直接走 fail 终态.

    例: 搜索结果为空 (业务上不可重试), 校验不通过.
    """


class RetryableError(Exception):
    """可重试错误 (网络 / 超时 / 429 / 5xx). 扣 per-node retries 后重试.

    budget 耗尽仍未成功 → stage fail.
    """


class FatalError(Exception):
    """程序 bug (框架 / stage 代码错误). 立即终, 不重试, 不消耗 retries.

    例: state 类型不兼容, stage 签名错误.
    """


class RunResult:
    """一次 DAG run 的结果 (task 粒度)."""

    __slots__ = ("dag_name", "error", "stage_statuses", "state", "status", "task_id")

    def __init__(self, task_id: str, dag_name: str):
        self.task_id = task_id
        self.dag_name = dag_name
        self.state: dict = {}
        self.stage_statuses: dict[str, str] = {}  # stage_name -> done/failed/skipped
        self.status: str = "running"  # running/done/failed
        self.error: str | None = None

    def __repr__(self) -> str:
        return f"<RunResult {self.dag_name} {self.task_id} status={self.status}>"
