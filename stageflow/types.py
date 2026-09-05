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
    """一次 DAG run 的结果 (task 粒度).

    Attributes:
        task_id: caller-supplied (or auto UUID4), stable across retries/resumes
        run_id: stageflow auto UUID4 per runtime.run() — distinguishes runs
        state: final merged state after all completed stages
        stage_statuses: {stage_name: "done" | "failed"} — only executed stages;
            resume-skipped stages are absent, never recorded as "skipped"
        status: "running" | "done" | "failed"
        error: error message if status == "failed"
    """

    __slots__ = ("dag_name", "error", "run_id", "stage_statuses", "state", "status", "task_id")

    def __init__(self, task_id: str, dag_name: str, run_id: str = ""):
        self.task_id = task_id
        self.dag_name = dag_name
        self.run_id = run_id
        self.state: dict = {}
        self.stage_statuses: dict[str, str] = {}
        self.status: str = "running"
        self.error: str | None = None

    def __repr__(self) -> str:
        short = self.run_id[:8] if self.run_id else "?"
        return f"<RunResult {self.dag_name} {self.task_id} run={short} status={self.status}>"
