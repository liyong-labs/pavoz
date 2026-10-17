"""pavoz 基础类型: 异常契约 + 运行结果."""

__all__ = [
    "FatalError",
    "MaxVisitsExceeded",
    "OrphanStagesError",
    "PavozError",
    "RetryableError",
    "RunResult",
    "StageError",
    "UnmappedRouteError",
]


class PavozError(Exception):
    """pavoz 异常基类: 可携带 caller-friendly category (D1.5, 0.5.4).

    库自身抛出的异常都挂在它下面 (StageError / RetryableError / FatalError /
    UnmappedRouteError / MaxVisitsExceeded / OrphanStagesError), 消费方一个
    `except PavozError` 就能兜住"编排引擎判的失败"。注意 run() 本身不抛这些 ——
    stage 失败会转成 RunResult(status="failed", error_class=...); 统一捕获用于
    包一层 SDK / 转错误码等场景。调用方用错造成的 CheckpointMismatchError
    不在此树下 (它是 caller bug, 不是引擎判的失败)。

    category 是自由 string (建议值: LLM_TIMEOUT / SEARCH_NO_RESULT / CODE_BUG /
    NETWORK / AUTH / INFRA / CHECKPOINT_LOAD_FAIL / STATE_VALIDATION /
    PROTOCOL_BREACH — 9 类, ai@home id=188 ack). 不传 → None, runtime 透传到
    RunResult.error_category, caller 据此分流重试/通知策略.
    """

    def __init__(self, *args, category: str | None = None):
        super().__init__(*args)
        self.category = category


class StageError(PavozError):
    """业务错误. 不重试, runtime 直接走 fail 终态.

    例: 搜索结果为空 (业务上不可重试), 校验不通过.
    """


class RetryableError(PavozError):
    """可重试错误 (网络 / 超时 / 429 / 5xx). 扣 per-node retries 后重试.

    budget 耗尽仍未成功 → stage fail.
    """


class FatalError(PavozError):
    """程序 bug (框架 / stage 代码错误). 立即终, 不重试, 不消耗 retries.

    例: state 类型不兼容, stage 签名错误.
    """


class UnmappedRouteError(PavozError):
    """条件路由返回了 mapping 未声明的 key (R1, 0.5.5).

    编排器抛 (route_fn 返回 key ∉ mapping) — 封闭集在声明期由 mapping 定义,
    运行时强制. fail-loud, 无 default 无静默. 注意与 route_fn 自己抛的异常区分:
    后者原样传播 (是 route_fn 的 bug, 编排器不包装).
    """


class MaxVisitsExceeded(PavozError):
    """router stage 单次 run 内执行次数超过 max_visits (R1, 0.5.5).

    cond_fn/路由 bug 死循环的安全网. 环图 (条件边构成 logical cycle) 在
    validate() 强制要求显式 max_visits; EnginePolicy.max_steps 仍全局兜底.
    """


class OrphanStagesError(PavozError):
    """条件边图 run 结束时有声明 stage 从未被执行 (R1, 0.5.5).

    路由未选中 + topo 不可达 → stage 被静默跳过 — 这类 wiring 错误 (常见:
    回路没闭合, 路由走进死胡同) 决不允许报 done. run 以 failed 终止,
    error 列出全部孤儿 stage 名 (ai@home id=270 BUG 2).
    """


class RunResult:
    """一次 DAG run 的结果 (task 粒度).

    Attributes:
        task_id: caller-supplied (or auto UUID4), stable across retries/resumes
        run_id: pavoz auto UUID4 per runtime.run() — distinguishes runs
        state: final merged state after all completed stages
        stage_statuses: {stage_name: "done" | "failed" | "cancelled"} — 含被 skip_unchanged
            跳过的 stage, 它们记的是 **"done"** (结果是从 cp 重放的, 但状态层面确实完成
            了)。⚠️ 此前这里写的是 "resume-skipped stages are absent", 与实现不符 (实测
            fork + skip_unchanged 下它们以 "done" 出现) —— 要分辨"跑过"与"重放过", 看
            `skipped_stages`
        skipped_stages: {skip_unchanged 跳过的 stage 名} — 这些的 fn **没有被调用**, 结果是
            从 checkpoint 重放的。空列表 = 每个完成的 stage 都真跑过。
            为什么单列: `stage_statuses` / `done_stages` / `stage_timings` 三者对它们都与
            "正常执行" 不可区分, 而"我改的那一段到底跑没跑"恰恰是调试 skip_unchanged 时
            唯一的疑问 —— 此前只有事件流 (stage_end status="skipped") 知道, 而
            `pavos trace` 不看事件流, 于是会理直气壮地报告"都跑了"
        stage_timings: {stage_name: wall-clock seconds} — executed stages only,
            includes retry backoff sleeps; a cancelled-intercepted stage may
            appear with ~0 duration
        status: "running" | "done" | "failed" | "cancelled"
        error: error message if status in ("failed", "cancelled")
        error_class: 原始异常类名 (如 "ValueError" / 业务 PipelineError), 失败时保留 —
            诊断元数据 (消费者据此分派告警: 业务失败→调参重跑, 代码 bug→找人), 类名
            可随重构变化, 不作跨版本稳定契约. cancelled → None; done → None.
            (R2, 2026-09-10 ai-research PR: 引擎吞异常不能连类型一起吞.)
        failed_stage: 失败/取消所在的 stage 名 (W1, 0.5.3); done → None.
        retryable: 失败是否具可重试语义 (W1, 0.5.3) — RetryableError/TimeoutError
            → True (重试耗尽但本质可重试, caller 可择机重跑), StageError/FatalError/
            未知异常 → False; done/cancelled → None.
        state_summary: 失败时 state 的截断 JSON (≤2048 字符, W1, 0.5.3) — caller
            接错即看现场, 不必重跑; done/cancelled → None.
        error_category: 失败的业务类别 (D1.5, 0.5.4) — caller raise 时传
            StageError(..., category="LLM_TIMEOUT") 等, runtime 透传; 未传 /
            done / cancelled → None. 任何**自带 .category 属性**的异常都会透传
            (含非 StageError 的自定义异常), 不要求继承 StageError。
    """

    __slots__ = (
        "dag_name", "error", "error_category", "error_class", "failed_stage",
        "retryable", "run_id", "skipped_stages", "stage_statuses", "stage_timings",
        "state",
        "state_summary", "status", "task_id",
    )

    def __init__(self, task_id: str, dag_name: str, run_id: str = ""):
        self.task_id = task_id
        self.dag_name = dag_name
        self.run_id = run_id
        self.state: dict = {}
        self.stage_statuses: dict[str, str] = {}
        self.skipped_stages: list[str] = []   # skip_unchanged 跳过的 (fn 没被调用)
        self.stage_timings: dict[str, float] = {}  # stage → 墙钟秒 (含 retry 退避)
        self.status: str = "running"
        self.error: str | None = None
        self.error_class: str | None = None
        self.failed_stage: str | None = None   # W1: 失败/取消所在的 stage
        self.retryable: bool | None = None     # W1: 失败是否可重试语义 (done/cancelled=None)
        self.state_summary: str | None = None  # W1: 失败时 state 截断 JSON (≤2048 字符)
        self.error_category: str | None = None  # D1.5: caller 传的业务类别 (未传=None)

    def __repr__(self) -> str:
        short = self.run_id[:8] if self.run_id else "?"
        return f"<RunResult {self.dag_name} {self.task_id} run={short} status={self.status}>"
