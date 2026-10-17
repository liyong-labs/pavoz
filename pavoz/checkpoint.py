"""Checkpoint: per-run 持久化 + workflow hash + mismatch 拒 resume.

- 每次 runtime.run() 一个 run_id (UUID4); checkpoint 存 runs/{task_id}/{run_id}/checkpoint.
- 指针文件 runs/{task_id}/latest 记 {"run_id": ...} — load_latest 靠它
  (uuid4 字典序 ≠ 时间序, 不能拿 list_keys 末位当 "最新").
- 同 task_id 可有多个 run_id 的 checkpoint (original / resume / replay),
  resume 复用同一 run_id.
- run_id 必填 — 无 legacy task 级单 cp lane (旧 key runs/{task_id}/checkpoint
  已 orphan). run_id="" 仅在 load_compatible 作 "加载最新" 哨兵.
"""

from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import dataclass, field
from typing import Any

from .dag import DAG
from .state import _state_diff, apply_overrides
from .storage import StorageBackend

__all__ = [
    "Checkpoint",
    "CheckpointMismatchError",
    "CheckpointStore",
    "diff_runs",
    "stage_input_hash",
    "workflow_hash",
]


def workflow_hash(dag: DAG) -> str:
    """DAG 结构的稳定指纹: stage 名 + 依赖 + retries + timeout + 条件边 (R1).

    任何影响执行语义的结构改动 → hash 变 → 旧 checkpoint 拒 resume. **dag 名也计入**
    (改名视为结构变, 见下方注释里的代价说明). 只改 stage 函数体 (fn) 不改结构 →
    hash 不变 (resume 兼容, fn 重新 import 即新代码).
    条件边 (R1, 0.5.5): route_fn 源码 + mapping + max_visits 全计入 — 改路由 =
    结构变 (skip_unchanged/fork 拿陈旧路由是这个特性最大的雷). 无条件边 DAG 的
    hash 行集与 0.5.4 完全一致.
    """
    # dag 名计入指纹 (2026-10-16): 结构相同但**名字不同**的两个 DAG (两条不同
    # 流水线) 此前拿到同一个 hash, 于是 task_id 撞车时 resume 会拿错 cp 且毫无
    # 提示 —— 实测: dag "extract" 的部分完成 cp 被 dag "classify" 顺利 resume,
    # 结果 RunResult.dag_name 报 "classify", 而 state 全是 extract 的数据
    # (by=EXTRACT)。task_id 是幂等键, 拿它续跑的前提是"同一个工作流", 名字是
    # 工作流身份的一部分。
    # **代价**: 改名 = 结构指纹变了 = 旧 cp 不能续跑。这是刻意的 —— 宁可拒绝,
    # 不可静默混流; 想要续跑就把名字改回去 (或换 task_id)。
    rows = [f"dag={dag.name}"]
    # dag.stages 是每次访问都建副本的 property —— 循环里取它 = 每个 stage 拷一次
    # (2026-10-17: 与 _run_inner 的两个热位置同一类问题)。一次提到循环外。
    _stages = dag.stages
    for name in dag.topo_order():
        s = _stages[name]
        rows.append(f"{name}:dep={','.join(s.depends_on)}:retry={s.retries}:to={s.timeout}")
    edges = dag.conditional_edges
    for fname in sorted(edges):
        e = edges[fname]
        mapping = ",".join(f"{k}->{v}" for k, v in sorted(e.mapping.items()))
        rows.append(
            f"{fname}:cond={mapping}:mv={e.max_visits}:fn={_stage_source_id(e.route_fn)}"
        )
    raw = "\n".join(rows)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _src_of(target) -> str | None:
    """inspect.getsource, 但只把"拿不到"当 None; 其他异常照抛 (由调用方兜)."""
    try:
        return inspect.getsource(target)
    except (OSError, TypeError):
        return None


def _code_fingerprint(target) -> str | None:
    """字节码指纹; 可调用对象取它的 __call__ (实例本身没有 __code__)."""
    for t in (target, getattr(target, "__call__", None)):
        if t is None:
            continue
        code = getattr(t, "__code__", None)
        if code is not None:
            return (code.co_code.hex() + "|" + repr(code.co_consts)
                    + "|" + repr(code.co_names))
        s = _src_of(t)
        if s is not None:
            return s
    return None


def _stage_source_id(fn) -> str:
    """stage / route fn 的源码指纹. 动态构造 (getsource 失败) 退化为字节码指纹.

    剥掉装饰器行: getsource 返回的块含 @dag.stage(...) — DAG 变量名/装饰
    参数文本会污染指纹 (改个变量名 → 无谓 hash 变化 → 无谓重跑).
    fallback 用 co_code + co_consts + co_names (仅 co_code 会对不同函数体
    产生相同字节码序列, 常量/名字不在其中).

    **本函数绝不能抛** —— 结果喂给 workflow_hash, 而后者出现在 _save_cp /
    resume 守卫 / fork_run / diff_runs / load_compatible / replay_from 里。
    抛异常的代价不是"指纹算不出", 而是"**run 报 done 但 checkpoint 根本没落盘**"
    (2026-10-16 实测: `class Router: def __call__` 当 route_fn → getsource 抛
    TypeError, 老 fallback 取 `fn.__code__` 又抛 AttributeError → _save_cp 的
    "失败不杀 run" 兜底把它吞掉 → 每次 save 都失败而 run 报成功; 事后 resume /
    replay / trace / fork / diff 全部 unavailable, 只剩一行没人读的日志)。

    四层兜底, 层层都不会抛:
      ① `inspect.getsource(fn)` (剥装饰器行)
      ② 字节码指纹 (fn 或它的 `__call__`)
      ③ 类型限定名 —— 跨进程稳定 (不能用 id(): 两个进程算出的 hash 不同, cp 就
         互相认不出来了)
      ④ ③ 的 again, 包在 try 里 —— 自定义 `__getattr__` 抛非 AttributeError 时
         连 ③ 都取不到属性 (getattr 的默认值对它无效)
    """
    try:
        src = _src_of(fn)
        if src is not None:
            lines = src.splitlines(keepends=True)
            for i, ln in enumerate(lines):
                if ln.lstrip().startswith(("async def", "def ")):
                    return "".join(lines[i:])
            return src
        code = _code_fingerprint(fn)
        if code is not None:
            return code
    except Exception:
        pass  # 探测属性时对方自己炸了 → 落到类型名兜底
    try:
        return f"<{type(fn).__module__}.{type(fn).__qualname__}>"
    except Exception:
        return "<unknown>"





def stage_input_hash(fn, before_state: dict) -> str:
    """stage 执行输入指纹 = fn 源码 + 执行前全量 state (R2, 0.5.3).

    改 fn (如改 prompt) 或上游产出变化 → hash 变 → fork/resume 侧判定需重跑;
    两者都没变 → 下游可安全跳过 (skip_unchanged). 纯函数语义由 caller 保证
    (副作用 stage 标 Stage.skip_unchanged=False 拒跳).
    """
    raw = _stage_source_id(fn) + "|" + json.dumps(
        before_state, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def dag_mismatch_msg(task_id: str, cp: Checkpoint, dag: DAG, *, action: str,
                     hint: str = "") -> str:
    """DAG 指纹不匹配的报错文案 —— **唯一构造点**.

    三处 (resume / fork_run / TestPipe.replay_from) 各写一份, 措辞已经漂过一次:
    runtime 那份少了 "只改 stage 函数体不影响 hash" 的提示, testing 那份少了
    "强制重跑" 的出路。而 dag 名计入指纹后 (2026-10-16), 三处都该提"名字不同"
    这条最常见的原因 —— 三处各改一遍就是下一轮漂移的起点。

    `action` = 接下来该做什么 ("不能 resume" / "不能 replay"), `hint` = 该入口特有
    的出路说明。
    """
    name_differs = cp.dag_name != dag.name
    cause = (
        f"尤其注意: checkpoint 属于 DAG '{cp.dag_name}', 你是拿 DAG '{dag.name}' "
        f"来续的 —— 两者只是结构相同 (2026-10-16 起 dag 名计入指纹, 就是为了拦住"
        f"这种跨流水线混流). "
        if name_differs else
        "常见原因: 改了 stage 依赖/retries/timeout/条件边, 或**改了 dag 名** "
        "(名字计入指纹). 只改 stage 函数体 (fn) 不影响 hash. "
    )
    return (
        f"task {task_id} run {cp.run_id[:8]} checkpoint 的 DAG hash "
        f"{cp.workflow_hash} ≠ 当前 DAG hash {workflow_hash(dag)}. {cause}"
        f"{action}.{hint}"
    )


class CheckpointMismatchError(Exception):
    """checkpoint 的 DAG hash ≠ 当前 DAG hash. 结构变了, 不能续跑."""


def diff_runs(cp_a: Checkpoint, cp_b: Checkpoint) -> dict:
    """两个 run 的 stage 级 structured diff (R4, 0.5.4; id=135 ai-writing).

    输入: 同一 task (或任意) 两个 run 的 checkpoint. 输出全 JSON-serializable
    (R5 闭合), 不做 AI 摘要 — 消费者 (AI agent / 人) 自行解读:
      - workflow_hash_changed: 结构指纹是否不同 (不同则 stage 语义可能不可比)
      - dag_names: [a, b] 两个 run 的 dag 名 (改名是 hash 变化最常见的唯一原因,
        而改名恰恰是最该被判成"这不是真差异"的那种 — 见下)
      - stages: stage 名 → {status: [a, b], input_hash_changed, visits, output_diff}
        (output_diff = 各 stage return delta 的 path 级差异, 复用 _state_diff;
         visits = 两 run 里该 stage 的执行轮次, 循环图上靠它才看得见"多跑了多少")
      - state_diff: 最终 merged state 的 path 级差异

    ⚠️ `output_diff` 用的是 `stage_deltas` —— **last-write 视图** (每个 stage 只取
    最后一次执行的 delta)。循环图上 (R1) 它既漏掉中间轮次, 又会把"终态相同但轮数
    不同"报成有差异。逐轮全貌在 `cp.stage_deltas_visits`, 要自己比就取那个字段。

    ⚠️ `workflow_hash_changed: true` **不说明差在哪** —— hash 是一个整串, 拆不出
    "是哪个 stage 的哪个属性变了"。这是有意的边界: 该字段的作用是叫消费者
    "别拿这两份比", 而不是替它解释。**但改名是例外**, 它必须能被单独判出来 ——
    dag 名自 d21aab8 起就是指纹的一部分, 而改名是最容易被误当成"结构真变了"的
    那种变化 (所有 stage 的 status/visits/output_diff 全都相同)。所以两个名字都
    给出来, 与 `stages[*].status` / `visits` 的 `[a, b]` 约定一致。
    """
    names = set(cp_a.stage_deltas) | set(cp_b.stage_deltas) \
        | set(cp_a.done_stages) | set(cp_b.done_stages)
    stages: dict[str, dict] = {}
    for name in sorted(names):
        ha = cp_a.stage_input_hashes.get(name)
        hb = cp_b.stage_input_hashes.get(name)
        stages[name] = {
            "status": [cp_a.stage_statuses.get(name), cp_b.stage_statuses.get(name)],
            "input_hash_changed": (ha != hb) if ha and hb else None,
            # 轮次 (R1 条件边引入, 2026-10-16 补): 循环 stage 的**执行次数**是这个
            # 图上最该被看见的量, 而 output_diff / state_diff 都看不见它 ——
            # 实测两个终态逐字节相同的 run (3 轮 vs 5 轮), state_diff 是空的,
            # output_diff 只比出"最后一次 visit 的 delta 差在哪"这种没用的细节。
            # 不给轮次, 消费者无从知道"这个 run 多干了 67% 的活"。
            "visits": [len(cp_a.stage_deltas_visits.get(name) or []),
                       len(cp_b.stage_deltas_visits.get(name) or [])],
            "output_diff": _state_diff(
                cp_a.stage_deltas.get(name) or {}, cp_b.stage_deltas.get(name) or {}),
        }
    return {
        "run_id_a": cp_a.run_id,
        "run_id_b": cp_b.run_id,
        "dag_name": cp_a.dag_name,          # 保留: 单边场景 (只关心 a) 下够用
        # 两个都给 (2026-10-16 补): dag 名自 d21aab8 起入指纹, 所以**改名**就会让
        # workflow_hash_changed 变 true —— 而改名恰恰是最该被判成"这不是真差异"
        # 的那种 (实测: 两个 run 唯一差别是 dag 名时, stages 里每项都显示"无差异",
        # 消费者只能得出"结构变了但行为没变"这个错误结论)。单看 dag_name 那一个
        # 字段看不见 b 叫什么, 于是那个最常见的假警报无法排除。
        "dag_names": [cp_a.dag_name, cp_b.dag_name],
        "workflow_hash_changed": cp_a.workflow_hash != cp_b.workflow_hash,
        "stages": stages,
        "state_diff": _state_diff(cp_a.state, cp_b.state),
    }


@dataclass
class Checkpoint:
    """一次 runtime.run() (一个 run_id) 的持久化状态.

    v0.8 (2026-09-06, user: 发布前向前看, 旧数据记录可弃): **state 不落盘** —
    全量 state 是 stage_deltas + initial_state 的确定性函数 (_rebuild_state),
    双份落盘 = 2x 体积 (真实业务 cp 2.8MB 量级, state 冗余约占一半). 序列化层
    只存增量 + 元数据; state 变 property (load 后惰性重建).
    """

    task_id: str
    run_id: str
    dag_name: str
    workflow_hash: str
    stage_statuses: dict[str, str]
    done_stages: list[str]  # 按完成顺序
    # v0.1.1: state key → producer stage. 链式覆盖判定用.
    producers: dict[str, str] = field(default_factory=dict)
    # v0.5.1 (M2): 每 stage 的原始 return delta + run 的初始 state.
    initial_state: dict[str, Any] = field(default_factory=dict)
    # R1 (0.5.5): stage 名 → 历次执行的 delta 列表 (追加序 = 执行序). 循环里同一
    # stage 多轮执行各有 delta — resume 重建按 done_stages 完成序逐 visit 折叠,
    # last-write dict 会把中间轮顺序折叠错. 无循环图每 stage 恰 1 条, 体积不变.
    stage_deltas_visits: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    # v0.7: stage 完成 epoch ts — 恢复侧内容过期 (TTL) gate 判定用.
    stage_ts: dict[str, float] = field(default_factory=dict)
    # v0.8: fork_run 的 overrides — fork cp 专用 (前序 keep 之上最后 merge).
    # 曾靠全量 state 落盘持久化; state 不落盘后 overrides 必须显式存, 否则
    # fork resume 时 rebuild 丢 overrides → 从 from_stage 重跑用旧输入 (bug).
    fork_overrides: dict[str, Any] = field(default_factory=dict)
    # R2 (0.5.3): stage 名 → 执行输入 hash (fn 源码 + 执行前 state). fork
    # skip_unchanged 判定用; 旧 cp 无记录时跳过逻辑自然退化为"全重跑" (A7 .get 默认).
    stage_input_hashes: dict[str, str] = field(default_factory=dict)
    # R2 (0.5.5, ai@home id=272): fork 截断点前各 stage 的出现次数 (keep 计数).
    # 只在 fork 产生的 cp 上非空. _rebuild_state 据此把截断点后的 done 条目与
    # visits 列表 *尾部* 对齐 (截断点前顺序对齐) — fork 重跑循环 stage 时
    # 折叠不串轮 (历史 visit 与新 visit 混排的正确配对).
    fork_keep_counts: dict[str, int] = field(default_factory=dict)
    # R2 (id=433 评审): route 重入待重跑集 — router 回跳已执行 stage 时, 其静态
    # 下游闭包中被 done_stages 拦截的成员 (循环静态链第二圈要重跑). 运行态为 set,
    # 落盘转 list (JSON). resume 中段续跑依赖它; 非 loop run 恒空 (A7 默认).
    # skip_unchanged 跳过的 stage 名 (结果从 cp 重放, fn 没被调用). 落盘是因为
    # `pavos trace` 只读 cp —— 不记的话它会拿 stage_statuses 的 "done" 报告"都跑了".
    # 旧 cp 无此字段 → 空 (A7 默认): 老 run 的跳过记录无从恢复, 但不影响任何行为.
    stage_skipped: list[str] = field(default_factory=list)
    reset_pending: list[str] = field(default_factory=list)
    # R2 (id=1132): stage 名 → 上次真实执行的 fn 源码指纹 (_stage_source_id).
    # fn 体不计 workflow_hash 是有意的 (改 prompt 免全链重跑), 代价是"改了代码
    # 跑的是新是旧"不可见 — 本字段让 fork/replay 能比对漂移, log 一行提示,
    # 不阻断不拒续. 只在真实执行时更新 (skip_replay 重放不覆盖, 它代表产出
    # 该结果的代码). 旧 cp 无此字段 → 无漂移检查, A7 默认空.
    stage_fn_sources: dict[str, str] = field(default_factory=dict)

    @property
    def state(self) -> dict[str, Any]:
        """全量 merge 后 state — 从 initial_state + stage_deltas_visits 惰性重建.

        不落盘 (v0.8 瘦身), 每次访问现算 (纯内存 dict merge, 2MB 级 ~10ms).
        """
        return self._rebuild_state()[0]

    @property
    def stage_deltas(self) -> dict[str, dict[str, Any]]:
        """每 stage 最后一次执行的 delta (last-write 视图).

        skip_unchanged 重放 / diff_runs / M3 单 stage replay 按名取 delta 用.
        循环多轮的完整历史在 stage_deltas_visits (R1) — state 重建必须走
        done_stages 完成序逐 visit 折叠, 不能用本视图 (顺序错).
        """
        return {
            name: deltas[-1]
            for name, deltas in self.stage_deltas_visits.items() if deltas
        }

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "run_id": self.run_id,
            "dag_name": self.dag_name,
            "workflow_hash": self.workflow_hash,
            "stage_statuses": self.stage_statuses,
            "done_stages": self.done_stages,
            "producers": self.producers,
            "initial_state": self.initial_state,
            "stage_deltas_visits": self.stage_deltas_visits,
            "stage_ts": self.stage_ts,
            "fork_overrides": self.fork_overrides,
            "stage_input_hashes": self.stage_input_hashes,
            "stage_skipped": self.stage_skipped,
            "fork_keep_counts": self.fork_keep_counts,
            "reset_pending": self.reset_pending,
            "stage_fn_sources": self.stage_fn_sources,
        }

    @classmethod
    def from_dict(cls, d: dict) -> Checkpoint:
        # run_id 必填 (旧无 run_id 数据已 hard cut orphan); 以后加新字段一律 .get 默认 (A7)
        # stage_deltas: 0.5.4 旧格式 (last-write dict) → 包装成单 visit 列表 (R1 兼容读)
        visits = d.get("stage_deltas_visits") or {}
        if not visits:
            visits = {k: [v] for k, v in (d.get("stage_deltas") or {}).items()}
        return cls(
            task_id=d["task_id"],
            run_id=d["run_id"],
            dag_name=d["dag_name"],
            workflow_hash=d["workflow_hash"],
            stage_statuses=d["stage_statuses"],
            done_stages=d["done_stages"],
            producers=dict(d.get("producers") or {}),
            initial_state=dict(d.get("initial_state") or {}),
            stage_deltas_visits={k: list(v) for k, v in visits.items()},
            stage_ts=dict(d.get("stage_ts") or {}),
            fork_overrides=dict(d.get("fork_overrides") or {}),
            stage_input_hashes=dict(d.get("stage_input_hashes") or {}),
            fork_keep_counts=dict(d.get("fork_keep_counts") or {}),
            reset_pending=list(d.get("reset_pending") or []),
            stage_skipped=list(d.get("stage_skipped") or []),
            stage_fn_sources=dict(d.get("stage_fn_sources") or {}),
        )

    def rebuild_state(self) -> dict[str, Any]:
        """重建所有已完成 stage merge 后的 state (给未完成的 stage 当执行前 state).

        重放"原始 run 失败/中断的 stage"用 — 顺序执行 + 首败即停下,
        未完成 stage 的依赖必已全部完成, 全量 merge done deltas 即其执行前 state.

        Returns: 新 dict (不 mutate).
        """
        return self.rebuild_state_and_producers()[0]

    def rebuild_state_and_producers(self) -> tuple[dict[str, Any], dict[str, str]]:
        """重建全量 merge 后的 (state, producers) — producers 为执行时点值.

        未完成但依赖已完成的 stage 重放用 (run_stage): 与 rebuild_state 同一次
        遍历收集 producers (initial keys → "<init>"; merged delta 的 keys → 其 stage).
        """
        return self._rebuild_state()

    def rebuild_state_before(self, stage_name: str) -> dict[str, Any]:
        """重建 stage 执行前的 state = initial + 已完成且在该 stage 前的 deltas.

        顺序按 done_stages 完成序 merge (dict.update) — chain-overwrite 由
        完成顺序天然处理 (后完成者覆盖先完成者). 要求 stage_name 的依赖已全完成
        (即 stage_name in done_stages 或它的 depends_on ⊆ done_stages) —
        否则它之前的 delta 不齐, raise KeyError.

        Returns: 新 dict (不 mutate).
        """
        return self.rebuild_state_and_producers_before(stage_name)[0]

    def rebuild_state_and_producers_before(
        self, stage_name: str
    ) -> tuple[dict[str, Any], dict[str, str]]:
        """重建 stage 执行前的 (state, producers) — producers 按执行时点重建.

        cp.producers 是终态 (chain-overwrite 后指向最后写者); 重放中间 stage 的
        覆盖判定必须用"该 stage 执行时点"的 producer — 这里随 state 重建同步
        收集: initial keys → "<init>", 已 merge delta (stage X) 的 keys → X.
        """
        if stage_name not in self.done_stages and stage_name not in self.stage_deltas:
            raise KeyError(
                f"stage '{stage_name}' 不在 cp 的完成记录里 "
                f"(done_stages={self.done_stages}). 无法重建其执行前 state."
            )
        return self._rebuild_state(stop_at=stage_name)

    def _rebuild_state(self, stop_at: str | None = None) -> tuple[dict[str, Any], dict[str, str]]:
        """initial + done_stages 完成序 deltas 的 merge + 执行时点 producers.

        stop_at 命中断 (不含). Returns: (state, producers) — producers 记录每个
        merged delta 的 keys → 其 stage; initial keys → "<init>".
        """
        rebuilt = dict(self.initial_state)
        producers = {k: "<init>" for k in self.initial_state}
        # fork_overrides 的落点 (2026-10-16): 必须是"**跨过 fork 截断点的那一刻**",
        # 不是重建的最后。此前一律最后 merge, 与 runtime 的顺序相反 ——
        # runtime 里 overrides 是**注入给 from_stage 的输入**, from_stage 重跑后
        # 可以覆盖它; 而重建最后 merge 会把刚跑出来的值又盖回 override 值。
        # 同一把钥匙开两把锁: runtime 说 a='A', cp 说 a='A9' (tests/
        # test_rebuild_consistency.py::test_fork_override_lost_when_rerun_
        # stage_overwrites_it)。同理 stop_at 落在截断点之后时也该生效 ——
        # run_stage 重放 from_stage 时此前根本看不到注入值。
        _pending_overrides = dict(self.fork_overrides) if self.fork_overrides else None
        # 只有**知道截断点**的 cp (fork_keep_counts 非空, 新格式) 才按边界注入。
        # 旧 cp (fork_overrides 存在但无 kcounts) 无从判断边界 —— 那就保持
        # "最后 merge" 的旧语义, 否则升级后老 checkpoint 的 state 会平白改写
        # (tests/test_checkpoint_size.py::test_fork_overrides_roundtrip_and_rebuild
        #  钉的就是这条)。
        # R1: done_stages 完成序可含重复 (循环) — 逐 visit 消费对应 delta,
        # 保证重建顺序 = 实际执行顺序 (last-write dict 在此会折叠错序)
        visit_idx: dict[str, int] = {}
        total: dict[str, int] = {}
        kcounts = self.fork_keep_counts
        # 非空 = 这是 fork cp (从第一个 stage fork 时 keep 为空, 故由 fork_run 显式
        # 记 0 来标识)。普通 run 的 kcounts 恒空 → 走顺序索引, 行为逐字节不变。
        is_fork_cp = bool(kcounts)
        if is_fork_cp:
            for done in self.done_stages:  # fork cp: 每 stage 总条目数 (尾对齐用)
                total[done] = total.get(done, 0) + 1
        stop_hit = False  # stop_at 是否真的在完成序里出现过 (见下方兜底判定)
        for done in self.done_stages:
            occ = visit_idx.get(done, 0)  # 本 stage 第几个 done 条目 (0 起)
            # fork cp 里"没条目"= 全部重跑 (kc=0), 不是"顺序索引" —— 这两件事以前
            # 被同一个 None 混为一谈, 正是 id=3241 的根因。
            # `else None` 是**刻意的哨兵**, 不是可以顺手清掉的死分支: 外层虽然
            # 已经判了 `is_fork_cp`, 但下面两处 (`kc is None` / `kc is not None`)
            # 仍靠它把"非 fork ⇒ 顺序索引"写成一件事。留着可读性更好; 真要合
            # 成 `if is_fork_cp` 的写法, 记得连带把两处判据一起改, 别只改一处。
            kc = kcounts.get(done, 0) if is_fork_cp else None
            # 注入点判定必须在 stop_at 的 break **之前**: overrides 是注入给
            # from_stage 的输入, 而"某 stage 执行前的快照" (stop_at 命中 from_stage)
            # 恰好就是它们该出现的那个点。放在 break 之后, run_stage 重放 from_stage
            # 就看不到注入值 (实测: 期望 C[F7], 实得 C[none])。
            if _pending_overrides is not None and is_fork_cp \
                    and (kc is None or occ >= kc):
                rebuilt = apply_overrides(rebuilt, _pending_overrides)
                for _k in _pending_overrides:
                    producers[_k] = "<fork>"
                _pending_overrides = None
            if done == stop_at:
                stop_hit = True
                break
            deltas = self.stage_deltas_visits.get(done, [])
            visit_idx[done] = occ + 1
            if kc is not None and occ >= kc:
                # fork 截断点后的条目: 与 visits 尾部对齐 (该 cp 重跑产生的新
                # visit 追加在历史之后) — 顺序对齐会错配到截断前的旧 delta.
                idx = len(deltas) - (total[done] - occ)
            else:
                idx = occ  # 截断点前 (或非 fork cp): 顺序对齐
            if idx < 0 or idx >= len(deltas):
                continue  # done 记录多于 delta (旧格式/手工残缺) → 跳过该 visit
            delta = deltas[idx]
            if delta:
                rebuilt.update(delta)
                for k in delta:
                    producers[k] = done
        # 没走到截断点 (done 全在 keep 内 / 全部被跳过) → overrides 仍要生效,
        # 落在最后。
        #
        # 判据是"stop_at **命中过**"而不是"stop_at is None" (2026-10-16 修)。
        # 两者在正常形状下等价 (stop_at 命中即 break, 未命中即走完), 但有一种
        # 形状下旧判据会错: stop_at 指向的 stage 在**本次 run 里还没完成**, 它
        # 只存在于被 fork 截断掉的历史 stage_deltas 里 —— rebuild_state_before
        # 的守卫按 deltas 放行了它, 而循环走的是 done_stages, 于是 stop_at 永远
        # 不命中, 兜底却被 `stop_at is None` 挡掉 → fork_overrides 静默丢失。
        # 实测: fork 的重跑在中途失败, export-input 少给注入值且 rc=0。
        # 命中过 = 那是真的回到"某 stage 执行前"的快照, overrides 那时还没注入;
        # 没命中 = 停在"这次 run 走到的最后", runtime 早就把 overrides 注进了
        # state, 重建反而比 cp.state 少东西是退步。
        if _pending_overrides is not None and not stop_hit:
            # v0.9 深合并: 浅 update 会把嵌套 dict 的兄弟键抹掉 (resume 重建才是
            # stage 实际看到的 state — 与 runtime.fork_run 应用语义保持一致)
            rebuilt = apply_overrides(rebuilt, _pending_overrides)
            for k in _pending_overrides:
                producers[k] = "<fork>"
        return rebuilt, producers

    def state_stats(self, top_n: int = 10) -> dict:
        """state 体积观测 (#36): 各 key json 体积 top N + 总构成. 调试大 state 用."""
        import json as _json

        def _size(o: Any) -> int:
            try:
                return len(_json.dumps(o, ensure_ascii=False, default=str))
            except (TypeError, ValueError):  # 不可序列化 → 记 -1
                return -1

        _sizes = {k: _size(v) for k, v in self.state.items()}
        _top = sorted(_sizes.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
        return {
            "state_keys": len(self.state),
            "state_total_bytes": sum(_sizes.values()),
            "initial_state_bytes": _size(self.initial_state),
            "deltas_bytes": _size(self.stage_deltas),
            "done_stages": list(self.done_stages),
            "top_keys": [{"key": k, "bytes": v} for k, v in _top],
        }


class CheckpointStore:
    """Per-task, per-run checkpoint 持久化.

    Key format: runs/{task_id}/{run_id}/checkpoint (run_id 必填)
    + 指针 runs/{task_id}/latest = {"run_id": ...}.
    """

    def __init__(self, storage: StorageBackend):
        self.storage = storage

    @staticmethod
    def _key(task_id: str, run_id: str) -> str:
        """run 级 key: runs/{task_id}/{run_id}/checkpoint."""
        return f"runs/{task_id}/{run_id}/checkpoint"

    @staticmethod
    def _latest_key(task_id: str) -> str:
        """最新 run 指针文件 key. 内容 = {"run_id": "<latest>"}."""
        return f"runs/{task_id}/latest"

    def save(self, cp: Checkpoint) -> None:
        self.storage.put(self._key(cp.task_id, cp.run_id), cp.to_dict())
        # 指针: 永远指向最后保存的 run (uuid4 字典序 ≠ 时间序, 不能靠排序).
        self.storage.put(self._latest_key(cp.task_id), {"run_id": cp.run_id})

    def load(self, task_id: str, run_id: str) -> Checkpoint | None:
        """读指定 run 的 checkpoint."""
        d = self.storage.get(self._key(task_id, run_id))
        return Checkpoint.from_dict(d) if d else None

    def list_runs(self, task_id: str) -> list[str]:
        """列 task 的所有 run_id (字典序, UUID4 下 ≠ 时间序).

        调试/清理用. 不含指针文件 — resume 一律走 load_latest (指针),
        不要拿排序当 "最新".
        """
        prefix = f"runs/{task_id}/"
        keys = self.storage.list_keys(prefix)
        run_ids: list[str] = []
        for k in keys:
            tail = k[len(prefix):]
            if tail.endswith("/checkpoint"):
                run_ids.append(tail[: -len("/checkpoint")])
        return sorted(run_ids)

    def list_tasks(self) -> list[str]:
        """列 storage 里出现过的所有 task_id (字典序). 跨 task 巡检/后台用 (R1b)."""
        out: set[str] = set()
        for k in self.storage.list_keys("runs/"):
            parts = k.split("/")
            if len(parts) >= 3:
                out.add(parts[1])
        return sorted(out)

    def diff_runs(self, task_id: str, run_id_a: str, run_id_b: str = "") -> dict:
        """两个 run 的 stage 级 diff (R4, 0.5.4). run_id_b="" → latest 指针.

        diff_runs(cp_a, cp_b) 的便捷包装: 加载两个 checkpoint 后委托.
        任一 run 无 checkpoint → RuntimeError (caller 明确收到, 不静默空 diff).
        """
        cp_a = self.load(task_id, run_id_a)
        cp_b = self.load(task_id, run_id_b) if run_id_b else self.load_latest(task_id)
        if cp_a is None or cp_b is None:
            missing = run_id_a if cp_a is None else (run_id_b or "latest")
            raise RuntimeError(
                f"task_id={task_id} run={missing} 无 checkpoint, 无法 diff"
            )
        return diff_runs(cp_a, cp_b)

    def load_latest(self, task_id: str) -> Checkpoint | None:
        """按指针文件加载该 task 最新 run 的 checkpoint.

        指针指向的 run 已被删 → load 返 None (边界: caller 收到 "无 checkpoint").
        """
        d = self.storage.get(self._latest_key(task_id))
        if d is None:
            return None
        return self.load(task_id, d["run_id"])

    def delete(self, task_id: str, run_id: str) -> None:
        """删指定 run 的 checkpoint.

        指针不随删除更新 (A2): 删的恰是指针指向的 run → 后续 load_latest
        读到已删 run → 返 None → caller 收到明确 "无 checkpoint".
        """
        self.storage.delete(self._key(task_id, run_id))

    def prune(self, task_id: str, *, keep_last: int, dry_run: bool = False) -> dict:
        """按保留数修剪旧 run 的 checkpoint (R1, 2026-09-10 ai-research PR).

        语义:
        - 排序 = max(stage_ts) (run 最后活动时间, v0.7 epoch); 无 stage_ts 的 run
          视为最旧. 最新 keep_last 个保留; **latest 指针指向的 run 无条件保留**
          (resume/fork 基线), 指针文件本身不动.
        - 被删 run 的后续行为 = 既有边界: load(task, rid)→None, load_latest 在
          指针指向被删 run 时→None (本实现不会让指针悬空), fork-run 报
          "无 checkpoint, 不能 fork".
        - 删除以 key 为粒度 (StorageBackend.delete), 跨 task 不触碰.
        - approx_bytes = 重新序列化 JSON 的字节数 (近似值, dry-run 预估用).
- FileStorage 只删 key 文件, 可能残留空目录 (0 字节, 无碍).

        不做: max_runs_per_task 自动修剪 (引擎不替客户决定保留策略,
        v0.4 storage adapters REVERSED 同一先例); 跨 task GC / 引用计数.
        """
        if keep_last < 0:
            raise ValueError(f"keep_last 必须 >= 0, got {keep_last}")
        run_ids = self.list_runs(task_id)
        latest_d = self.storage.get(self._latest_key(task_id))
        latest_run = (latest_d or {}).get("run_id")

        def _mtime(rid: str) -> tuple[float, str]:
            cp = self.load(task_id, rid)
            ts = max(cp.stage_ts.values()) if cp and cp.stage_ts else 0.0
            return (ts, rid)

        ordered = sorted(run_ids, key=_mtime)  # 旧 → 新
        keep = set(ordered[-keep_last:]) if keep_last > 0 else set()
        if latest_run:
            keep.add(latest_run)
        victims = [r for r in ordered if r not in keep]

        deleted: list[dict] = []
        freed = 0
        for rid in victims:
            raw = self.storage.get(self._key(task_id, rid))
            size = len(json.dumps(raw).encode("utf-8")) if raw else 0
            if not dry_run:
                self.delete(task_id, rid)
            deleted.append({"run_id": rid, "approx_bytes": size})
            freed += size
        kept = [r for r in ordered if r in keep]
        return {
            "task_id": task_id,
            "dry_run": dry_run,
            "protected_latest": latest_run,
            "kept": kept,
            "deleted": deleted,
            "approx_bytes_freed": freed,
        }

    def load_compatible(self, task_id: str, run_id: str, dag: DAG) -> Checkpoint | None:
        """加载 + 校验 workflow hash. mismatch → CheckpointMismatchError.

        run_id="" → 加载该 task 最新 run (load_latest 哨兵).
        """
        if run_id:
            cp = self.load(task_id, run_id)
        else:
            cp = self.load_latest(task_id)
        if cp is None:
            return None
        cur_hash = workflow_hash(dag)
        if cp.workflow_hash != cur_hash:
            raise CheckpointMismatchError(dag_mismatch_msg(
                task_id, cp, dag, action="DAG 结构变了, 不能 resume",
                hint=" 如需强制重跑: 删 checkpoint 或 Runtime(..., resume=False)"))
        return cp
