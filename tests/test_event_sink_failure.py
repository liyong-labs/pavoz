"""EventRecorder 的 sink 失败必须是**一次**事故, 不是每个事件一次.

起因 (2026-10-17 实测): sink 指向不可写的目录时, 一个 N 个 stage 的 run 吐
**2N+2 条完整 traceback**, run 照样报 done、磁盘上一个字节没有:

    stages      1     5    30   100
    traceback   4    12    62   202        ← 每事件一条, 线性

那正是 `_ensure_sink_dir` 当初要杀掉的形状 —— 它的注释原话是"长 run 能刷几千行同样
的错误 (**把真正的错误淹掉**)"。但它只治了 `FileNotFoundError` (建父目录),
`PermissionError` / 只读挂载 / ENOSPC / 路径不是目录 全都原样漏过, 每次事件重试一次。

现在分两类处理, 且**故意不同**:
  · I/O 失败 → sink 判死, 说清理由, 之后不再试 (省掉每事件一次注定失败的 open)
  · 数据侧失败 (序列化) → 只丢**这一行**, sink 继续可用; 但按原因去重, 免得
    同一个循环引用载荷把日志刷爆

判据用**条数**而不是"有没有报错": 修前是 2N+2, 修后是常数。N 取两个值断言条数相同 ——
只断言"少了很多"会被一个把系数从 2 降到 1 的半吊子修复蒙混过去。
"""
from __future__ import annotations

import json
import logging

import pytest

from pavoz import DAG, EventRecorder, Runtime


class TracebackCounter(logging.Handler):
    """数 logger.exception 打出来的完整 traceback 条数。"""

    def __init__(self) -> None:
        super().__init__()
        self.n = 0
        self.all = 0        # 收到的记录总数 —— 用来证明计数器真的接上了

    def emit(self, record: logging.LogRecord) -> None:
        self.all += 1
        if record.exc_info:
            self.n += 1


@pytest.fixture
def counting_logs():
    """数完整 traceback 条数。挂 **root** logger, 跑完自动摘掉。

    ⚠️ 2026-10-17: 这个 fixture 第一版写死了包名去 `getLogger("<手打的包名>")`, 而
    那个名字是错的 —— 计数器于是**一条记录都收不到**, `n <= 2` 恒过, 判据是死的。
    拆掉去重逻辑做变异验证时它仍然全绿, 正是这条恒过把真盲点放了过去。
    教训: 计数器类判据必须先**证明自己数到了东西** (下面 `emit` 里的计数非 0 才
    算数), 光看断言绿不绿不够。挂 root 与包名解耦, 也不再动 propagate。
    """
    counter = TracebackCounter()
    root = logging.getLogger()
    root.addHandler(counter)
    old_level = root.level
    root.setLevel(logging.DEBUG)
    yield counter
    root.removeHandler(counter)
    root.setLevel(old_level)


def _wide_dag(n: int):
    """n 个互不依赖、**各写各的 key** 的 stage (同 key 会撞单生产者规则)。"""
    dag = DAG(f"sinkfail_{n}")
    src = []
    for i in range(n):
        src += ["@dag.stage()", f"async def s{i}(ctx):", f"    return {{'k{i}': {i}}}"]
    exec("\n".join(src), {"dag": dag})          # noqa: S102 — 生成测试用 DAG
    return dag


def _unwritable_sink(tmp_path) -> str:
    """造一个"父路径是个普通文件"的 sink —— 用它触发 I/O 失败, 且**不依赖 uid**
    (root 下 chmod 挡不住写入, 判据不能只在非 root 上成立)。"""
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory", encoding="utf-8")
    return str(blocker / "ev.jsonl")


@pytest.mark.parametrize("n", [2, 16])
async def test_sink_io_failure_logs_once_not_once_per_event(tmp_path, counting_logs, n):
    """★ traceback 条数与 stage 数**无关** (修前是 2N+2: 16 个 stage → 34 条)。"""
    rec = EventRecorder(sink_path=_unwritable_sink(tmp_path))
    r = await Runtime(on_event=rec).run(_wide_dag(n), f"task{n}")

    assert r.status == "done", f"前置: sink 写不了不该影响 run, 实得 {r.status}: {r.error}"
    assert rec.sink_disabled, "I/O 失败后 sink 应当被判死并给出理由"
    assert counting_logs.n <= 2, (
        f"{n} 个 stage 吐了 {counting_logs.n} 条完整 traceback —— "
        f"线性于事件数, 长 run 会把真正的错误淹掉 (见本文件头)")
    assert counting_logs.all > 0, "计数器一条记录都没收到 —— 判据失明, 这条断言恒过"


async def test_sink_io_failure_leaves_no_traceback_for_later_events(tmp_path, counting_logs):
    """判死之后**一次都不再试** —— 否则只是把每事件一次降成每事件一次 open 失败。"""
    rec = EventRecorder(sink_path=_unwritable_sink(tmp_path))
    for i in range(50):                      # 判死后还硬喂 50 个事件
        rec("synthetic", {"i": i})
    assert counting_logs.n <= 2, f"判死后仍在刷 traceback: {counting_logs.n} 条"
    assert counting_logs.all > 0, "计数器一条记录都没收到 —— 判据失明, 这条断言恒过"


def test_data_error_skips_only_that_line_sink_stays_usable(tmp_path):
    """数据侧失败 ≠ sink 坏了: 循环引用只丢那一行, 后面的事件照写。

    这条钉的是两个分支的**区别**。若把数据侧失败也判死 sink, 一次坏载荷就会让
    整场 run 的观测记录全部消失 —— 那比丢几行糟得多 (注释里写的 sink 职责是
    "留住观测记录")。
    """
    sink = tmp_path / "ev.jsonl"
    rec = EventRecorder(sink_path=str(sink))

    circular: dict = {"x": 1}
    circular["self"] = circular
    # 实际先炸的是 `_json_safe` 自己的递归 (RecursionError), 不是 json.dumps 的
    # ValueError —— 它在 json 之前就把循环结构递归到底了。两者都是 Exception, 分支
    # 一样, 但注释得写对, 否则下一个人会去 json.dumps 里找原因。
    rec("stage_start", circular)
    rec("stage_done", {"ok": True})

    assert rec.sink_disabled is None, "数据侧失败不该判死 sink"
    lines = sink.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1, f"只该留下第二条事件, 实得 {len(lines)} 行: {lines}"
    assert json.loads(lines[0])["event"] == "stage_done"


def test_same_data_error_is_not_relogged_every_time(tmp_path, counting_logs):
    """同一个坏载荷连着来 —— 只报一次, 不刷屏。"""
    sink = tmp_path / "ev.jsonl"
    rec = EventRecorder(sink_path=str(sink))
    circular: dict = {}
    circular["self"] = circular
    for _ in range(20):
        rec("stage_start", circular)
    assert counting_logs.n <= 1, f"同一个序列化错误报了 {counting_logs.n} 次"
    assert counting_logs.all > 0, "计数器一条记录都没收到 —— 判据失明, 这条断言恒过"


def test_repr_surfaces_that_sink_is_dead(tmp_path):
    """caller 得能从外面看出"落盘已经停了", 否则它只会以为日志没写而已。"""
    rec = EventRecorder(sink_path=_unwritable_sink(tmp_path))
    rec("e", {})
    assert "DISABLED" in repr(rec)
    assert rec.sink_disabled and rec.sink_disabled in repr(rec)
