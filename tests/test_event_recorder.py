"""W2: EventRecorder — on_event 的收集器 + 可选 JSONL 落盘."""

import json

from pavoz import DAG, EventRecorder, Runtime


async def test_recorder_collects_lifecycle():
    dag = DAG("rec1")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    rec = EventRecorder()
    rt = Runtime(on_event=rec)
    r = await rt.run(dag, "rec1")
    assert r.status == "done"
    assert rec.names()[0] == "run_start"
    assert rec.names()[-1] == "run_end"
    assert rec.names().count("stage_start") == 1
    assert isinstance(rec.events[0][1], dict)


async def test_recorder_jsonl_sink(tmp_path):
    sink = tmp_path / "ev.jsonl"
    dag = DAG("rec2")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    rec = EventRecorder(sink_path=str(sink))
    await Runtime(on_event=rec).run(dag, "rec2")
    lines = sink.read_text().strip().splitlines()
    assert len(lines) == len(rec.events) >= 2
    first = json.loads(lines[0])
    assert first["event"] == "run_start" and "data" in first

# ── sink 必须是"给日志工具读的合法 JSON" (2026-10-16 巡检) ──────────────────
#
# 实测 (修复前): 一个上报 `set_progress(float("nan"))` 的 stage 就能让 sink 的
# 7 行里有 2 行不是合法 RFC 8259 JSON —— Python 的 json 默认 allow_nan=True, 把
# NaN/±Inf 写成字面量。jq / Go / Splunk / Loki 一行都读不了。
#
# 注意与另一件事的区别: `set_progress` 的 fraction **不校验 0~1 是故意的** ——
# 它是 best-effort 瞬态信号, 让一个装饰性错误把业务 stage 带崩更糟。而 sink 是
# pavoz **自己产出的**机器可读产物, 格式该由 pavoz 负责。

import math  # noqa: E402  (放在追加块前, 保持文件原有 import 区不动)


def _strict_lines(path):
    """按 RFC 8259 逐行解析 sink; 非标准常量直接判失败。"""
    def reject(c):
        raise AssertionError(f"非标准 JSON 常量: {c}")
    return [json.loads(line, parse_constant=reject)
            for line in path.read_text(encoding="utf-8").splitlines()]


async def test_sink_stays_valid_json_when_payload_has_nonfinite(tmp_path):
    sink = tmp_path / "ev.jsonl"
    dag = DAG("rec_nonfinite")

    @dag.stage()
    async def s_a(ctx):
        ctx.set_progress(0.5, note="batch 1/2")
        ctx.set_progress(float("nan"))
        ctx.set_progress(float("inf"))
        ctx.set_progress(float("-inf"))
        return {"a": 1}

    rec = EventRecorder(str(sink))
    r = await Runtime(on_event=rec).run(dag, "rec_nonfinite")
    assert r.status == "done", "set_progress 是 best-effort, 坏值不该让 stage 失败"

    rows = _strict_lines(sink)          # 修复前这里会 AssertionError
    prog = [d["data"] for d in rows if d["event"] == "stage_progress"]
    assert len(prog) == 4
    assert [p["fraction"] for p in prog] == [0.5, "nan", "inf", "-inf"], (
        "非有限值在落盘时字符串化, 而不是写成非法 JSON 字面量")


async def test_sink_keeps_ordinary_floats_untouched(tmp_path):
    """反向约束: 修的是"非有限", 不是"浮点" —— 正常值一个字都不该变。"""
    sink = tmp_path / "ev.jsonl"
    dag = DAG("rec_ok_float")

    @dag.stage()
    async def s_a(ctx):
        ctx.set_progress(0.25)
        ctx.set_progress(1.0)
        return {"a": 1}

    rec = EventRecorder(str(sink))
    await Runtime(on_event=rec).run(dag, "rec_ok_float")

    prog = [d["data"] for d in _strict_lines(sink) if d["event"] == "stage_progress"]
    assert [p["fraction"] for p in prog] == [0.25, 1.0]
    assert all(isinstance(p["fraction"], float) for p in prog)


async def test_in_memory_events_keep_the_original_float(tmp_path):
    """只动序列化, 不动进程内看到的值 —— 同一份 payload, 两种表示各取所需。"""
    sink = tmp_path / "ev.jsonl"
    dag = DAG("rec_mem")

    @dag.stage()
    async def s_a(ctx):
        ctx.set_progress(float("nan"))
        return {"a": 1}

    rec = EventRecorder(str(sink))
    await Runtime(on_event=rec).run(dag, "rec_mem")

    mem = [d["fraction"] for n, d in rec.events if n == "stage_progress"]
    assert len(mem) == 1 and math.isnan(mem[0]), "内存里应仍是原始 NaN float"
    disk = [d["data"]["fraction"] for d in _strict_lines(sink)
            if d["event"] == "stage_progress"]
    assert disk == ["nan"]


async def test_sink_handles_nested_nonfinite(tmp_path):
    """嵌套在 dict / list / tuple 里的非有限值也要处理 —— 事件载荷是 caller 自报的,
    引擎不保证它的形状。直接喂 recorder 一个嵌套 payload 来钉这件事。"""
    sink = tmp_path / "ev.jsonl"
    EventRecorder(str(sink))("custom", {
        "m": {"xs": [1.0, float("nan")]},
        "t": (1, float("inf")),
    })

    last = _strict_lines(sink)[-1]
    assert last["data"]["m"]["xs"] == [1.0, "nan"]
    assert last["data"]["t"] == [1, "inf"], "tuple 落盘成 JSON 数组"


# ── sink 的父目录不存在: 每个事件一条 traceback, 而文件一个都没有 ──────────
#
# 实测 (修复前), sink = <tmp>/nope/deep/ev.jsonl:
#   · 每一**个**事件都抛 FileNotFoundError (1 个 stage 的 run 就 5 条完整 traceback)
#   · run 照样 status=done, `recorder.events` 5 条齐全, **磁盘上一个字节没有**
#
# 关键不是"失败了" (它确实 ERROR + traceback, 不是静默), 关键是这个失败是
# **永久性**的却按事件重复报: 长 run 能刷几千行同样的 traceback, 把真正的错误
# 淹掉; 而 run 报 done, 不看 stderr 的人只会以为"日志开着"。
#
# 为什么该修而不是留给调用方自己 mkdir: 同仓的 `_dump_debug` (debug_dir 快照)
# 干的是**同一类事** —— 用户给一个路径, 我们往里写 JSON —— 而它
# `os.makedirs(d, exist_ok=True)` 把 task 子目录建了出来。两个功能对"父目录
# 怎么办"的答案不一致, 而 sink 这条更常踩 (日志路径常常指向还没建的日志目录)。
# 统一到 debug_dir 的口径, 不是新发明。


async def test_sink_creates_missing_parent_directory(tmp_path):
    """父目录不存在 → 照样落盘, 而不是每个事件一条 traceback。"""
    sink = tmp_path / "nope" / "deep" / "ev.jsonl"
    dag = DAG("sink1")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    rec = EventRecorder(sink_path=str(sink))
    r = await Runtime(on_event=rec).run(dag, "sink1")
    assert r.status == "done"
    assert sink.exists(), f"落盘文件没建出来 —— 实得目录内容 {list(sink.parent.iterdir()) if sink.parent.exists() else '(父目录都没建)'}"
    lines = sink.read_text(encoding="utf-8").splitlines()
    assert len(lines) == len(rec.names()), (
        f"每个事件一行, 实得 {len(lines)} 行 / {len(rec.names)} 个事件")
    assert all(json.loads(ln) for ln in lines), "每一行都必须是合法 JSON"


def test_parent_dir_is_created_lazily_not_at_construction(tmp_path):
    """构造时不建目录 —— 没有东西要写之前, 不该留下副作用。"""
    target = tmp_path / "later" / "ev.jsonl"
    EventRecorder(sink_path=str(target))
    assert not target.parent.exists(), (
        "构造 EventRecorder 就有副作用了 —— 一个从不落盘的 recorder 不该建目录")


def test_bare_filename_needs_no_parent_dir(tmp_path, monkeypatch):
    """相对裸文件名 (dirname 为空) 不能去建 '.' 之类的目录。"""
    monkeypatch.chdir(tmp_path)
    rec = EventRecorder(sink_path="ev.jsonl")
    rec("run_start", {"task_id": "t", "run_id": "r"})
    rec("run_end", {"task_id": "t", "run_id": "r"})
    assert (tmp_path / "ev.jsonl").exists()
    assert len((tmp_path / "ev.jsonl").read_text(encoding="utf-8").splitlines()) == 2


async def test_sink_creation_is_done_once_not_per_event(tmp_path):
    """目录只建一次 (每个事件都 makedirs 是白做的系统调用)。"""
    sink = tmp_path / "once" / "ev.jsonl"
    rec = EventRecorder(sink_path=str(sink))
    for i in range(50):
        rec("stage_progress", {"task_id": "t", "run_id": "r", "stage": "s", "i": i})
    assert len(sink.read_text(encoding="utf-8").splitlines()) == 50
    assert rec._sink_dir_ready is True
