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
