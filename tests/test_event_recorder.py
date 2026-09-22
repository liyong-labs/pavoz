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
