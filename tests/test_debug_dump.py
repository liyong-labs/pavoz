"""W2: debug_dir — 每 stage 后把 state 快照落 JSON, caller 报错时 ls 即见."""

import json

from pavoz import DAG, Runtime, StageError


async def test_debug_dir_dumps_per_stage(tmp_path):
    dag = DAG("dbg1")

    @dag.stage()
    async def s_a(ctx):
        return {"a": 1}

    @dag.stage(depends_on=["s_a"])
    async def s_b(ctx):
        return {"b": 2}

    d = tmp_path / "dump"
    r = await Runtime(debug_dir=str(d)).run(dag, "dbg1")
    assert r.status == "done"
    files = sorted(p.name for p in d.iterdir())
    assert len(files) == 2
    assert files[0] == "00_s_a_done.json"
    payload = json.loads((d / files[1]).read_text())
    assert payload["stage"] == "s_b" and payload["state"]["a"] == 1
    assert payload["state"]["b"] == 2 and payload["run_id"] == r.run_id


async def test_debug_dir_on_failure(tmp_path):
    dag = DAG("dbg2")

    @dag.stage()
    async def s_bad(ctx):
        raise StageError("x")

    d = tmp_path / "dump"
    r = await Runtime(debug_dir=str(d)).run(dag, "dbg2")
    assert r.status == "failed"
    files = list(d.iterdir())
    assert len(files) == 1 and files[0].name == "00_s_bad_failed.json"
