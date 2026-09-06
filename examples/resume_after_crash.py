"""Example 3 — crash recovery: resume skips finished stages.

Run 1 crashes inside s_process (simulated). Run 2 with resume=True starts
from s_process — s_fetch is NOT re-executed (its checkpointed delta is
reused), so its expensive work survives the crash.

Run:  python examples/resume_after_crash.py          # crashes at stage 2
      python examples/resume_after_crash.py --resume  # continues from stage 2
"""

import asyncio
import sys

from stageflow import CheckpointStore, DAG, FileStorage, Runtime

dag = DAG("resume")

CRASHED = {"fetch_ran": False}   # survives in-process; a real crash loses it
                                 # — which is exactly what the checkpoint saves


@dag.stage()
async def s_fetch(ctx):
    CRASHED["fetch_ran"] = True
    return {"items": ["expensive", "upstream", "work"]}


@dag.stage(depends_on=["s_fetch"])
async def s_process(ctx):
    if not sys.argv[1:] == ["--resume"]:
        raise RuntimeError("simulated crash before stage 3")
    return {"summary": f"processed {len(ctx.state['items'])} items"}


@dag.stage(depends_on=["s_process"])
async def s_save(ctx):
    print("done:", ctx.state["summary"])
    return {"saved": True}


async def main() -> None:
    rt = Runtime(checkpoint_store=CheckpointStore(FileStorage("./data")))
    result = await rt.run(dag, task_id="resume-1", resume=bool(sys.argv[1:] == ["--resume"]))
    if result.status == "failed":
        print(f"run failed at {result.error!r} — retry with --resume")
        print("note: s_fetch's checkpoint survived; --resume will skip it")
        return
    print("fetch re-ran on resume?", CRASHED["fetch_ran"])


if __name__ == "__main__":
    asyncio.run(main())
