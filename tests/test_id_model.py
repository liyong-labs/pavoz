"""stageflow ID model: task_id (caller) + run_id (auto) + attempt (auto int).

Resumes reuse run_id. Storage key includes run_id so multiple runs of
the same task don't overwrite each other.
"""
from stageflow.checkpoint import Checkpoint, CheckpointStore, workflow_hash
from stageflow.dag import DAG
from stageflow.storage import FileStorage
from stageflow.types import RunResult

# ── RunResult ──────────────────────────────────────────────

def test_run_result_has_run_id():
    """RunResult must expose run_id."""
    rr = RunResult(task_id="t-1", dag_name="d", run_id="run-abc")
    assert rr.run_id == "run-abc"


def test_run_result_repr_includes_run_id():
    """RunResult repr should include short run_id for log readability."""
    rr = RunResult(task_id="t-1", dag_name="d", run_id="abcdef1234567890")
    r = repr(rr)
    assert "abcdef12" in r


# ── Checkpoint schema ─────────────────────────────────────

def test_checkpoint_has_run_id_field():
    """Checkpoint must carry run_id (each runtime.run() = one cp)."""
    dag = DAG("d")

    @dag.stage()
    async def s_x(ctx):
        return {}

    cp = Checkpoint(
        task_id="t-1",
        run_id="run-abc",
        dag_name="d",
        workflow_hash=workflow_hash(dag),
        stage_statuses={},
        state={},
        done_stages=[],
    )
    d = cp.to_dict()
    assert d["run_id"] == "run-abc"


def test_checkpoint_storage_key_includes_run_id():
    """Storage key format: runs/{task_id}/{run_id}/checkpoint."""
    storage = FileStorage(root_dir="/tmp/sf-test-cp-key")
    store = CheckpointStore(storage)
    key = store._key("task-1", "run-abc")
    assert key == "runs/task-1/run-abc/checkpoint"
