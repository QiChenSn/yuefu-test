from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from yuefu_test.api import StatusResult, SubmitResult
from yuefu_test.runner import RunConfig, run_workflow


def _make_parquet(tmp_path: Path) -> Path:
    struct_type = pa.struct([("bytes", pa.binary()), ("path", pa.string())])
    table = pa.table(
        {
            "filename": pa.array(["take.png"]),
            "image": pa.array([{ "bytes": b"123", "path": "" }], type=struct_type),
            "transcription": pa.array(["score"]),
        }
    )
    target = tmp_path / "sample.parquet"
    pq.write_table(table, target)
    return target


class FakeClient:
    def __init__(self, status_sequences: list[list[str | None]]):
        self.status_sequences = status_sequences
        self.submissions = 0
        self.pending: dict[str, list[str | None]] = {}

    def submit(self, engine: str, filename: str, content: bytes) -> SubmitResult:  # noqa: ARG002
        task_id = f"task-{self.submissions}"
        self.pending[task_id] = list(self.status_sequences[self.submissions])
        self.submissions += 1
        return SubmitResult(engine=engine, task_id=task_id, response={"data": {"taskId": task_id}})

    def fetch_status(self, task_id: str) -> StatusResult:
        queue = self.pending[task_id]
        status = queue.pop(0) if queue else "SUCCESS"
        return StatusResult(task_id=task_id, status=status, response={"data": {"status": status}})


def test_run_workflow_succeeds_within_first_attempt(monkeypatch, tmp_path):
    monkeypatch.setattr("yuefu_test.runner.random.choice", lambda choices: choices[0])
    monkeypatch.setattr("yuefu_test.runner.time.sleep", lambda _: None)

    parquet_path = _make_parquet(tmp_path)
    config = RunConfig(
        parquet_path=parquet_path,
        row_index=0,
        max_attempts=3,
        poll_timeout=1.0,
        poll_interval=0.0,
    )
    client = FakeClient([["PROCESSING", "SUCCESS"]])

    metrics = run_workflow(config, client)

    assert metrics.success is True
    assert metrics.success_attempt == 1
    assert metrics.attempts[0].status_history[-1] == "SUCCESS"


def test_run_workflow_retries_after_failure(monkeypatch, tmp_path):
    monkeypatch.setattr("yuefu_test.runner.random.choice", lambda choices: choices[-1])
    monkeypatch.setattr("yuefu_test.runner.time.sleep", lambda _: None)

    parquet_path = _make_parquet(tmp_path)
    config = RunConfig(
        parquet_path=parquet_path,
        row_index=0,
        max_attempts=3,
        poll_timeout=1.0,
        poll_interval=0.0,
    )
    client = FakeClient([["FAILED"], ["PROCESSING", "DONE"]])

    metrics = run_workflow(config, client)

    assert metrics.success is True
    assert metrics.success_attempt == 2
    assert len(metrics.attempts) == 2
    assert metrics.attempts[0].success is False
    assert metrics.attempts[1].success is True

