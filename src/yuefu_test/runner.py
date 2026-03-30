from __future__ import annotations

import json
import random
import time
import asyncio
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import List, Sequence
import logging

from .api import OmrClient
from .data import FilePayload, load_payload, get_row_count

logger = logging.getLogger(__name__)

ENGINE_CHOICES: Sequence[str] = ("LEGATO_FP16", "LEGATO_FP32")
SUCCESS_STATUSES = {"SUCCESS", "SUCCEEDED", "DONE", "FINISHED", "COMPLETED"}
FAILURE_STATUSES = {"FAILED", "ERROR", "CANCELLED", "CANCELED"}


@dataclass(slots=True)
class RunConfig:
    parquet_path: Path
    row_index: int = 0
    max_attempts: int = 3
    poll_timeout: float = 300.0
    poll_interval: float = 5.0


@dataclass(slots=True)
class AttemptMetrics:
    index: int
    engine: str
    task_id: str | None
    success: bool
    duration_seconds: float
    final_status: str | None
    status_history: List[str | None] = field(default_factory=list)
    timed_out: bool = False
    error: str | None = None


@dataclass(slots=True)
class RunMetrics:
    attempts: List[AttemptMetrics]
    success_attempt: int | None
    elapsed_seconds: float
    config: RunConfig

    @property
    def success(self) -> bool:
        return self.success_attempt is not None

    @property
    def success_rate(self) -> float:
        if not self.attempts:
            return 0.0
        succeeded = sum(1 for attempt in self.attempts if attempt.success)
        return succeeded / len(self.attempts)

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "success_attempt": self.success_attempt,
            "elapsed_seconds": self.elapsed_seconds,
            "success_rate": self.success_rate,
            "attempts": [asdict(attempt) for attempt in self.attempts],
            "config": {
                "parquet_path": str(self.config.parquet_path),
                "row_index": self.config.row_index,
                "max_attempts": self.config.max_attempts,
                "poll_timeout": self.config.poll_timeout,
                "poll_interval": self.config.poll_interval,
            },
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

async def run_batch(config: RunConfig, client: OmrClient, output_json: Path | None = None) -> List[RunMetrics]:
    total_rows = get_row_count(config.parquet_path)
    
    start_idx = 0 if config.row_index < 0 else config.row_index
    end_idx = total_rows if config.row_index < 0 else config.row_index + 1

    if config.row_index < 0:
        end_idx = min(total_rows, 20)
        logger.info(f"Starting batch workflow for first {end_idx} rows in {config.parquet_path} (max concurrency = 2)")
    else:
        logger.info(f"Starting batch workflow for row {config.row_index} in {config.parquet_path} (max concurrency = 2)")

    sem = asyncio.Semaphore(2)

    async def _sem_workflow(row_idx: int) -> RunMetrics:
        async with sem:
            logger.info(f"--- Processing row {row_idx}/{end_idx - 1} ---")
            row_config = RunConfig(
                parquet_path=config.parquet_path,
                row_index=row_idx,
                max_attempts=config.max_attempts,
                poll_timeout=config.poll_timeout,
                poll_interval=config.poll_interval,
            )
            return await run_workflow(row_config, client)

    tasks = [_sem_workflow(row_idx) for row_idx in range(start_idx, end_idx)]
    all_metrics = await asyncio.gather(*tasks)

    if output_json:
        summary_data = []
        for m in all_metrics:
            summary_data.append({
                "row_index": m.config.row_index,
                "success": m.success,
                "elapsed_seconds": m.elapsed_seconds,
                "total_attempts": len(m.attempts),
                "engines_used": [a.engine for a in m.attempts],
                "task_ids": [a.task_id for a in m.attempts],
            })
        
        output_data = {
            "total_processed": len(all_metrics),
            "successful": sum(1 for m in all_metrics if m.success),
            "details": summary_data
        }
        output_json.parent.mkdir(parents=True, exist_ok=True)
        with output_json.open("w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Batch summary saved to {output_json}")

    return all_metrics

async def run_workflow(config: RunConfig, client: OmrClient) -> RunMetrics:
    if config.max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    payload = load_payload(config.parquet_path, config.row_index)
    attempts: List[AttemptMetrics] = []
    run_start = time.perf_counter()

    logger.info(f"Starting workflow for {config.parquet_path} (row: {config.row_index}) | Max Attempts: 1")

    engine = random.choice(ENGINE_CHOICES)
    logger.info(f"\n--- Attempt 1/1 for row {config.row_index} using Engine '{engine}' ---")
    attempt_metrics = await _run_single_attempt(
        attempt_index=1,
        engine=engine,
        payload=payload,
        client=client,
        config=config,
    )
    attempts.append(attempt_metrics)
    if attempt_metrics.success:
        logger.info(f"Attempt 1 succeeded!")
    else:
        logger.warning(f"Attempt 1 failed (Final Status: {attempt_metrics.final_status}, Error: {attempt_metrics.error}). No retries will be made.")

    elapsed_seconds = time.perf_counter() - run_start
    success_attempt = next((attempt.index for attempt in attempts if attempt.success), None)
    
    if success_attempt is None:
        logger.error(f"Workflow failed. Elapsed: {elapsed_seconds:.2f}s")
    else:
        logger.info(f"Workflow successfully finished on attempt {success_attempt}. Elapsed: {elapsed_seconds:.2f}s")

    return RunMetrics(
        attempts=attempts,
        success_attempt=success_attempt,
        elapsed_seconds=elapsed_seconds,
        config=config,
    )


async def _run_single_attempt(
    *,
    attempt_index: int,
    engine: str,
    payload: FilePayload,
    client: OmrClient,
    config: RunConfig,
) -> AttemptMetrics:
    status_history: List[str | None] = []
    error: str | None = None
    success = False
    timed_out = False
    final_status: str | None = None
    task_id: str | None = None
    start_time = time.perf_counter()

    try:
        submit_result = await client.submit(engine, payload.filename, payload.content)
        task_id = submit_result.task_id
    except Exception as exc:  # pragma: no cover - http/network branch
        error = str(exc)
        duration = time.perf_counter() - start_time
        return AttemptMetrics(
            index=attempt_index,
            engine=engine,
            task_id=task_id,
            success=False,
            duration_seconds=duration,
            final_status=final_status,
            status_history=status_history,
            timed_out=False,
            error=error,
        )

    deadline = start_time + config.poll_timeout
    while True:
        now = time.perf_counter()
        if now >= deadline:
            logger.error(f"Task '{task_id}' polling timed out after {config.poll_timeout} seconds.")
            timed_out = True
            break

        try:
            status_result = await client.fetch_status(task_id)
        except Exception as exc:  # pragma: no cover - http/network branch
            error = str(exc)
            logger.error(f"Error fetching status for task '{task_id}': {error}")
            break

        final_status = status_result.status
        status_history.append(final_status)
        logger.info(f"Task '{task_id}' polled status: {final_status}")

        if _is_success(final_status):
            success = True
            break
        if _is_failure(final_status):
            logger.error(f"Task '{task_id}' reported failure status: {final_status}")
            break

        await asyncio.sleep(config.poll_interval)

    duration = time.perf_counter() - start_time
    return AttemptMetrics(
        index=attempt_index,
        engine=engine,
        task_id=task_id,
        success=success,
        duration_seconds=duration,
        final_status=final_status,
        status_history=status_history,
        timed_out=timed_out,
        error=error,
    )


def _is_success(status: str | None) -> bool:
    if status is None:
        return False
    return status.upper() in SUCCESS_STATUSES


def _is_failure(status: str | None) -> bool:
    if status is None:
        return False
    return status.upper() in FAILURE_STATUSES
