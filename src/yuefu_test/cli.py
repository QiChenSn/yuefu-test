from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Optional
import logging
import asyncio

import typer

from .api import OmrClient
from .runner import RunConfig, run_workflow, run_batch

app = typer.Typer(add_completion=False, no_args_is_help=True)


def _parse_headers(header_values: Iterable[str]) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for raw in header_values:
        if ":" not in raw:
            raise typer.BadParameter("Headers must be in KEY:VALUE format")
        key, value = raw.split(":", 1)
        headers[key.strip()] = value.strip()
    return headers


@app.command()
def run(
    base_url: str = typer.Option(..., "--base-url", envvar="OMR_BASE_URL", help="OMR service base URL"),
    parquet_path: Path = typer.Option(
        Path("test_data/test-00000-of-00001.parquet"),
        "--data",
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Path to the parquet shard to sample",
    ),
    row_index: int = typer.Option(0, "--row", help="Row index inside the parquet file (set to -1 to process all rows)"),
    output_json: Optional[Path] = typer.Option(None, "--output", help="Path to save the summary JSON file"),
    max_attempts: int = typer.Option(3, "--max-attempts", min=1, help="Maximum number of upload attempts"),
    poll_timeout: float = typer.Option(300.0, "--poll-timeout", help="Seconds to wait for a task before retrying"),
    poll_interval: float = typer.Option(5.0, "--poll-interval", help="Seconds between status checks"),
    timeout: float = typer.Option(30.0, "--timeout", help="HTTP client timeout in seconds"),
    verify_ssl: bool = typer.Option(True, "--verify-ssl/--no-verify-ssl", help="Toggle TLS verification"),
    header: Optional[list[str]] = typer.Option(None, "-H", "--header", help="Additional request headers (KEY:VALUE)"),
    verbose: bool = typer.Option(False, "-v", "--verbose", help="Enable verbose logging output"),
) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    headers = _parse_headers(header or [])
    config = RunConfig(
        parquet_path=parquet_path,
        row_index=row_index,
        max_attempts=max_attempts,
        poll_timeout=poll_timeout,
        poll_interval=poll_interval,
    )

    # Default output_json to summary.json if we are running a batch and no output was provided
    actual_output_json = output_json
    if row_index < 0 and actual_output_json is None:
        actual_output_json = Path("summary.json")

    async def _run():
        async with OmrClient(base_url, timeout=timeout, headers=headers or None, verify_ssl=verify_ssl) as client:
            if row_index < 0:
                metrics_list = await run_batch(config, client, output_json=actual_output_json)
                success = all(m.success for m in metrics_list)
            else:
                metrics = await run_workflow(config, client)
                success = metrics.success
                if actual_output_json:
                    # Optional: Output summary for single run too, but run_batch might not be what we want here
                    # To just write single run to json:
                    with actual_output_json.open("w", encoding="utf-8") as f:
                        f.write(metrics.to_json())
                    logger.info(f"Summary saved to {actual_output_json}")
                else:
                    typer.echo(metrics.to_json())
        return success

    success = asyncio.run(_run())

    if not success:
        raise typer.Exit(code=1)

if __name__ == "__main__":  # pragma: no cover
    app()
