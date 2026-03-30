from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Optional
import logging

import typer

from .api import OmrClient
from .runner import RunConfig, run_workflow

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
    row_index: int = typer.Option(0, "--row", min=0, help="Row index inside the parquet file"),
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

    with OmrClient(base_url, timeout=timeout, headers=headers or None, verify_ssl=verify_ssl) as client:
        metrics = run_workflow(config, client)

    typer.echo(metrics.to_json())
    if not metrics.success:
        raise typer.Exit(code=1)


if __name__ == "__main__":  # pragma: no cover
    app()
