from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pyarrow.parquet as pq


@dataclass(slots=True)
class FilePayload:
    """Decoded file extracted from the Parquet test set."""

    filename: str
    content: bytes
    transcription: Optional[str] = None
    row_index: Optional[int] = None
    source_path: Optional[Path] = None


def load_payload(parquet_path: Path, row_index: int = 0) -> FilePayload:
    """Load a single row from the Parquet shard and return the binary payload."""

    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found: {path}")

    table = pq.read_table(path, columns=["filename", "image", "transcription"])
    if row_index < 0 or row_index >= table.num_rows:
        raise IndexError(f"Row index {row_index} is outside the table bounds ({table.num_rows} rows)")

    filename = table.column("filename")[row_index].as_py()
    image_struct = table.column("image")[row_index].as_py() or {}
    file_bytes = image_struct.get("bytes")
    if file_bytes is None:
        raise ValueError(f"Row {row_index} does not contain raw bytes in the 'image.bytes' field")

    transcription = table.column("transcription")[row_index].as_py()

    return FilePayload(
        filename=filename,
        content=file_bytes,
        transcription=transcription,
        row_index=row_index,
        source_path=path,
    )


def get_row_count(parquet_path: Path) -> int:
    """Return the total number of rows in the Parquet file."""
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found: {path}")
    return pq.read_table(path, columns=["filename"]).num_rows


def persist_payload(payload: FilePayload, output_dir: Path) -> Path:
    """Write the decoded bytes to *output_dir* and return the saved file path."""

    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    target_path = directory / payload.filename
    target_path.write_bytes(payload.content)
    return target_path

