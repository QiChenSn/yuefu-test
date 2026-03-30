from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from yuefu_test.data import FilePayload, load_payload, persist_payload


def _write_sample_parquet(tmp_path: Path) -> Path:
    image_type = pa.struct([("bytes", pa.binary()), ("path", pa.string())])
    table = pa.table(
        {
            "filename": pa.array(["sample.png"]),
            "image": pa.array([{ "bytes": b"abc", "path": "memory" }], type=image_type),
            "transcription": pa.array(["hello"]),
        }
    )
    target = tmp_path / "sample.parquet"
    pq.write_table(table, target)
    return target


def test_load_payload_reads_bytes(tmp_path: Path) -> None:
    parquet_path = _write_sample_parquet(tmp_path)
    payload = load_payload(parquet_path, 0)

    assert payload.filename == "sample.png"
    assert payload.content == b"abc"
    assert payload.transcription == "hello"
    assert payload.row_index == 0
    assert payload.source_path == parquet_path


def test_persist_payload_writes_file(tmp_path: Path) -> None:
    payload = FilePayload(filename="foo.bin", content=b"xyz")
    output_path = persist_payload(payload, tmp_path)

    assert output_path.exists()
    assert output_path.read_bytes() == b"xyz"

