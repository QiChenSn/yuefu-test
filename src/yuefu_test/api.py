from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping
import mimetypes
import logging

import httpx

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SubmitResult:
    engine: str
    task_id: str
    response: Mapping[str, Any]


@dataclass(slots=True)
class StatusResult:
    task_id: str
    status: str | None
    response: Mapping[str, Any]


class OmrClient:
    """HTTP client that wraps the Yuefu OMR endpoints."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 30.0,
        headers: MutableMapping[str, str] | None = None,
        verify_ssl: bool = True,
        transport: httpx.BaseTransport | httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not base_url:
            raise ValueError("base_url must be provided")

        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            headers=headers,
            verify=verify_ssl,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "OmrClient":  # pragma: no cover
        return self

    async def __aexit__(self, *_: object) -> None:  # pragma: no cover
        await self.aclose()

    async def submit(self, engine: str, filename: str, content: bytes) -> SubmitResult:
        mime_type = mimetypes.guess_type(filename)[0] or "image/jpeg"
        if mime_type not in ("image/jpeg", "image/png"):
            mime_type = "image/jpeg"
        logger.info(f"Submitting file '{filename}' (MIME: {mime_type}) to engine '{engine}'")
        response = await self._client.post(
            f"/omr/submit/{engine}",
            files={"file": (filename, content, mime_type)},
        )
        response.raise_for_status()
        payload = response.json()
        task_id = _lookup_task_id(payload)
        logger.info(f"Submit successful, task ID is '{task_id}'")
        return SubmitResult(engine=engine, task_id=task_id, response=payload)

    async def fetch_status(self, task_id: str) -> StatusResult:
        logger.debug(f"Fetching status for task '{task_id}'")
        response = await self._client.get(f"/omr/status/{task_id}")
        response.raise_for_status()
        payload = response.json()
        status = _lookup_status(payload)
        logger.debug(f"Task '{task_id}' status: {status}")
        return StatusResult(task_id=task_id, status=status, response=payload)


def _lookup_task_id(payload: Mapping[str, Any]) -> str:
    data = payload.get("data") if isinstance(payload, Mapping) else None
    if isinstance(data, Mapping):
        for key in ("taskId", "task_id", "id"):
            value = data.get(key)
            if value is not None:
                return str(value)
    for key in ("taskId", "task_id", "id"):
        value = payload.get(key) if isinstance(payload, Mapping) else None
        if value is not None:
            return str(value)
    print(payload)
    raise ValueError("Response payload does not contain a task identifier")


def _lookup_status(payload: Mapping[str, Any]) -> str | None:
    data = payload.get("data") if isinstance(payload, Mapping) else None
    if isinstance(data, str):
        return data
    if isinstance(data, Mapping):
        for key in ("status", "state", "progress", "phase"):
            value = data.get(key)
            if value is not None:
                return str(value)
    for key in ("status", "state"):
        value = payload.get(key) if isinstance(payload, Mapping) else None
        if value is not None:
            return str(value)
    return None

