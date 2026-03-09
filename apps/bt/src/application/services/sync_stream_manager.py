"""
Sync SSE Stream Manager

DB sync 向けの軽量な Pub/Sub 管理。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Literal

from loguru import logger

SyncStreamEventType = Literal["job", "fetch-detail"]


@dataclass(frozen=True)
class SyncStreamEvent:
    event: SyncStreamEventType
    payload: dict[str, Any] | None = None


class SyncStreamManager:
    """Sync SSE サブスクライバー管理"""

    def __init__(self) -> None:
        self._subscribers: dict[str, list[asyncio.Queue[SyncStreamEvent | None]]] = {}

    def subscribe(self, job_id: str) -> asyncio.Queue[SyncStreamEvent | None]:
        queue: asyncio.Queue[SyncStreamEvent | None] = asyncio.Queue()
        self._subscribers.setdefault(job_id, []).append(queue)
        logger.debug(f"Sync SSE subscription started: {job_id}")
        return queue

    def unsubscribe(self, job_id: str, queue: asyncio.Queue[SyncStreamEvent | None]) -> None:
        subscribers = self._subscribers.get(job_id)
        if subscribers is None:
            return
        try:
            subscribers.remove(queue)
        except ValueError:
            pass
        if not subscribers:
            del self._subscribers[job_id]
        logger.debug(f"Sync SSE subscription ended: {job_id}")

    def publish(self, job_id: str, event: SyncStreamEvent) -> None:
        for queue in list(self._subscribers.get(job_id, [])):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning(f"Sync SSE queue full: {job_id}")

    def close(self, job_id: str) -> None:
        for queue in list(self._subscribers.get(job_id, [])):
            try:
                queue.put_nowait(None)
            except asyncio.QueueFull:
                logger.warning(f"Sync SSE queue full while closing: {job_id}")


sync_stream_manager = SyncStreamManager()
