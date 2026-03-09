from __future__ import annotations

import asyncio

import pytest

from src.application.services.sync_stream_manager import SyncStreamEvent, SyncStreamManager


@pytest.mark.asyncio
async def test_sync_stream_manager_publish_and_close() -> None:
    manager = SyncStreamManager()

    queue = manager.subscribe("job-1")
    event = SyncStreamEvent(
        event="fetch-detail",
        payload={"endpoint": "/equities/bars/daily"},
    )

    manager.publish("job-1", event)
    assert await queue.get() == event

    manager.close("job-1")
    assert await queue.get() is None

    manager.unsubscribe("job-1", queue)
    assert "job-1" not in manager._subscribers


def test_sync_stream_manager_ignores_missing_subscribers() -> None:
    manager = SyncStreamManager()

    manager.publish("missing-job", SyncStreamEvent(event="job"))
    manager.close("missing-job")
    manager.unsubscribe(
        "missing-job",
        asyncio.Queue(),
    )

