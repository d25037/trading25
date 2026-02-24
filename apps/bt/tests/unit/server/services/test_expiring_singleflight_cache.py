"""Unit tests for ExpiringSingleFlightCache."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from src.application.services.expiring_singleflight_cache import ExpiringSingleFlightCache


@pytest.mark.asyncio
async def test_get_or_set_returns_hit_before_ttl() -> None:
    cache = ExpiringSingleFlightCache[int]()
    fetcher = AsyncMock(return_value=42)

    value1, state1 = await cache.get_or_set("k", ttl_seconds=60, fetcher=fetcher)
    value2, state2 = await cache.get_or_set("k", ttl_seconds=60, fetcher=fetcher)

    assert value1 == 42
    assert value2 == 42
    assert state1 == "miss"
    assert state2 == "hit"
    assert fetcher.await_count == 1


@pytest.mark.asyncio
async def test_get_or_set_expires_after_ttl() -> None:
    cache = ExpiringSingleFlightCache[int]()
    fetcher = AsyncMock(side_effect=[1, 2])

    value1, state1 = await cache.get_or_set("k", ttl_seconds=0.01, fetcher=fetcher)
    await asyncio.sleep(0.02)
    value2, state2 = await cache.get_or_set("k", ttl_seconds=0.01, fetcher=fetcher)

    assert value1 == 1
    assert value2 == 2
    assert state1 == "miss"
    assert state2 == "miss"
    assert fetcher.await_count == 2


@pytest.mark.asyncio
async def test_get_or_set_coalesces_in_flight_requests() -> None:
    cache = ExpiringSingleFlightCache[int]()
    started = asyncio.Event()
    unblock = asyncio.Event()

    async def fetcher() -> int:
        started.set()
        await unblock.wait()
        return 99

    task1 = asyncio.create_task(cache.get_or_set("k", ttl_seconds=60, fetcher=fetcher))
    await started.wait()
    task2 = asyncio.create_task(cache.get_or_set("k", ttl_seconds=60, fetcher=fetcher))

    unblock.set()
    value1, state1 = await task1
    value2, state2 = await task2

    assert value1 == 99
    assert value2 == 99
    assert state1 == "miss"
    assert state2 == "wait"


@pytest.mark.asyncio
async def test_get_or_set_does_not_cache_exceptions() -> None:
    cache = ExpiringSingleFlightCache[int]()
    fetcher = AsyncMock(side_effect=[RuntimeError("boom"), 7])

    with pytest.raises(RuntimeError, match="boom"):
        await cache.get_or_set("k", ttl_seconds=60, fetcher=fetcher)

    value, state = await cache.get_or_set("k", ttl_seconds=60, fetcher=fetcher)

    assert value == 7
    assert state == "miss"
    assert fetcher.await_count == 2


@pytest.mark.asyncio
async def test_get_or_set_evicts_other_expired_entries() -> None:
    cache = ExpiringSingleFlightCache[int]()
    fetch_short = AsyncMock(return_value=1)
    fetch_long = AsyncMock(return_value=2)

    await cache.get_or_set("short", ttl_seconds=0.01, fetcher=fetch_short)
    await cache.get_or_set("long", ttl_seconds=60, fetcher=fetch_long)
    await asyncio.sleep(0.02)

    value, state = await cache.get_or_set("long", ttl_seconds=60, fetcher=fetch_long)

    assert value == 2
    assert state == "hit"
    assert "short" not in cache._entries  # noqa: SLF001


@pytest.mark.asyncio
async def test_clear_and_invalidate() -> None:
    cache = ExpiringSingleFlightCache[int]()
    fetcher = AsyncMock(side_effect=[1, 2, 3])

    await cache.get_or_set("k", ttl_seconds=60, fetcher=fetcher)
    await cache.invalidate("k")
    value_after_invalidate, state_after_invalidate = await cache.get_or_set("k", ttl_seconds=60, fetcher=fetcher)

    await cache.clear()
    value_after_clear, state_after_clear = await cache.get_or_set("k", ttl_seconds=60, fetcher=fetcher)

    assert value_after_invalidate == 2
    assert state_after_invalidate == "miss"
    assert value_after_clear == 3
    assert state_after_clear == "miss"
