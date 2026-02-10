"""In-memory expiring cache with singleflight deduplication."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Generic, Literal, TypeVar

T = TypeVar("T")
CacheState = Literal["hit", "miss", "wait"]


@dataclass(slots=True)
class _CacheEntry(Generic[T]):
    value: T
    expires_at: float


class ExpiringSingleFlightCache(Generic[T]):
    """Thread-safe async cache with TTL and in-flight request coalescing.

    - `hit`: cached value returned
    - `miss`: caller fetched value and cached it
    - `wait`: caller waited for another in-flight fetch
    """

    def __init__(self) -> None:
        self._entries: dict[str, _CacheEntry[T]] = {}
        self._in_flight: dict[str, asyncio.Future[T]] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _evict_expired_locked(
        entries: dict[str, _CacheEntry[T]],
        now: float,
    ) -> None:
        """Drop expired entries to keep memory usage bounded."""
        expired_keys = [key for key, entry in entries.items() if entry.expires_at <= now]
        for key in expired_keys:
            entries.pop(key, None)

    async def get_or_set(
        self,
        key: str,
        ttl_seconds: float,
        fetcher: Callable[[], Awaitable[T]],
    ) -> tuple[T, CacheState]:
        """Get cached value or fetch and set with TTL.

        Exceptions from `fetcher` are propagated and never cached.
        """
        now = time.monotonic()
        should_fetch = False

        async with self._lock:
            self._evict_expired_locked(self._entries, now)
            entry = self._entries.get(key)
            if entry is not None and entry.expires_at > now:
                return entry.value, "hit"

            in_flight = self._in_flight.get(key)
            if in_flight is None:
                in_flight = asyncio.get_running_loop().create_future()
                self._in_flight[key] = in_flight
                should_fetch = True

        if not should_fetch:
            value = await in_flight
            return value, "wait"

        try:
            value = await fetcher()
        except Exception as exc:
            async with self._lock:
                future = self._in_flight.pop(key, None)
            if future is not None and not future.done():
                future.set_exception(exc)
                # If there are no waiters, consume the exception to avoid
                # "Future exception was never retrieved" warnings.
                future.exception()
            raise

        expires_at = time.monotonic() + max(ttl_seconds, 0.0)
        async with self._lock:
            self._entries[key] = _CacheEntry(value=value, expires_at=expires_at)
            future = self._in_flight.pop(key, None)

        if future is not None and not future.done():
            future.set_result(value)

        return value, "miss"

    async def clear(self) -> None:
        """Clear all cache entries.

        In-flight requests are untouched to avoid cancelling active callers.
        """
        async with self._lock:
            self._entries.clear()

    async def invalidate(self, key: str) -> None:
        """Invalidate one cache key if present."""
        async with self._lock:
            self._entries.pop(key, None)
