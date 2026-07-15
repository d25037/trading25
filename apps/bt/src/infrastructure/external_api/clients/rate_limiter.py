"""
Async FIFO Rate Limiter

JQuants API プランベースのレート制限を非同期で制御する。
"""

from __future__ import annotations

import asyncio
import time


class RateLimiter:
    """非同期 FIFO レートリミッター

    プランベースの RPM 制限を asyncio.Lock で直列化し、
    最小インターバルを遵守する。

    Args:
        plan: JQuants プラン名 ("free", "light", "standard", "premium")
    """

    PLAN_LIMITS: dict[str, int] = {
        "free": 5,
        "light": 60,
        "standard": 120,
        "premium": 500,
    }

    def __init__(self, plan: str = "free") -> None:
        rpm = self.PLAN_LIMITS.get(plan, 5)
        # 10% safety margin
        self._interval: float = (60.0 / rpm) * 1.1
        self._lock = asyncio.Lock()
        self._last_request: float = 0.0
        self._cooldown_until: float = 0.0

    @property
    def interval(self) -> float:
        """リクエスト間の最小インターバル（秒）"""
        return self._interval

    async def acquire(self) -> None:
        """レートリミットスロットを取得する。

        FIFO 順序を保証し、必要に応じてスリープする。
        """
        async with self._lock:
            now = time.monotonic()
            interval_wait = self._interval - (now - self._last_request)
            cooldown_wait = self._cooldown_until - now
            wait = max(interval_wait, cooldown_wait)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request = time.monotonic()

    async def defer(
        self,
        seconds: float,
        *,
        minimum_interval: float | None = None,
    ) -> None:
        """Share an upstream-requested cooldown with subsequent callers.

        The state update intentionally has no await point: it is atomic within the
        owning event loop and cannot queue behind an acquire that is sleeping while
        holding the FIFO lock.
        """
        if minimum_interval is not None:
            self._interval = max(self._interval, minimum_interval)
        self._cooldown_until = max(
            self._cooldown_until,
            time.monotonic() + max(seconds, 0.0),
        )
