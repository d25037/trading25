"""
Async FIFO Rate Limiter

JQuants API プランベースのレート制限を非同期で制御する。
Hono BaseJQuantsClient.ts の RateLimitQueue と同等のロジック。
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
        # 10% safety margin (Hono 実装と同一)
        self._interval: float = (60.0 / rpm) * 1.1
        self._lock = asyncio.Lock()
        self._last_request: float = 0.0

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
            wait = self._interval - (now - self._last_request)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request = time.monotonic()
