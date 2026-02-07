"""
RateLimiter Unit Tests
"""

import asyncio
import time

import pytest

from src.server.clients.rate_limiter import RateLimiter


class TestRateLimiter:
    def test_plan_limits(self):
        """プランベースのリミット確認"""
        assert RateLimiter.PLAN_LIMITS["free"] == 5
        assert RateLimiter.PLAN_LIMITS["light"] == 60
        assert RateLimiter.PLAN_LIMITS["standard"] == 120
        assert RateLimiter.PLAN_LIMITS["premium"] == 500

    def test_interval_calculation_free(self):
        """free プランのインターバル計算（10% safety margin）"""
        limiter = RateLimiter(plan="free")
        # 60/5 * 1.1 = 13.2s
        assert abs(limiter.interval - 13.2) < 0.01

    def test_interval_calculation_premium(self):
        """premium プランのインターバル計算"""
        limiter = RateLimiter(plan="premium")
        # 60/500 * 1.1 = 0.132s
        assert abs(limiter.interval - 0.132) < 0.001

    def test_unknown_plan_defaults_to_free(self):
        """不明なプランは free と同じ"""
        limiter = RateLimiter(plan="unknown")
        expected = (60.0 / 5) * 1.1
        assert abs(limiter.interval - expected) < 0.01

    @pytest.mark.asyncio
    async def test_acquire_enforces_interval(self):
        """acquire が最小インターバルを遵守する"""
        limiter = RateLimiter(plan="premium")  # 短いインターバル

        start = time.monotonic()
        await limiter.acquire()
        await limiter.acquire()
        elapsed = time.monotonic() - start

        # 少なくとも 1 インターバル分待つ
        assert elapsed >= limiter.interval * 0.9  # 10% tolerance

    @pytest.mark.asyncio
    async def test_first_acquire_is_immediate(self):
        """最初の acquire は即座に完了する"""
        limiter = RateLimiter(plan="free")

        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start

        assert elapsed < 0.1  # 100ms 以内

    @pytest.mark.asyncio
    async def test_fifo_ordering(self):
        """複数の acquire が FIFO 順で処理される"""
        limiter = RateLimiter(plan="premium")
        order: list[int] = []

        async def acquire_and_record(idx: int):
            await limiter.acquire()
            order.append(idx)

        # 並行して 3 つの acquire を発行
        tasks = [
            asyncio.create_task(acquire_and_record(i))
            for i in range(3)
        ]
        await asyncio.gather(*tasks)

        # FIFO 順で処理される
        assert order == [0, 1, 2]
