"""
JQuants Async API Client

JQuants API v2 への非同期 HTTP クライアント。
レートリミッター、指数バックオフリトライ、ページネーションを内蔵。
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
from loguru import logger

from src.server.clients.rate_limiter import RateLimiter


class JQuantsAsyncClient:
    """JQuants API v2 非同期クライアント

    Args:
        api_key: JQuants API キー
        plan: JQuants プラン名 ("free", "light", "standard", "premium")
        timeout: リクエストタイムアウト（秒）
    """

    MAX_RETRIES = 3
    RETRY_STATUSES = {429, 500, 502, 503, 504}
    BASE_URL = "https://api.jquants.com/v2"

    # JQuants v2 エンドポイント → レスポンスデータキー マッピング
    _DATA_KEYS: dict[str, str] = {
        "/equities/bars/daily": "daily_quotes",
        "/indices/bars/daily": "indices",
        "/equities/master": "info",
        "/fins/statements": "statements",
        "/markets/margin-interest": "weekly_margin_interest",
    }

    def __init__(
        self,
        api_key: str,
        plan: str = "free",
        timeout: float = 30.0,
    ) -> None:
        self._api_key = api_key
        self._client = httpx.AsyncClient(
            base_url=self.BASE_URL,
            headers={"x-api-key": api_key},
            timeout=timeout,
        )
        self._rate_limiter = RateLimiter(plan=plan)

    @property
    def has_api_key(self) -> bool:
        return bool(self._api_key)

    @property
    def masked_key(self) -> str | None:
        if not self._api_key:
            return None
        key = self._api_key
        if len(key) > 8:
            return f"{key[:4]}...{key[-4:]}"
        return "****"

    async def _get_with_retry(
        self, path: str, params: dict[str, Any] | None = None
    ) -> httpx.Response:
        """GET with exponential backoff for 429/5xx/timeout."""
        last_exc: Exception | None = None
        for attempt in range(self.MAX_RETRIES + 1):
            try:
                await self._rate_limiter.acquire()
                resp = await self._client.get(path, params=params)
                if resp.status_code in self.RETRY_STATUSES and attempt < self.MAX_RETRIES:
                    wait = 2**attempt
                    logger.warning(
                        f"JQuants API {path} returned {resp.status_code}, "
                        f"retrying in {wait}s (attempt {attempt + 1}/{self.MAX_RETRIES})"
                    )
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp
            except httpx.TimeoutException as e:
                last_exc = e
                if attempt >= self.MAX_RETRIES:
                    raise
                wait = 2**attempt
                logger.warning(
                    f"JQuants API {path} timeout, "
                    f"retrying in {wait}s (attempt {attempt + 1}/{self.MAX_RETRIES})"
                )
                await asyncio.sleep(wait)
        # unreachable, but satisfies type checker
        raise last_exc or httpx.TimeoutException("max retries exceeded")

    def _extract_data_key(self, path: str, body: dict[str, Any]) -> str | None:
        """レスポンスからデータ配列のキーを特定する。

        既知のエンドポイントはマッピングを使い、
        未知の場合はリスト型の最初のキーを返す。
        """
        # 既知のエンドポイントマッピング
        if path in self._DATA_KEYS:
            return self._DATA_KEYS[path]
        # フォールバック: 最初のリスト型フィールド
        for key, value in body.items():
            if isinstance(value, list):
                return key
        return None

    async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """単一ページ GET リクエスト"""
        resp = await self._get_with_retry(path, params)
        result: dict[str, Any] = resp.json()
        return result

    async def get_paginated(
        self, path: str, params: dict[str, Any] | None = None, max_pages: int = 10
    ) -> list[dict[str, Any]]:
        """ページネーション付き GET リクエスト

        JQuants v2 の pagination_key ベースのページネーションを処理する。
        """
        all_data: list[dict[str, Any]] = []
        current_params = dict(params) if params else {}
        page_count = 0

        while page_count < max_pages:
            page_count += 1
            resp = await self._get_with_retry(path, current_params)
            body: dict[str, Any] = resp.json()

            # データキーを特定して配列を収集
            data_key = self._extract_data_key(path, body)
            if data_key and isinstance(body.get(data_key), list):
                all_data.extend(body[data_key])

            # 次ページ確認
            pagination_key = body.get("pagination_key")
            if not pagination_key:
                break
            current_params = {**current_params, "pagination_key": pagination_key}

        logger.debug(f"JQuants paginated {path}: {len(all_data)} records in {page_count} pages")
        return all_data

    async def close(self) -> None:
        """HTTP クライアントをクローズ"""
        await self._client.aclose()
