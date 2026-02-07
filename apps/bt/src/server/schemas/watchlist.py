"""
Watchlist Schemas

Hono Watchlist API 互換のリクエスト/レスポンススキーマ。
"""

from __future__ import annotations

from pydantic import BaseModel, Field


# ===== Request Schemas =====


class WatchlistCreateRequest(BaseModel):
    name: str = Field(min_length=1)
    description: str | None = None


class WatchlistUpdateRequest(BaseModel):
    name: str | None = Field(None, min_length=1)
    description: str | None = None


class WatchlistItemCreateRequest(BaseModel):
    code: str = Field(pattern=r"^\d[0-9A-Z]\d[0-9A-Z]$")
    companyName: str = Field(min_length=1)
    memo: str | None = None


# ===== Response Schemas =====


class WatchlistSummaryResponse(BaseModel):
    """Watchlist list item (stockCount 付き)"""

    id: int
    name: str
    description: str | None = None
    stockCount: int
    createdAt: str
    updatedAt: str


class WatchlistResponse(BaseModel):
    """Watchlist 単体レスポンス (create/update)"""

    id: int
    name: str
    description: str | None = None
    createdAt: str
    updatedAt: str


class WatchlistItemResponse(BaseModel):
    """Watchlist item レスポンス"""

    id: int
    watchlistId: int
    code: str
    companyName: str
    memo: str | None = None
    createdAt: str


class WatchlistDetailResponse(BaseModel):
    """GET /api/watchlist/{id} — watchlist + items"""

    id: int
    name: str
    description: str | None = None
    createdAt: str
    updatedAt: str
    items: list[WatchlistItemResponse]
