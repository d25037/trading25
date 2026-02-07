"""
Chart Response Schemas

Hono hono-openapi-baseline.json と完全互換のレスポンススキーマ。
"""

from __future__ import annotations

from pydantic import BaseModel, Field


# --- Index Master ---


class IndexInfo(BaseModel):
    """指数マスタ情報"""

    code: str = Field(description="指数コード")
    name: str = Field(description="指数名")
    nameEnglish: str | None = Field(default=None, description="指数名（英語）")
    category: str = Field(description="カテゴリ")
    dataStartDate: str | None = Field(default=None, description="データ開始日")


class IndicesListResponse(BaseModel):
    """指数一覧レスポンス"""

    indices: list[IndexInfo]
    lastUpdated: str = Field(description="最終更新日時 (ISO 8601)")


# --- Index OHLC Data ---


class IndexOHLCRecord(BaseModel):
    """指数 OHLC レコード"""

    date: str
    open: float
    high: float
    low: float
    close: float


class IndexDataResponse(BaseModel):
    """指数チャートデータレスポンス"""

    code: str
    name: str
    data: list[IndexOHLCRecord]
    lastUpdated: str = Field(description="最終更新日時 (ISO 8601)")


# --- TOPIX Data (with volume) ---


class TopixDataPoint(BaseModel):
    """TOPIX データポイント"""

    date: str
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: float = Field(ge=0, default=0)


class TopixDataResponse(BaseModel):
    """TOPIX データレスポンス"""

    topix: list[TopixDataPoint]
    lastUpdated: str = Field(description="最終更新日時 (ISO 8601)")


# --- Stock Chart Data ---


class StockDataPoint(BaseModel):
    """銘柄チャートデータポイント"""

    time: str
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: float = Field(ge=0, default=0)


class StockDataResponse(BaseModel):
    """銘柄チャートデータレスポンス"""

    symbol: str
    companyName: str = Field(default="")
    timeframe: str
    data: list[StockDataPoint]
    lastUpdated: str = Field(description="最終更新日時 (ISO 8601)")


# --- Stock Search ---


class StockSearchResultItem(BaseModel):
    """銘柄検索結果アイテム"""

    code: str = Field(min_length=4, max_length=4)
    companyName: str
    companyNameEnglish: str | None = None
    marketCode: str
    marketName: str
    sector33Name: str


class StockSearchResponse(BaseModel):
    """銘柄検索レスポンス"""

    query: str
    results: list[StockSearchResultItem]
    count: int = Field(ge=0)


# --- Sector Stocks ---


class SectorStockItem(BaseModel):
    """セクター別銘柄アイテム"""

    rank: int
    code: str
    companyName: str
    marketCode: str
    sector33Name: str
    currentPrice: float
    volume: float
    tradingValue: float | None = None
    tradingValueAverage: float | None = None
    basePrice: float | None = None
    changeAmount: float | None = None
    changePercentage: float | None = None
    lookbackDays: int | None = None


class SectorStocksResponse(BaseModel):
    """セクター別銘柄レスポンス"""

    sector33Name: str | None = None
    sector17Name: str | None = None
    markets: list[str]
    lookbackDays: int
    sortBy: str
    sortOrder: str
    stocks: list[SectorStockItem]
    lastUpdated: str = Field(description="最終更新日時 (ISO 8601)")
