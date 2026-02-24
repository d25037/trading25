"""
Market Data Response Schemas

Hono hono-openapi-baseline.json と完全互換のレスポンススキーマ。
"""

from __future__ import annotations

from pydantic import BaseModel, Field


# --- OHLCV (with volume) ---


class MarketOHLCVRecord(BaseModel):
    """OHLCV レコード（出来高あり）"""

    date: str = Field(description="日付 (YYYY-MM-DD)")
    open: float
    high: float
    low: float
    close: float
    volume: int


# --- OHLC (no volume, for TOPIX) ---


class MarketOHLCRecord(BaseModel):
    """OHLC レコード（出来高なし、TOPIX 用）"""

    date: str = Field(description="日付 (YYYY-MM-DD)")
    open: float
    high: float
    low: float
    close: float


# --- Stock Info ---


class StockInfo(BaseModel):
    """銘柄情報（単一銘柄）"""

    code: str = Field(description="銘柄コード (5桁)")
    companyName: str = Field(description="会社名（日本語）")
    companyNameEnglish: str = Field(default="", description="会社名（英語）")
    marketCode: str = Field(default="", description="市場コード")
    marketName: str = Field(default="", description="市場名")
    sector17Code: str = Field(default="", description="17業種コード")
    sector17Name: str = Field(default="", description="17業種名")
    sector33Code: str = Field(default="", description="33業種コード")
    sector33Name: str = Field(default="", description="33業種名")
    scaleCategory: str = Field(default="", description="規模区分")
    listedDate: str = Field(default="", description="上場日")


# --- Market Stock Data (bulk screening) ---


class MarketStockData(BaseModel):
    """市場別銘柄データ（スクリーニング用バルクレスポンス）"""

    code: str = Field(description="銘柄コード")
    company_name: str = Field(description="会社名（日本語）")
    data: list[MarketOHLCVRecord] = Field(description="OHLCV データ配列")
