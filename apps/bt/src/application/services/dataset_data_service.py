"""
Dataset Data Service

Dataset Data エンドポイント（15 EP）のデータ変換ロジック。
DatasetDb の Row オブジェクトを Pydantic スキーマに変換する。
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import Row

from src.entrypoints.http.schemas.dataset_data import (
    IndexListItem,
    MarginListItem,
    MarginRecord,
    OHLCRecord,
    OHLCVRecord,
    SectorWithCount,
    StatementRecord,
    StockListItem,
)


def rows_to_ohlcv(rows: list[Row[Any]]) -> list[OHLCVRecord]:
    """Row → OHLCVRecord 変換"""
    return [
        OHLCVRecord(
            date=r.date,
            open=r.open,
            high=r.high,
            low=r.low,
            close=r.close,
            volume=r.volume,
        )
        for r in rows
    ]


def rows_to_ohlc(rows: list[Row[Any]]) -> list[OHLCRecord]:
    """Row → OHLCRecord 変換（TOPIX/Indices 用、volume なし）"""
    return [
        OHLCRecord(
            date=r.date,
            open=r.open,
            high=r.high,
            low=r.low,
            close=r.close,
        )
        for r in rows
    ]


def rows_to_stock_list(rows: list[Row[Any]]) -> list[StockListItem]:
    """Row → StockListItem 変換"""
    return [
        StockListItem(
            stockCode=r.stockCode,
            record_count=r.record_count,
            start_date=r.start_date,
            end_date=r.end_date,
        )
        for r in rows
    ]


def rows_to_index_list(rows: list[Row[Any]]) -> list[IndexListItem]:
    """Row → IndexListItem 変換"""
    return [
        IndexListItem(
            indexCode=r.indexCode,
            indexName=r.indexName,
            record_count=r.record_count,
            start_date=r.start_date,
            end_date=r.end_date,
        )
        for r in rows
    ]


def rows_to_margin(rows: list[Row[Any]]) -> list[MarginRecord]:
    """Row → MarginRecord 変換"""
    return [
        MarginRecord(
            date=r.date,
            longMarginVolume=r.long_margin_volume,
            shortMarginVolume=r.short_margin_volume,
        )
        for r in rows
    ]


def rows_to_margin_list(rows: list[Row[Any]]) -> list[MarginListItem]:
    """Row → MarginListItem 変換"""
    return [
        MarginListItem(
            stockCode=r.stockCode,
            record_count=r.record_count,
            start_date=r.start_date,
            end_date=r.end_date,
            avg_long_margin=r.avg_long_margin,
            avg_short_margin=r.avg_short_margin,
        )
        for r in rows
    ]


def rows_to_statements(rows: list[Row[Any]]) -> list[StatementRecord]:
    """Row → StatementRecord 変換"""
    return [
        StatementRecord(
            code=r.code,
            disclosedDate=r.disclosed_date,
            earningsPerShare=r.earnings_per_share,
            profit=r.profit,
            equity=r.equity,
            typeOfCurrentPeriod=r.type_of_current_period,
            typeOfDocument=r.type_of_document,
            nextYearForecastEarningsPerShare=r.next_year_forecast_earnings_per_share,
            bps=r.bps,
            sales=r.sales,
            operatingProfit=r.operating_profit,
            ordinaryProfit=r.ordinary_profit,
            operatingCashFlow=r.operating_cash_flow,
            dividendFy=r.dividend_fy,
            forecastDividendFy=r.forecast_dividend_fy,
            nextYearForecastDividendFy=r.next_year_forecast_dividend_fy,
            payoutRatio=r.payout_ratio,
            forecastPayoutRatio=r.forecast_payout_ratio,
            nextYearForecastPayoutRatio=r.next_year_forecast_payout_ratio,
            forecastEps=r.forecast_eps,
            investingCashFlow=r.investing_cash_flow,
            financingCashFlow=r.financing_cash_flow,
            cashAndEquivalents=r.cash_and_equivalents,
            totalAssets=r.total_assets,
            sharesOutstanding=r.shares_outstanding,
            treasuryShares=r.treasury_shares,
        )
        for r in rows
    ]


def rows_to_sector_with_count(rows: list[Row[Any]]) -> list[SectorWithCount]:
    """Row → SectorWithCount 変換"""
    return [
        SectorWithCount(sectorName=r[0], count=r[1])
        for r in rows
    ]


def batch_to_ohlcv(batch: dict[str, list[Row[Any]]]) -> dict[str, list[OHLCVRecord]]:
    """Batch → {code: [OHLCVRecord]} 変換"""
    return {code: rows_to_ohlcv(rows) for code, rows in batch.items()}


def batch_to_margin(batch: dict[str, list[Row[Any]]]) -> dict[str, list[MarginRecord]]:
    """Batch → {code: [MarginRecord]} 変換"""
    return {code: rows_to_margin(rows) for code, rows in batch.items()}


def batch_to_statements(batch: dict[str, list[Row[Any]]]) -> dict[str, list[StatementRecord]]:
    """Batch → {code: [StatementRecord]} 変換"""
    return {code: rows_to_statements(rows) for code, rows in batch.items()}
