"""
Margin Analytics Service

local market.duckdb から信用取引データを取得し、マージン指標を計算する。
"""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from src.application.services.analytics_data_provider import (
    AnalyticsDataProvider,
    MarketAnalyticsDataProvider,
)
from src.application.services.analytics_provenance import build_market_provenance
from src.entrypoints.http.schemas.analytics_common import ResponseDiagnostics
from src.entrypoints.http.schemas.analytics_margin import (
    MarginFlowPressureData,
    MarginLongPressureData,
    MarginPressureIndicatorsResponse,
    MarginTurnoverDaysData,
    MarginVolumeRatioData,
    MarginVolumeRatioResponse,
)
from src.domains.analytics.margin_metrics import (
    compute_margin_flow_pressure,
    compute_margin_long_pressure,
    compute_margin_turnover_days,
    compute_margin_volume_ratio,
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


class MarginAnalyticsService:
    """マージン分析サービス"""

    def __init__(self, provider: AnalyticsDataProvider) -> None:
        self._provider = provider

    def close(self) -> None:
        close = getattr(self._provider, "close", None)
        if callable(close):
            close()

    def _fetch_margin_and_quotes(
        self, symbol: str
    ) -> tuple[pd.DataFrame, pd.Series[float], ResponseDiagnostics]:
        margin_df = self._provider.get_margin(symbol)
        quotes_df = self._provider.get_stock_ohlcv(symbol)
        diagnostics = ResponseDiagnostics(
            missing_required_data=[],
            used_fields=[
                "margin_data.long_margin_volume",
                "margin_data.short_margin_volume",
                "stock_data.volume",
            ],
        )

        if margin_df.empty:
            diagnostics.missing_required_data.append("margin_data")
        if quotes_df.empty or "Volume" not in quotes_df.columns:
            diagnostics.missing_required_data.append("stock_data.volume")

        volume = quotes_df["Volume"] if "Volume" in quotes_df.columns else pd.Series(dtype=float)
        return margin_df, volume, diagnostics

    async def get_margin_pressure(
        self, symbol: str, period: int = 15
    ) -> MarginPressureIndicatorsResponse:
        margin_df, volume, diagnostics = self._fetch_margin_and_quotes(symbol)

        long_pressure_raw = compute_margin_long_pressure(margin_df, volume, period)
        flow_pressure_raw = compute_margin_flow_pressure(margin_df, volume, period)
        turnover_days_raw = compute_margin_turnover_days(margin_df, volume, period)

        long_pressure = [MarginLongPressureData.model_validate(r) for r in long_pressure_raw]
        flow_pressure = [MarginFlowPressureData.model_validate(r) for r in flow_pressure_raw]
        turnover_days = [MarginTurnoverDaysData.model_validate(r) for r in turnover_days_raw]

        return MarginPressureIndicatorsResponse(
            symbol=symbol,
            averagePeriod=period,
            longPressure=long_pressure,
            flowPressure=flow_pressure,
            turnoverDays=turnover_days,
            lastUpdated=_now_iso(),
            provenance=build_market_provenance(
                loaded_domains=("margin_data", "stock_data"),
            ),
            diagnostics=diagnostics,
        )

    async def get_margin_ratio(self, symbol: str) -> MarginVolumeRatioResponse:
        margin_df, volume, diagnostics = self._fetch_margin_and_quotes(symbol)

        ratio_raw = compute_margin_volume_ratio(margin_df, volume)

        long_ratio = [
            MarginVolumeRatioData(
                date=r["date"],
                ratio=r.get("longRatio", 0),
                weeklyAvgVolume=r.get("weeklyAvgVolume", 0),
                marginVolume=r.get("longVol", 0),
            )
            for r in ratio_raw
        ]
        short_ratio = [
            MarginVolumeRatioData(
                date=r["date"],
                ratio=r.get("shortRatio", 0),
                weeklyAvgVolume=r.get("weeklyAvgVolume", 0),
                marginVolume=r.get("shortVol", 0),
            )
            for r in ratio_raw
        ]

        return MarginVolumeRatioResponse(
            symbol=symbol,
            longRatio=long_ratio,
            shortRatio=short_ratio,
            lastUpdated=_now_iso(),
            provenance=build_market_provenance(
                loaded_domains=("margin_data", "stock_data"),
            ),
            diagnostics=diagnostics,
        )


def create_market_margin_analytics_service(reader: object | None) -> MarginAnalyticsService:
    return MarginAnalyticsService(MarketAnalyticsDataProvider(reader=reader))
