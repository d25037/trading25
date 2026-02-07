"""
Margin Analytics Service

JQuants API から信用取引データを取得し、マージン指標を計算する。
既存の indicator_service.py の計算関数を再利用する。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd

from src.server.clients.jquants_client import JQuantsAsyncClient
from src.server.schemas.analytics_margin import (
    MarginFlowPressureData,
    MarginLongPressureData,
    MarginPressureIndicatorsResponse,
    MarginTurnoverDaysData,
    MarginVolumeRatioData,
    MarginVolumeRatioResponse,
)
from src.server.services.indicator_service import (
    compute_margin_flow_pressure,
    compute_margin_long_pressure,
    compute_margin_turnover_days,
    compute_margin_volume_ratio,
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _build_margin_df(raw_data: list[dict[str, Any]]) -> pd.DataFrame:
    """JQuants margin interest データを DataFrame に変換"""
    if not raw_data:
        return pd.DataFrame(columns=["long_volume", "short_volume"], dtype=float)

    records = []
    for item in raw_data:
        date_str = item.get("Date", "")
        records.append(
            {
                "date": pd.Timestamp(date_str),
                "long_volume": float(item.get("MarginBuyingOutstandingBalance", 0) or 0),
                "short_volume": float(item.get("ShortSellingOutstandingBalance", 0) or 0),
            }
        )

    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(columns=["long_volume", "short_volume"], dtype=float)

    df = df.set_index("date").sort_index()
    return df


def _build_volume_series(raw_quotes: list[dict[str, Any]]) -> pd.Series[float]:
    """日足クォートデータから出来高 Series を構築"""
    if not raw_quotes:
        return pd.Series(dtype=float)

    records = []
    for item in raw_quotes:
        date_str = item.get("Date", "")
        vol = item.get("Volume", item.get("AdjVo", 0))
        records.append({"date": pd.Timestamp(date_str), "volume": float(vol or 0)})

    df = pd.DataFrame(records)
    if df.empty:
        return pd.Series(dtype=float)

    df = df.set_index("date").sort_index()
    return df["volume"]


class MarginAnalyticsService:
    """マージン分析サービス"""

    def __init__(self, client: JQuantsAsyncClient) -> None:
        self._client = client

    async def _fetch_margin_and_quotes(
        self, symbol: str
    ) -> tuple[pd.DataFrame, pd.Series[float]]:
        """信用取引データと出来高データを取得"""
        code5 = f"{symbol}0"

        # 信用取引データ
        margin_body = await self._client.get(
            "/markets/margin-interest", {"code": code5}
        )
        margin_raw = margin_body.get("weekly_margin_interest", [])
        margin_df = _build_margin_df(margin_raw)

        # 出来高データ（日足）
        quotes_body = await self._client.get(
            "/equities/bars/daily", {"code": code5}
        )
        quotes_raw = quotes_body.get("daily_quotes", [])
        volume = _build_volume_series(quotes_raw)

        return margin_df, volume

    async def get_margin_pressure(
        self, symbol: str, period: int = 15
    ) -> MarginPressureIndicatorsResponse:
        """マージンプレッシャー指標を計算"""
        margin_df, volume = await self._fetch_margin_and_quotes(symbol)

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
        )

    async def get_margin_ratio(self, symbol: str) -> MarginVolumeRatioResponse:
        """マージン出来高比率を計算"""
        margin_df, volume = await self._fetch_margin_and_quotes(symbol)

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
        )
