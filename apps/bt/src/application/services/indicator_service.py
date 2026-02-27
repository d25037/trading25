"""
Indicator Service

インジケーター計算のユースケース層。
計算本体は domains 側のレジストリ/計算関数を利用する。
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
from loguru import logger

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.application.services.market_ohlcv_loader import load_stock_ohlcv_df, load_topix_df
from src.domains.analytics.margin_metrics import (
    compute_margin_flow_pressure,
    compute_margin_long_pressure,
    compute_margin_turnover_days,
    compute_margin_volume_ratio,
)
from src.domains.strategy.indicators.indicator_registry import (
    INDICATOR_REGISTRY,
    _clean_value,
    _format_date,
)
from src.domains.strategy.indicators.relative_ohlcv import (
    calculate_relative_ohlcv,
)


MARGIN_REGISTRY: dict[str, Any] = {
    "margin_long_pressure": compute_margin_long_pressure,
    "margin_flow_pressure": compute_margin_flow_pressure,
    "margin_turnover_days": compute_margin_turnover_days,
    "margin_volume_ratio": compute_margin_volume_ratio,
}


class IndicatorService:
    """インジケーター計算サービス"""

    def __init__(self, market_reader: MarketDbReader | None = None) -> None:
        self._market_reader = market_reader
        self._market_client = None

    @property
    def market_client(self):
        if self._market_client is None:
            from src.infrastructure.external_api.market_client import MarketAPIClient

            self._market_client = MarketAPIClient()
        return self._market_client

    def close(self) -> None:
        if self._market_client is not None:
            self._market_client.close()
            self._market_client = None

    def load_ohlcv(
        self,
        stock_code: str,
        source: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """OHLCVデータをロード"""
        from src.infrastructure.external_api.dataset import DatasetAPIClient

        sd = start_date.isoformat() if start_date else None
        ed = end_date.isoformat() if end_date else None

        if source == "market":
            if self._market_reader is not None:
                df = load_stock_ohlcv_df(self._market_reader, stock_code, sd, ed)
            else:
                df = self.market_client.get_stock_ohlcv(stock_code, sd, ed)
        else:
            with DatasetAPIClient(source) as client:
                df = client.get_stock_ohlcv(stock_code, sd, ed)

        if df.empty:
            raise ValueError(f"銘柄 {stock_code} のOHLCVデータが取得できません")
        return df

    @staticmethod
    def resample_timeframe(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """時間枠でリサンプル"""
        if timeframe == "daily":
            return df

        agg_rules: dict[str, str] = {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }

        if timeframe == "weekly":
            resampled = df.resample("W").agg(agg_rules).dropna(subset=["Close"])  # type: ignore[arg-type]
            resampled.index = resampled.index - pd.Timedelta(days=6)
            return resampled

        if timeframe == "monthly":
            resampled = df.resample("ME").agg(agg_rules).dropna(subset=["Close"])  # type: ignore[arg-type]
            resampled.index = resampled.index.to_period("M").to_timestamp()
            return resampled

        raise ValueError(f"未対応のtimeframe: {timeframe}")

    def load_benchmark_ohlcv(
        self,
        benchmark_code: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """ベンチマークOHLCデータをロード"""
        sd = start_date.isoformat() if start_date else None
        ed = end_date.isoformat() if end_date else None

        if benchmark_code == "topix":
            if self._market_reader is not None:
                df = load_topix_df(self._market_reader, sd, ed)
            else:
                df = self.market_client.get_topix(sd, ed)
        else:
            raise ValueError(f"未対応のベンチマーク: {benchmark_code}")

        if df.empty:
            raise ValueError(f"ベンチマーク '{benchmark_code}' のデータが取得できません")
        return df

    def compute_indicators(
        self,
        stock_code: str,
        source: str,
        timeframe: str,
        indicators: list[dict[str, Any]],
        start_date: date | None = None,
        end_date: date | None = None,
        nan_handling: str = "include",
        benchmark_code: str | None = None,
        relative_options: dict[str, Any] | None = None,
        output: str = "indicators",
    ) -> dict[str, Any]:
        """複数インジケーターを一括計算"""
        ohlcv = self.load_ohlcv(stock_code, source, start_date, end_date)
        source_bars = len(ohlcv)

        if benchmark_code:
            benchmark_df = self.load_benchmark_ohlcv(benchmark_code, start_date, end_date)
            opts = relative_options or {}
            handle_zero = opts.get("handle_zero_division", "skip")
            ohlcv = calculate_relative_ohlcv(ohlcv, benchmark_df, handle_zero)

        ohlcv = self.resample_timeframe(ohlcv, timeframe)

        if output == "ohlcv":
            ohlcv_records: list[dict[str, Any]] = []
            for idx, row in ohlcv.iterrows():
                ohlcv_records.append(
                    {
                        "date": _format_date(idx),
                        "open": _clean_value(row["Open"]),
                        "high": _clean_value(row["High"]),
                        "low": _clean_value(row["Low"]),
                        "close": _clean_value(row["Close"]),
                        "volume": _clean_value(row["Volume"]),
                    }
                )
            return {
                "stock_code": stock_code,
                "timeframe": timeframe,
                "meta": {
                    "source_bars": source_bars,
                    "bars": len(ohlcv),
                },
                "indicators": {},
                "ohlcv": ohlcv_records,
            }

        results: dict[str, list[dict[str, Any]]] = {}
        for spec in indicators:
            ind_type = spec["type"]
            params = spec.get("params", {})
            compute_fn = INDICATOR_REGISTRY.get(ind_type)
            if compute_fn is None:
                logger.warning(f"未知のインジケータータイプ: {ind_type}")
                continue
            key, records = compute_fn(ohlcv, params, nan_handling)
            results[key] = records

        return {
            "stock_code": stock_code,
            "timeframe": timeframe,
            "meta": {"bars": len(ohlcv)},
            "indicators": results,
        }

    def compute_margin_indicators(
        self,
        stock_code: str,
        source: str,
        indicator_types: list[str],
        average_period: int = 15,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        """信用指標を計算"""
        from src.infrastructure.external_api.jquants_client import JQuantsAPIClient

        _ = source  # for backward compatibility
        sd = start_date.isoformat() if start_date else None
        ed = end_date.isoformat() if end_date else None

        with JQuantsAPIClient() as jquants_client:
            margin_df = jquants_client.get_margin_interest(stock_code, sd, ed)
            if margin_df.empty:
                raise ValueError(f"銘柄 {stock_code} の信用データが取得できません")

        if self._market_reader is not None:
            ohlcv = load_stock_ohlcv_df(self._market_reader, stock_code, sd, ed)
        else:
            from src.infrastructure.external_api.market_client import MarketAPIClient

            with MarketAPIClient() as market_client:
                ohlcv = market_client.get_stock_ohlcv(stock_code, sd, ed)

        if ohlcv.empty:
            raise ValueError(f"銘柄 {stock_code} のOHLCVデータが取得できません")

        volume = ohlcv["Volume"]

        results: dict[str, list[dict[str, Any]]] = {}
        for ind_type in indicator_types:
            fn = MARGIN_REGISTRY.get(ind_type)
            if fn is None:
                logger.warning(f"未知の信用指標タイプ: {ind_type}")
                continue
            results[ind_type] = fn(margin_df, volume, average_period)

        return {
            "stock_code": stock_code,
            "indicators": results,
        }


# グローバルインスタンス
indicator_service = IndicatorService()
