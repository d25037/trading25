"""
Indicator Service

インジケーター計算サービス。Registry patternで11種類のインジケーターを管理。
相対OHLC（ベンチマーク比較）モードにも対応。
"""

from __future__ import annotations

from datetime import date
from typing import Any, Literal, Protocol

import numpy as np
import pandas as pd
import vectorbt as vbt
from loguru import logger

from src.utils.indicators import (
    compute_atr_support_line,
    compute_nbar_support,
    compute_trading_value_ma,
    compute_volume_mas,
)


class ComputeFn(Protocol):
    """インジケーター計算関数のシグネチャ"""

    def __call__(
        self,
        ohlcv: pd.DataFrame,
        params: dict[str, Any],
        nan_handling: str,
    ) -> tuple[str, list[dict[str, Any]]]: ...


# ===== Helpers =====


def _format_date(idx: Any) -> str:
    """日付インデックスを文字列に変換"""
    return idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)


def _clean_value(val: Any) -> float | None:
    """NaN/infを除去し、有効な値は4桁に丸める"""
    if isinstance(val, float) and np.isinf(val):
        return None
    if pd.isna(val):
        return None
    return round(float(val), 4)


def _series_to_records(
    series: pd.Series[float],
    nan_handling: str,
    value_name: str = "value",
) -> list[dict[str, Any]]:
    """単一値インジケーターをレコードリストに変換"""
    records: list[dict[str, Any]] = []
    for idx, val in series.items():
        cleaned = _clean_value(val)
        if cleaned is None and nan_handling == "omit":
            continue
        records.append({
            "date": _format_date(idx),
            value_name: cleaned,
        })
    return records


def _multi_series_to_records(
    series_dict: dict[str, pd.Series[float]],
    nan_handling: str,
) -> list[dict[str, Any]]:
    """複数値インジケーターをレコードリストに変換"""
    df = pd.DataFrame(series_dict)
    records: list[dict[str, Any]] = []
    for idx, row in df.iterrows():
        record: dict[str, Any] = {"date": _format_date(idx)}
        all_null = True
        for col in series_dict:
            cleaned = _clean_value(row[col])
            record[col] = cleaned
            if cleaned is not None:
                all_null = False

        if nan_handling == "omit" and all_null:
            continue
        records.append(record)
    return records


def _make_key(indicator_type: str, **params: Any) -> str:
    """インジケーターキーを生成: {type}_{param1}_{param2}..."""
    return "_".join([indicator_type, *(str(v) for v in params.values())])


# ===== 11 Indicator Compute Functions =====


def _compute_sma(
    ohlcv: pd.DataFrame, params: dict[str, Any], nan_handling: str
) -> tuple[str, list[dict[str, Any]]]:
    period = params["period"]
    ma: pd.Series[float] = vbt.MA.run(ohlcv["Close"], period).ma
    key = _make_key("sma", period=period)
    return key, _series_to_records(ma, nan_handling)


def _compute_ema(
    ohlcv: pd.DataFrame, params: dict[str, Any], nan_handling: str
) -> tuple[str, list[dict[str, Any]]]:
    period = params["period"]
    ma: pd.Series[float] = vbt.MA.run(ohlcv["Close"], period, ewm=True).ma
    key = _make_key("ema", period=period)
    return key, _series_to_records(ma, nan_handling)


def _compute_rsi(
    ohlcv: pd.DataFrame, params: dict[str, Any], nan_handling: str
) -> tuple[str, list[dict[str, Any]]]:
    period = params.get("period", 14)
    rsi: pd.Series[float] = vbt.RSI.run(ohlcv["Close"], period).rsi
    key = _make_key("rsi", period=period)
    return key, _series_to_records(rsi, nan_handling)


def _compute_macd(
    ohlcv: pd.DataFrame, params: dict[str, Any], nan_handling: str
) -> tuple[str, list[dict[str, Any]]]:
    fast = params.get("fast_period", 12)
    slow = params.get("slow_period", 26)
    signal_period = params.get("signal_period", 9)
    macd_result = vbt.MACD.run(
        ohlcv["Close"],
        fast_window=fast,
        slow_window=slow,
        signal_window=signal_period,
    )
    key = _make_key("macd", fast=fast, slow=slow, signal=signal_period)
    return key, _multi_series_to_records(
        {
            "macd": macd_result.macd,
            "signal": macd_result.signal,
            "histogram": macd_result.hist,
        },
        nan_handling,
    )


def _compute_ppo(
    ohlcv: pd.DataFrame, params: dict[str, Any], nan_handling: str
) -> tuple[str, list[dict[str, Any]]]:
    fast = params.get("fast_period", 12)
    slow = params.get("slow_period", 26)
    signal_period = params.get("signal_period", 9)

    close = ohlcv["Close"]
    fast_ema: pd.Series[float] = vbt.MA.run(close, fast, ewm=True).ma
    slow_ema: pd.Series[float] = vbt.MA.run(close, slow, ewm=True).ma

    # PPO = (fast_ema - slow_ema) / slow_ema * 100
    ppo_line: pd.Series[float] = (fast_ema - slow_ema) / slow_ema.replace(0, np.nan) * 100
    signal_line: pd.Series[float] = vbt.MA.run(ppo_line, signal_period, ewm=True).ma
    histogram: pd.Series[float] = ppo_line - signal_line

    key = _make_key("ppo", fast=fast, slow=slow, signal=signal_period)
    return key, _multi_series_to_records(
        {"ppo": ppo_line, "signal": signal_line, "histogram": histogram},
        nan_handling,
    )


def _compute_bollinger(
    ohlcv: pd.DataFrame, params: dict[str, Any], nan_handling: str
) -> tuple[str, list[dict[str, Any]]]:
    period = params.get("period", 20)
    std_dev = params.get("std_dev", 2.0)
    bb = vbt.BBANDS.run(ohlcv["Close"], window=period, alpha=std_dev)
    key = _make_key("bollinger", period=period, std=std_dev)
    return key, _multi_series_to_records(
        {"upper": bb.upper, "middle": bb.middle, "lower": bb.lower},
        nan_handling,
    )


def _compute_atr(
    ohlcv: pd.DataFrame, params: dict[str, Any], nan_handling: str
) -> tuple[str, list[dict[str, Any]]]:
    period = params.get("period", 14)
    atr_result = vbt.ATR.run(ohlcv["High"], ohlcv["Low"], ohlcv["Close"], window=period)
    key = _make_key("atr", period=period)
    return key, _series_to_records(atr_result.atr, nan_handling)


def _compute_atr_support(
    ohlcv: pd.DataFrame, params: dict[str, Any], nan_handling: str
) -> tuple[str, list[dict[str, Any]]]:
    lookback = params.get("lookback_period", 20)
    multiplier = params.get("atr_multiplier", 2.0)
    support = compute_atr_support_line(
        ohlcv["High"], ohlcv["Low"], ohlcv["Close"], lookback, multiplier
    )
    key = _make_key("atr_support", lookback=lookback, mult=multiplier)
    return key, _series_to_records(support, nan_handling)


def _compute_nbar_support(
    ohlcv: pd.DataFrame, params: dict[str, Any], nan_handling: str
) -> tuple[str, list[dict[str, Any]]]:
    period = params.get("period", 20)
    support = compute_nbar_support(ohlcv["Low"], period)
    key = _make_key("nbar_support", period=period)
    return key, _series_to_records(support, nan_handling)


def _compute_volume_comparison(
    ohlcv: pd.DataFrame, params: dict[str, Any], nan_handling: str
) -> tuple[str, list[dict[str, Any]]]:
    short_period = params.get("short_period", 20)
    long_period = params.get("long_period", 100)
    lower_mult = params.get("lower_multiplier", 1.0)
    higher_mult = params.get("higher_multiplier", 1.5)
    ma_type = params.get("ma_type", "sma")

    short_ma, long_ma = compute_volume_mas(
        ohlcv["Volume"], short_period, long_period, ma_type
    )
    key = _make_key(
        "volume_comparison",
        short=short_period,
        long=long_period,
        lo=lower_mult,
        hi=higher_mult,
        ma=ma_type,
    )
    return key, _multi_series_to_records(
        {
            "shortMA": short_ma,
            "longThresholdLower": long_ma * lower_mult,
            "longThresholdHigher": long_ma * higher_mult,
        },
        nan_handling,
    )


def _compute_trading_value_ma(
    ohlcv: pd.DataFrame, params: dict[str, Any], nan_handling: str
) -> tuple[str, list[dict[str, Any]]]:
    period = params.get("period", 20)
    ma = compute_trading_value_ma(ohlcv["Close"], ohlcv["Volume"], period)
    key = _make_key("trading_value_ma", period=period)
    return key, _series_to_records(ma, nan_handling)


# ===== Relative OHLC =====


def _compute_relative_ohlc_column(
    stock_col: pd.Series[float],
    bench_col: pd.Series[float],
    handle_zero_division: Literal["skip", "zero", "null"],
) -> pd.Series[float]:
    """単一OHLC列の相対値を計算"""
    if handle_zero_division == "skip":
        return stock_col / bench_col

    fill_value = 0.0 if handle_zero_division == "zero" else np.nan
    return pd.Series(
        np.where(bench_col == 0, fill_value, stock_col / bench_col),
        index=stock_col.index,
    )


def calculate_relative_ohlcv(
    stock_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
    handle_zero_division: Literal["skip", "zero", "null"] = "skip",
) -> pd.DataFrame:
    """相対OHLCVを計算（stock / benchmark）

    apps/ts/shared/ta/relative/RelativeOHLCConverter.ts のロジックをPythonに移植。
    日付でalignし、ベンチマークのOHLC値がゼロの日はskip/zero/nullで処理。

    Args:
        stock_df: 銘柄OHLCV DataFrame (Open, High, Low, Close, Volume)
        benchmark_df: ベンチマークOHLC DataFrame (Open, High, Low, Close)
        handle_zero_division: ゼロ除算の処理方法
            - skip: ゼロ除算が発生する日を除外
            - zero: ゼロ除算時は0.0を返す
            - null: ゼロ除算時はNaNを返す

    Returns:
        相対OHLCV DataFrame（日付が整合された分のみ）
    """
    common_dates = stock_df.index.intersection(benchmark_df.index)
    if len(common_dates) == 0:
        raise ValueError("銘柄とベンチマークに共通する日付がありません")

    stock_aligned = stock_df.loc[common_dates]
    bench_aligned = benchmark_df.loc[common_dates]

    ohlc_cols = ["Open", "High", "Low", "Close"]
    bench_has_zero = (bench_aligned[ohlc_cols] == 0).any(axis=1)

    if handle_zero_division == "skip":
        valid_mask = ~bench_has_zero
        stock_aligned = stock_aligned.loc[valid_mask]
        bench_aligned = bench_aligned.loc[valid_mask]

    if stock_aligned.empty:
        raise ValueError("相対計算可能なデータがありません（全日がゼロ除算）")

    result = pd.DataFrame(index=stock_aligned.index)
    for col in ohlc_cols:
        result[col] = _compute_relative_ohlc_column(
            stock_aligned[col], bench_aligned[col], handle_zero_division
        )
    result["Volume"] = stock_aligned["Volume"]

    return result


# ===== Registry =====

INDICATOR_REGISTRY: dict[str, ComputeFn] = {
    "sma": _compute_sma,
    "ema": _compute_ema,
    "rsi": _compute_rsi,
    "macd": _compute_macd,
    "ppo": _compute_ppo,
    "bollinger": _compute_bollinger,
    "atr": _compute_atr,
    "atr_support": _compute_atr_support,
    "nbar_support": _compute_nbar_support,
    "volume_comparison": _compute_volume_comparison,
    "trading_value_ma": _compute_trading_value_ma,
}


# ===== Margin Indicators =====


def compute_margin_long_pressure(
    margin_df: pd.DataFrame,
    volume: pd.Series[float],
    average_period: int = 15,
) -> list[dict[str, Any]]:
    """信用買い圧力: (LongVol - ShortVol) / N-day avg volume"""
    net_margin = margin_df["longMarginVolume"] - margin_df["shortMarginVolume"]
    avg_vol = volume.rolling(average_period).mean()

    records: list[dict[str, Any]] = []
    for idx in net_margin.index:
        if idx not in avg_vol.index:
            continue
        av = avg_vol.get(idx)
        if pd.isna(av) or av == 0:
            continue
        nm = float(net_margin[idx])  # type: ignore[arg-type]
        if pd.isna(nm):
            continue
        lv = float(margin_df.at[idx, "longMarginVolume"])  # type: ignore[arg-type]
        sv = float(margin_df.at[idx, "shortMarginVolume"])  # type: ignore[arg-type]
        records.append({
            "date": _format_date(idx),
            "pressure": round(nm / float(av), 4),
            "longVol": int(lv),
            "shortVol": int(sv),
            "avgVolume": round(float(av), 2),
        })
    return records


def compute_margin_flow_pressure(
    margin_df: pd.DataFrame,
    volume: pd.Series[float],
    average_period: int = 15,
) -> list[dict[str, Any]]:
    """信用フロー圧力: Delta(LongVol - ShortVol) / N-day avg volume"""
    net_margin = margin_df["longMarginVolume"] - margin_df["shortMarginVolume"]
    prev_net_margin = net_margin.shift(1)
    delta = net_margin - prev_net_margin
    avg_vol = volume.rolling(average_period).mean()

    records: list[dict[str, Any]] = []
    for idx in delta.index:
        if idx not in avg_vol.index:
            continue
        av = avg_vol.get(idx)
        d = delta.get(idx)
        if pd.isna(av) or av == 0 or pd.isna(d):
            continue
        prev_val = float(prev_net_margin[idx])  # type: ignore[arg-type]
        records.append({
            "date": _format_date(idx),
            "flowPressure": round(float(d) / float(av), 4),
            "currentNetMargin": int(float(net_margin[idx])),  # type: ignore[arg-type]
            "previousNetMargin": int(prev_val) if not pd.isna(prev_val) else None,
            "avgVolume": round(float(av), 2),
        })
    return records


def compute_margin_turnover_days(
    margin_df: pd.DataFrame,
    volume: pd.Series[float],
    average_period: int = 15,
) -> list[dict[str, Any]]:
    """信用回転日数: LongVol / N-day avg volume"""
    long_vol = margin_df["longMarginVolume"]
    avg_vol = volume.rolling(average_period).mean()

    records: list[dict[str, Any]] = []
    for idx in long_vol.index:
        if idx not in avg_vol.index:
            continue
        av = avg_vol.get(idx)
        lv = long_vol.get(idx)
        if pd.isna(av) or av == 0 or pd.isna(lv):
            continue
        records.append({
            "date": _format_date(idx),
            "turnoverDays": round(float(lv / av), 4),
            "longVol": int(lv),
            "avgVolume": round(float(av), 2),
        })
    return records


def _get_iso_week_key(dt: Any) -> str:
    """日付からISO週キー（YYYY-WW形式）を生成"""
    iso = dt.isocalendar()
    return f"{iso[0]}-{iso[1]:02d}"


def compute_margin_volume_ratio(
    margin_df: pd.DataFrame,
    volume: pd.Series[float],
    average_period: int = 15,
) -> list[dict[str, Any]]:
    """信用残高 / 週間平均出来高 比率

    apps/ts/shared/ta/margin-volume-ratio.tsのロジックを移植。
    ISO週単位で日次出来高を平均し、信用残高との比率を算出。
    """
    positive_vol = volume[volume > 0]
    if positive_vol.empty:
        return []

    week_keys = positive_vol.index.to_series().apply(_get_iso_week_key)
    weekly_avg_map: dict[str, float] = positive_vol.groupby(week_keys).mean().to_dict()

    records: list[dict[str, Any]] = []
    for idx in margin_df.index:
        week_key = _get_iso_week_key(idx)
        avg_vol = weekly_avg_map.get(week_key)
        if avg_vol is None or avg_vol == 0:
            continue

        lv = float(margin_df.at[idx, "longMarginVolume"])  # type: ignore[arg-type]
        sv = float(margin_df.at[idx, "shortMarginVolume"])  # type: ignore[arg-type]
        if pd.isna(lv) or pd.isna(sv):
            continue
        avg_vol_f = float(avg_vol)
        records.append({
            "date": _format_date(idx),
            "longRatio": round(lv / avg_vol_f, 4),
            "shortRatio": round(sv / avg_vol_f, 4),
            "longVol": int(lv),
            "shortVol": int(sv),
            "weeklyAvgVolume": round(avg_vol_f, 2),
        })
    return records


MARGIN_REGISTRY: dict[str, Any] = {
    "margin_long_pressure": compute_margin_long_pressure,
    "margin_flow_pressure": compute_margin_flow_pressure,
    "margin_turnover_days": compute_margin_turnover_days,
    "margin_volume_ratio": compute_margin_volume_ratio,
}


# ===== Service Class =====


class IndicatorService:
    """インジケーター計算サービス"""

    def __init__(self) -> None:
        self._market_client = None

    @property
    def market_client(self):
        if self._market_client is None:
            from src.api.market_client import MarketAPIClient

            self._market_client = MarketAPIClient()
        return self._market_client

    def load_ohlcv(
        self,
        stock_code: str,
        source: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """OHLCVデータをロード

        Args:
            stock_code: 銘柄コード
            source: データソース ("market" またはデータセット名)
            start_date: 開始日
            end_date: 終了日
        """
        from src.api.dataset import DatasetAPIClient

        sd = start_date.isoformat() if start_date else None
        ed = end_date.isoformat() if end_date else None

        if source == "market":
            df = self.market_client.get_stock_ohlcv(stock_code, sd, ed)
        else:
            # sourceをデータセット名として使用
            with DatasetAPIClient(source) as client:
                df = client.get_stock_ohlcv(stock_code, sd, ed)

        if df.empty:
            raise ValueError(f"銘柄 {stock_code} のOHLCVデータが取得できません")
        return df

    @staticmethod
    def resample_timeframe(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """時間枠でリサンプル

        仕様: docs/spec-timeframe-resample.md
        - 週足: インデックスを週開始日（月曜）に調整
        - 月足: インデックスを月初日（1日）に調整
        """
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
            # 週開始日（月曜）に調整（pandasの"W"は日曜アンカー）
            resampled.index = resampled.index - pd.Timedelta(days=6)
            return resampled

        if timeframe == "monthly":
            resampled = df.resample("ME").agg(agg_rules).dropna(subset=["Close"])  # type: ignore[arg-type]
            # 月初日に調整（pandasの"ME"は月末アンカー）
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
        """複数インジケーターを一括計算

        benchmark_code指定時は相対OHLC（stock/benchmark）でインジケーターを計算する。

        Args:
            output: "indicators" = インジケーター計算結果を返す
                    "ohlcv" = 変換後OHLCVのみを返す（インジケーター計算はスキップ）
        """
        ohlcv = self.load_ohlcv(stock_code, source, start_date, end_date)
        source_bars = len(ohlcv)

        # 相対モード: ベンチマークでOHLCVを変換
        if benchmark_code:
            benchmark_df = self.load_benchmark_ohlcv(
                benchmark_code, start_date, end_date
            )
            opts = relative_options or {}
            handle_zero = opts.get("handle_zero_division", "skip")
            ohlcv = calculate_relative_ohlcv(ohlcv, benchmark_df, handle_zero)

        ohlcv = self.resample_timeframe(ohlcv, timeframe)

        # output=ohlcv の場合はOHLCVのみを返す
        if output == "ohlcv":
            ohlcv_records: list[dict[str, Any]] = []
            for idx, row in ohlcv.iterrows():
                date_str = _format_date(idx)
                ohlcv_records.append({
                    "date": date_str,
                    "open": _clean_value(row["Open"]),
                    "high": _clean_value(row["High"]),
                    "low": _clean_value(row["Low"]),
                    "close": _clean_value(row["Close"]),
                    "volume": _clean_value(row["Volume"]),
                })
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

        # output=indicators の場合はインジケーター計算
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
        """信用指標を計算

        データはJQuants API経由で取得（Single Source of Truth）。
        sourceパラメータは後方互換性のため残すが、現在は使用しない。

        Args:
            stock_code: 銘柄コード
            source: データソース（後方互換性のため残す、現在は使用しない）
            indicator_types: 計算する指標タイプのリスト
            average_period: 出来高平均期間
            start_date: 開始日
            end_date: 終了日
        """
        from src.api.jquants_client import JQuantsAPIClient
        from src.api.market_client import MarketAPIClient

        sd = start_date.isoformat() if start_date else None
        ed = end_date.isoformat() if end_date else None

        # JQuants API経由でmarginデータを取得
        with JQuantsAPIClient() as jquants_client:
            margin_df = jquants_client.get_margin_interest(stock_code, sd, ed)
            if margin_df.empty:
                raise ValueError(
                    f"銘柄 {stock_code} の信用データが取得できません"
                )

        # Market API経由でOHLCVデータを取得
        with MarketAPIClient() as market_client:
            ohlcv = market_client.get_stock_ohlcv(stock_code, sd, ed)
            if ohlcv.empty:
                raise ValueError(
                    f"銘柄 {stock_code} のOHLCVデータが取得できません"
                )

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
