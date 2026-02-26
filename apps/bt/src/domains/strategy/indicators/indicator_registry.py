"""Indicator registry and shared formatting helpers."""

from __future__ import annotations

from typing import Any, Literal, Protocol

import numpy as np
import pandas as pd
import vectorbt as vbt

from src.domains.strategy.indicators import (
    compute_atr_support_line,
    compute_nbar_support,
    compute_risk_adjusted_return,
    compute_trading_value_ma,
    compute_volume_mas,
)


class ComputeFn(Protocol):
    """Indicator compute function signature."""

    def __call__(
        self,
        ohlcv: pd.DataFrame,
        params: dict[str, Any],
        nan_handling: str,
    ) -> tuple[str, list[dict[str, Any]]]: ...


def _format_date(idx: Any) -> str:
    return idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)


def _clean_value(val: Any) -> float | None:
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
    records: list[dict[str, Any]] = []
    for idx, val in series.items():
        cleaned = _clean_value(val)
        if cleaned is None and nan_handling == "omit":
            continue
        records.append(
            {
                "date": _format_date(idx),
                value_name: cleaned,
            }
        )
    return records


def _multi_series_to_records(
    series_dict: dict[str, pd.Series[float]],
    nan_handling: str,
) -> list[dict[str, Any]]:
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
    return "_".join([indicator_type, *(str(v) for v in params.values())])


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

    short_ma, long_ma = compute_volume_mas(ohlcv["Volume"], short_period, long_period, ma_type)
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


def _compute_risk_adjusted_return(
    ohlcv: pd.DataFrame, params: dict[str, Any], nan_handling: str
) -> tuple[str, list[dict[str, Any]]]:
    lookback_period = params.get("lookback_period", 60)
    ratio_type_raw = params.get("ratio_type", "sortino")
    if ratio_type_raw not in ("sharpe", "sortino"):
        raise ValueError(f"不正なratio_type: {ratio_type_raw} (sharpe/sortinoのみ)")
    ratio_type: Literal["sharpe", "sortino"] = ratio_type_raw

    ratio = compute_risk_adjusted_return(
        close=ohlcv["Close"],
        lookback_period=lookback_period,
        ratio_type=ratio_type,
    )
    key = _make_key("risk_adjusted_return", lookback=lookback_period, ratio=ratio_type)
    return key, _series_to_records(ratio, nan_handling)


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
    "risk_adjusted_return": _compute_risk_adjusted_return,
}
