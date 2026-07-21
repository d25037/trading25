"""
Stock Data Row Builder

J-Quants の日足レスポンスを DuckDB stock_data_raw 行へ安全に変換する。
新規上場銘柄などで OHLCV が欠損している行は None を返してスキップする。
"""

from __future__ import annotations

import math
from datetime import UTC, date, datetime
from typing import Any

from src.infrastructure.db.market.query_helpers import normalize_stock_code


def _pick_first(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = data.get(key)
        if value is not None:
            return value
    return None


def _coerce_date(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = date.fromisoformat(text)
    except ValueError:
        return None
    return text if parsed.isoformat() == text else None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        f = float(value)
        return f if math.isfinite(f) else None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            f = float(text)
        except ValueError:
            return None
        return f if math.isfinite(f) else None
    return None


def _coerce_int(value: Any) -> int | None:
    f = _coerce_float(value)
    if f is None or not f.is_integer():
        return None
    return int(f)


def is_provider_no_trade_row(quote: dict[str, Any]) -> bool:
    """Return whether the provider emitted an ordinary all-null daily quote."""
    prices_are_null = all(
        quote.get(key) is None
        for key in (
            "O",
            "H",
            "L",
            "C",
            "Vo",
            "Va",
            "AdjO",
            "AdjH",
            "AdjL",
            "AdjC",
            "AdjVo",
        )
    )
    if not prices_are_null:
        return False

    factor_value = quote.get("AdjFactor")
    if factor_value is None:
        return True
    return _coerce_float(factor_value) == 1.0


def build_stock_data_row(
    quote: dict[str, Any],
    *,
    normalized_code: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any] | None:
    """J-Quants 日足1件を stock_data_raw 行へ変換（欠損値がある場合は None）"""
    payload_code = normalize_stock_code(quote.get("Code", ""))
    requested_code = normalize_stock_code(normalized_code or "")
    if not payload_code or (requested_code and payload_code != requested_code):
        return None
    code = requested_code or payload_code

    date = _coerce_date(quote.get("Date"))
    if date is None:
        return None

    open_value = _coerce_float(_pick_first(quote, "O"))
    high_value = _coerce_float(_pick_first(quote, "H"))
    low_value = _coerce_float(_pick_first(quote, "L"))
    close_value = _coerce_float(_pick_first(quote, "C"))
    volume_value = _coerce_int(_pick_first(quote, "Vo"))
    turnover_value = _coerce_float(_pick_first(quote, "Va"))
    adjustment_factor = _coerce_float(quote.get("AdjFactor"))
    adjusted_open = _coerce_float(quote.get("AdjO"))
    adjusted_high = _coerce_float(quote.get("AdjH"))
    adjusted_low = _coerce_float(quote.get("AdjL"))
    adjusted_close = _coerce_float(quote.get("AdjC"))
    adjusted_volume = _coerce_float(quote.get("AdjVo"))

    required_values = (
        open_value,
        high_value,
        low_value,
        close_value,
        volume_value,
        turnover_value,
        adjustment_factor,
        adjusted_open,
        adjusted_high,
        adjusted_low,
        adjusted_close,
        adjusted_volume,
    )
    if any(value is None for value in required_values):
        return None
    if adjustment_factor is None or adjustment_factor <= 0 or adjusted_volume < 0:
        return None

    return {
        "code": code,
        "date": date,
        "open": open_value,
        "high": high_value,
        "low": low_value,
        "close": close_value,
        "volume": volume_value,
        "turnover_value": turnover_value,
        "adjustment_factor": adjustment_factor,
        "adjusted_open": adjusted_open,
        "adjusted_high": adjusted_high,
        "adjusted_low": adjusted_low,
        "adjusted_close": adjusted_close,
        "adjusted_volume": adjusted_volume,
        "created_at": created_at or datetime.now(UTC).isoformat(),
    }
