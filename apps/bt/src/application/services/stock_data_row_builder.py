"""
Stock Data Row Builder

J-Quants の日足レスポンスを market.db の stock_data 行へ安全に変換する。
新規上場銘柄などで OHLCV が欠損している行は None を返してスキップする。
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
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
    return text if text else None


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
    if f is None:
        return None
    return int(f)


def build_stock_data_row(
    quote: dict[str, Any],
    *,
    normalized_code: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any] | None:
    """J-Quants 日足1件を stock_data 行へ変換（欠損値がある場合は None）"""
    code = normalized_code or normalize_stock_code(quote.get("Code", ""))
    if not code:
        return None

    date = _coerce_date(quote.get("Date"))
    if date is None:
        return None

    open_value = _coerce_float(_pick_first(quote, "AdjO", "O"))
    high_value = _coerce_float(_pick_first(quote, "AdjH", "H"))
    low_value = _coerce_float(_pick_first(quote, "AdjL", "L"))
    close_value = _coerce_float(_pick_first(quote, "AdjC", "C"))
    volume_value = _coerce_int(_pick_first(quote, "AdjVo", "Vo"))

    if any(v is None for v in (open_value, high_value, low_value, close_value, volume_value)):
        return None

    adjustment_factor = _coerce_float(quote.get("AdjFactor"))

    return {
        "code": code,
        "date": date,
        "open": open_value,
        "high": high_value,
        "low": low_value,
        "close": close_value,
        "volume": volume_value,
        "adjustment_factor": adjustment_factor,
        "created_at": created_at or datetime.now(UTC).isoformat(),
    }
