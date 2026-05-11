"""Share-adjusted free-float helpers for liquidity research panels."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd

from src.domains.analytics.readonly_duckdb_support import normalize_code_sql
from src.shared.utils.share_adjustment import (
    ShareAdjustmentEvent,
    adjust_free_float_shares_to_price_basis,
)


def load_adjustment_events_by_code(
    conn: Any,
    *,
    codes: list[str],
    end_date: str | None,
) -> dict[str, list[ShareAdjustmentEvent]]:
    if not codes:
        return {}
    table_exists = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE lower(table_name) = lower('stock_data_raw')
        LIMIT 1
        """
    ).fetchone()
    if table_exists is None:
        return {}

    normalized_code = normalize_code_sql("code")
    placeholders = ",".join("?" for _ in codes)
    date_clause = "AND date <= ?" if end_date else ""
    params: list[Any] = [*codes]
    if end_date:
        params.append(end_date)
    rows = conn.execute(
        f"""
        SELECT
            {normalized_code} AS code,
            date,
            adjustment_factor
        FROM stock_data_raw
        WHERE {normalized_code} IN ({placeholders})
          {date_clause}
          AND adjustment_factor IS NOT NULL
          AND adjustment_factor != 1.0
        ORDER BY code, date
        """,
        params,
    ).fetchall()
    grouped: dict[str, list[ShareAdjustmentEvent]] = {}
    for code, date, adjustment_factor in rows:
        grouped.setdefault(str(code), []).append(
            ShareAdjustmentEvent(
                date=str(date),
                adjustment_factor=float(adjustment_factor),
            )
        )
    return grouped


def apply_adjusted_free_float_market_cap(
    frame: pd.DataFrame,
    *,
    adjustment_events_by_code: dict[str, list[ShareAdjustmentEvent]],
    date_column: str = "date",
    disclosed_date_column: str = "share_disclosed_date",
    close_column: str = "close",
    output_column: str = "free_float_market_cap_jpy",
) -> pd.DataFrame:
    """Compute price-basis adjusted free-float market cap for each row."""
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    values: list[float | None] = []
    for row in result.itertuples(index=False):
        code = str(getattr(row, "code"))
        close = _to_float_or_none(getattr(row, close_column))
        free_float_shares = adjust_free_float_shares_to_price_basis(
            _to_float_or_none(getattr(row, "shares_outstanding", None)),
            _to_float_or_none(getattr(row, "treasury_shares", None)),
            adjustment_events_by_code.get(code, []),
            from_date=_to_date_str(getattr(row, disclosed_date_column, None)),
            through_date=_to_date_str(getattr(row, date_column, None)),
        )
        value = (
            close * free_float_shares
            if close is not None and free_float_shares is not None
            else None
        )
        values.append(value if value is not None and value > 0 else None)
    result[output_column] = values
    return result


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _to_date_str(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    return str(value)
