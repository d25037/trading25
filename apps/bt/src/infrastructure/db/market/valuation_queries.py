"""Adjusted fundamentals and daily valuation read helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.infrastructure.db.market.market_schema import (
    DAILY_VALUATION_COLUMNS as _DAILY_VALUATION_COLUMNS,
    STATEMENT_METRICS_ADJUSTED_COLUMNS as _STATEMENT_METRICS_ADJUSTED_COLUMNS,
)
from src.infrastructure.db.market.query_helpers import normalize_stock_code


def get_adjusted_statement_metrics(
    table_exists: Callable[[str], bool],
    fetchall_dicts: Callable[[str, list[Any] | tuple[Any, ...] | None], list[dict[str, Any]]],
    code: str,
    as_of_date: str | None = None,
) -> list[dict[str, Any]]:
    """Canonical adjusted statement metrics を code/as-of で取得。"""
    if not table_exists("statement_metrics_adjusted"):
        return []
    normalized_code = normalize_stock_code(code)
    conditions = ["code = ?"]
    params: list[Any] = [normalized_code]
    if as_of_date is not None:
        conditions.append("disclosed_date <= ?")
        params.append(as_of_date)
    return fetchall_dicts(
        f"""
        SELECT {', '.join(_STATEMENT_METRICS_ADJUSTED_COLUMNS)}
        FROM statement_metrics_adjusted
        WHERE {' AND '.join(conditions)}
        ORDER BY disclosed_date, period_end, period_type, basis_version
        """,
        params,
    )


def get_daily_valuation(
    table_exists: Callable[[str], bool],
    fetchall_dicts: Callable[[str, list[Any] | tuple[Any, ...] | None], list[dict[str, Any]]],
    code: str,
    start: str | None = None,
    end: str | None = None,
) -> list[dict[str, Any]]:
    """Canonical daily valuation metrics を code/date range で取得。"""
    if not table_exists("daily_valuation"):
        return []
    normalized_code = normalize_stock_code(code)
    conditions = ["code = ?"]
    params: list[Any] = [normalized_code]
    if start is not None:
        conditions.append("date >= ?")
        params.append(start)
    if end is not None:
        conditions.append("date <= ?")
        params.append(end)
    return fetchall_dicts(
        f"""
        SELECT {', '.join(_DAILY_VALUATION_COLUMNS)}
        FROM daily_valuation
        WHERE {' AND '.join(conditions)}
        ORDER BY date, basis_version
        """,
        params,
    )


def get_daily_valuation_for_codes(
    table_exists: Callable[[str], bool],
    fetchall_dicts: Callable[[str, list[Any] | tuple[Any, ...] | None], list[dict[str, Any]]],
    codes: list[str],
    date: str,
) -> list[dict[str, Any]]:
    """Canonical daily valuation metrics を同一日付の複数codeで取得。"""
    if not codes or not table_exists("daily_valuation"):
        return []
    normalized_codes = sorted({normalize_stock_code(code) for code in codes if code})
    if not normalized_codes:
        return []
    placeholders = ", ".join("?" for _ in normalized_codes)
    return fetchall_dicts(
        f"""
        SELECT {', '.join(_DAILY_VALUATION_COLUMNS)}
        FROM daily_valuation
        WHERE date = ?
          AND code IN ({placeholders})
        ORDER BY code, basis_version
        """,
        [date, *normalized_codes],
    )


def get_adjusted_metrics_snapshot(
    table_exists: Callable[[str], bool],
    count_rows: Callable[[str], int],
    fetchone: Callable[[str, list[Any] | tuple[Any, ...] | None], Any],
) -> dict[str, Any]:
    """Adjusted metrics materialization freshness snapshot."""
    statement_rows = count_rows("statement_metrics_adjusted")
    daily_rows = count_rows("daily_valuation")
    row = None
    if table_exists("daily_valuation"):
        row = fetchone(
            """
            SELECT price_basis_date, basis_version
            FROM daily_valuation
            WHERE price_basis_date IS NOT NULL
            ORDER BY price_basis_date DESC, basis_version DESC
            LIMIT 1
            """,
            None,
        )
    return {
        "statementRows": statement_rows,
        "dailyValuationRows": daily_rows,
        "priceBasisDate": str(row[0]) if row and row[0] is not None else None,
        "basisVersion": str(row[1]) if row and row[1] is not None else None,
    }
