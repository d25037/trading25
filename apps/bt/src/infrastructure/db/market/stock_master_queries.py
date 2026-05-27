"""Stock master and trading-date reader queries for MarketDb."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.infrastructure.db.market.query_helpers import normalize_stock_code

TableExists = Callable[[str], bool]
FetchOne = Callable[[str, list[Any] | tuple[Any, ...] | None], Any]
FetchAll = Callable[[str, list[Any] | tuple[Any, ...] | None], list[Any]]


def get_latest_table_date(
    table_exists: TableExists,
    fetchone: FetchOne,
    *,
    table_name: str,
) -> str | None:
    if not table_exists(table_name):
        return None
    row = fetchone(f"SELECT MAX(date) FROM {table_name}", None)
    return str(row[0]) if row and row[0] is not None else None


def get_topix_dates(
    table_exists: TableExists,
    fetchall: FetchAll,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[str]:
    if not table_exists("topix_data"):
        return []
    sql = "SELECT date FROM topix_data"
    params: list[Any] = []
    conditions: list[str] = []
    if start_date:
        conditions.append("date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("date <= ?")
        params.append(end_date)
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY date"
    rows = fetchall(sql, params)
    return [str(row[0]) for row in rows if row and row[0]]


def get_missing_stock_master_dates(
    table_exists: TableExists,
    fetchall: FetchAll,
    *,
    limit: int | None = 20,
) -> list[str]:
    if not table_exists("topix_data") or not table_exists("stock_master_daily"):
        return []
    sql = """
        SELECT t.date
        FROM topix_data t
        LEFT JOIN stock_master_daily m ON m.date = t.date
        GROUP BY t.date
        HAVING COUNT(m.code) = 0
        ORDER BY t.date
    """
    params: list[Any] = []
    if limit is not None:
        sql += " LIMIT ?"
        params.append(max(limit, 0))
    rows = fetchall(sql, params)
    return [str(row[0]) for row in rows if row and row[0]]


def get_missing_stock_master_dates_count(
    table_exists: TableExists,
    fetchone: FetchOne,
) -> int:
    if not table_exists("topix_data") or not table_exists("stock_master_daily"):
        return 0
    row = fetchone(
        """
        SELECT COUNT(*)
        FROM (
            SELECT t.date
            FROM topix_data t
            LEFT JOIN stock_master_daily m ON m.date = t.date
            GROUP BY t.date
            HAVING COUNT(m.code) = 0
        ) missing
        """,
        None,
    )
    return int(row[0] or 0) if row else 0


def get_stock_master_rows_for_date(
    table_exists: TableExists,
    fetchall: FetchAll,
    as_of_date: str,
    *,
    market_codes: list[str] | None = None,
    scale_categories: list[str] | None = None,
) -> list[dict[str, Any]]:
    if not table_exists("stock_master_daily"):
        return []
    conditions, params = _build_stock_master_conditions(
        date_condition="date = ?",
        date_params=[as_of_date],
        market_codes=market_codes,
        scale_categories=scale_categories,
        exclude_scale_categories=None,
    )
    rows = fetchall(
        f"""
        SELECT
            date, code, company_name, company_name_english, market_code, market_name,
            sector_17_code, sector_17_name, sector_33_code, sector_33_name,
            scale_category, listed_date
        FROM stock_master_daily
        WHERE {' AND '.join(conditions)}
        ORDER BY code
        """,
        params,
    )
    return [
        {
            "date": row[0],
            "code": row[1],
            "company_name": row[2],
            "company_name_english": row[3],
            "market_code": row[4],
            "market_name": row[5],
            "sector_17_code": row[6],
            "sector_17_name": row[7],
            "sector_33_code": row[8],
            "sector_33_name": row[9],
            "scale_category": row[10],
            "listed_date": row[11],
        }
        for row in rows
    ]


def get_stock_master_codes_for_date(
    table_exists: TableExists,
    fetchall: FetchAll,
    as_of_date: str,
    *,
    market_codes: list[str] | None = None,
    scale_categories: list[str] | None = None,
    exclude_scale_categories: list[str] | None = None,
) -> list[str]:
    if not table_exists("stock_master_daily"):
        return []
    conditions, params = _build_stock_master_conditions(
        date_condition="date = ?",
        date_params=[as_of_date],
        market_codes=market_codes,
        scale_categories=scale_categories,
        exclude_scale_categories=exclude_scale_categories,
    )
    rows = fetchall(
        f"""
        SELECT code
        FROM stock_master_daily
        WHERE {' AND '.join(conditions)}
        ORDER BY code
        """,
        params,
    )
    return [code for row in rows if row and (code := normalize_stock_code(row[0]))]


def get_stock_master_codes_for_date_range(
    table_exists: TableExists,
    fetchall: FetchAll,
    start_date: str,
    end_date: str,
    *,
    market_codes: list[str] | None = None,
    scale_categories: list[str] | None = None,
    exclude_scale_categories: list[str] | None = None,
) -> list[str]:
    if not table_exists("stock_master_daily"):
        return []
    conditions, params = _build_stock_master_conditions(
        date_condition="date BETWEEN ? AND ?",
        date_params=[start_date, end_date],
        market_codes=market_codes,
        scale_categories=scale_categories,
        exclude_scale_categories=exclude_scale_categories,
    )
    rows = fetchall(
        f"""
        SELECT DISTINCT code
        FROM stock_master_daily
        WHERE {' AND '.join(conditions)}
        ORDER BY code
        """,
        params,
    )
    return [code for row in rows if row and (code := normalize_stock_code(row[0]))]


def get_stock_master_code_dates_for_date_range(
    table_exists: TableExists,
    fetchall: FetchAll,
    start_date: str,
    end_date: str,
    *,
    codes: list[str] | None = None,
    market_codes: list[str] | None = None,
    scale_categories: list[str] | None = None,
    exclude_scale_categories: list[str] | None = None,
) -> list[tuple[str, str]]:
    if not table_exists("stock_master_daily"):
        return []
    conditions, params = _build_stock_master_conditions(
        date_condition="date BETWEEN ? AND ?",
        date_params=[start_date, end_date],
        codes=codes,
        market_codes=market_codes,
        scale_categories=scale_categories,
        exclude_scale_categories=exclude_scale_categories,
    )
    rows = fetchall(
        f"""
        SELECT date, code
        FROM stock_master_daily
        WHERE {' AND '.join(conditions)}
        ORDER BY date, code
        """,
        params,
    )
    return [
        (str(row[0]), code)
        for row in rows
        if row and (code := normalize_stock_code(row[1]))
    ]


def get_index_membership_codes(
    table_exists: TableExists,
    fetchall: FetchAll,
    as_of_date: str,
    index_code: str,
) -> set[str]:
    if not table_exists("index_membership_daily"):
        return set()
    rows = fetchall(
        """
        SELECT code
        FROM index_membership_daily
        WHERE date = ? AND index_code = ?
        ORDER BY code
        """,
        [as_of_date, index_code],
    )
    return {code for row in rows if row and (code := normalize_stock_code(row[0]))}


def _build_stock_master_conditions(
    *,
    date_condition: str,
    date_params: list[Any],
    codes: list[str] | None = None,
    market_codes: list[str] | None = None,
    scale_categories: list[str] | None = None,
    exclude_scale_categories: list[str] | None = None,
) -> tuple[list[str], list[Any]]:
    conditions = [date_condition]
    params = list(date_params)
    if codes:
        placeholders = ", ".join("?" for _ in codes)
        conditions.append(f"code IN ({placeholders})")
        params.extend(codes)
    if market_codes:
        placeholders = ", ".join("?" for _ in market_codes)
        conditions.append(f"market_code IN ({placeholders})")
        params.extend(market_codes)
    if scale_categories:
        placeholders = ", ".join("?" for _ in scale_categories)
        conditions.append(f"coalesce(scale_category, '') IN ({placeholders})")
        params.extend(scale_categories)
    if exclude_scale_categories:
        placeholders = ", ".join("?" for _ in exclude_scale_categories)
        conditions.append(f"coalesce(scale_category, '') NOT IN ({placeholders})")
        params.extend(exclude_scale_categories)
    return conditions, params
