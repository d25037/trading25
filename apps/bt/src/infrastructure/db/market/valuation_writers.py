"""Adjusted fundamentals and daily valuation writer helpers."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

import pandas as pd

from src.infrastructure.db.market.market_schema import (
    DAILY_VALUATION_COLUMNS as _DAILY_VALUATION_COLUMNS,
    STATEMENT_METRICS_ADJUSTED_COLUMNS as _STATEMENT_METRICS_ADJUSTED_COLUMNS,
    STATEMENT_METRICS_ADJUSTED_RELATION as _STATEMENT_METRICS_ADJUSTED_RELATION,
)
from src.infrastructure.db.market.query_helpers import normalize_stock_code


def upsert_statement_metrics_adjusted(
    conn: Any,
    lock: Any,
    rows: list[dict[str, Any]],
) -> int:
    """Canonical split-adjusted statement metrics を relation-based upsert。"""
    if not rows:
        return 0

    now_iso = datetime.now().isoformat()  # noqa: DTZ005
    columns = _STATEMENT_METRICS_ADJUSTED_COLUMNS
    update_columns = [
        column
        for column in columns
        if column
        not in {"code", "disclosed_date", "period_end", "period_type", "basis_version"}
    ]
    update_clause = ", ".join(
        f"{column} = excluded.{column}"
        for column in update_columns
    )
    conflict_columns = (
        "code",
        "disclosed_date",
        "period_end",
        "period_type",
        "basis_version",
    )
    deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        payload = {
            column: row.get(column)
            if column != "created_at" or column in row
            else now_iso
            for column in columns
        }
        deduped[tuple(payload[column] for column in conflict_columns)] = payload
    dataframe = pd.DataFrame.from_records(
        list(deduped.values()),
        columns=columns,
    )
    columns_sql = ", ".join(columns)
    conflict_sql = ", ".join(conflict_columns)
    sql = f"""
        INSERT INTO statement_metrics_adjusted ({columns_sql})
        SELECT {columns_sql} FROM {_STATEMENT_METRICS_ADJUSTED_RELATION}
        ON CONFLICT ({conflict_sql}) DO UPDATE SET {update_clause}
        """
    with lock:
        conn.register(_STATEMENT_METRICS_ADJUSTED_RELATION, dataframe)
        try:
            conn.execute(sql)
        finally:
            conn.unregister(_STATEMENT_METRICS_ADJUSTED_RELATION)
    return len(rows)


def upsert_daily_valuation(
    executemany: Callable[[str, list[tuple[Any, ...]]], None],
    rows: list[dict[str, Any]],
) -> int:
    """Canonical daily valuation metrics に upsert。"""
    if not rows:
        return 0

    now_iso = datetime.now().isoformat()  # noqa: DTZ005
    columns = _DAILY_VALUATION_COLUMNS
    placeholders = ", ".join("?" for _ in columns)
    update_columns = [
        column
        for column in columns
        if column not in {"code", "date", "basis_version"}
    ]
    update_clause = ", ".join(
        f"{column} = excluded.{column}"
        for column in update_columns
    )
    sql = (
        f"INSERT INTO daily_valuation ({', '.join(columns)}) "
        f"VALUES ({placeholders}) "
        "ON CONFLICT (code, date, basis_version) "
        f"DO UPDATE SET {update_clause}"
    )
    params = [
        tuple(
            row.get(column)
            if column != "created_at"
            else row.get("created_at", now_iso)
            for column in columns
        )
        for row in rows
    ]
    executemany(sql, params)
    return len(rows)


_DAILY_VALUATION_REBUILD_SQL_TEMPLATE = """
        WITH
        stock_prices AS (
            SELECT code, date, close
            FROM stock_data
            {code_filter}
            ORDER BY code, date
        ),
        actual_metrics AS (
            SELECT code, disclosed_date, adjusted_eps
            FROM statement_metrics_adjusted
            WHERE basis_version = ?
              AND upper(period_type) = 'FY'
              AND adjusted_eps IS NOT NULL
            ORDER BY code, disclosed_date
        ),
        bps_metrics AS (
            SELECT code, disclosed_date, adjusted_bps
            FROM statement_metrics_adjusted
            WHERE basis_version = ?
              AND upper(period_type) = 'FY'
              AND adjusted_bps IS NOT NULL
            ORDER BY code, disclosed_date
        ),
        fy_cycle_anchors AS (
            SELECT m.code, m.disclosed_date
            FROM statement_metrics_adjusted AS m
            LEFT JOIN statements AS s
              ON s.code = m.code
             AND s.disclosed_date = m.disclosed_date
            WHERE m.basis_version = ?
              AND upper(m.period_type) = 'FY'
              AND (
                  m.adjusted_eps > 0
                  OR m.adjusted_bps > 0
              )
              AND (
                  s.type_of_document IS NULL
                  OR s.type_of_document NOT LIKE '%EarnForecastRevision%'
              )
            ORDER BY m.code, m.disclosed_date
        ),
        forward_metrics AS (
            SELECT
                m.code,
                m.disclosed_date,
                m.period_type,
                m.adjusted_forecast_eps,
                CASE
                    WHEN s.type_of_document LIKE '%EarnForecastRevision%'
                    THEN 'revised'
                    WHEN upper(m.period_type) = 'FY'
                    THEN 'fy'
                    ELSE 'revised'
                END AS forward_source
            FROM statement_metrics_adjusted AS m
            LEFT JOIN fy_cycle_anchors AS fy
              ON fy.code = m.code
             AND fy.disclosed_date = m.disclosed_date
            LEFT JOIN statements AS s
              ON s.code = m.code
             AND s.disclosed_date = m.disclosed_date
            WHERE m.basis_version = ?
              AND m.adjusted_forecast_eps IS NOT NULL
              AND (
                  upper(m.period_type) != 'FY'
                  OR fy.disclosed_date IS NOT NULL
                  OR s.type_of_document LIKE '%EarnForecastRevision%'
              )
            ORDER BY m.code, m.disclosed_date
        ),
        actual_operating_profit_metrics AS (
            SELECT st.code, st.disclosed_date, st.operating_profit
            FROM statements AS st
            JOIN fy_cycle_anchors AS fy
              ON fy.code = st.code
             AND fy.disclosed_date = st.disclosed_date
            WHERE upper(st.type_of_current_period) = 'FY'
              AND st.operating_profit IS NOT NULL
            ORDER BY st.code, st.disclosed_date
        ),
        forward_operating_profit_metrics AS (
            SELECT
                st.code,
                st.disclosed_date,
                st.type_of_current_period AS period_type,
                CASE
                    WHEN st.type_of_document LIKE '%EarnForecastRevision%'
                    THEN 'revised'
                    WHEN upper(st.type_of_current_period) = 'FY'
                    THEN 'fy'
                    ELSE 'revised'
                END AS forward_source,
                CASE
                    WHEN st.type_of_document LIKE '%EarnForecastRevision%'
                    THEN COALESCE(st.forecast_operating_profit, st.next_year_forecast_operating_profit)
                    WHEN upper(st.type_of_current_period) = 'FY'
                    THEN COALESCE(st.next_year_forecast_operating_profit, st.forecast_operating_profit)
                    ELSE st.forecast_operating_profit
                END AS forecast_operating_profit
            FROM statements AS st
            LEFT JOIN fy_cycle_anchors AS fy
              ON fy.code = st.code
             AND fy.disclosed_date = st.disclosed_date
            WHERE (
                upper(st.type_of_current_period) = 'FY'
                AND (
                    fy.disclosed_date IS NOT NULL
                    OR st.type_of_document LIKE '%EarnForecastRevision%'
                )
                AND (
                    st.forecast_operating_profit IS NOT NULL
                    OR st.next_year_forecast_operating_profit IS NOT NULL
                )
            )
               OR (
                   upper(st.type_of_current_period) != 'FY'
                   AND st.forecast_operating_profit IS NOT NULL
               )
            ORDER BY st.code, st.disclosed_date
        ),
        shares_metrics AS (
            SELECT
                code,
                disclosed_date,
                adjusted_shares_outstanding,
                adjusted_treasury_shares
            FROM statement_metrics_adjusted
            WHERE basis_version = ?
              AND adjusted_shares_outstanding IS NOT NULL
            ORDER BY code, disclosed_date
        )
        INSERT INTO daily_valuation ({daily_valuation_columns})
        SELECT
            s.code,
            s.date,
            ? AS price_basis_date,
            s.close,
            a.adjusted_eps AS eps,
            b.adjusted_bps AS bps,
            CASE
                WHEN f.disclosed_date IS NOT NULL
                 AND fy.disclosed_date IS NOT NULL
                 AND (
                     (
                         f.forward_source = 'fy'
                         AND f.disclosed_date = fy.disclosed_date
                     )
                     OR (
                         f.forward_source = 'revised'
                         AND f.disclosed_date > fy.disclosed_date
                     )
                 )
                THEN f.adjusted_forecast_eps
                ELSE NULL
            END AS forward_eps,
            CASE
                WHEN s.close > 0 AND a.adjusted_eps > 0
                THEN s.close / a.adjusted_eps
                ELSE NULL
            END AS per,
            CASE
                WHEN s.close > 0
                 AND f.adjusted_forecast_eps > 0
                 AND fy.disclosed_date IS NOT NULL
                 AND (
                     (
                         f.forward_source = 'fy'
                         AND f.disclosed_date = fy.disclosed_date
                     )
                     OR (
                         f.forward_source = 'revised'
                         AND f.disclosed_date > fy.disclosed_date
                     )
                 )
                THEN s.close / f.adjusted_forecast_eps
                ELSE NULL
            END AS forward_per,
            CASE
                WHEN s.close > 0
                 AND sh.adjusted_shares_outstanding > 0
                 AND op.operating_profit > 0
                 AND op.disclosed_date = fy.disclosed_date
                THEN s.close * sh.adjusted_shares_outstanding / op.operating_profit
                ELSE NULL
            END AS p_op,
            CASE
                WHEN s.close > 0
                 AND sh.adjusted_shares_outstanding > 0
                 AND fop.forecast_operating_profit > 0
                 AND fy.disclosed_date IS NOT NULL
                 AND (
                     (
                         fop.forward_source = 'fy'
                         AND fop.disclosed_date = fy.disclosed_date
                     )
                     OR (
                         fop.forward_source = 'revised'
                         AND fop.disclosed_date > fy.disclosed_date
                     )
                 )
                THEN s.close * sh.adjusted_shares_outstanding / fop.forecast_operating_profit
                ELSE NULL
            END AS forward_p_op,
            CASE
                WHEN s.close > 0 AND b.adjusted_bps > 0
                THEN s.close / b.adjusted_bps
                ELSE NULL
            END AS pbr,
            CASE
                WHEN s.close > 0 AND sh.adjusted_shares_outstanding > 0
                THEN s.close * sh.adjusted_shares_outstanding
                ELSE NULL
            END AS market_cap,
            CASE
                WHEN s.close > 0
                 AND sh.adjusted_shares_outstanding > 0
                 AND sh.adjusted_shares_outstanding - COALESCE(sh.adjusted_treasury_shares, 0) > 0
                THEN s.close * (
                    sh.adjusted_shares_outstanding
                    - COALESCE(sh.adjusted_treasury_shares, 0)
                )
                ELSE NULL
            END AS free_float_market_cap,
            COALESCE(a.disclosed_date, b.disclosed_date) AS statement_disclosed_date,
            CASE
                WHEN f.adjusted_forecast_eps IS NOT NULL
                 AND fy.disclosed_date IS NOT NULL
                 AND (
                     (
                         f.forward_source = 'fy'
                         AND f.disclosed_date = fy.disclosed_date
                     )
                     OR (
                         f.forward_source = 'revised'
                         AND f.disclosed_date > fy.disclosed_date
                     )
                 )
                THEN f.disclosed_date
                ELSE NULL
            END AS forward_eps_disclosed_date,
            CASE
                 WHEN f.adjusted_forecast_eps IS NOT NULL
                 AND f.forward_source = 'fy'
                 AND fy.disclosed_date IS NOT NULL
                 AND f.disclosed_date = fy.disclosed_date
                THEN 'fy'
                WHEN f.adjusted_forecast_eps IS NOT NULL
                 AND fy.disclosed_date IS NOT NULL
                 AND f.forward_source = 'revised'
                 AND f.disclosed_date > fy.disclosed_date
                THEN 'revised'
                ELSE NULL
            END AS forward_eps_source,
            ? AS basis_version,
            ? AS created_at
        FROM stock_prices AS s
        ASOF LEFT JOIN actual_metrics AS a
          ON s.code = a.code
         AND s.date >= a.disclosed_date
        ASOF LEFT JOIN bps_metrics AS b
          ON s.code = b.code
         AND s.date >= b.disclosed_date
        ASOF LEFT JOIN forward_metrics AS f
          ON s.code = f.code
         AND s.date >= f.disclosed_date
        ASOF LEFT JOIN fy_cycle_anchors AS fy
          ON s.code = fy.code
         AND s.date >= fy.disclosed_date
        ASOF LEFT JOIN actual_operating_profit_metrics AS op
          ON s.code = op.code
         AND s.date >= op.disclosed_date
        ASOF LEFT JOIN forward_operating_profit_metrics AS fop
          ON s.code = fop.code
         AND s.date >= fop.disclosed_date
        ASOF LEFT JOIN shares_metrics AS sh
          ON s.code = sh.code
         AND s.date >= sh.disclosed_date
        WHERE a.disclosed_date IS NOT NULL
           OR b.disclosed_date IS NOT NULL
           OR (
               f.disclosed_date IS NOT NULL
               AND fy.disclosed_date IS NOT NULL
               AND (
                   (
                       f.forward_source = 'fy'
                       AND f.disclosed_date = fy.disclosed_date
                   )
                   OR (
                       f.forward_source = 'revised'
                       AND f.disclosed_date > fy.disclosed_date
                   )
               )
           )
           OR op.disclosed_date IS NOT NULL
           OR (
               fop.disclosed_date IS NOT NULL
               AND fy.disclosed_date IS NOT NULL
               AND (
                   (
                       fop.forward_source = 'fy'
                       AND fop.disclosed_date = fy.disclosed_date
                   )
                   OR (
                       fop.forward_source = 'revised'
                       AND fop.disclosed_date > fy.disclosed_date
                   )
               )
           )
           OR sh.disclosed_date IS NOT NULL
        """


def upsert_daily_valuation_from_adjusted_metrics(
    conn: Any,
    lock: Any,
    table_exists: Callable[[str], bool],
    basis_version: str,
    price_basis_date: str,
    codes: list[str] | None = None,
) -> int:
    """Canonical daily valuation metrics を DuckDB relation で一括生成する。"""
    if (
        not table_exists("stock_data")
        or not table_exists("statement_metrics_adjusted")
    ):
        return 0

    normalized_codes = sorted({normalize_stock_code(code) for code in codes or [] if code})
    code_filter = ""
    if normalized_codes:
        placeholders = ", ".join("?" for _ in normalized_codes)
        code_filter = f"WHERE code IN ({placeholders})"

    now_iso = datetime.now().isoformat()  # noqa: DTZ005
    target_code_filter = ""
    if normalized_codes:
        placeholders = ", ".join("?" for _ in normalized_codes)
        target_code_filter = f" AND code IN ({placeholders})"
    delete_sql = f"""
        DELETE FROM daily_valuation
        WHERE basis_version = ?{target_code_filter}
        """
    target_params = [basis_version, *normalized_codes]
    insert_sql = _DAILY_VALUATION_REBUILD_SQL_TEMPLATE.format(
        code_filter=code_filter,
        daily_valuation_columns=", ".join(_DAILY_VALUATION_COLUMNS),
    )
    insert_params = [
        *normalized_codes,
        basis_version,
        basis_version,
        basis_version,
        basis_version,
        basis_version,
        price_basis_date,
        basis_version,
        now_iso,
    ]
    count_sql = f"""
        SELECT COUNT(*)
        FROM daily_valuation
        WHERE basis_version = ?{target_code_filter}
        """
    with lock:
        try:
            conn.execute("BEGIN TRANSACTION")
            conn.execute(delete_sql, target_params)
            conn.execute(insert_sql, insert_params)
            count_row = conn.execute(count_sql, target_params).fetchone()
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return int(count_row[0] or 0) if count_row else 0
