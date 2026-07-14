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
        FROM (
            SELECT
                {', '.join(_STATEMENT_METRICS_ADJUSTED_COLUMNS)},
                ROW_NUMBER() OVER (
                    PARTITION BY code, disclosed_date, period_end, period_type
                    ORDER BY price_basis_date DESC NULLS LAST, basis_version DESC
                ) AS rn
            FROM statement_metrics_adjusted
            WHERE {' AND '.join(conditions)}
        )
        WHERE rn = 1
        ORDER BY disclosed_date, period_end, period_type, basis_version
        """,
        params,
    )


def get_adjusted_statement_metrics_for_basis(
    table_exists: Callable[[str], bool],
    fetchall_dicts: Callable[[str, list[Any] | tuple[Any, ...] | None], list[dict[str, Any]]],
    code: str,
    *,
    basis_id: str,
    as_of_date: str | None = None,
) -> list[dict[str, Any]]:
    """Read adjusted statement metrics for one exact event-time basis."""
    if not table_exists("statement_metrics_adjusted"):
        return []
    conditions = ["code = ?", "basis_version = ?"]
    params: list[Any] = [normalize_stock_code(code), basis_id]
    if as_of_date is not None:
        conditions.append("disclosed_date <= ?")
        params.append(as_of_date)
    return fetchall_dicts(
        f"""
        SELECT {', '.join(_STATEMENT_METRICS_ADJUSTED_COLUMNS)}
        FROM statement_metrics_adjusted
        WHERE {' AND '.join(conditions)}
        ORDER BY disclosed_date, period_end, period_type
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
        FROM (
            SELECT
                {', '.join(_DAILY_VALUATION_COLUMNS)},
                ROW_NUMBER() OVER (
                    PARTITION BY code, date
                    ORDER BY price_basis_date DESC NULLS LAST, basis_version DESC
                ) AS rn
            FROM daily_valuation
            WHERE {' AND '.join(conditions)}
        )
        WHERE rn = 1
        ORDER BY date, basis_version
        """,
        params,
    )


def get_daily_valuation_for_basis(
    table_exists: Callable[[str], bool],
    fetchall_dicts: Callable[[str, list[Any] | tuple[Any, ...] | None], list[dict[str, Any]]],
    code: str,
    *,
    basis_id: str,
    start: str | None = None,
    end: str | None = None,
) -> list[dict[str, Any]]:
    """Read valuation rows for one exact event-time basis."""
    if not table_exists("daily_valuation"):
        return []
    conditions = ["code = ?", "basis_version = ?"]
    params: list[Any] = [normalize_stock_code(code), basis_id]
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
        ORDER BY date
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
        FROM (
            SELECT
                {', '.join(_DAILY_VALUATION_COLUMNS)},
                ROW_NUMBER() OVER (
                    PARTITION BY code, date
                    ORDER BY price_basis_date DESC NULLS LAST, basis_version DESC
                ) AS rn
            FROM daily_valuation
            WHERE date = ?
              AND code IN ({placeholders})
        )
        WHERE rn = 1
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
    daily_technical_rows = count_rows("daily_technical_metrics")
    row = None
    coverage_row = None
    basis_version_count = 0
    retained_basis_count = 0
    ready_basis_count = 0
    invalid_basis_count = 0
    active_coverage_frontier = None
    under_covered_active_basis_count = 0
    overlapping_basis_count = 0
    orphan_adjusted_statement_rows = 0
    orphan_daily_valuation_rows = 0
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
        coverage_row = fetchone(
            """
            WITH daily_counts AS (
                SELECT date, COUNT(DISTINCT code) AS code_count
                FROM daily_valuation
                GROUP BY date
            ),
            ranked AS (
                SELECT
                    date,
                    code_count,
                    ROW_NUMBER() OVER (ORDER BY date DESC) AS rn
                FROM daily_counts
            )
            SELECT
                MAX(CASE WHEN rn = 1 THEN date END) AS latest_date,
                MAX(CASE WHEN rn = 1 THEN code_count END) AS latest_code_count,
                MAX(CASE WHEN rn = 2 THEN code_count END) AS previous_code_count
            FROM ranked
            WHERE rn <= 2
            """,
            None,
        )
        basis_count_row = fetchone(
            """
            SELECT COUNT(DISTINCT basis_version)
            FROM daily_valuation
            WHERE basis_version IS NOT NULL
            """,
            None,
        )
        basis_version_count = int(basis_count_row[0] or 0) if basis_count_row else 0
    if table_exists("statement_metrics_adjusted"):
        basis_count_row = fetchone(
            """
            SELECT COUNT(DISTINCT basis_version)
            FROM statement_metrics_adjusted
            WHERE basis_version IS NOT NULL
            """,
            None,
        )
        basis_version_count = max(
            basis_version_count,
            int(basis_count_row[0] or 0) if basis_count_row else 0,
        )
    if table_exists("stock_adjustment_bases"):
        basis_state_row = fetchone(
            """
            SELECT
                COUNT(*) AS retained_basis_count,
                COUNT(*) FILTER (WHERE status = 'ready') AS ready_basis_count,
                COUNT(*) FILTER (WHERE status = 'invalid') AS invalid_basis_count,
                MIN(materialized_through_date) FILTER (
                    WHERE status = 'ready' AND valid_to_exclusive IS NULL
                ) AS active_coverage_frontier
            FROM stock_adjustment_bases
            """,
            None,
        )
        if basis_state_row:
            retained_basis_count = int(basis_state_row[0] or 0)
            ready_basis_count = int(basis_state_row[1] or 0)
            invalid_basis_count = int(basis_state_row[2] or 0)
            active_coverage_frontier = basis_state_row[3]
        overlap_row = fetchone(
            """
            SELECT COUNT(*)
            FROM stock_adjustment_bases AS earlier
            JOIN stock_adjustment_bases AS later
              ON later.code = earlier.code
             AND later.valid_from > earlier.valid_from
             AND (
                 earlier.valid_to_exclusive IS NULL
                 OR later.valid_from < earlier.valid_to_exclusive
             )
            """,
            None,
        )
        overlapping_basis_count = int(overlap_row[0] or 0) if overlap_row else 0
        if table_exists("stock_data_raw"):
            under_covered_row = fetchone(
                """
                WITH raw_frontiers AS (
                    SELECT
                        CASE
                            WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                            THEN left(code, length(code) - 1)
                            ELSE code
                        END AS code,
                        MAX(date) AS frontier
                    FROM stock_data_raw
                    GROUP BY 1
                )
                SELECT COUNT(*)
                FROM raw_frontiers
                LEFT JOIN stock_adjustment_bases AS basis
                  ON basis.code = raw_frontiers.code
                 AND basis.valid_to_exclusive IS NULL
                WHERE basis.basis_id IS NULL
                   OR basis.status != 'ready'
                   OR basis.materialized_through_date IS NULL
                   OR basis.materialized_through_date < raw_frontiers.frontier
                """,
                None,
            )
            under_covered_active_basis_count = (
                int(under_covered_row[0] or 0) if under_covered_row else 0
            )
        if table_exists("statement_metrics_adjusted"):
            orphan_row = fetchone(
                """
                SELECT COUNT(*)
                FROM statement_metrics_adjusted AS metrics
                LEFT JOIN stock_adjustment_bases AS basis
                  ON basis.code = metrics.code
                 AND basis.basis_id = metrics.basis_version
                WHERE basis.basis_id IS NULL
                """,
                None,
            )
            orphan_adjusted_statement_rows = int(orphan_row[0] or 0) if orphan_row else 0
        if table_exists("daily_valuation"):
            orphan_row = fetchone(
                """
                SELECT COUNT(*)
                FROM daily_valuation AS valuation
                LEFT JOIN stock_adjustment_bases AS basis
                  ON basis.code = valuation.code
                 AND basis.basis_id = valuation.basis_version
                WHERE basis.basis_id IS NULL
                """,
                None,
            )
            orphan_daily_valuation_rows = int(orphan_row[0] or 0) if orphan_row else 0
    return {
        "statementRows": statement_rows,
        "dailyValuationRows": daily_rows,
        "dailyTechnicalMetricRows": daily_technical_rows,
        "dailyValuationLatestDate": (
            str(coverage_row[0])
            if coverage_row and coverage_row[0] is not None
            else None
        ),
        "dailyValuationLatestCodeCount": (
            int(coverage_row[1])
            if coverage_row and coverage_row[1] is not None
            else 0
        ),
        "dailyValuationPreviousCodeCount": (
            int(coverage_row[2])
            if coverage_row and coverage_row[2] is not None
            else 0
        ),
        "priceBasisDate": str(row[0]) if row and row[0] is not None else None,
        "basisVersion": str(row[1]) if row and row[1] is not None else None,
        "basisVersionCount": basis_version_count,
        "retainedBasisCount": retained_basis_count,
        "readyBasisCount": ready_basis_count,
        "invalidBasisCount": invalid_basis_count,
        "activeCoverageFrontier": (
            str(active_coverage_frontier)
            if active_coverage_frontier is not None
            else None
        ),
        "underCoveredActiveBasisCount": under_covered_active_basis_count,
        "overlappingBasisCount": overlapping_basis_count,
        "orphanAdjustedStatementRows": orphan_adjusted_statement_rows,
        "orphanDailyValuationRows": orphan_daily_valuation_rows,
    }
