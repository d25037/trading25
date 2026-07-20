"""Adjusted fundamentals and daily valuation read helpers."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from src.infrastructure.db.market.market_schema import (
    DAILY_VALUATION_COLUMNS as _DAILY_VALUATION_COLUMNS,
    STATEMENT_METRICS_ADJUSTED_COLUMNS as _STATEMENT_METRICS_ADJUSTED_COLUMNS,
)
from src.infrastructure.db.market.query_helpers import normalize_stock_code


_SOURCE_DIAGNOSTIC_DEFAULTS: dict[str, int] = {
    "sourceStatementKeyCount": 0,
    "expectedAdjustedStatementRows": 0,
    "missingAdjustedStatementRows": 0,
    "extraAdjustedStatementRows": 0,
    "staleAdjustedStatementRows": 0,
    "wrongBasisAdjustedStatementRows": 0,
    "missingDailyValuationRows": 0,
    "extraDailyValuationRows": 0,
    "wrongBasisDailyValuationRows": 0,
}


def get_adjusted_metrics_source_diagnostics(
    table_exists: Callable[[str], bool],
    fetchone: Callable[[str, Sequence[Any] | None], Any],
) -> dict[str, int]:
    """Compare current-basis metrics with statement/provider-window sources."""
    diagnostics = dict(_SOURCE_DIAGNOSTIC_DEFAULTS)
    statement_tables = {
        "statements",
        "stock_provider_windows",
        "statement_metrics_adjusted",
    }
    if all(table_exists(table) for table in statement_tables):
        row = fetchone(
            """
            WITH source AS (
                SELECT
                    code,
                    statement_id,
                    earnings_per_share AS raw_eps,
                    diluted_earnings_per_share AS raw_diluted_eps,
                    bps AS raw_bps,
                    CASE
                        WHEN contains(coalesce(type_of_document, ''), 'ForecastRevision')
                        THEN coalesce(forecast_eps, next_year_forecast_earnings_per_share)
                        WHEN upper(coalesce(type_of_current_period, '')) = 'FY'
                        THEN coalesce(next_year_forecast_earnings_per_share, forecast_eps)
                        ELSE coalesce(forecast_eps, next_year_forecast_earnings_per_share)
                    END AS raw_forecast_eps,
                    dividend_fy AS raw_dividend_fy,
                    CASE
                        WHEN contains(coalesce(type_of_document, ''), 'ForecastRevision')
                        THEN coalesce(forecast_dividend_fy, next_year_forecast_dividend_fy)
                        WHEN upper(coalesce(type_of_current_period, '')) = 'FY'
                        THEN coalesce(next_year_forecast_dividend_fy, forecast_dividend_fy)
                        ELSE coalesce(forecast_dividend_fy, next_year_forecast_dividend_fy)
                    END AS raw_forecast_dividend_fy,
                    shares_outstanding AS raw_shares_outstanding,
                    treasury_shares AS raw_treasury_shares
                FROM statements
            ),
            expected AS (
                SELECT source.*, provider_window.coverage_end AS basis_date
                FROM source
                JOIN stock_provider_windows AS provider_window
                  ON provider_window.code = source.code
            ),
            comparison AS (
                SELECT
                    expected.code AS expected_code,
                    actual.code AS actual_code,
                    expected.statement_id AS expected_statement_id,
                    actual.statement_id AS actual_statement_id,
                    expected.basis_date,
                    actual.fundamentals_adjustment_basis_date AS actual_basis_date,
                    expected.raw_eps AS expected_raw_eps,
                    expected.raw_diluted_eps AS expected_raw_diluted_eps,
                    expected.raw_bps AS expected_raw_bps,
                    expected.raw_forecast_eps AS expected_raw_forecast_eps,
                    expected.raw_dividend_fy AS expected_raw_dividend_fy,
                    expected.raw_forecast_dividend_fy AS expected_raw_forecast_dividend_fy,
                    expected.raw_shares_outstanding AS expected_raw_shares_outstanding,
                    expected.raw_treasury_shares AS expected_raw_treasury_shares,
                    actual.raw_eps AS actual_raw_eps,
                    actual.raw_diluted_eps AS actual_raw_diluted_eps,
                    actual.raw_bps AS actual_raw_bps,
                    actual.raw_forecast_eps AS actual_raw_forecast_eps,
                    actual.raw_dividend_fy AS actual_raw_dividend_fy,
                    actual.raw_forecast_dividend_fy AS actual_raw_forecast_dividend_fy,
                    actual.raw_shares_outstanding AS actual_raw_shares_outstanding,
                    actual.raw_treasury_shares AS actual_raw_treasury_shares,
                    source_key.code AS source_code
                FROM expected
                FULL OUTER JOIN statement_metrics_adjusted AS actual
                  ON actual.code = expected.code
                 AND actual.statement_id = expected.statement_id
                LEFT JOIN source AS source_key
                  ON source_key.code = actual.code
                 AND source_key.statement_id = actual.statement_id
            )
            SELECT
                (SELECT COUNT(*) FROM source) AS source_statement_key_count,
                (SELECT COUNT(*) FROM expected) AS expected_adjusted_statement_rows,
                count(*) FILTER (
                    WHERE expected_code IS NOT NULL AND actual_code IS NULL
                ) AS missing_adjusted_statement_rows,
                count(*) FILTER (
                    WHERE expected_code IS NULL
                      AND actual_code IS NOT NULL
                      AND source_code IS NULL
                ) AS extra_adjusted_statement_rows,
                count(*) FILTER (
                    WHERE expected_code IS NOT NULL
                      AND actual_code IS NOT NULL
                      AND (
                        expected_raw_eps IS DISTINCT FROM actual_raw_eps
                        OR expected_raw_diluted_eps IS DISTINCT FROM actual_raw_diluted_eps
                        OR expected_raw_bps IS DISTINCT FROM actual_raw_bps
                        OR expected_raw_forecast_eps IS DISTINCT FROM actual_raw_forecast_eps
                        OR expected_raw_dividend_fy IS DISTINCT FROM actual_raw_dividend_fy
                        OR expected_raw_forecast_dividend_fy IS DISTINCT FROM actual_raw_forecast_dividend_fy
                        OR expected_raw_shares_outstanding IS DISTINCT FROM actual_raw_shares_outstanding
                        OR expected_raw_treasury_shares IS DISTINCT FROM actual_raw_treasury_shares
                      )
                ) AS stale_adjusted_statement_rows,
                count(*) FILTER (
                    WHERE expected_code IS NULL
                      AND actual_code IS NOT NULL
                      AND source_code IS NOT NULL
                       OR (
                            expected_code IS NOT NULL
                            AND actual_code IS NOT NULL
                            AND basis_date IS DISTINCT FROM actual_basis_date
                       )
                ) AS wrong_basis_adjusted_statement_rows
            FROM comparison
            """,
            None,
        )
        if row is not None:
            keys = (
                "sourceStatementKeyCount",
                "expectedAdjustedStatementRows",
                "missingAdjustedStatementRows",
                "extraAdjustedStatementRows",
                "staleAdjustedStatementRows",
                "wrongBasisAdjustedStatementRows",
            )
            diagnostics.update(
                {key: int(value or 0) for key, value in zip(keys, row, strict=True)}
            )

    return diagnostics


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
        ORDER BY disclosed_at, statement_id
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
    """Reject retained-basis reads removed by the Market v5 contract."""
    del table_exists, fetchall_dicts, code, basis_id, as_of_date
    raise ValueError(
        "basis-specific adjusted statement metrics are unsupported in Market v5"
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
        ORDER BY date
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
    """Reject retained-basis valuation reads removed by the Market v5 contract."""
    del table_exists, fetchall_dicts, code, basis_id, start, end
    raise ValueError(
        "basis-specific daily valuation is unsupported in Market v5"
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
        ORDER BY code
        """,
        [date, *normalized_codes],
    )


def get_adjusted_metrics_snapshot(
    table_exists: Callable[[str], bool],
    count_rows: Callable[[str], int],
    fetchone: Callable[[str, list[Any] | tuple[Any, ...] | None], Any],
) -> dict[str, Any]:
    """Current provider-basis materialization freshness snapshot."""
    statement_rows = count_rows("statement_metrics_adjusted")
    daily_rows = count_rows("daily_valuation")
    daily_technical_rows = count_rows("daily_technical_metrics")
    coverage_row = None
    if table_exists("daily_valuation"):
        coverage_row = fetchone(
            """
            WITH daily_counts AS (
                SELECT date, COUNT(DISTINCT code) AS code_count
                FROM daily_valuation GROUP BY date
            ), ranked AS (
                SELECT date, code_count,
                       ROW_NUMBER() OVER (ORDER BY date DESC) AS rn
                FROM daily_counts
            )
            SELECT
                MAX(CASE WHEN rn = 1 THEN date END),
                MAX(CASE WHEN rn = 1 THEN code_count END),
                MAX(CASE WHEN rn = 2 THEN code_count END)
            FROM ranked WHERE rn <= 2
            """,
            None,
        )
    provider_row = None
    if table_exists("stock_provider_windows"):
        provider_row = fetchone(
            """
            SELECT COUNT(*), MIN(coverage_end), MAX(coverage_end)
            FROM stock_provider_windows
            """,
            None,
        )
    pending_count = 0
    if table_exists("current_basis_recompute_pending"):
        pending_row = fetchone(
            "SELECT COUNT(*) FROM current_basis_recompute_pending", None
        )
        pending_count = int(pending_row[0] or 0) if pending_row else 0
    missing_window_count = 0
    if table_exists("stock_data_raw") and table_exists("stock_provider_windows"):
        missing_row = fetchone(
            """
            SELECT COUNT(*) FROM (
                SELECT DISTINCT code FROM stock_data_raw
            ) raw LEFT JOIN stock_provider_windows provider_window USING (code)
            WHERE provider_window.code IS NULL
            """,
            None,
        )
        missing_window_count = int(missing_row[0] or 0) if missing_row else 0
    orphan_statement_rows = 0
    if table_exists("statement_metrics_adjusted") and table_exists("statements"):
        orphan_row = fetchone(
            """
            SELECT COUNT(*)
            FROM statement_metrics_adjusted metrics
            LEFT JOIN statements source
              ON source.code = metrics.code
             AND source.statement_id = metrics.statement_id
            WHERE source.statement_id IS NULL
            """,
            None,
        )
        orphan_statement_rows = int(orphan_row[0] or 0) if orphan_row else 0
    provider_count = int(provider_row[0] or 0) if provider_row else 0
    ready_count = max(provider_count - pending_count, 0)
    return {
        "statementRows": statement_rows,
        "dailyValuationRows": daily_rows,
        "dailyTechnicalMetricRows": daily_technical_rows,
        "dailyValuationLatestDate": (
            str(coverage_row[0]) if coverage_row and coverage_row[0] else None
        ),
        "dailyValuationLatestCodeCount": (
            int(coverage_row[1]) if coverage_row and coverage_row[1] else 0
        ),
        "dailyValuationPreviousCodeCount": (
            int(coverage_row[2]) if coverage_row and coverage_row[2] else 0
        ),
        "priceBasisDate": (
            str(provider_row[2]) if provider_row and provider_row[2] else None
        ),
        "basisVersion": None,
        "basisVersionCount": 0,
        "retainedBasisCount": 0,
        "readyBasisCount": ready_count,
        "invalidBasisCount": 0,
        "activeCoverageFrontier": (
            str(provider_row[1]) if provider_row and provider_row[1] else None
        ),
        "underCoveredActiveBasisCount": pending_count + missing_window_count,
        "overlappingBasisCount": 0,
        "orphanAdjustedStatementRows": orphan_statement_rows,
        "orphanDailyValuationRows": 0,
    }


def _get_adjusted_metrics_snapshot_v4_removed(  # pyright: ignore[reportUnusedFunction]
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
             AND later.basis_id > earlier.basis_id
             AND (
                 later.valid_to_exclusive IS NULL
                 OR earlier.valid_from < later.valid_to_exclusive
             )
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
