"""Current-provider-basis adjusted fundamentals and valuation read helpers."""

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
    required = {
        "statements",
        "stock_provider_windows",
        "statement_metrics_adjusted",
    }
    if not all(table_exists(table) for table in required):
        return diagnostics
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
        ), expected AS (
            SELECT source.*, provider_window.coverage_end AS basis_date
            FROM source
            JOIN stock_provider_windows AS provider_window
              ON provider_window.code = source.code
        ), comparison AS (
            SELECT
                expected.code AS expected_code,
                actual.code AS actual_code,
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
            (SELECT COUNT(*) FROM source),
            (SELECT COUNT(*) FROM expected),
            count(*) FILTER (
                WHERE expected_code IS NOT NULL AND actual_code IS NULL
            ),
            count(*) FILTER (
                WHERE expected_code IS NULL
                  AND actual_code IS NOT NULL
                  AND source_code IS NULL
            ),
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
            ),
            count(*) FILTER (
                WHERE (
                    expected_code IS NULL
                    AND actual_code IS NOT NULL
                    AND source_code IS NOT NULL
                ) OR (
                    expected_code IS NOT NULL
                    AND actual_code IS NOT NULL
                    AND basis_date IS DISTINCT FROM actual_basis_date
                )
            )
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
    fetchall_dicts: Callable[
        [str, list[Any] | tuple[Any, ...] | None], list[dict[str, Any]]
    ],
    code: str,
    as_of_date: str | None = None,
) -> list[dict[str, Any]]:
    if not table_exists("statement_metrics_adjusted"):
        return []
    conditions = ["code = ?"]
    params: list[Any] = [normalize_stock_code(code)]
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


def get_daily_valuation(
    table_exists: Callable[[str], bool],
    fetchall_dicts: Callable[
        [str, list[Any] | tuple[Any, ...] | None], list[dict[str, Any]]
    ],
    code: str,
    start: str | None = None,
    end: str | None = None,
) -> list[dict[str, Any]]:
    if not table_exists("daily_valuation"):
        return []
    conditions = ["code = ?"]
    params: list[Any] = [normalize_stock_code(code)]
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
    fetchall_dicts: Callable[
        [str, list[Any] | tuple[Any, ...] | None], list[dict[str, Any]]
    ],
    codes: list[str],
    date: str,
) -> list[dict[str, Any]]:
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
        WHERE date = ? AND code IN ({placeholders})
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
            "SELECT COUNT(*), MIN(coverage_end), MAX(coverage_end) "
            "FROM stock_provider_windows",
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
    return {
        "currentBasisStatementCount": count_rows("statement_metrics_adjusted"),
        "dailyValuationRows": count_rows("daily_valuation"),
        "dailyTechnicalMetricRows": count_rows("daily_technical_metrics"),
        "dailyValuationLatestDate": (
            str(coverage_row[0]) if coverage_row and coverage_row[0] else None
        ),
        "dailyValuationLatestCodeCount": (
            int(coverage_row[1]) if coverage_row and coverage_row[1] else 0
        ),
        "dailyValuationPreviousCodeCount": (
            int(coverage_row[2]) if coverage_row and coverage_row[2] else 0
        ),
        "fundamentalsAdjustmentBasisDate": (
            str(provider_row[2]) if provider_row and provider_row[2] else None
        ),
        "providerWindowCount": provider_count,
        "readyProviderWindowCount": max(provider_count - pending_count, 0),
        "providerWindowCoverageFrontier": (
            str(provider_row[1]) if provider_row and provider_row[1] else None
        ),
        "pendingCurrentBasisCodeCount": pending_count + missing_window_count,
        "orphanAdjustedStatementRows": orphan_statement_rows,
        "orphanDailyValuationRows": 0,
    }
