"""Current-provider-basis adjusted fundamentals and valuation read helpers."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date
import re
from typing import Any

from src.infrastructure.db.market.market_schema import (
    DAILY_VALUATION_COLUMNS as _DAILY_VALUATION_COLUMNS,
    STATEMENT_METRICS_ADJUSTED_COLUMNS as _STATEMENT_METRICS_ADJUSTED_COLUMNS,
)
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.shared.provider_stock_window import (
    combine_provider_stock_source_fingerprints,
    provider_stock_source_fingerprint,
)


_SOURCE_DIAGNOSTIC_DEFAULTS: dict[str, int] = {
    "providerWindowFingerprintCount": 0,
    "invalidProviderWindowCount": 0,
    "adjustmentEventFingerprintCount": 0,
    "invalidAdjustmentEventCount": 0,
    "providerAdjustedMismatchCount": 0,
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


def get_provider_vintage_snapshot(
    table_exists: Callable[[str], bool],
    fetchall_dicts: Callable[
        [str, list[Any] | tuple[Any, ...] | None], list[dict[str, Any]]
    ],
) -> dict[str, Any]:
    """Recompute provider-vintage ownership from bounded per-code source windows."""
    defaults: dict[str, Any] = {
        "providerAsOf": None,
        "providerAsOfMin": None,
        "providerAsOfMax": None,
        "effectiveCoverageStart": None,
        "effectiveCoverageEnd": None,
        "providerSourceFingerprint": None,
        "providerWindowCoherent": False,
        "providerWindowFingerprintCount": 0,
        "invalidProviderWindowCount": 0,
        "adjustmentEventCount": 0,
        "adjustmentEventFingerprintCount": 0,
        "invalidAdjustmentEventCount": 0,
    }
    required = {
        "stock_provider_windows",
        "stock_data_raw",
        "stock_adjustment_events",
    }
    if not all(table_exists(table) for table in required):
        return defaults

    windows = fetchall_dicts(
        """
        SELECT code, coverage_start, coverage_end, provider_as_of, source_fingerprint
        FROM stock_provider_windows ORDER BY code
        """,
        None,
    )
    if not windows:
        return defaults

    valid_fingerprints: list[str] = []
    starts: list[str] = []
    ends: list[str] = []
    as_ofs: list[str] = []
    valid_window_count = 0
    event_count = 0
    valid_event_count = 0
    invalid_event_count = 0
    for window in windows:
        code = str(window.get("code", ""))
        coverage_start = str(window.get("coverage_start", ""))
        coverage_end = str(window.get("coverage_end", ""))
        provider_as_of = str(window.get("provider_as_of", ""))
        owned_fingerprint = str(window.get("source_fingerprint", ""))
        metadata_valid = bool(re.fullmatch(r"[0-9a-f]{64}", owned_fingerprint))
        try:
            parsed_start = date.fromisoformat(coverage_start)
            parsed_end = date.fromisoformat(coverage_end)
            parsed_as_of = date.fromisoformat(provider_as_of)
            metadata_valid = metadata_valid and all(
                parsed.isoformat() == raw
                for parsed, raw in (
                    (parsed_start, coverage_start),
                    (parsed_end, coverage_end),
                    (parsed_as_of, provider_as_of),
                )
            )
            metadata_valid = metadata_valid and parsed_start <= parsed_end <= parsed_as_of
        except ValueError:
            metadata_valid = False

        raw_rows = fetchall_dicts(
            "SELECT * FROM stock_data_raw WHERE code = ? ORDER BY date",
            [code],
        )
        calculated_fingerprint = provider_stock_source_fingerprint(raw_rows)
        raw_dates = [str(row.get("date", "")) for row in raw_rows]
        bounds_valid = bool(raw_dates) and raw_dates[0] == coverage_start and raw_dates[-1] == coverage_end
        fingerprint_valid = owned_fingerprint == calculated_fingerprint
        window_valid = metadata_valid and bounds_valid and fingerprint_valid
        if window_valid:
            valid_window_count += 1
            valid_fingerprints.append(calculated_fingerprint)
            starts.append(coverage_start)
            ends.append(coverage_end)
            as_ofs.append(provider_as_of)

        actual_events = fetchall_dicts(
            """
            SELECT date, adjustment_factor, source_fingerprint
            FROM stock_adjustment_events WHERE code = ? ORDER BY date
            """,
            [code],
        )
        expected_events = {
            str(row["date"]): float(row["adjustment_factor"])
            for row in raw_rows
            if row.get("adjustment_factor") is not None
            and float(row["adjustment_factor"]) != 1.0
        }
        actual_dates: set[str] = set()
        for event in actual_events:
            event_count += 1
            event_date = str(event.get("date", ""))
            actual_dates.add(event_date)
            valid = (
                event_date in expected_events
                and float(event.get("adjustment_factor", 0)) == expected_events[event_date]
                and str(event.get("source_fingerprint", "")) == calculated_fingerprint
                and window_valid
            )
            if valid:
                valid_event_count += 1
            else:
                invalid_event_count += 1
        invalid_event_count += len(set(expected_events) - actual_dates)

    coherent = valid_window_count == len(windows) and max(starts) <= min(ends)
    return {
        **defaults,
        "providerAsOf": as_ofs[0] if coherent and len(set(as_ofs)) == 1 else None,
        "providerAsOfMin": min(as_ofs) if as_ofs else None,
        "providerAsOfMax": max(as_ofs) if as_ofs else None,
        "effectiveCoverageStart": max(starts) if coherent else None,
        "effectiveCoverageEnd": min(ends) if coherent else None,
        "providerSourceFingerprint": (
            combine_provider_stock_source_fingerprints(*valid_fingerprints)
            if valid_fingerprints and valid_window_count == len(windows)
            else None
        ),
        "providerWindowCoherent": coherent,
        "providerWindowFingerprintCount": valid_window_count,
        "invalidProviderWindowCount": len(windows) - valid_window_count,
        "adjustmentEventCount": event_count,
        "adjustmentEventFingerprintCount": valid_event_count,
        "invalidAdjustmentEventCount": invalid_event_count,
    }


def get_adjusted_metrics_source_diagnostics(
    table_exists: Callable[[str], bool],
    fetchone: Callable[[str, Sequence[Any] | None], Any],
) -> dict[str, int]:
    """Compare current-basis metrics with statement/provider-window sources."""
    diagnostics = dict(_SOURCE_DIAGNOSTIC_DEFAULTS)
    provider_required = {
        "stock_data_raw",
        "stock_data",
        "stock_provider_windows",
        "stock_adjustment_events",
    }
    if all(table_exists(table) for table in provider_required):
        provider_row = fetchone(
            """
            WITH raw_bounds AS (
                SELECT code, MIN(date) AS coverage_start, MAX(date) AS coverage_end
                FROM stock_data_raw GROUP BY code
            ), invalid_windows AS (
                SELECT provider_window.code
                FROM stock_provider_windows AS provider_window
                LEFT JOIN raw_bounds AS raw USING (code)
                WHERE trim(provider_window.source_fingerprint) = ''
                   OR provider_window.coverage_start > provider_window.coverage_end
                   OR provider_window.provider_as_of < provider_window.coverage_end
                   OR raw.code IS NULL
                   OR raw.coverage_start IS DISTINCT FROM provider_window.coverage_start
                   OR raw.coverage_end IS DISTINCT FROM provider_window.coverage_end
            ), ledger_comparison AS (
                SELECT
                    coalesce(raw.code, event.code) AS code,
                    coalesce(raw.date, event.date) AS date,
                    raw.adjustment_factor AS raw_factor,
                    event.adjustment_factor AS event_factor,
                    event.source_fingerprint AS event_fingerprint
                FROM (
                    SELECT code, date, adjustment_factor
                    FROM stock_data_raw
                    WHERE adjustment_factor <> 1
                ) AS raw
                FULL OUTER JOIN stock_adjustment_events AS event USING (code, date)
            ), adjusted_comparison AS (
                SELECT coalesce(raw.code, adjusted.code) AS code,
                       coalesce(raw.date, adjusted.date) AS date,
                       raw.code IS NULL
                       OR adjusted.code IS NULL
                       OR raw.adjusted_open IS DISTINCT FROM adjusted.open
                       OR raw.adjusted_high IS DISTINCT FROM adjusted.high
                       OR raw.adjusted_low IS DISTINCT FROM adjusted.low
                       OR raw.adjusted_close IS DISTINCT FROM adjusted.close
                       OR raw.adjusted_volume IS DISTINCT FROM adjusted.volume AS invalid
                FROM stock_data_raw AS raw
                FULL OUTER JOIN stock_data AS adjusted USING (code, date)
            )
            SELECT
                (SELECT COUNT(*) FROM stock_provider_windows
                 WHERE trim(source_fingerprint) <> ''),
                (SELECT COUNT(*) FROM invalid_windows),
                (SELECT COUNT(*) FROM stock_adjustment_events
                 WHERE trim(source_fingerprint) <> ''),
                (SELECT COUNT(*) FROM ledger_comparison
                 WHERE raw_factor IS NULL
                    OR event_factor IS NULL
                    OR raw_factor IS DISTINCT FROM event_factor
                    OR trim(coalesce(event_fingerprint, '')) = ''),
                (SELECT COUNT(*) FROM adjusted_comparison WHERE invalid)
            """,
            None,
        )
        if provider_row is not None:
            provider_keys = (
                "providerWindowFingerprintCount",
                "invalidProviderWindowCount",
                "adjustmentEventFingerprintCount",
                "invalidAdjustmentEventCount",
                "providerAdjustedMismatchCount",
            )
            diagnostics.update(
                {
                    key: int(value or 0)
                    for key, value in zip(provider_keys, provider_row, strict=True)
                }
            )

    required = {
        "statements",
        "stock_provider_windows",
        "statement_metrics_adjusted",
    }
    if not all(table_exists(table) for table in required):
        return diagnostics
    row = fetchone(
        """
        WITH source_candidates AS (
            SELECT
                CASE
                    WHEN length(code) = 5 AND right(code, 1) = '0'
                    THEN left(code, 4)
                    ELSE code
                END AS code,
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
                treasury_shares AS raw_treasury_shares,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        CASE
                            WHEN length(code) = 5 AND right(code, 1) = '0'
                            THEN left(code, 4)
                            ELSE code
                        END,
                        statement_id
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
                ) AS rn
            FROM statements
        ), source AS (
            SELECT * EXCLUDE (rn) FROM source_candidates WHERE rn = 1
        ), expected AS (
            SELECT source.*, provider_window.coverage_end AS basis_date
            FROM source
            JOIN stock_provider_windows AS provider_window
              ON provider_window.code = source.code
        ), actual AS (
            SELECT
                CASE
                    WHEN length(code) = 5 AND right(code, 1) = '0'
                    THEN left(code, 4)
                    ELSE code
                END AS code,
                statement_id,
                fundamentals_adjustment_basis_date,
                raw_eps,
                raw_diluted_eps,
                raw_bps,
                raw_forecast_eps,
                raw_dividend_fy,
                raw_forecast_dividend_fy,
                raw_shares_outstanding,
                raw_treasury_shares
            FROM statement_metrics_adjusted
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
            FULL OUTER JOIN actual
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
    state_validation_row = None
    state_required = (
        "stock_provider_windows",
        "current_basis_fundamentals_state",
        "current_basis_recompute_pending",
        "statement_metrics_adjusted",
        "statements",
        "stock_data_raw",
    )
    if all(table_exists(table) for table in state_required):
        state_validation_row = fetchone(
            """
            WITH metric_summary AS (
                SELECT CASE
                           WHEN length(metrics.code) = 5
                            AND right(metrics.code, 1) = '0'
                           THEN left(metrics.code, 4)
                           ELSE metrics.code
                       END AS code,
                       COUNT(*) AS metric_count,
                       MIN(metrics.source_fingerprint) AS min_fingerprint,
                       MAX(metrics.source_fingerprint) AS max_fingerprint,
                       MIN(metrics.fundamentals_adjustment_basis_date) AS min_basis,
                       MAX(metrics.fundamentals_adjustment_basis_date) AS max_basis,
                       COUNT(*) FILTER (
                           WHERE source.statement_id IS NULL
                              OR metrics.disclosed_date
                                 IS DISTINCT FROM source.disclosed_date
                              OR metrics.disclosed_at
                                 IS DISTINCT FROM source.disclosed_at
                              OR metrics.period_end
                                 IS DISTINCT FROM source.period_end
                              OR upper(COALESCE(metrics.period_type, ''))
                                 IS DISTINCT FROM upper(
                                     COALESCE(source.type_of_current_period, '')
                                 )
                       ) AS orphan_count
                FROM statement_metrics_adjusted AS metrics
                LEFT JOIN statements AS source
                  ON CASE
                         WHEN length(source.code) = 5
                          AND right(source.code, 1) = '0'
                         THEN left(source.code, 4)
                         ELSE source.code
                     END = CASE
                         WHEN length(metrics.code) = 5
                          AND right(metrics.code, 1) = '0'
                         THEN left(metrics.code, 4)
                         ELSE metrics.code
                     END
                 AND source.statement_id = metrics.statement_id
                GROUP BY 1
            ), raw_summary AS (
                SELECT CASE
                           WHEN length(code) = 5 AND right(code, 1) = '0'
                           THEN left(code, 4)
                           ELSE code
                       END AS code,
                       COUNT(DISTINCT statement_id) AS raw_count
                FROM statements GROUP BY 1
            ), provider_state AS (
                SELECT provider.code,
                       state.code IS NOT NULL
                       AND state.fundamentals_adjustment_basis_date = provider.coverage_end
                       AND trim(state.source_fingerprint) <> ''
                       AND trim(state.materialized_at) <> ''
                       AND state.statement_count = COALESCE(metrics.metric_count, 0)
                       AND state.statement_count = COALESCE(raw.raw_count, 0)
                       AND (
                           state.statement_count = 0
                           OR (
                               metrics.min_fingerprint = state.source_fingerprint
                               AND metrics.max_fingerprint = state.source_fingerprint
                               AND metrics.min_basis = state.fundamentals_adjustment_basis_date
                               AND metrics.max_basis = state.fundamentals_adjustment_basis_date
                               AND metrics.orphan_count = 0
                           )
                       ) AS valid
                FROM stock_provider_windows AS provider
                LEFT JOIN current_basis_fundamentals_state AS state USING (code)
                LEFT JOIN metric_summary AS metrics USING (code)
                LEFT JOIN raw_summary AS raw USING (code)
            ), invalid_state_codes AS (
                SELECT code FROM provider_state WHERE NOT valid
                UNION
                SELECT state.code
                FROM current_basis_fundamentals_state AS state
                LEFT JOIN stock_provider_windows AS provider USING (code)
                WHERE provider.code IS NULL
            ), unresolved_codes AS (
                SELECT CASE
                           WHEN length(code) = 5 AND right(code, 1) = '0'
                           THEN left(code, 4)
                           ELSE code
                       END AS code
                FROM current_basis_recompute_pending
                UNION
                SELECT raw.code
                FROM (
                    SELECT DISTINCT CASE
                               WHEN length(code) = 5 AND right(code, 1) = '0'
                               THEN left(code, 4)
                               ELSE code
                           END AS code
                    FROM stock_data_raw
                ) AS raw
                LEFT JOIN stock_provider_windows AS provider USING (code)
                WHERE provider.code IS NULL
                UNION
                SELECT code FROM invalid_state_codes
            )
            SELECT
                (SELECT COUNT(*) FROM current_basis_fundamentals_state),
                (SELECT COUNT(*) FROM invalid_state_codes),
                (
                    SELECT COUNT(*) FROM provider_state
                    WHERE valid
                      AND code NOT IN (
                          SELECT CASE
                                     WHEN length(code) = 5
                                      AND right(code, 1) = '0'
                                     THEN left(code, 4)
                                     ELSE code
                                 END
                          FROM current_basis_recompute_pending
                      )
                ),
                (SELECT COUNT(*) FROM unresolved_codes)
            """,
            None,
        )
    orphan_statement_rows = 0
    if table_exists("statement_metrics_adjusted") and table_exists("statements"):
        orphan_row = fetchone(
            """
            SELECT COUNT(*)
            FROM statement_metrics_adjusted metrics
            LEFT JOIN statements source
              ON CASE
                     WHEN length(source.code) = 5 AND right(source.code, 1) = '0'
                     THEN left(source.code, 4)
                     ELSE source.code
                 END = CASE
                     WHEN length(metrics.code) = 5 AND right(metrics.code, 1) = '0'
                     THEN left(metrics.code, 4)
                     ELSE metrics.code
                 END
             AND source.statement_id = metrics.statement_id
            WHERE source.statement_id IS NULL
            """,
            None,
        )
        orphan_statement_rows = int(orphan_row[0] or 0) if orphan_row else 0
    provider_count = int(provider_row[0] or 0) if provider_row else 0
    current_state_count = (
        int(state_validation_row[0] or 0) if state_validation_row else 0
    )
    invalid_state_count = (
        int(state_validation_row[1] or 0) if state_validation_row else 0
    )
    ready_provider_count = (
        int(state_validation_row[2] or 0) if state_validation_row else 0
    )
    unresolved_count = (
        int(state_validation_row[3] or 0) if state_validation_row else 0
    )
    return {
        "currentBasisStatementCount": count_rows("statement_metrics_adjusted"),
        "currentBasisStateCount": current_state_count,
        "invalidCurrentBasisStateCount": invalid_state_count,
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
        "readyProviderWindowCount": ready_provider_count,
        "providerWindowCoverageFrontier": (
            str(provider_row[1]) if provider_row and provider_row[1] else None
        ),
        "pendingCurrentBasisCodeCount": unresolved_count,
        "orphanAdjustedStatementRows": orphan_statement_rows,
        "orphanDailyValuationRows": 0,
    }
