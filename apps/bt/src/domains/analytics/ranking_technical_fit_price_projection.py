"""Technical-Fit-only event-time raw-price projection relations."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Any, Sequence

from src.domains.analytics.readonly_duckdb_support import normalize_code_sql


SIGNAL_FEATURE_RELATION = "ranking_technical_fit_signal_price_features"
FORWARD_OUTCOME_RELATION = "ranking_technical_fit_forward_price_outcomes"


@dataclass(frozen=True)
class EventTimePriceRelations:
    signal_features: str = SIGNAL_FEATURE_RELATION
    forward_outcomes: str = FORWARD_OUTCOME_RELATION


@dataclass(frozen=True)
class EventTimePriceAudit:
    canonical_raw_row_count: int
    signal_feature_row_count: int
    outcome_request_row_count: int
    completed_outcome_row_count: int
    signal_basis_row_count: int
    signal_segment_row_count: int
    completion_basis_row_count: int
    completion_segment_row_count: int
    signal_basis_sha256: str
    signal_segment_sha256: str
    completion_basis_sha256: str
    completion_segment_sha256: str
    forward_outcome_sha256: str
    price_projection_sha256: str
    signal_basis_policy: str
    completion_basis_policy: str
    adjustment_formula: str
    verification_status: str
    no_stock_data_fallback: bool

    def to_manifest_payload(self) -> dict[str, object]:
        return {
            "physical_price_source": "stock_data_raw",
            "canonical_raw_row_count": self.canonical_raw_row_count,
            "signal_feature_row_count": self.signal_feature_row_count,
            "outcome_request_row_count": self.outcome_request_row_count,
            "completed_outcome_row_count": self.completed_outcome_row_count,
            "signal_basis_row_count": self.signal_basis_row_count,
            "signal_segment_row_count": self.signal_segment_row_count,
            "completion_basis_row_count": self.completion_basis_row_count,
            "completion_segment_row_count": self.completion_segment_row_count,
            "signal_basis_sha256": self.signal_basis_sha256,
            "signal_segment_sha256": self.signal_segment_sha256,
            "completion_basis_sha256": self.completion_basis_sha256,
            "completion_segment_sha256": self.completion_segment_sha256,
            "forward_outcome_sha256": self.forward_outcome_sha256,
            "price_projection_sha256": self.price_projection_sha256,
            "signal_basis_policy": self.signal_basis_policy,
            "completion_basis_policy": self.completion_basis_policy,
            "adjustment_formula": self.adjustment_formula,
            "verification_status": self.verification_status,
            "no_stock_data_fallback": self.no_stock_data_fallback,
        }


def _ordered_sha256(conn: Any, query: str) -> str:
    digest = hashlib.sha256()
    cursor = conn.execute(query)
    while rows := cursor.fetchmany(10_000):
        for row in rows:
            digest.update(repr(tuple(row)).encode("utf-8"))
            digest.update(b"\n")
    return digest.hexdigest()


def _count(conn: Any, relation: str) -> int:
    return int(conn.execute(f"SELECT count(*) FROM {relation}").fetchone()[0])


def create_event_time_price_relations(
    conn: Any,
    *,
    query_start: str | None,
    query_end: str | None,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    horizons: Sequence[int],
) -> tuple[EventTimePriceRelations, EventTimePriceAudit]:
    """Project Technical Fit features/outcomes without reading ``stock_data``."""

    resolved_horizons = tuple(sorted({int(value) for value in horizons}))
    if not resolved_horizons or any(value <= 0 for value in resolved_horizons):
        raise ValueError("horizons must contain positive integers")

    raw_code = normalize_code_sql("raw.code")
    master_code = normalize_code_sql("smd.code")
    valuation_code = normalize_code_sql("dv.code")
    raw_conditions: list[str] = []
    raw_params: list[str] = []
    if query_start is not None:
        raw_conditions.append("raw.date >= ?")
        raw_params.append(query_start)
    if query_end is not None:
        raw_conditions.append("raw.date <= ?")
        raw_params.append(query_end)
    raw_where = "" if not raw_conditions else "WHERE " + " AND ".join(raw_conditions)
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_technical_fit_normalized_raw AS
        WITH ranked AS (
            SELECT
                {raw_code} AS code,
                raw.code AS source_code,
                CAST(raw.date AS DATE) AS date,
                CAST(raw.open AS DOUBLE) AS open,
                CAST(raw.high AS DOUBLE) AS high,
                CAST(raw.low AS DOUBLE) AS low,
                CAST(raw.close AS DOUBLE) AS close,
                CAST(raw.volume AS BIGINT) AS volume,
                row_number() OVER (
                    PARTITION BY {raw_code}, raw.date
                    ORDER BY CASE WHEN raw.code = {raw_code} THEN 0 ELSE 1 END,
                             length(raw.code), raw.code
                ) AS alias_rank,
                count(*) OVER (PARTITION BY {raw_code}, raw.date) AS alias_count,
                count(DISTINCT concat_ws('|',
                    coalesce(CAST(raw.open AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.high AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.low AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.close AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.volume AS VARCHAR), '<null>')
                )) OVER (PARTITION BY {raw_code}, raw.date) AS alias_value_count
            FROM stock_data_raw raw
            {raw_where}
        )
        SELECT * FROM ranked WHERE alias_rank = 1
        """,
        raw_params,
    )
    alias_conflicts = int(
        conn.execute(
            """
            SELECT count(*) FROM ranking_technical_fit_normalized_raw
            WHERE alias_count > 1 AND alias_value_count > 1
            """
        ).fetchone()[0]
    )
    if alias_conflicts:
        raise RuntimeError(
            "price projection alias conflict in stock_data_raw; "
            f"conflicting code/date rows={alias_conflicts}"
        )

    signal_conditions = ["smd.market_code IN ('0101', '0111')"]
    signal_params: list[str] = []
    if analysis_start_date is not None:
        signal_conditions.append("smd.date >= ?")
        signal_params.append(analysis_start_date)
    if analysis_end_date is not None:
        signal_conditions.append("smd.date <= ?")
        signal_params.append(analysis_end_date)
    signal_where = " AND ".join(signal_conditions)
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_technical_fit_signal_requests AS
        SELECT DISTINCT {master_code} AS code, CAST(smd.date AS DATE) AS date
        FROM stock_master_daily smd
        JOIN ranking_technical_fit_normalized_raw raw
          ON raw.code = {master_code} AND raw.date = CAST(smd.date AS DATE)
        WHERE {signal_where}
        """,
        signal_params,
    )
    invalid_signal_basis = int(
        conn.execute(
            """
            SELECT count(*)
            FROM ranking_technical_fit_signal_requests signal
            WHERE (
                SELECT count(*) FROM stock_adjustment_bases basis
                WHERE basis.code = signal.code
                  AND CAST(basis.valid_from AS DATE) <= signal.date
                  AND (basis.valid_to_exclusive IS NULL
                       OR signal.date < CAST(basis.valid_to_exclusive AS DATE))
            ) <> 1
            """
        ).fetchone()[0]
    )
    if invalid_signal_basis:
        raise RuntimeError(
            "price projection signal basis cardinality must be exactly one; "
            f"invalid rows={invalid_signal_basis}"
        )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_technical_fit_signal_bases AS
        SELECT
            signal.code,
            signal.date,
            CAST(basis.basis_id AS VARCHAR) AS signal_basis_id
        FROM ranking_technical_fit_signal_requests signal
        JOIN stock_adjustment_bases basis
          ON basis.code = signal.code
         AND CAST(basis.valid_from AS DATE) <= signal.date
         AND (basis.valid_to_exclusive IS NULL
              OR signal.date < CAST(basis.valid_to_exclusive AS DATE))
        WHERE basis.status = 'ready'
          AND CAST(basis.materialized_through_date AS DATE) >= signal.date
          AND CAST(basis.adjustment_through_date AS DATE)
              = CAST(basis.valid_from AS DATE)
          AND basis.source_fingerprint IS NOT NULL
          AND trim(basis.source_fingerprint) <> ''
          AND basis.basis_id = 'event-pit-v1:' || basis.code || ':' || basis.valid_from
          AND (
              SELECT count(*) FROM daily_valuation dv
              WHERE {valuation_code} = signal.code
                AND CAST(dv.date AS DATE) = signal.date
                AND CAST(dv.basis_version AS VARCHAR) = CAST(basis.basis_id AS VARCHAR)
          ) = 1
        """
    )
    signal_request_count = _count(conn, "ranking_technical_fit_signal_requests")
    if _count(conn, "ranking_technical_fit_signal_bases") != signal_request_count:
        raise RuntimeError(
            "price projection signal basis is not ready/materialized or has a "
            "missing cutoff-valid daily_valuation basis match"
        )

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_technical_fit_signal_projection_requests AS
        WITH basis_ranges AS (
            SELECT
                code,
                signal_basis_id AS basis_id,
                max(date) AS max_signal_date
            FROM ranking_technical_fit_signal_bases
            GROUP BY code, signal_basis_id
        )
        SELECT
            basis_range.code,
            basis_range.basis_id,
            raw.date,
            raw.open, raw.high, raw.low, raw.close, raw.volume
        FROM basis_ranges basis_range
        JOIN ranking_technical_fit_normalized_raw raw
          ON raw.code = basis_range.code AND raw.date <= basis_range.max_signal_date
        """
    )
    invalid_signal_segments = int(
        conn.execute(
            """
            SELECT count(*)
            FROM ranking_technical_fit_signal_projection_requests request
            WHERE (
                SELECT count(*) FROM stock_adjustment_basis_segments segment
                WHERE segment.code = request.code
                  AND segment.basis_id = request.basis_id
                  AND CAST(segment.source_date_from AS DATE) <= request.date
                  AND (segment.source_date_to_exclusive IS NULL
                       OR request.date < CAST(segment.source_date_to_exclusive AS DATE))
            ) <> 1
            """
        ).fetchone()[0]
    )
    if invalid_signal_segments:
        raise RuntimeError(
            "price projection signal segment cardinality must be exactly one; "
            f"invalid rows={invalid_signal_segments}"
        )
    invalid_signal_factors = int(
        conn.execute(
            """
            SELECT count(*)
            FROM ranking_technical_fit_signal_projection_requests request
            JOIN stock_adjustment_basis_segments segment
              ON segment.code = request.code AND segment.basis_id = request.basis_id
             AND CAST(segment.source_date_from AS DATE) <= request.date
             AND (segment.source_date_to_exclusive IS NULL
                  OR request.date < CAST(segment.source_date_to_exclusive AS DATE))
            WHERE segment.cumulative_factor IS NULL
               OR NOT isfinite(segment.cumulative_factor)
               OR segment.cumulative_factor <= 0
            """
        ).fetchone()[0]
    )
    if invalid_signal_factors:
        raise RuntimeError(
            "price projection signal segment factor must be finite and positive; "
            f"invalid rows={invalid_signal_factors}"
        )

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_technical_fit_basis_prices AS
        SELECT
            request.code,
            request.basis_id,
            request.date,
            request.open * segment.cumulative_factor AS open,
            request.high * segment.cumulative_factor AS high,
            request.low * segment.cumulative_factor AS low,
            request.close * segment.cumulative_factor AS close,
            CAST(ROUND(request.volume / segment.cumulative_factor) AS BIGINT) AS volume
        FROM ranking_technical_fit_signal_projection_requests request
        JOIN stock_adjustment_basis_segments segment
          ON segment.code = request.code AND segment.basis_id = request.basis_id
         AND CAST(segment.source_date_from AS DATE) <= request.date
         AND (segment.source_date_to_exclusive IS NULL
              OR request.date < CAST(segment.source_date_to_exclusive AS DATE))
        """
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {SIGNAL_FEATURE_RELATION} AS
        WITH ordered AS (
            SELECT
                *,
                row_number() OVER (PARTITION BY code, basis_id ORDER BY date) - 1
                    AS session_index,
                lag(close) OVER (PARTITION BY code, basis_id ORDER BY date) AS prev_close
            FROM ranking_technical_fit_basis_prices
            WHERE open > 0 AND high > 0 AND low > 0 AND close > 0 AND volume >= 0
        ),
        ranged AS (
            SELECT
                *,
                greatest(
                    high - low,
                    coalesce(abs(high - prev_close), 0.0),
                    coalesce(abs(low - prev_close), 0.0)
                ) AS true_range
            FROM ordered
        ),
        featured AS (
            SELECT
                *,
                median(close * volume) OVER (
                    PARTITION BY code, basis_id ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS med_adv60_jpy,
                count(close * volume) OVER (
                    PARTITION BY code, basis_id ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS med_adv60_sessions,
                avg(true_range) OVER (PARTITION BY code, basis_id ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS atr20,
                count(true_range) OVER (PARTITION BY code, basis_id ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS atr20_sessions,
                avg(true_range) OVER (PARTITION BY code, basis_id ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS atr60,
                count(true_range) OVER (PARTITION BY code, basis_id ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS atr60_sessions,
                lag(close, 20) OVER (PARTITION BY code, basis_id ORDER BY date) AS close_lag_20d,
                lag(close, 60) OVER (PARTITION BY code, basis_id ORDER BY date) AS close_lag_60d,
                lag(close, 120) OVER (PARTITION BY code, basis_id ORDER BY date) AS close_lag_120d,
                lag(close, 150) OVER (PARTITION BY code, basis_id ORDER BY date) AS close_lag_150d,
                lag(close, 252) OVER (PARTITION BY code, basis_id ORDER BY date) AS close_lag_252d,
                lag(close, 504) OVER (PARTITION BY code, basis_id ORDER BY date) AS close_lag_504d,
                regr_slope(ln(close), session_index) OVER (PARTITION BY code, basis_id ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ols_slope_20,
                regr_r2(ln(close), session_index) OVER (PARTITION BY code, basis_id ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ols_r2_raw_20,
                count(close) OVER (PARTITION BY code, basis_id ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ols_count_20,
                var_pop(ln(close)) OVER (PARTITION BY code, basis_id ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ols_var_20,
                regr_slope(ln(close), session_index) OVER (PARTITION BY code, basis_id ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ols_slope_60,
                regr_r2(ln(close), session_index) OVER (PARTITION BY code, basis_id ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ols_r2_raw_60,
                count(close) OVER (PARTITION BY code, basis_id ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ols_count_60,
                var_pop(ln(close)) OVER (PARTITION BY code, basis_id ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ols_var_60
            FROM ranged
        ),
        with_atr_lag AS (
            SELECT *, lag(atr20, 20) OVER (PARTITION BY code, basis_id ORDER BY date) AS atr20_lag_20d
            FROM featured
        )
        SELECT
            signal.code,
            signal.date,
            signal.signal_basis_id AS price_basis_id,
            f.open, f.high, f.low, f.close, f.volume,
            f.med_adv60_jpy, f.med_adv60_sessions,
            f.close_lag_20d, f.close_lag_60d,
            f.close_lag_120d, f.close_lag_150d,
            f.close_lag_252d, f.close_lag_504d,
            f.atr20, f.atr20_sessions, f.atr60, f.atr60_sessions,
            CASE WHEN f.close > 0 AND f.atr20_sessions = 20 THEN f.atr20 / f.close * 100.0 END AS atr20_pct,
            CASE WHEN f.close > 0 AND f.atr60_sessions = 60 THEN f.atr60 / f.close * 100.0 END AS atr60_pct,
            CASE WHEN f.atr60 > 0 AND f.atr20_sessions = 20 AND f.atr60_sessions = 60 THEN f.atr20 / f.atr60 END AS atr20_to_atr60,
            CASE WHEN f.atr20_lag_20d > 0 AND f.atr20_sessions = 20 THEN (f.atr20 / f.atr20_lag_20d - 1.0) * 100.0 END AS atr20_change_20d_pct,
            CASE WHEN f.close_lag_20d > 0 THEN (f.close / f.close_lag_20d - 1.0) * 100.0 END AS recent_return_20d_pct,
            CASE WHEN f.close_lag_60d > 0 THEN (f.close / f.close_lag_60d - 1.0) * 100.0 END AS recent_return_60d_pct,
            CASE WHEN f.close_lag_120d > 0 THEN (f.close / f.close_lag_120d - 1.0) * 100.0 END AS recent_return_120d_pct,
            CASE WHEN f.close_lag_150d > 0 THEN (f.close / f.close_lag_150d - 1.0) * 100.0 END AS recent_return_150d_pct,
            CASE WHEN f.close_lag_252d > 0 THEN (f.close / f.close_lag_252d - 1.0) * 100.0 END AS recent_return_252d_pct,
            CASE WHEN f.close_lag_504d > 0 THEN (f.close / f.close_lag_504d - 1.0) * 100.0 END AS recent_return_504d_pct,
            CASE WHEN f.ols_count_20 = 20 THEN (exp(f.ols_slope_20 * 19.0) - 1.0) * 100.0 END AS ols_move_20d_pct,
            CASE WHEN f.ols_count_20 = 20 THEN CASE WHEN f.ols_var_20 = 0 THEN 0.0 ELSE f.ols_r2_raw_20 END END AS ols_r2_20,
            CASE WHEN f.ols_count_60 = 60 THEN (exp(f.ols_slope_60 * 59.0) - 1.0) * 100.0 END AS ols_move_60d_pct,
            CASE WHEN f.ols_count_60 = 60 THEN CASE WHEN f.ols_var_60 = 0 THEN 0.0 ELSE f.ols_r2_raw_60 END END AS ols_r2_60
        FROM ranking_technical_fit_signal_bases signal
        JOIN with_atr_lag f
          ON f.code = signal.code AND f.basis_id = signal.signal_basis_id
         AND f.date = signal.date
        """
    )

    lead_exprs = ",\n".join(
        f"lead(date, {horizon}) OVER (PARTITION BY code ORDER BY date) AS completion_date_{horizon}d"
        for horizon in resolved_horizons
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_technical_fit_raw_sessions AS
        SELECT code, date, {lead_exprs}
        FROM ranking_technical_fit_normalized_raw
        """
    )
    outcome_unions = " UNION ALL ".join(
        f"""
        SELECT signal.code, signal.date AS signal_date, {horizon} AS horizon,
               sessions.completion_date_{horizon}d AS completion_date
        FROM ranking_technical_fit_signal_bases signal
        JOIN ranking_technical_fit_raw_sessions sessions
          ON sessions.code = signal.code AND sessions.date = signal.date
        """
        for horizon in resolved_horizons
    )
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE ranking_technical_fit_outcome_requests AS "
        + outcome_unions
    )
    invalid_completion_basis = int(
        conn.execute(
            """
            SELECT count(*)
            FROM ranking_technical_fit_outcome_requests request
            WHERE request.completion_date IS NOT NULL
              AND (
                SELECT count(*) FROM stock_adjustment_bases basis
                WHERE basis.code = request.code
                  AND CAST(basis.valid_from AS DATE) <= request.completion_date
                  AND (basis.valid_to_exclusive IS NULL
                       OR request.completion_date < CAST(basis.valid_to_exclusive AS DATE))
              ) <> 1
            """
        ).fetchone()[0]
    )
    if invalid_completion_basis:
        raise RuntimeError(
            "price projection completion basis cardinality must be exactly one; "
            f"invalid rows={invalid_completion_basis}"
        )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_technical_fit_completion_bases AS
        SELECT
            request.code, request.signal_date, request.horizon,
            request.completion_date,
            CAST(basis.basis_id AS VARCHAR) AS completion_basis_id
        FROM ranking_technical_fit_outcome_requests request
        JOIN stock_adjustment_bases basis
          ON basis.code = request.code
         AND CAST(basis.valid_from AS DATE) <= request.completion_date
         AND (basis.valid_to_exclusive IS NULL
              OR request.completion_date < CAST(basis.valid_to_exclusive AS DATE))
        WHERE request.completion_date IS NOT NULL
          AND basis.status = 'ready'
          AND CAST(basis.materialized_through_date AS DATE) >= request.completion_date
          AND CAST(basis.adjustment_through_date AS DATE)
              = CAST(basis.valid_from AS DATE)
          AND basis.source_fingerprint IS NOT NULL
          AND trim(basis.source_fingerprint) <> ''
          AND basis.basis_id = 'event-pit-v1:' || basis.code || ':' || basis.valid_from
        """
    )
    completed_requests = int(
        conn.execute(
            "SELECT count(*) FROM ranking_technical_fit_outcome_requests WHERE completion_date IS NOT NULL"
        ).fetchone()[0]
    )
    if _count(conn, "ranking_technical_fit_completion_bases") != completed_requests:
        raise RuntimeError(
            "price projection completion basis is not ready/materialized through completion"
        )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_technical_fit_outcome_endpoint_requests AS
        SELECT code, signal_date, horizon, completion_date, completion_basis_id,
               'signal' AS endpoint, signal_date AS endpoint_date
        FROM ranking_technical_fit_completion_bases
        UNION ALL
        SELECT code, signal_date, horizon, completion_date, completion_basis_id,
               'completion' AS endpoint, completion_date AS endpoint_date
        FROM ranking_technical_fit_completion_bases
        """
    )
    invalid_outcome_segments = int(
        conn.execute(
            """
            SELECT count(*)
            FROM ranking_technical_fit_outcome_endpoint_requests request
            WHERE (
                SELECT count(*) FROM stock_adjustment_basis_segments segment
                WHERE segment.code = request.code
                  AND segment.basis_id = request.completion_basis_id
                  AND CAST(segment.source_date_from AS DATE) <= request.endpoint_date
                  AND (segment.source_date_to_exclusive IS NULL
                       OR request.endpoint_date < CAST(segment.source_date_to_exclusive AS DATE))
            ) <> 1
            """
        ).fetchone()[0]
    )
    if invalid_outcome_segments:
        raise RuntimeError(
            "price projection completion segment cardinality must be exactly one "
            "for both endpoints; "
            f"invalid rows={invalid_outcome_segments}"
        )
    invalid_outcome_factors = int(
        conn.execute(
            """
            SELECT count(*)
            FROM ranking_technical_fit_outcome_endpoint_requests request
            JOIN stock_adjustment_basis_segments segment
              ON segment.code = request.code
             AND segment.basis_id = request.completion_basis_id
             AND CAST(segment.source_date_from AS DATE) <= request.endpoint_date
             AND (segment.source_date_to_exclusive IS NULL
                  OR request.endpoint_date < CAST(segment.source_date_to_exclusive AS DATE))
            WHERE segment.cumulative_factor IS NULL
               OR NOT isfinite(segment.cumulative_factor)
               OR segment.cumulative_factor <= 0
            """
        ).fetchone()[0]
    )
    if invalid_outcome_factors:
        raise RuntimeError(
            "price projection completion segment factor must be finite and positive; "
            f"invalid rows={invalid_outcome_factors}"
        )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_technical_fit_projected_outcomes_long AS
        WITH endpoints AS (
            SELECT
                request.*,
                raw.close * segment.cumulative_factor AS projected_close
            FROM ranking_technical_fit_outcome_endpoint_requests request
            JOIN ranking_technical_fit_normalized_raw raw
              ON raw.code = request.code AND raw.date = request.endpoint_date
            JOIN stock_adjustment_basis_segments segment
              ON segment.code = request.code
             AND segment.basis_id = request.completion_basis_id
             AND CAST(segment.source_date_from AS DATE) <= request.endpoint_date
             AND (segment.source_date_to_exclusive IS NULL
                  OR request.endpoint_date < CAST(segment.source_date_to_exclusive AS DATE))
        ),
        pivoted AS (
            SELECT
                code, signal_date, horizon, completion_date, completion_basis_id,
                max(projected_close) FILTER (endpoint = 'signal') AS signal_close,
                max(projected_close) FILTER (endpoint = 'completion') AS completion_close
            FROM endpoints
            GROUP BY code, signal_date, horizon, completion_date, completion_basis_id
        )
        SELECT
            p.*,
            CASE WHEN p.signal_close > 0 AND p.completion_close > 0
                THEN (p.completion_close / p.signal_close - 1.0) * 100.0
            END AS forward_close_return_pct,
            CASE WHEN p.signal_close > 0 AND p.completion_close > 0
                       AND ts.close > 0 AND tc.close > 0
                THEN (p.completion_close / p.signal_close - 1.0) * 100.0
                   - (tc.close / ts.close - 1.0) * 100.0
            END AS forward_close_excess_return_pct
        FROM pivoted p
        LEFT JOIN topix_data ts ON CAST(ts.date AS DATE) = p.signal_date
        LEFT JOIN topix_data tc ON CAST(tc.date AS DATE) = p.completion_date
        """
    )
    outcome_columns = ",\n".join(
        expression
        for horizon in resolved_horizons
        for expression in (
            f"max(completion_date) FILTER (horizon = {horizon}) AS forward_outcome_completion_date_{horizon}d",
            f"max(forward_close_return_pct) FILTER (horizon = {horizon}) AS forward_close_return_{horizon}d_pct",
            f"max(forward_close_excess_return_pct) FILTER (horizon = {horizon}) AS forward_close_excess_return_{horizon}d_pct",
            f"max(completion_basis_id) FILTER (horizon = {horizon}) AS completion_basis_id_{horizon}d",
        )
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {FORWARD_OUTCOME_RELATION} AS
        SELECT code, signal_date AS date, {outcome_columns}
        FROM ranking_technical_fit_projected_outcomes_long
        GROUP BY code, signal_date
        """
    )

    signal_basis_hash = _ordered_sha256(
        conn,
        "SELECT code, date, signal_basis_id FROM ranking_technical_fit_signal_bases ORDER BY code, date, signal_basis_id",
    )
    completion_basis_hash = _ordered_sha256(
        conn,
        "SELECT code, signal_date, horizon, completion_date, completion_basis_id FROM ranking_technical_fit_completion_bases ORDER BY code, signal_date, horizon",
    )
    projection_hash = _ordered_sha256(
        conn,
        f"SELECT * FROM {SIGNAL_FEATURE_RELATION} ORDER BY code, date",
    )
    signal_segment_hash = _ordered_sha256(
        conn,
        """
        SELECT DISTINCT segment.code, segment.basis_id,
               segment.source_date_from, segment.source_date_to_exclusive,
               segment.cumulative_factor
        FROM ranking_technical_fit_signal_projection_requests request
        JOIN stock_adjustment_basis_segments segment
          ON segment.code = request.code AND segment.basis_id = request.basis_id
         AND CAST(segment.source_date_from AS DATE) <= request.date
         AND (segment.source_date_to_exclusive IS NULL
              OR request.date < CAST(segment.source_date_to_exclusive AS DATE))
        ORDER BY segment.code, segment.basis_id, segment.source_date_from
        """,
    )
    completion_segment_hash = _ordered_sha256(
        conn,
        """
        SELECT DISTINCT segment.code, segment.basis_id,
               segment.source_date_from, segment.source_date_to_exclusive,
               segment.cumulative_factor
        FROM ranking_technical_fit_outcome_endpoint_requests request
        JOIN stock_adjustment_basis_segments segment
          ON segment.code = request.code
         AND segment.basis_id = request.completion_basis_id
         AND CAST(segment.source_date_from AS DATE) <= request.endpoint_date
         AND (segment.source_date_to_exclusive IS NULL
              OR request.endpoint_date < CAST(segment.source_date_to_exclusive AS DATE))
        ORDER BY segment.code, segment.basis_id, segment.source_date_from
        """,
    )
    forward_outcome_hash = _ordered_sha256(
        conn,
        f"SELECT * FROM {FORWARD_OUTCOME_RELATION} ORDER BY code, date",
    )
    completion_segment_count = int(
        conn.execute(
            """
            SELECT count(*) FROM (
                SELECT DISTINCT segment.code, segment.basis_id,
                       segment.source_date_from, segment.source_date_to_exclusive
                FROM ranking_technical_fit_outcome_endpoint_requests request
                JOIN stock_adjustment_basis_segments segment
                  ON segment.code = request.code
                 AND segment.basis_id = request.completion_basis_id
                 AND CAST(segment.source_date_from AS DATE) <= request.endpoint_date
                 AND (segment.source_date_to_exclusive IS NULL
                      OR request.endpoint_date < CAST(segment.source_date_to_exclusive AS DATE))
            )
            """
        ).fetchone()[0]
    )
    signal_segment_count = int(
        conn.execute(
            """
            SELECT count(*) FROM (
                SELECT DISTINCT segment.code, segment.basis_id,
                       segment.source_date_from, segment.source_date_to_exclusive
                FROM ranking_technical_fit_signal_projection_requests request
                JOIN stock_adjustment_basis_segments segment
                  ON segment.code = request.code AND segment.basis_id = request.basis_id
                 AND CAST(segment.source_date_from AS DATE) <= request.date
                 AND (segment.source_date_to_exclusive IS NULL
                      OR request.date < CAST(segment.source_date_to_exclusive AS DATE))
            )
            """
        ).fetchone()[0]
    )
    audit = EventTimePriceAudit(
        canonical_raw_row_count=_count(conn, "ranking_technical_fit_normalized_raw"),
        signal_feature_row_count=_count(conn, SIGNAL_FEATURE_RELATION),
        outcome_request_row_count=_count(conn, "ranking_technical_fit_outcome_requests"),
        completed_outcome_row_count=_count(conn, "ranking_technical_fit_projected_outcomes_long"),
        signal_basis_row_count=int(
            conn.execute(
                "SELECT count(*) FROM (SELECT DISTINCT code, signal_basis_id FROM ranking_technical_fit_signal_bases)"
            ).fetchone()[0]
        ),
        signal_segment_row_count=signal_segment_count,
        completion_basis_row_count=int(
            conn.execute(
                "SELECT count(*) FROM (SELECT DISTINCT code, completion_basis_id FROM ranking_technical_fit_completion_bases)"
            ).fetchone()[0]
        ),
        completion_segment_row_count=completion_segment_count,
        signal_basis_sha256=signal_basis_hash,
        signal_segment_sha256=signal_segment_hash,
        completion_basis_sha256=completion_basis_hash,
        completion_segment_sha256=completion_segment_hash,
        forward_outcome_sha256=forward_outcome_hash,
        price_projection_sha256=hashlib.sha256(
            (
                f"{projection_hash}\n{signal_basis_hash}\n{signal_segment_hash}\n"
                f"{completion_basis_hash}\n{completion_segment_hash}\n"
                f"{forward_outcome_hash}"
            ).encode("utf-8")
        ).hexdigest(),
        signal_basis_policy="exact_signal_date_basis_across_full_lookback",
        completion_basis_policy=(
            "exact_completion_date_basis_applied_to_signal_and_completion_endpoints"
        ),
        adjustment_formula=(
            "ohlc=raw_ohlc*cumulative_factor;volume=round(raw_volume/cumulative_factor)"
        ),
        verification_status="verified",
        no_stock_data_fallback=True,
    )
    return EventTimePriceRelations(), audit
