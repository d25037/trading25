"""Event-time signal-price SQL for production Daily Ranking."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
from typing import Any, Sequence
from uuid import uuid4

from src.domains.analytics.readonly_duckdb_support import (
    normalize_code_sql,
    require_market_v4_compatibility,
)


EVENT_TIME_SIGNAL_RELATION = "event_time_signal_prices"
EVENT_TIME_SIGNAL_MATERIALIZED_RELATION = "daily_ranking_event_time_materialized"
EVENT_TIME_SIGNAL_MAX_ROWS = 2_000_000
EVENT_TIME_SIGNAL_MAX_CODES = 10_000
EVENT_TIME_SIGNAL_COLUMNS = (
    "normalized_code",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_lag_1d",
    "close_lag_20d",
    "close_lag_60d",
    "recent_return_1d_pct",
    "recent_return_20d_pct",
    "recent_return_60d_pct",
    "signal_basis_id",
)

_RELATION_NAMESPACE_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_NIKKEI_SYNTHETIC_INDEX_CODE = "N225_UNDERPX"
_RESEARCH_PRICE_REQUIRED_COLUMNS = {
    "stock_data_raw": {
        "code",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adjustment_factor",
    },
    "stock_master_daily": {"date", "code", "market_code"},
    "daily_valuation": {"code", "date", "basis_version"},
    "stock_adjustment_bases": {
        "code",
        "basis_id",
        "valid_from",
        "valid_to_exclusive",
        "adjustment_through_date",
        "source_fingerprint",
        "materialized_through_date",
        "status",
    },
    "stock_adjustment_basis_segments": {
        "code",
        "basis_id",
        "source_date_from",
        "source_date_to_exclusive",
        "cumulative_factor",
    },
    "topix_data": {"date", "close"},
    "indices_data": {"code", "date", "close"},
}
DAILY_RANKING_SIGNAL_FEATURE_COLUMNS = (
    "code",
    "date",
    "price_basis_id",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "med_adv60_jpy",
    "med_adv60_sessions",
    "close_lag_20d",
    "close_lag_60d",
    "close_lag_120d",
    "close_lag_150d",
    "close_lag_252d",
    "close_lag_504d",
    "atr20",
    "atr20_sessions",
    "atr60",
    "atr60_sessions",
    "atr20_pct",
    "atr60_pct",
    "atr20_to_atr60",
    "atr20_change_20d_pct",
    "recent_return_20d_pct",
    "recent_return_60d_pct",
    "recent_return_120d_pct",
    "recent_return_150d_pct",
    "recent_return_252d_pct",
    "recent_return_504d_pct",
    "ols_move_20d_pct",
    "ols_r2_20",
    "ols_move_60d_pct",
    "ols_r2_60",
)
DAILY_RANKING_PRICE_HISTORY_COLUMNS = (
    "code",
    "date",
    "price_basis_id",
    "open",
    "high",
    "low",
    "close",
    "volume",
)


@dataclass(frozen=True)
class DailyRankingPriceRequest:
    """Research-only request for namespaced signal and outcome relations."""

    namespace: str
    query_start: str | None
    query_end: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    market_codes: tuple[str, ...] = ("0101", "0111")

    def __post_init__(self) -> None:
        if not _RELATION_NAMESPACE_RE.fullmatch(self.namespace):
            raise ValueError(f"invalid relation namespace: {self.namespace!r}")
        if len(self.namespace) > 48:
            raise ValueError("relation namespace must be at most 48 characters")
        resolved_horizons = tuple(sorted({int(value) for value in self.horizons}))
        if not resolved_horizons or any(value <= 0 for value in resolved_horizons):
            raise ValueError("horizons must contain positive integers")
        object.__setattr__(self, "horizons", resolved_horizons)


@dataclass(frozen=True)
class DailyRankingPriceLineage:
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


@dataclass(frozen=True)
class DailyRankingPriceDiagnostics:
    canonical_raw_rows: int
    signal_request_rows: int
    signal_feature_rows: int
    outcome_request_rows: int
    completed_request_rows: int
    endpoint_rows: int
    forward_outcome_rows: int
    signal_feature_key_rows: int
    forward_outcome_key_rows: int
    topix_benchmark_rows: int
    n225_benchmark_rows: int
    signal_feature_schema: tuple[str, ...]
    forward_outcome_schema: tuple[str, ...]


@dataclass(frozen=True)
class DailyRankingPriceRelations:
    signal_features: str
    forward_outcomes: str
    price_history: str
    lineage: DailyRankingPriceLineage
    diagnostics: DailyRankingPriceDiagnostics


@dataclass(frozen=True)
class _DailyRankingPriceRelationNames:
    normalized_raw: str
    signal_requests: str
    signal_bases: str
    eligible_signal_requests: str
    signal_projection_requests: str
    basis_prices: str
    price_history: str
    signal_features: str
    raw_sessions: str
    outcome_requests: str
    completion_bases: str
    endpoint_requests: str
    projected_outcomes_long: str
    topix_benchmark: str
    n225_benchmark: str
    forward_outcomes: str


def _price_relation_names(namespace: str) -> _DailyRankingPriceRelationNames:
    return _DailyRankingPriceRelationNames(
        normalized_raw=f"{namespace}_normalized_raw",
        signal_requests=f"{namespace}_signal_requests",
        signal_bases=f"{namespace}_signal_bases",
        eligible_signal_requests=f"{namespace}_eligible_signal_requests",
        signal_projection_requests=f"{namespace}_signal_projection_requests",
        basis_prices=f"{namespace}_basis_prices",
        price_history=f"{namespace}_price_history",
        signal_features=f"{namespace}_signal_price_features",
        raw_sessions=f"{namespace}_raw_sessions",
        outcome_requests=f"{namespace}_outcome_requests",
        completion_bases=f"{namespace}_completion_bases",
        endpoint_requests=f"{namespace}_outcome_endpoint_requests",
        projected_outcomes_long=f"{namespace}_projected_outcomes_long",
        topix_benchmark=f"{namespace}_topix_benchmark",
        n225_benchmark=f"{namespace}_n225_benchmark",
        forward_outcomes=f"{namespace}_forward_price_outcomes",
    )


def _price_relation_name_values(
    names: _DailyRankingPriceRelationNames,
) -> tuple[str, ...]:
    return tuple(
        getattr(names, field_name) for field_name in names.__dataclass_fields__
    )


def _drop_price_relations(
    conn: Any,
    names: _DailyRankingPriceRelationNames,
    *,
    retain: tuple[str, ...] = (),
) -> None:
    retained = set(retain)
    for relation_name in reversed(_price_relation_name_values(names)):
        if relation_name not in retained:
            conn.execute(f"DROP TABLE IF EXISTS {relation_name}")


def _require_market_v4_price_columns(conn: Any) -> None:
    missing_by_table: dict[str, list[str]] = {}
    for table_name, required_columns in _RESEARCH_PRICE_REQUIRED_COLUMNS.items():
        observed_columns = {
            str(row[1])
            for row in conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        }
        missing_columns = sorted(required_columns - observed_columns)
        if missing_columns:
            missing_by_table[table_name] = missing_columns
    if missing_by_table:
        details = "; ".join(
            f"{table_name}=({', '.join(columns)})"
            for table_name, columns in sorted(missing_by_table.items())
        )
        raise RuntimeError(
            "Incompatible market.duckdb: missing required Market v4 columns "
            f"({details}). Run initial sync with reset enabled "
            "(resetBeforeSync=true) to recreate the Market Data Plane."
        )


def daily_ranking_forward_outcome_columns(
    horizons: Sequence[int],
) -> tuple[str, ...]:
    return (
        "code",
        "date",
        *(
            column
            for horizon in tuple(sorted({int(value) for value in horizons}))
            for column in (
                f"forward_outcome_completion_date_{horizon}d",
                f"forward_close_return_{horizon}d_pct",
                f"forward_close_excess_return_{horizon}d_pct",
                f"forward_close_n225_excess_return_{horizon}d_pct",
                f"completion_basis_id_{horizon}d",
            )
        ),
    )


@dataclass(frozen=True)
class EventTimeSignalRequest:
    signal_date: str
    start_date: str | None = None
    market_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class EventTimeSignalSql:
    relation_name: str
    columns: tuple[str, ...]
    cte_sql: str
    params: tuple[Any, ...]
    validation_sql: str
    validation_params: tuple[Any, ...]
    materialization_sql: str
    materialization_params: tuple[Any, ...]
    row_count: int | None = None
    code_count: int | None = None


def build_event_time_signal_sql(request: EventTimeSignalRequest) -> EventTimeSignalSql:
    """Build one read-only CTE graph using the signal-date adjustment basis."""

    market_clause = ""
    params: list[Any] = [request.signal_date, request.start_date or "0001-01-01"]
    if request.market_codes:
        placeholders = ",".join("?" for _ in request.market_codes)
        market_clause = f" AND smd.market_code IN ({placeholders})"
        params.extend(request.market_codes)

    raw_code = _normalized_code_sql("raw.code")
    master_code = _normalized_code_sql("smd.code")
    basis_code = _normalized_code_sql("basis.code")
    segment_code = _normalized_code_sql("segment.code")
    cte_sql = f"""
        event_time_signal_config AS (
            SELECT CAST(? AS VARCHAR) AS signal_date,
                   CAST(? AS VARCHAR) AS start_date
        ),
        event_time_signal_universe AS (
            SELECT DISTINCT {master_code} AS normalized_code
            FROM stock_master_daily AS smd
            CROSS JOIN event_time_signal_config AS config
            WHERE smd.date = config.signal_date{market_clause}
        ),
        event_time_signal_raw_ranked AS (
            SELECT
                {raw_code} AS normalized_code,
                raw.code AS source_code,
                raw.date,
                raw.open,
                raw.high,
                raw.low,
                raw.close,
                raw.volume,
                raw.adjustment_factor,
                ROW_NUMBER() OVER (
                    PARTITION BY {raw_code}, raw.date
                    ORDER BY
                        CASE WHEN raw.code = {raw_code} THEN 0 ELSE 1 END,
                        length(raw.code),
                        raw.code
                ) AS alias_rank,
                COUNT(*) OVER (
                    PARTITION BY {raw_code}, raw.date
                ) AS alias_count,
                COUNT(DISTINCT concat_ws(
                    '|',
                    coalesce(CAST(raw.open AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.high AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.low AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.close AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.volume AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.adjustment_factor AS VARCHAR), '<null>')
                )) OVER (
                    PARTITION BY {raw_code}, raw.date
                ) AS alias_value_count
            FROM stock_data_raw AS raw
            JOIN event_time_signal_universe AS universe
              ON universe.normalized_code = {raw_code}
            CROSS JOIN event_time_signal_config AS config
            WHERE raw.date >= config.start_date
              AND raw.date <= config.signal_date
        ),
        event_time_signal_raw AS (
            SELECT *
            FROM event_time_signal_raw_ranked
            WHERE alias_rank = 1
        ),
        event_time_signal_codes AS (
            SELECT DISTINCT raw.normalized_code
            FROM event_time_signal_raw AS raw
            CROSS JOIN event_time_signal_config AS config
            WHERE raw.date = config.signal_date
        ),
        event_time_signal_basis_candidates AS (
            SELECT
                signal.normalized_code,
                basis.basis_id,
                basis.valid_from,
                basis.valid_to_exclusive,
                basis.adjustment_through_date,
                basis.source_fingerprint,
                basis.materialized_through_date,
                basis.status,
                COUNT(basis.basis_id) OVER (
                    PARTITION BY signal.normalized_code
                ) AS basis_count,
                ROW_NUMBER() OVER (
                    PARTITION BY signal.normalized_code
                    ORDER BY basis.valid_from, basis.basis_id
                ) AS basis_rank
            FROM event_time_signal_codes AS signal
            CROSS JOIN event_time_signal_config AS config
            LEFT JOIN stock_adjustment_bases AS basis
              ON {basis_code} = signal.normalized_code
             AND basis.valid_from <= config.signal_date
             AND (
                 basis.valid_to_exclusive IS NULL
                 OR config.signal_date < basis.valid_to_exclusive
             )
        ),
        event_time_signal_selected_basis AS (
            SELECT *,
                status = 'ready'
                AND materialized_through_date >= config.signal_date
                AND adjustment_through_date = valid_from
                AND valid_from <= config.signal_date
                AND source_fingerprint IS NOT NULL
                AND trim(source_fingerprint) <> ''
                AND basis_id = (
                    'event-pit-v1:' || normalized_code || ':' || valid_from
                ) AS basis_is_ready
            FROM event_time_signal_basis_candidates
            CROSS JOIN event_time_signal_config AS config
            WHERE basis_rank = 1
        ),
        event_time_signal_projection_lineage AS (
            SELECT
                raw.normalized_code,
                raw.date,
                raw.open,
                raw.high,
                raw.low,
                raw.close,
                raw.volume,
                basis.basis_id AS signal_basis_id,
                COUNT(segment.basis_id) AS segment_count,
                MIN(segment.cumulative_factor) AS cumulative_factor,
                COUNT(segment.basis_id) FILTER (
                    WHERE segment.cumulative_factor IS NULL
                       OR NOT isfinite(segment.cumulative_factor)
                       OR segment.cumulative_factor <= 0
                ) AS invalid_factor_count
            FROM event_time_signal_raw AS raw
            JOIN event_time_signal_selected_basis AS basis
              ON basis.normalized_code = raw.normalized_code
             AND basis.basis_count = 1
             AND basis.basis_is_ready
            LEFT JOIN stock_adjustment_basis_segments AS segment
              ON {segment_code} = raw.normalized_code
             AND segment.basis_id = basis.basis_id
             AND segment.source_date_from <= raw.date
             AND (
                 segment.source_date_to_exclusive IS NULL
                 OR raw.date < segment.source_date_to_exclusive
             )
            GROUP BY
                raw.normalized_code,
                raw.date,
                raw.open,
                raw.high,
                raw.low,
                raw.close,
                raw.volume,
                basis.basis_id
        ),
        event_time_signal_lineage_issues AS (
            SELECT
                'raw_alias_conflict' AS issue,
                normalized_code,
                date
            FROM event_time_signal_raw
            WHERE alias_count > 1 AND alias_value_count > 1
            UNION ALL
            SELECT
                CASE
                    WHEN basis_count = 0 THEN 'signal_basis_missing'
                    ELSE 'signal_basis_ambiguous'
                END AS issue,
                basis.normalized_code,
                config.signal_date AS date
            FROM event_time_signal_selected_basis AS basis
            CROSS JOIN event_time_signal_config AS config
            WHERE basis.basis_count <> 1
            UNION ALL
            SELECT
                'signal_basis_not_ready' AS issue,
                basis.normalized_code,
                config.signal_date AS date
            FROM event_time_signal_selected_basis AS basis
            CROSS JOIN event_time_signal_config AS config
            WHERE basis.basis_count = 1 AND NOT basis.basis_is_ready
            UNION ALL
            SELECT
                CASE
                    WHEN segment_count <> 1 THEN 'signal_segment_cardinality'
                    ELSE 'signal_segment_factor'
                END AS issue,
                normalized_code,
                date
            FROM event_time_signal_projection_lineage
            WHERE segment_count <> 1 OR invalid_factor_count <> 0
        ),
        event_time_signal_projected AS (
            SELECT
                normalized_code,
                date,
                open * cumulative_factor AS open,
                high * cumulative_factor AS high,
                low * cumulative_factor AS low,
                close * cumulative_factor AS close,
                CAST(ROUND(volume / cumulative_factor) AS BIGINT) AS volume,
                signal_basis_id
            FROM event_time_signal_projection_lineage
            WHERE segment_count = 1 AND invalid_factor_count = 0
        ),
        event_time_signal_lagged AS (
            SELECT
                *,
                LAG(close, 1) OVER (
                    PARTITION BY normalized_code ORDER BY date
                ) AS close_lag_1d,
                LAG(close, 20) OVER (
                    PARTITION BY normalized_code ORDER BY date
                ) AS close_lag_20d,
                LAG(close, 60) OVER (
                    PARTITION BY normalized_code ORDER BY date
                ) AS close_lag_60d
            FROM event_time_signal_projected
        ),
        {EVENT_TIME_SIGNAL_RELATION} AS (
            SELECT
                normalized_code,
                date,
                open,
                high,
                low,
                close,
                volume,
                close_lag_1d,
                close_lag_20d,
                close_lag_60d,
                CASE WHEN close_lag_1d > 0
                    THEN (close / close_lag_1d - 1.0) * 100.0
                END AS recent_return_1d_pct,
                CASE WHEN close_lag_20d > 0
                    THEN (close / close_lag_20d - 1.0) * 100.0
                END AS recent_return_20d_pct,
                CASE WHEN close_lag_60d > 0
                    THEN (close / close_lag_60d - 1.0) * 100.0
                END AS recent_return_60d_pct,
                signal_basis_id
            FROM event_time_signal_lagged
        )
    """
    resolved_params = tuple(params)
    materialization_sql = f"""
        /* daily_ranking_event_time_physical_projection */
        WITH {cte_sql}
        SELECT
            CAST(NULL AS VARCHAR) AS issue,
            normalized_code,
            date,
            open,
            high,
            low,
            close,
            volume,
            close_lag_1d,
            close_lag_20d,
            close_lag_60d,
            recent_return_1d_pct,
            recent_return_20d_pct,
            recent_return_60d_pct,
            signal_basis_id
        FROM {EVENT_TIME_SIGNAL_RELATION}
        WHERE NOT EXISTS (SELECT 1 FROM event_time_signal_lineage_issues)
        UNION ALL
        SELECT
            issue,
            normalized_code,
            date,
            CAST(NULL AS DOUBLE) AS open,
            CAST(NULL AS DOUBLE) AS high,
            CAST(NULL AS DOUBLE) AS low,
            CAST(NULL AS DOUBLE) AS close,
            CAST(NULL AS BIGINT) AS volume,
            CAST(NULL AS DOUBLE) AS close_lag_1d,
            CAST(NULL AS DOUBLE) AS close_lag_20d,
            CAST(NULL AS DOUBLE) AS close_lag_60d,
            CAST(NULL AS DOUBLE) AS recent_return_1d_pct,
            CAST(NULL AS DOUBLE) AS recent_return_20d_pct,
            CAST(NULL AS DOUBLE) AS recent_return_60d_pct,
            CAST(NULL AS VARCHAR) AS signal_basis_id
        FROM event_time_signal_lineage_issues
        LIMIT ?
    """
    return EventTimeSignalSql(
        relation_name=EVENT_TIME_SIGNAL_RELATION,
        columns=EVENT_TIME_SIGNAL_COLUMNS,
        cte_sql=cte_sql,
        params=resolved_params,
        validation_sql=(
            f"WITH {cte_sql} "
            "SELECT issue, normalized_code, date "
            "FROM event_time_signal_lineage_issues "
            "ORDER BY issue, normalized_code, date"
        ),
        validation_params=resolved_params,
        materialization_sql=materialization_sql,
        materialization_params=(*resolved_params, EVENT_TIME_SIGNAL_MAX_ROWS + 1),
    )


def _normalized_code_sql(column_ref: str) -> str:
    return (
        "CASE "
        f"WHEN length({column_ref}) IN (5, 6) AND right({column_ref}, 1) = '0' "
        f"THEN left({column_ref}, length({column_ref}) - 1) "
        f"ELSE {column_ref} "
        "END"
    )


def build_daily_ranking_event_time_prices(
    conn: Any,
    request: DailyRankingPriceRequest,
) -> DailyRankingPriceRelations:
    """Build research-only signal features and completion-aligned outcomes."""

    require_market_v4_compatibility(
        conn,
        required_tables=_RESEARCH_PRICE_REQUIRED_COLUMNS,
    )
    _require_market_v4_price_columns(conn)
    generation_namespace = f"{request.namespace}_g_{uuid4().hex}"
    names = _price_relation_names(generation_namespace)
    try:
        result = _build_daily_ranking_event_time_prices(conn, request, names)
    except Exception:
        _drop_price_relations(conn, names)
        raise
    _drop_price_relations(
        conn,
        names,
        retain=(result.signal_features, result.forward_outcomes, result.price_history),
    )
    return result


def _build_daily_ranking_event_time_prices(
    conn: Any,
    request: DailyRankingPriceRequest,
    names: _DailyRankingPriceRelationNames,
) -> DailyRankingPriceRelations:
    valid_bar = "open > 0 AND high > 0 AND low > 0 AND close > 0 AND volume >= 0"
    raw_code = normalize_code_sql("raw.code")
    master_code = normalize_code_sql("smd.code")
    valuation_code = normalize_code_sql("dv.code")

    raw_conditions: list[str] = []
    raw_params: list[str] = []
    if request.query_start is not None:
        raw_conditions.append("raw.date >= ?")
        raw_params.append(request.query_start)
    if request.query_end is not None:
        raw_conditions.append("raw.date <= ?")
        raw_params.append(request.query_end)
    raw_where = "" if not raw_conditions else "WHERE " + " AND ".join(raw_conditions)
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.normalized_raw} AS
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
                CAST(raw.adjustment_factor AS DOUBLE) AS adjustment_factor,
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
                    coalesce(CAST(raw.volume AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.adjustment_factor AS VARCHAR), '<null>')
                )) OVER (PARTITION BY {raw_code}, raw.date) AS alias_value_count
            FROM stock_data_raw raw
            {raw_where}
        )
        SELECT * FROM ranked WHERE alias_rank = 1
        """,
        raw_params,
    )
    alias_conflicts = _aggregate_count(
        conn,
        names.normalized_raw,
        "alias_count > 1 AND alias_value_count > 1",
    )
    if alias_conflicts:
        raise RuntimeError(
            "price projection alias conflict in stock_data_raw; "
            f"conflicting code/date rows={alias_conflicts}"
        )

    signal_conditions: list[str] = []
    signal_params: list[str] = []
    if request.market_codes:
        placeholders = ",".join("?" for _ in request.market_codes)
        signal_conditions.append(f"smd.market_code IN ({placeholders})")
        signal_params.extend(request.market_codes)
    if request.analysis_start_date is not None:
        signal_conditions.append("smd.date >= ?")
        signal_params.append(request.analysis_start_date)
    if request.analysis_end_date is not None:
        signal_conditions.append("smd.date <= ?")
        signal_params.append(request.analysis_end_date)
    signal_where = " AND ".join(signal_conditions) if signal_conditions else "TRUE"
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.signal_requests} AS
        SELECT DISTINCT {master_code} AS code, CAST(smd.date AS DATE) AS date
        FROM stock_master_daily smd
        JOIN {names.normalized_raw} raw
          ON raw.code = {master_code} AND raw.date = CAST(smd.date AS DATE)
        WHERE {signal_where}
        """,
        signal_params,
    )
    invalid_signal_basis = int(
        conn.execute(
            f"""
            SELECT count(*)
            FROM {names.signal_requests} signal
            WHERE (
                SELECT count(*) FROM stock_adjustment_bases basis
                WHERE {normalize_code_sql("basis.code")} = signal.code
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
        CREATE OR REPLACE TEMP TABLE {names.signal_bases} AS
        SELECT
            signal.code,
            signal.date,
            CAST(basis.basis_id AS VARCHAR) AS signal_basis_id
        FROM {names.signal_requests} signal
        JOIN stock_adjustment_bases basis
          ON {normalize_code_sql("basis.code")} = signal.code
         AND CAST(basis.valid_from AS DATE) <= signal.date
         AND (basis.valid_to_exclusive IS NULL
              OR signal.date < CAST(basis.valid_to_exclusive AS DATE))
        WHERE basis.status = 'ready'
          AND CAST(basis.materialized_through_date AS DATE) >= signal.date
          AND CAST(basis.adjustment_through_date AS DATE)
              = CAST(basis.valid_from AS DATE)
          AND basis.source_fingerprint IS NOT NULL
          AND trim(basis.source_fingerprint) <> ''
          AND basis.basis_id = (
              'event-pit-v1:' || signal.code || ':' || CAST(basis.valid_from AS DATE)
          )
          AND (
              SELECT count(*) FROM daily_valuation dv
              WHERE {valuation_code} = signal.code
                AND CAST(dv.date AS DATE) = signal.date
                AND CAST(dv.basis_version AS VARCHAR) = CAST(basis.basis_id AS VARCHAR)
          ) = 1
        """
    )
    signal_request_count = _aggregate_count(conn, names.signal_requests)
    if _aggregate_count(conn, names.signal_bases) != signal_request_count:
        raise RuntimeError(
            "price projection signal basis is not ready/materialized or has a "
            "missing cutoff-valid daily_valuation basis match"
        )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.eligible_signal_requests} AS
        SELECT basis.code, basis.date, basis.signal_basis_id
        FROM {names.signal_bases} basis
        JOIN {names.normalized_raw} raw
          ON raw.code = basis.code AND raw.date = basis.date
        WHERE raw.open > 0 AND raw.high > 0 AND raw.low > 0
          AND raw.close > 0 AND raw.volume >= 0
        """
    )

    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.signal_projection_requests} AS
        WITH basis_ranges AS (
            SELECT code, signal_basis_id AS basis_id, max(date) AS max_signal_date
            FROM {names.signal_bases}
            GROUP BY code, signal_basis_id
        )
        SELECT
            basis_range.code,
            basis_range.basis_id,
            raw.date,
            raw.open, raw.high, raw.low, raw.close, raw.volume
        FROM basis_ranges basis_range
        JOIN {names.normalized_raw} raw
          ON raw.code = basis_range.code AND raw.date <= basis_range.max_signal_date
        """
    )
    _validate_covering_segments(
        conn,
        request_relation=names.signal_projection_requests,
        basis_column="basis_id",
        date_column="date",
        label="signal",
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.basis_prices} AS
        SELECT
            request.code,
            request.basis_id,
            request.date,
            request.open * segment.cumulative_factor AS open,
            request.high * segment.cumulative_factor AS high,
            request.low * segment.cumulative_factor AS low,
            request.close * segment.cumulative_factor AS close,
            CAST(ROUND(request.volume / segment.cumulative_factor) AS BIGINT) AS volume
        FROM {names.signal_projection_requests} request
        JOIN stock_adjustment_basis_segments segment
          ON {normalize_code_sql("segment.code")} = request.code
         AND segment.basis_id = request.basis_id
         AND CAST(segment.source_date_from AS DATE) <= request.date
         AND (segment.source_date_to_exclusive IS NULL
              OR request.date < CAST(segment.source_date_to_exclusive AS DATE))
        """
    )
    history_upper_bound = (
        "TRUE" if request.analysis_end_date is None else "date <= CAST(? AS DATE)"
    )
    history_params: list[str] = (
        [] if request.analysis_end_date is None else [request.analysis_end_date]
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.price_history} AS
        SELECT
            code,
            date,
            CAST(basis_id AS VARCHAR) AS price_basis_id,
            open,
            high,
            low,
            close,
            volume
        FROM {names.basis_prices}
        WHERE {history_upper_bound}
        """,
        history_params,
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.signal_features} AS
        WITH ordered AS (
            SELECT
                *,
                row_number() OVER (PARTITION BY code, basis_id ORDER BY date) - 1
                    AS session_index,
                lag(close) OVER (PARTITION BY code, basis_id ORDER BY date) AS prev_close
            FROM {names.basis_prices}
            WHERE {valid_bar}
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
        FROM {names.eligible_signal_requests} signal
        JOIN with_atr_lag f
          ON f.code = signal.code AND f.basis_id = signal.signal_basis_id
         AND f.date = signal.date
        """
    )

    _materialize_daily_ranking_raw_sessions(conn, request, names, valid_bar)
    _materialize_daily_ranking_forward_outcomes(conn, request, names)
    return _build_price_relation_result(conn, request, names)


def _materialize_daily_ranking_raw_sessions(
    conn: Any,
    request: DailyRankingPriceRequest,
    names: _DailyRankingPriceRelationNames,
    valid_bar: str,
) -> None:
    lead_exprs = ",\n".join(
        f"lead(date, {horizon}) OVER (PARTITION BY code ORDER BY date) "
        f"AS completion_date_{horizon}d"
        for horizon in request.horizons
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.raw_sessions} AS
        SELECT code, date, {lead_exprs}
        FROM {names.normalized_raw}
        WHERE {valid_bar}
        """
    )


def _build_price_relation_result(
    conn: Any,
    request: DailyRankingPriceRequest,
    names: _DailyRankingPriceRelationNames,
) -> DailyRankingPriceRelations:
    relation_specs = (
        (names.normalized_raw, "code, date"),
        (names.eligible_signal_requests, "code, date"),
        (names.price_history, "code, date, price_basis_id"),
        (names.signal_features, "code, date"),
        (names.outcome_requests, "code, signal_date, horizon"),
        (names.completion_bases, "code, signal_date, horizon"),
        (names.endpoint_requests, "code, signal_date, horizon, endpoint"),
        (names.topix_benchmark, "date"),
        (names.n225_benchmark, "date"),
        (names.forward_outcomes, "code, date"),
    )
    diagnostics_sql = " UNION ALL ".join(
        f"SELECT '{name}' AS relation_name, count(*) AS row_count, "
        f"count(DISTINCT ({key_columns})) AS key_count FROM {name}"
        for name, key_columns in relation_specs
    )
    counts = {
        str(relation_name): (int(row_count), int(key_count))
        for relation_name, row_count, key_count in conn.execute(
            diagnostics_sql
        ).fetchall()
    }
    signal_schema = _relation_schema(conn, names.signal_features)
    history_schema = _relation_schema(conn, names.price_history)
    outcome_schema = _relation_schema(conn, names.forward_outcomes)
    expected_outcome_schema = daily_ranking_forward_outcome_columns(request.horizons)
    if signal_schema != DAILY_RANKING_SIGNAL_FEATURE_COLUMNS:
        raise RuntimeError(
            "price projection signal feature schema mismatch; "
            f"expected={DAILY_RANKING_SIGNAL_FEATURE_COLUMNS!r}, actual={signal_schema!r}"
        )
    if history_schema != DAILY_RANKING_PRICE_HISTORY_COLUMNS:
        raise RuntimeError(
            "price projection history schema mismatch; "
            f"expected={DAILY_RANKING_PRICE_HISTORY_COLUMNS!r}, "
            f"actual={history_schema!r}"
        )
    if outcome_schema != expected_outcome_schema:
        raise RuntimeError(
            "price projection forward outcome schema mismatch; "
            f"expected={expected_outcome_schema!r}, actual={outcome_schema!r}"
        )
    diagnostics = DailyRankingPriceDiagnostics(
        canonical_raw_rows=counts[names.normalized_raw][0],
        signal_request_rows=counts[names.eligible_signal_requests][0],
        signal_feature_rows=counts[names.signal_features][0],
        outcome_request_rows=counts[names.outcome_requests][0],
        completed_request_rows=counts[names.completion_bases][0],
        endpoint_rows=counts[names.endpoint_requests][0],
        forward_outcome_rows=counts[names.forward_outcomes][0],
        signal_feature_key_rows=counts[names.signal_features][1],
        forward_outcome_key_rows=counts[names.forward_outcomes][1],
        topix_benchmark_rows=counts[names.topix_benchmark][0],
        n225_benchmark_rows=counts[names.n225_benchmark][0],
        signal_feature_schema=signal_schema,
        forward_outcome_schema=outcome_schema,
    )
    if diagnostics.signal_feature_rows != diagnostics.signal_request_rows:
        raise RuntimeError("price projection signal feature cardinality mismatch")
    if diagnostics.outcome_request_rows != (
        diagnostics.signal_request_rows * len(request.horizons)
    ):
        raise RuntimeError("price projection outcome request cardinality mismatch")
    if diagnostics.endpoint_rows != 2 * diagnostics.completed_request_rows:
        raise RuntimeError("price projection endpoint cardinality mismatch")
    if diagnostics.forward_outcome_rows > diagnostics.signal_request_rows:
        raise RuntimeError("price projection forward outcome cardinality mismatch")
    if diagnostics.signal_feature_key_rows != diagnostics.signal_feature_rows:
        raise RuntimeError("price projection signal feature keys are not unique")
    if diagnostics.forward_outcome_key_rows != diagnostics.forward_outcome_rows:
        raise RuntimeError("price projection forward outcome keys are not unique")

    signal_basis_hash = _ordered_sha256(
        conn,
        f"SELECT code, date, signal_basis_id FROM {names.signal_bases} "
        "ORDER BY code, date, signal_basis_id",
    )
    completion_basis_hash = _ordered_sha256(
        conn,
        f"SELECT code, signal_date, horizon, completion_date, completion_basis_id "
        f"FROM {names.completion_bases} ORDER BY code, signal_date, horizon",
    )
    projection_hash = _ordered_sha256(
        conn,
        f"SELECT * FROM {names.signal_features} ORDER BY code, date",
    )
    signal_segment_hash = _ordered_sha256(
        conn,
        f"""
        SELECT DISTINCT segment.code, segment.basis_id,
               segment.source_date_from, segment.source_date_to_exclusive,
               segment.cumulative_factor
        FROM {names.signal_projection_requests} request
        JOIN stock_adjustment_basis_segments segment
          ON {normalize_code_sql("segment.code")} = request.code
         AND segment.basis_id = request.basis_id
         AND CAST(segment.source_date_from AS DATE) <= request.date
         AND (segment.source_date_to_exclusive IS NULL
              OR request.date < CAST(segment.source_date_to_exclusive AS DATE))
        ORDER BY segment.code, segment.basis_id, segment.source_date_from
        """,
    )
    completion_segment_hash = _ordered_sha256(
        conn,
        f"""
        SELECT DISTINCT segment.code, segment.basis_id,
               segment.source_date_from, segment.source_date_to_exclusive,
               segment.cumulative_factor
        FROM {names.endpoint_requests} request
        JOIN stock_adjustment_basis_segments segment
          ON {normalize_code_sql("segment.code")} = request.code
         AND segment.basis_id = request.completion_basis_id
         AND CAST(segment.source_date_from AS DATE) <= request.endpoint_date
         AND (segment.source_date_to_exclusive IS NULL
              OR request.endpoint_date < CAST(segment.source_date_to_exclusive AS DATE))
        ORDER BY segment.code, segment.basis_id, segment.source_date_from
        """,
    )
    compatibility_outcome_columns = ", ".join(
        column
        for horizon in request.horizons
        for column in (
            f"forward_outcome_completion_date_{horizon}d",
            f"forward_close_return_{horizon}d_pct",
            f"forward_close_excess_return_{horizon}d_pct",
            f"completion_basis_id_{horizon}d",
        )
    )
    forward_outcome_hash = _ordered_sha256(
        conn,
        f"SELECT code, date, {compatibility_outcome_columns} "
        f"FROM {names.forward_outcomes} ORDER BY code, date",
    )
    signal_segment_count = _distinct_segment_count(
        conn,
        request_relation=names.signal_projection_requests,
        basis_column="basis_id",
        date_column="date",
    )
    completion_segment_count = _distinct_segment_count(
        conn,
        request_relation=names.endpoint_requests,
        basis_column="completion_basis_id",
        date_column="endpoint_date",
    )
    lineage = DailyRankingPriceLineage(
        canonical_raw_row_count=diagnostics.canonical_raw_rows,
        signal_feature_row_count=diagnostics.signal_feature_rows,
        outcome_request_row_count=diagnostics.outcome_request_rows,
        completed_outcome_row_count=_aggregate_count(
            conn, names.projected_outcomes_long
        ),
        signal_basis_row_count=int(
            conn.execute(
                f"SELECT count(*) FROM (SELECT DISTINCT code, signal_basis_id "
                f"FROM {names.signal_bases})"
            ).fetchone()[0]
        ),
        signal_segment_row_count=signal_segment_count,
        completion_basis_row_count=int(
            conn.execute(
                f"SELECT count(*) FROM (SELECT DISTINCT code, completion_basis_id "
                f"FROM {names.completion_bases})"
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
    return DailyRankingPriceRelations(
        signal_features=names.signal_features,
        forward_outcomes=names.forward_outcomes,
        price_history=names.price_history,
        lineage=lineage,
        diagnostics=diagnostics,
    )


def _validate_covering_segments(
    conn: Any,
    *,
    request_relation: str,
    basis_column: str,
    date_column: str,
    label: str,
    endpoint_suffix: str = "",
) -> None:
    invalid_cardinality = int(
        conn.execute(
            f"""
            SELECT count(*)
            FROM {request_relation} request
            WHERE (
                SELECT count(*) FROM stock_adjustment_basis_segments segment
                WHERE {normalize_code_sql("segment.code")} = request.code
                  AND segment.basis_id = request.{basis_column}
                  AND CAST(segment.source_date_from AS DATE) <= request.{date_column}
                  AND (segment.source_date_to_exclusive IS NULL
                       OR request.{date_column} < CAST(segment.source_date_to_exclusive AS DATE))
            ) <> 1
            """
        ).fetchone()[0]
    )
    if invalid_cardinality:
        raise RuntimeError(
            f"price projection {label} segment cardinality must be exactly one"
            f"{endpoint_suffix}; invalid rows={invalid_cardinality}"
        )
    invalid_factors = int(
        conn.execute(
            f"""
            SELECT count(*)
            FROM {request_relation} request
            JOIN stock_adjustment_basis_segments segment
              ON {normalize_code_sql("segment.code")} = request.code
             AND segment.basis_id = request.{basis_column}
             AND CAST(segment.source_date_from AS DATE) <= request.{date_column}
             AND (segment.source_date_to_exclusive IS NULL
                  OR request.{date_column} < CAST(segment.source_date_to_exclusive AS DATE))
            WHERE segment.cumulative_factor IS NULL
               OR NOT isfinite(segment.cumulative_factor)
               OR segment.cumulative_factor <= 0
            """
        ).fetchone()[0]
    )
    if invalid_factors:
        raise RuntimeError(
            f"price projection {label} segment factor must be finite and positive; "
            f"invalid rows={invalid_factors}"
        )


def _aggregate_count(conn: Any, relation: str, predicate: str | None = None) -> int:
    where = "" if predicate is None else f" WHERE {predicate}"
    return int(conn.execute(f"SELECT count(*) FROM {relation}{where}").fetchone()[0])


def _relation_schema(conn: Any, relation: str) -> tuple[str, ...]:
    return tuple(
        str(row[1])
        for row in conn.execute(f"PRAGMA table_info('{relation}')").fetchall()
    )


def _ordered_sha256(conn: Any, query: str) -> str:
    digest = hashlib.sha256()
    cursor = conn.execute(query)
    while rows := cursor.fetchmany(10_000):
        for row in rows:
            digest.update(repr(tuple(row)).encode("utf-8"))
            digest.update(b"\n")
    return digest.hexdigest()


def _distinct_segment_count(
    conn: Any,
    *,
    request_relation: str,
    basis_column: str,
    date_column: str,
) -> int:
    return int(
        conn.execute(
            f"""
            SELECT count(*) FROM (
                SELECT DISTINCT segment.code, segment.basis_id,
                       segment.source_date_from, segment.source_date_to_exclusive
                FROM {request_relation} request
                JOIN stock_adjustment_basis_segments segment
                  ON {normalize_code_sql("segment.code")} = request.code
                 AND segment.basis_id = request.{basis_column}
                 AND CAST(segment.source_date_from AS DATE) <= request.{date_column}
                 AND (segment.source_date_to_exclusive IS NULL
                      OR request.{date_column} < CAST(segment.source_date_to_exclusive AS DATE))
            )
            """
        ).fetchone()[0]
    )


def _materialize_daily_ranking_forward_outcomes(
    conn: Any,
    request: DailyRankingPriceRequest,
    names: _DailyRankingPriceRelationNames,
) -> None:
    outcome_unions = " UNION ALL ".join(
        f"""
        SELECT signal.code, signal.date AS signal_date, {horizon} AS horizon,
               sessions.completion_date_{horizon}d AS completion_date
        FROM {names.eligible_signal_requests} signal
        JOIN {names.raw_sessions} sessions
          ON sessions.code = signal.code AND sessions.date = signal.date
        """
        for horizon in request.horizons
    )
    conn.execute(
        f"CREATE OR REPLACE TEMP TABLE {names.outcome_requests} AS {outcome_unions}"
    )
    invalid_completion_basis = int(
        conn.execute(
            f"""
            SELECT count(*)
            FROM {names.outcome_requests} request
            WHERE request.completion_date IS NOT NULL
              AND (
                SELECT count(*) FROM stock_adjustment_bases basis
                WHERE {normalize_code_sql("basis.code")} = request.code
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
        f"""
        CREATE OR REPLACE TEMP TABLE {names.completion_bases} AS
        SELECT
            request.code, request.signal_date, request.horizon,
            request.completion_date,
            CAST(basis.basis_id AS VARCHAR) AS completion_basis_id
        FROM {names.outcome_requests} request
        JOIN stock_adjustment_bases basis
          ON {normalize_code_sql("basis.code")} = request.code
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
          AND basis.basis_id = (
              'event-pit-v1:' || request.code || ':' || CAST(basis.valid_from AS DATE)
          )
        """
    )
    completed_requests = _aggregate_count(
        conn, names.outcome_requests, "completion_date IS NOT NULL"
    )
    if _aggregate_count(conn, names.completion_bases) != completed_requests:
        raise RuntimeError(
            "price projection completion basis is not ready/materialized through completion"
        )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.endpoint_requests} AS
        SELECT code, signal_date, horizon, completion_date, completion_basis_id,
               'signal' AS endpoint, signal_date AS endpoint_date
        FROM {names.completion_bases}
        UNION ALL
        SELECT code, signal_date, horizon, completion_date, completion_basis_id,
               'completion' AS endpoint, completion_date AS endpoint_date
        FROM {names.completion_bases}
        """
    )
    _validate_covering_segments(
        conn,
        request_relation=names.endpoint_requests,
        basis_column="completion_basis_id",
        date_column="endpoint_date",
        label="completion",
        endpoint_suffix=" for both endpoints",
    )

    duplicate_topix_dates = int(
        conn.execute(
            """
            SELECT count(*) FROM (
                SELECT CAST(date AS DATE)
                FROM topix_data
                GROUP BY CAST(date AS DATE)
                HAVING count(*) > 1
            )
            """
        ).fetchone()[0]
    )
    if duplicate_topix_dates:
        raise RuntimeError(
            "price projection TOPIX benchmark dates must be unique; "
            f"duplicate dates={duplicate_topix_dates}"
        )
    duplicate_n225_dates = int(
        conn.execute(
            f"""
            SELECT count(*) FROM (
                SELECT upper(code), CAST(date AS DATE)
                FROM indices_data
                WHERE upper(code) = '{_NIKKEI_SYNTHETIC_INDEX_CODE}'
                GROUP BY upper(code), CAST(date AS DATE)
                HAVING count(*) > 1
            )
            """
        ).fetchone()[0]
    )
    if duplicate_n225_dates:
        raise RuntimeError(
            "price projection N225 benchmark keys must be unique; "
            f"duplicate keys={duplicate_n225_dates}"
        )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.topix_benchmark} AS
        SELECT CAST(date AS DATE) AS date, CAST(close AS DOUBLE) AS close
        FROM topix_data
        WHERE close > 0
        """
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.n225_benchmark} AS
        SELECT CAST(date AS DATE) AS date, CAST(close AS DOUBLE) AS close
        FROM indices_data
        WHERE upper(code) = '{_NIKKEI_SYNTHETIC_INDEX_CODE}'
          AND close > 0
        """
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.projected_outcomes_long} AS
        WITH endpoints AS (
            SELECT
                request.*,
                raw.close * segment.cumulative_factor AS projected_close
            FROM {names.endpoint_requests} request
            JOIN {names.normalized_raw} raw
              ON raw.code = request.code AND raw.date = request.endpoint_date
            JOIN stock_adjustment_basis_segments segment
              ON {normalize_code_sql("segment.code")} = request.code
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
                       AND topix_signal.close > 0 AND topix_completion.close > 0
                THEN (p.completion_close / p.signal_close - 1.0) * 100.0
                   - (topix_completion.close / topix_signal.close - 1.0) * 100.0
            END AS forward_close_excess_return_pct,
            CASE WHEN p.signal_close > 0 AND p.completion_close > 0
                       AND n225_signal.close > 0 AND n225_completion.close > 0
                THEN (p.completion_close / p.signal_close - 1.0) * 100.0
                   - (n225_completion.close / n225_signal.close - 1.0) * 100.0
            END AS forward_close_n225_excess_return_pct
        FROM pivoted p
        LEFT JOIN {names.topix_benchmark} topix_signal
          ON topix_signal.date = p.signal_date
        LEFT JOIN {names.topix_benchmark} topix_completion
          ON topix_completion.date = p.completion_date
        LEFT JOIN {names.n225_benchmark} n225_signal
          ON n225_signal.date = p.signal_date
        LEFT JOIN {names.n225_benchmark} n225_completion
          ON n225_completion.date = p.completion_date
        """
    )
    outcome_columns = ",\n".join(
        expression
        for horizon in request.horizons
        for expression in (
            f"max(completion_date) FILTER (horizon = {horizon}) AS forward_outcome_completion_date_{horizon}d",
            f"max(forward_close_return_pct) FILTER (horizon = {horizon}) AS forward_close_return_{horizon}d_pct",
            f"max(forward_close_excess_return_pct) FILTER (horizon = {horizon}) AS forward_close_excess_return_{horizon}d_pct",
            f"max(forward_close_n225_excess_return_pct) FILTER (horizon = {horizon}) AS forward_close_n225_excess_return_{horizon}d_pct",
            f"max(completion_basis_id) FILTER (horizon = {horizon}) AS completion_basis_id_{horizon}d",
        )
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.forward_outcomes} AS
        SELECT code, signal_date AS date, {outcome_columns}
        FROM {names.projected_outcomes_long}
        GROUP BY code, signal_date
        """
    )
