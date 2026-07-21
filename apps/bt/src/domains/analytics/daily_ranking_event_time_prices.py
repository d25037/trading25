"""Event-time signal-price SQL for production Daily Ranking."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
from typing import Any, Sequence
from uuid import uuid4

from src.domains.analytics.readonly_duckdb_support import (
    normalize_code_sql,
    require_market_v5_compatibility,
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
        "turnover_value",
        "adjustment_factor",
        "adjusted_open",
        "adjusted_high",
        "adjusted_low",
        "adjusted_close",
        "adjusted_volume",
    },
    "stock_data": {"code", "date", "open", "high", "low", "close", "volume"},
    "stock_master_daily": {"date", "code", "market_code"},
    "daily_valuation": {
        "code",
        "date",
        "price_basis_date",
        "fundamentals_adjustment_basis_date",
        "source_fingerprint",
    },
    "stock_provider_windows": {
        "code",
        "coverage_start",
        "coverage_end",
        "provider_as_of",
        "source_fingerprint",
    },
    "stock_adjustment_events": {
        "code",
        "date",
        "adjustment_factor",
        "source_fingerprint",
    },
    "topix_data": {"date", "open", "close"},
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
DAILY_RANKING_VALID_RAW_BAR_PRICE_COLUMNS = ("open", "high", "low", "close")


def daily_ranking_valid_raw_bar_sql(qualifier: str | None = None) -> str:
    """Return the canonical valid raw-session predicate, optionally qualified."""

    if qualifier is not None and not _RELATION_NAMESPACE_RE.fullmatch(qualifier):
        raise ValueError(f"invalid raw-bar qualifier: {qualifier!r}")
    prefix = "" if qualifier is None else f"{qualifier}."
    prices = " AND ".join(
        f"{prefix}{column} > 0" for column in DAILY_RANKING_VALID_RAW_BAR_PRICE_COLUMNS
    )
    return f"{prices} AND {prefix}volume >= 0"


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
    next_open_outcome_sha256: str
    price_projection_sha256: str
    signal_basis_policy: str
    completion_basis_policy: str
    next_open_integrity_policy: str
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
            "next_open_outcome_sha256": self.next_open_outcome_sha256,
            "price_projection_sha256": self.price_projection_sha256,
            "signal_basis_policy": self.signal_basis_policy,
            "completion_basis_policy": self.completion_basis_policy,
            "next_open_integrity_policy": self.next_open_integrity_policy,
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


def _require_market_v5_price_columns(conn: Any) -> None:
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
            "Incompatible market.duckdb: missing required Market v5 columns "
            f"({details}). Run bt market-cutover cutover to rebuild the "
            "Market Data Plane."
        )


def daily_ranking_forward_outcome_columns(
    horizons: Sequence[int],
) -> tuple[str, ...]:
    normalized_horizons = tuple(sorted({int(value) for value in horizons}))
    return (
        "code",
        "date",
        *(
            column
            for horizon in normalized_horizons
            for column in (
                f"forward_outcome_completion_date_{horizon}d",
                f"forward_close_return_{horizon}d_pct",
                f"forward_close_excess_return_{horizon}d_pct",
                f"forward_close_n225_excess_return_{horizon}d_pct",
                f"completion_basis_id_{horizon}d",
            )
        ),
        *(
            column
            for horizon in normalized_horizons
            for column in (
                f"forward_next_open_return_{horizon}d_pct",
                f"forward_next_open_excess_return_{horizon}d_pct",
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
    """Build one read-only CTE graph using one exact provider vintage."""

    market_clause = ""
    params: list[Any] = [
        request.signal_date,
        request.start_date,
        request.start_date or "0001-01-01",
    ]
    if request.market_codes:
        placeholders = ",".join("?" for _ in request.market_codes)
        market_clause = f" AND smd.market_code IN ({placeholders})"
        params.extend(request.market_codes)

    raw_code = _normalized_code_sql("raw.code")
    master_code = _normalized_code_sql("smd.code")
    window_code = _normalized_code_sql("provider.code")
    projection_code = _normalized_code_sql("projection.code")
    event_code = _normalized_code_sql("event.code")
    cte_sql = f"""
        event_time_signal_config AS (
            SELECT CAST(? AS VARCHAR) AS signal_date,
                   CAST(? AS VARCHAR) AS requested_start_date,
                   CAST(? AS VARCHAR) AS scan_start_date
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
                raw.open AS raw_open,
                raw.high AS raw_high,
                raw.low AS raw_low,
                raw.close AS raw_close,
                raw.volume AS raw_volume,
                raw.turnover_value,
                raw.adjustment_factor,
                raw.adjusted_open,
                raw.adjusted_high,
                raw.adjusted_low,
                raw.adjusted_close,
                raw.adjusted_volume,
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
                    coalesce(CAST(raw.turnover_value AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.adjustment_factor AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.adjusted_open AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.adjusted_high AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.adjusted_low AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.adjusted_close AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.adjusted_volume AS VARCHAR), '<null>')
                )) OVER (
                    PARTITION BY {raw_code}, raw.date
                ) AS alias_value_count
            FROM stock_data_raw AS raw
            JOIN event_time_signal_universe AS universe
              ON universe.normalized_code = {raw_code}
            CROSS JOIN event_time_signal_config AS config
            WHERE raw.date >= config.scan_start_date
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
        event_time_signal_provider_rows_ranked AS (
            SELECT
                {raw_code} AS normalized_code,
                raw.code AS source_code,
                raw.date,
                raw.open, raw.high, raw.low, raw.close, raw.volume,
                raw.turnover_value, raw.adjustment_factor,
                raw.adjusted_open, raw.adjusted_high, raw.adjusted_low,
                raw.adjusted_close, raw.adjusted_volume,
                row_number() OVER (
                    PARTITION BY {raw_code}, raw.date
                    ORDER BY CASE WHEN raw.code = {raw_code} THEN 0 ELSE 1 END,
                             length(raw.code), raw.code
                ) AS alias_rank
            FROM stock_data_raw AS raw
            JOIN event_time_signal_codes AS signal
              ON signal.normalized_code = {raw_code}
        ),
        event_time_signal_provider_rows AS (
            SELECT * FROM event_time_signal_provider_rows_ranked WHERE alias_rank = 1
        ),
        event_time_signal_provider_row_hashes AS (
            SELECT normalized_code, date, adjustment_factor,
                   from_hex(sha256(to_json(struct_pack(
                       adjusted_close := adjusted_close,
                       adjusted_high := adjusted_high,
                       adjusted_low := adjusted_low,
                       adjusted_open := adjusted_open,
                       adjusted_volume := adjusted_volume,
                       adjustment_factor := adjustment_factor,
                       close := close,
                       code := source_code,
                       date := date,
                       high := high,
                       low := low,
                       open := open,
                       turnover_value := turnover_value,
                       volume := volume
                   ))))::BIT AS row_hash
            FROM event_time_signal_provider_rows
        ),
        event_time_signal_provider_evidence AS (
            SELECT normalized_code,
                   lower(hex(bit_xor(row_hash)::BLOB)) AS calculated_fingerprint,
                   min(date) AS raw_min,
                   max(date) AS raw_max,
                   count(*) FILTER (
                       WHERE adjustment_factor IS NOT NULL
                         AND adjustment_factor != 1.0
                   ) AS expected_event_count
            FROM event_time_signal_provider_row_hashes
            GROUP BY normalized_code
        ),
        event_time_signal_window_candidates AS (
            SELECT signal.normalized_code,
                   provider.coverage_start, provider.coverage_end,
                   provider.provider_as_of, provider.source_fingerprint,
                   count(provider.code) OVER (
                       PARTITION BY signal.normalized_code
                   ) AS window_count,
                   row_number() OVER (
                       PARTITION BY signal.normalized_code
                       ORDER BY provider.code
                   ) AS window_rank
            FROM event_time_signal_codes AS signal
            LEFT JOIN stock_provider_windows AS provider
              ON {window_code} = signal.normalized_code
        ),
        event_time_signal_selected_window AS (
            SELECT candidate.*,
                   evidence.calculated_fingerprint,
                   evidence.raw_min, evidence.raw_max,
                   evidence.expected_event_count,
                   candidate.window_count = 1
                   AND regexp_full_match(
                       coalesce(candidate.source_fingerprint, ''), '[0-9a-f]{{64}}'
                   )
                   AND CAST(candidate.coverage_start AS DATE) = CAST(evidence.raw_min AS DATE)
                   AND CAST(candidate.coverage_end AS DATE) = CAST(evidence.raw_max AS DATE)
                   AND CAST(candidate.coverage_start AS DATE) <= CAST(candidate.coverage_end AS DATE)
                   AND CAST(candidate.coverage_end AS DATE) <= CAST(candidate.provider_as_of AS DATE)
                   AND CAST(candidate.provider_as_of AS DATE) >= CAST(config.signal_date AS DATE)
                   AND candidate.source_fingerprint = evidence.calculated_fingerprint
                       AS window_is_valid,
                   'provider-v1:' || candidate.normalized_code || ':'
                       || candidate.provider_as_of || ':'
                       || candidate.source_fingerprint AS provider_vintage_id
            FROM event_time_signal_window_candidates AS candidate
            LEFT JOIN event_time_signal_provider_evidence AS evidence USING (
                normalized_code
            )
            CROSS JOIN event_time_signal_config AS config
            WHERE candidate.window_rank = 1
        ),
        event_time_signal_event_evidence AS (
            SELECT provider_window.normalized_code,
                   count(event.code) AS event_count,
                   count(event.code) FILTER (
                       WHERE raw.date IS NOT NULL
                         AND raw.adjustment_factor != 1.0
                         AND event.adjustment_factor = raw.adjustment_factor
                         AND event.source_fingerprint = provider_window.source_fingerprint
                   ) AS valid_event_count
            FROM event_time_signal_selected_window AS provider_window
            LEFT JOIN stock_adjustment_events AS event
              ON {event_code} = provider_window.normalized_code
            LEFT JOIN event_time_signal_provider_rows AS raw
              ON raw.normalized_code = provider_window.normalized_code
             AND raw.date = event.date
            GROUP BY provider_window.normalized_code
        ),
        event_time_signal_projection_ranked AS (
            SELECT {projection_code} AS normalized_code,
                   projection.date, projection.open, projection.high,
                   projection.low, projection.close, projection.volume,
                   row_number() OVER (
                       PARTITION BY {projection_code}, projection.date
                       ORDER BY CASE
                           WHEN projection.code = {projection_code} THEN 0 ELSE 1
                       END, length(projection.code), projection.code
                   ) AS alias_rank
            FROM stock_data AS projection
        ),
        event_time_signal_projection AS (
            SELECT * FROM event_time_signal_projection_ranked WHERE alias_rank = 1
        ),
        event_time_signal_projection_evidence AS (
            SELECT raw.normalized_code,
                   count(*) AS raw_count,
                   count(projection.normalized_code) AS projection_count,
                   count(projection.normalized_code) FILTER (
                       WHERE projection.open = raw.adjusted_open
                         AND projection.high = raw.adjusted_high
                         AND projection.low = raw.adjusted_low
                         AND projection.close = raw.adjusted_close
                         AND projection.volume = raw.adjusted_volume
                   ) AS matching_projection_count
            FROM event_time_signal_provider_rows AS raw
            LEFT JOIN event_time_signal_projection AS projection
              ON projection.normalized_code = raw.normalized_code
             AND projection.date = raw.date
            GROUP BY raw.normalized_code
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
                    WHEN window_count = 0 THEN 'provider_window_missing'
                    WHEN window_count <> 1 THEN 'provider_window_ambiguous'
                    ELSE 'provider_window_invalid'
                END AS issue,
                provider.normalized_code,
                config.signal_date AS date
            FROM event_time_signal_selected_window AS provider
            CROSS JOIN event_time_signal_config AS config
            WHERE provider.window_count <> 1 OR NOT provider.window_is_valid
            UNION ALL
            SELECT
                'provider_event_ledger_mismatch' AS issue,
                provider.normalized_code,
                config.signal_date AS date
            FROM event_time_signal_selected_window AS provider
            JOIN event_time_signal_event_evidence AS event USING (normalized_code)
            CROSS JOIN event_time_signal_config AS config
            WHERE provider.window_is_valid
              AND (
                  event.event_count <> provider.expected_event_count
                  OR event.valid_event_count <> provider.expected_event_count
              )
            UNION ALL
            SELECT
                'provider_projection_mismatch' AS issue,
                provider.normalized_code,
                config.signal_date AS date
            FROM event_time_signal_selected_window AS provider
            JOIN event_time_signal_projection_evidence AS projection USING (
                normalized_code
            )
            CROSS JOIN event_time_signal_config AS config
            WHERE provider.window_is_valid
              AND (
                  projection.projection_count <> projection.raw_count
                  OR projection.matching_projection_count <> projection.raw_count
              )
        ),
        event_time_signal_projected AS (
            SELECT
                raw.normalized_code,
                raw.date,
                raw.adjusted_open AS open,
                raw.adjusted_high AS high,
                raw.adjusted_low AS low,
                raw.adjusted_close AS close,
                raw.adjusted_volume AS volume,
                provider.provider_vintage_id AS signal_basis_id
            FROM event_time_signal_raw AS raw
            JOIN event_time_signal_selected_window AS provider USING (
                normalized_code
            )
            WHERE provider.window_is_valid
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

    require_market_v5_compatibility(
        conn,
        required_tables=_RESEARCH_PRICE_REQUIRED_COLUMNS,
    )
    _require_market_v5_price_columns(conn)
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
    valid_bar = daily_ranking_valid_raw_bar_sql()
    raw_code = normalize_code_sql("raw.code")
    master_code = normalize_code_sql("smd.code")
    valuation_code = normalize_code_sql("dv.code")
    window_code = normalize_code_sql("provider.code")

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
                CAST(raw.adjusted_open AS DOUBLE) AS open,
                CAST(raw.adjusted_high AS DOUBLE) AS high,
                CAST(raw.adjusted_low AS DOUBLE) AS low,
                CAST(raw.adjusted_close AS DOUBLE) AS close,
                CAST(raw.adjusted_volume AS BIGINT) AS volume,
                CAST(raw.open AS DOUBLE) AS raw_open,
                CAST(raw.high AS DOUBLE) AS raw_high,
                CAST(raw.low AS DOUBLE) AS raw_low,
                CAST(raw.close AS DOUBLE) AS raw_close,
                CAST(raw.volume AS BIGINT) AS raw_volume,
                CAST(raw.turnover_value AS DOUBLE) AS turnover_value,
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
                    coalesce(CAST(raw.turnover_value AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.adjustment_factor AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.adjusted_open AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.adjusted_high AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.adjusted_low AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.adjusted_close AS VARCHAR), '<null>'),
                    coalesce(CAST(raw.adjusted_volume AS VARCHAR), '<null>')
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
            "provider-adjusted price alias conflict in stock_data_raw; "
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
    _validate_research_provider_vintage(conn, names.signal_requests, request)
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.signal_bases} AS
        SELECT
            signal.code,
            signal.date,
            'provider-v1:' || signal.code || ':' || provider.provider_as_of || ':'
                || provider.source_fingerprint AS signal_basis_id
        FROM {names.signal_requests} signal
        JOIN stock_provider_windows provider
          ON {window_code} = signal.code
         AND CAST(provider.coverage_start AS DATE) <= signal.date
         AND signal.date <= CAST(provider.provider_as_of AS DATE)
        WHERE regexp_full_match(provider.source_fingerprint, '[0-9a-f]{{64}}')
          AND (
              SELECT count(*) FROM daily_valuation dv
              WHERE {valuation_code} = signal.code
                AND CAST(dv.date AS DATE) = signal.date
                AND CAST(dv.price_basis_date AS DATE) = signal.date
          ) = 1
        """
    )
    signal_request_count = _aggregate_count(conn, names.signal_requests)
    if _aggregate_count(conn, names.signal_bases) != signal_request_count:
        raise RuntimeError(
            "provider-adjusted signal vintage is unavailable or has a missing "
            "current-basis daily_valuation price match"
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
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.basis_prices} AS
        SELECT
            request.code,
            request.basis_id,
            request.date,
            request.open,
            request.high,
            request.low,
            request.close,
            request.volume
        FROM {names.signal_projection_requests} request
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
        SELECT code, date,
               lead(date, 1) OVER (PARTITION BY code ORDER BY date) AS next_session_date,
               {lead_exprs}
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
    if diagnostics.endpoint_rows != 3 * diagnostics.completed_request_rows:
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
        SELECT DISTINCT event.code, event.date, event.adjustment_factor,
               event.source_fingerprint
        FROM {names.signal_projection_requests} request
        JOIN stock_adjustment_events event
          ON {normalize_code_sql("event.code")} = request.code
        ORDER BY event.code, event.date
        """,
    )
    completion_segment_hash = _ordered_sha256(
        conn,
        f"""
        SELECT DISTINCT event.code, event.date, event.adjustment_factor,
               event.source_fingerprint
        FROM {names.endpoint_requests} request
        JOIN stock_adjustment_events event
          ON {normalize_code_sql("event.code")} = request.code
        ORDER BY event.code, event.date
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
    next_open_outcome_columns = ", ".join(
        column
        for horizon in request.horizons
        for column in (
            f"forward_next_open_return_{horizon}d_pct",
            f"forward_next_open_excess_return_{horizon}d_pct",
        )
    )
    next_open_outcome_hash = _ordered_sha256(
        conn,
        f"SELECT code, date, {next_open_outcome_columns} "
        f"FROM {names.forward_outcomes} ORDER BY code, date",
    )
    signal_segment_count = _distinct_event_count(
        conn,
        request_relation=names.signal_projection_requests,
    )
    completion_segment_count = _distinct_event_count(
        conn,
        request_relation=names.endpoint_requests,
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
        next_open_outcome_sha256=next_open_outcome_hash,
        price_projection_sha256=hashlib.sha256(
            (
                f"{projection_hash}\n{signal_basis_hash}\n{signal_segment_hash}\n"
                f"{completion_basis_hash}\n{completion_segment_hash}\n"
                f"{forward_outcome_hash}"
            ).encode("utf-8")
        ).hexdigest(),
        signal_basis_policy=(
            "exact_provider_window_adjusted_prices_across_full_lookback"
        ),
        completion_basis_policy=(
            "exact_provider_window_applied_to_signal_and_completion_endpoints"
        ),
        next_open_integrity_policy=(
            "exact_stock_entry_session_and_topix_entry_endpoint_no_backfill"
        ),
        adjustment_formula="provider_adjusted_ohlcv_direct",
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


def _validate_research_provider_vintage(
    conn: Any,
    signal_request_relation: str,
    request: DailyRankingPriceRequest,
) -> None:
    raw_code = normalize_code_sql("raw.code")
    window_code = normalize_code_sql("provider.code")
    event_code = normalize_code_sql("event.code")
    projection_code = normalize_code_sql("projection.code")
    issue_rows = conn.execute(
        f"""
        WITH requested_codes AS (
            SELECT DISTINCT code FROM {signal_request_relation}
        ), provider_rows AS MATERIALIZED (
            SELECT {raw_code} AS code, raw.code AS physical_code,
                   raw.date, raw.open, raw.high, raw.low, raw.close, raw.volume,
                   raw.turnover_value, raw.adjustment_factor,
                   raw.adjusted_open, raw.adjusted_high, raw.adjusted_low,
                   raw.adjusted_close, raw.adjusted_volume
            FROM stock_data_raw raw
            JOIN requested_codes requested ON requested.code = {raw_code}
        ), row_hashes AS (
            SELECT code, date, adjustment_factor,
                   from_hex(sha256(to_json(struct_pack(
                       adjusted_close := adjusted_close,
                       adjusted_high := adjusted_high,
                       adjusted_low := adjusted_low,
                       adjusted_open := adjusted_open,
                       adjusted_volume := adjusted_volume,
                       adjustment_factor := adjustment_factor,
                       close := close,
                       code := physical_code,
                       date := date,
                       high := high,
                       low := low,
                       open := open,
                       turnover_value := turnover_value,
                       volume := volume
                   ))))::BIT AS row_hash
            FROM provider_rows
        ), raw_summary AS (
            SELECT code, min(date) AS raw_min, max(date) AS raw_max,
                   lower(hex(bit_xor(row_hash)::BLOB)) AS calculated_fingerprint,
                   count(*) AS raw_count,
                   count(*) FILTER (
                       WHERE adjustment_factor IS NOT NULL
                         AND adjustment_factor != 1.0
                   ) AS expected_event_count,
                   count(*) FILTER (
                       WHERE adjusted_open IS NULL OR NOT isfinite(adjusted_open)
                          OR adjusted_high IS NULL OR NOT isfinite(adjusted_high)
                          OR adjusted_low IS NULL OR NOT isfinite(adjusted_low)
                          OR adjusted_close IS NULL OR NOT isfinite(adjusted_close)
                          OR adjusted_volume IS NULL OR adjusted_volume < 0
                   ) AS invalid_adjusted_count
            FROM row_hashes
            JOIN provider_rows USING (code, date, adjustment_factor)
            GROUP BY code
        ), window_candidates AS (
            SELECT requested.code, provider.coverage_start, provider.coverage_end,
                   provider.provider_as_of, provider.source_fingerprint,
                   count(provider.code) OVER (PARTITION BY requested.code) AS window_count,
                   row_number() OVER (
                       PARTITION BY requested.code ORDER BY provider.code
                   ) AS window_rank
            FROM requested_codes requested
            LEFT JOIN stock_provider_windows provider
              ON {window_code} = requested.code
        ), selected_window AS (
            SELECT candidate.*, raw.raw_min, raw.raw_max,
                   raw.calculated_fingerprint, raw.raw_count,
                   raw.expected_event_count, raw.invalid_adjusted_count
            FROM window_candidates candidate
            LEFT JOIN raw_summary raw USING (code)
            WHERE candidate.window_rank = 1
        ), event_summary AS (
            SELECT provider_window.code,
                   count(event.code) AS event_count,
                   count(event.code) FILTER (
                       WHERE raw.date IS NOT NULL
                         AND raw.adjustment_factor != 1.0
                         AND event.adjustment_factor = raw.adjustment_factor
                         AND event.source_fingerprint = provider_window.source_fingerprint
                   ) AS valid_event_count
            FROM selected_window provider_window
            LEFT JOIN stock_adjustment_events event
              ON {event_code} = provider_window.code
            LEFT JOIN provider_rows raw
              ON raw.code = provider_window.code AND raw.date = event.date
            GROUP BY provider_window.code
        ), projection_ranked AS (
            SELECT {projection_code} AS code, projection.date,
                   projection.open, projection.high, projection.low,
                   projection.close, projection.volume,
                   row_number() OVER (
                       PARTITION BY {projection_code}, projection.date
                       ORDER BY CASE
                           WHEN projection.code = {projection_code} THEN 0 ELSE 1
                       END, length(projection.code), projection.code
                   ) AS alias_rank
            FROM stock_data projection
        ), canonical_projection AS (
            SELECT * FROM projection_ranked WHERE alias_rank = 1
        ), projection_summary AS (
            SELECT raw.code, count(*) AS raw_count,
                   count(projection.code) AS projection_count,
                   count(projection.code) FILTER (
                       WHERE projection.open = raw.adjusted_open
                         AND projection.high = raw.adjusted_high
                         AND projection.low = raw.adjusted_low
                         AND projection.close = raw.adjusted_close
                         AND projection.volume = raw.adjusted_volume
                   ) AS matching_projection_count
            FROM provider_rows raw
            LEFT JOIN canonical_projection projection
              ON projection.code = raw.code AND projection.date = raw.date
            GROUP BY raw.code
        )
        SELECT provider_window.code
        FROM selected_window provider_window
        LEFT JOIN event_summary event USING (code)
        LEFT JOIN projection_summary projection USING (code)
        WHERE provider_window.window_count <> 1
           OR provider_window.raw_count IS NULL OR provider_window.raw_count = 0
           OR NOT regexp_full_match(
               coalesce(provider_window.source_fingerprint, ''), '[0-9a-f]{{64}}'
           )
           OR CAST(provider_window.coverage_start AS DATE) IS DISTINCT FROM CAST(provider_window.raw_min AS DATE)
           OR CAST(provider_window.coverage_end AS DATE) IS DISTINCT FROM CAST(provider_window.raw_max AS DATE)
           OR CAST(provider_window.coverage_start AS DATE) > CAST(provider_window.coverage_end AS DATE)
           OR CAST(provider_window.coverage_end AS DATE) > CAST(provider_window.provider_as_of AS DATE)
           OR provider_window.source_fingerprint IS DISTINCT FROM provider_window.calculated_fingerprint
           OR provider_window.invalid_adjusted_count <> 0
           OR event.event_count <> provider_window.expected_event_count
           OR event.valid_event_count <> provider_window.expected_event_count
           OR projection.projection_count <> provider_window.raw_count
           OR projection.matching_projection_count <> provider_window.raw_count
        ORDER BY provider_window.code
        """,
    ).fetchall()
    if issue_rows:
        raise RuntimeError(
            "provider vintage lineage is missing or inconsistent for Daily Ranking; "
            f"codes={', '.join(str(row[0]) for row in issue_rows[:10])}. "
            "Run market_db_sync before retrying."
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


def _distinct_event_count(
    conn: Any,
    *,
    request_relation: str,
) -> int:
    return int(
        conn.execute(
            f"""
            SELECT count(*) FROM (
                SELECT DISTINCT event.code, event.date
                FROM {request_relation} request
                JOIN stock_adjustment_events event
                  ON {normalize_code_sql("event.code")} = request.code
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
               sessions.next_session_date,
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
    window_code = normalize_code_sql("provider.code")
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.completion_bases} AS
        SELECT
            request.code, request.signal_date, request.horizon,
            request.next_session_date, request.completion_date,
            'provider-v1:' || request.code || ':' || provider.provider_as_of || ':'
                || provider.source_fingerprint AS completion_basis_id
        FROM {names.outcome_requests} request
        JOIN stock_provider_windows provider
          ON {window_code} = request.code
         AND CAST(provider.coverage_start AS DATE) <= request.signal_date
         AND request.completion_date <= CAST(provider.provider_as_of AS DATE)
        WHERE request.completion_date IS NOT NULL
          AND regexp_full_match(provider.source_fingerprint, '[0-9a-f]{{64}}')
        """
    )
    completed_requests = _aggregate_count(
        conn, names.outcome_requests, "completion_date IS NOT NULL"
    )
    if _aggregate_count(conn, names.completion_bases) != completed_requests:
        raise RuntimeError(
            "provider-adjusted completion vintage does not cover every outcome"
        )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.endpoint_requests} AS
        SELECT code, signal_date, horizon, next_session_date, completion_date,
               completion_basis_id,
               'signal' AS endpoint, signal_date AS endpoint_date
        FROM {names.completion_bases}
        UNION ALL
        SELECT code, signal_date, horizon, next_session_date, completion_date,
               completion_basis_id,
               'entry' AS endpoint, next_session_date AS endpoint_date
        FROM {names.completion_bases}
        UNION ALL
        SELECT code, signal_date, horizon, next_session_date, completion_date,
               completion_basis_id,
               'completion' AS endpoint, completion_date AS endpoint_date
        FROM {names.completion_bases}
        """
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
        SELECT CAST(date AS DATE) AS date,
               CAST(open AS DOUBLE) AS open,
               CAST(close AS DOUBLE) AS close
        FROM topix_data
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
                CASE request.endpoint
                    WHEN 'entry' THEN raw.open
                    ELSE raw.close
                END AS projected_price
            FROM {names.endpoint_requests} request
            JOIN {names.normalized_raw} raw
              ON raw.code = request.code AND raw.date = request.endpoint_date
        ),
        pivoted AS (
            SELECT
                code, signal_date, horizon, next_session_date, completion_date,
                completion_basis_id,
                max(projected_price) FILTER (endpoint = 'signal') AS signal_close,
                max(projected_price) FILTER (endpoint = 'entry') AS next_open,
                max(projected_price) FILTER (endpoint = 'completion') AS completion_close
            FROM endpoints
            GROUP BY code, signal_date, horizon, next_session_date, completion_date,
                     completion_basis_id
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
            END AS forward_close_n225_excess_return_pct,
            CASE WHEN p.next_open > 0 AND p.completion_close > 0
                THEN (p.completion_close / p.next_open - 1.0) * 100.0
            END AS forward_next_open_return_pct,
            CASE WHEN p.next_open > 0 AND p.completion_close > 0
                       AND topix_entry.open > 0 AND topix_completion.close > 0
                THEN (p.completion_close / p.next_open - 1.0) * 100.0
                   - (topix_completion.close / topix_entry.open - 1.0) * 100.0
            END AS forward_next_open_excess_return_pct
        FROM pivoted p
        LEFT JOIN {names.topix_benchmark} topix_signal
          ON topix_signal.date = p.signal_date
        LEFT JOIN {names.topix_benchmark} topix_completion
          ON topix_completion.date = p.completion_date
        LEFT JOIN {names.topix_benchmark} topix_entry
          ON topix_entry.date = p.next_session_date
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
    next_open_outcome_columns = ",\n".join(
        expression
        for horizon in request.horizons
        for expression in (
            f"max(forward_next_open_return_pct) FILTER (horizon = {horizon}) AS forward_next_open_return_{horizon}d_pct",
            f"max(forward_next_open_excess_return_pct) FILTER (horizon = {horizon}) AS forward_next_open_excess_return_{horizon}d_pct",
        )
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {names.forward_outcomes} AS
        SELECT code, signal_date AS date, {outcome_columns},
               {next_open_outcome_columns}
        FROM {names.projected_outcomes_long}
        GROUP BY code, signal_date
        """
    )
