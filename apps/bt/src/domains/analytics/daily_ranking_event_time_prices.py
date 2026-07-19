"""Event-time signal-price SQL for production Daily Ranking."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


EVENT_TIME_SIGNAL_RELATION = "event_time_signal_prices"
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
                    coalesce(CAST(raw.volume AS VARCHAR), '<null>')
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
    )


def _normalized_code_sql(column_ref: str) -> str:
    return (
        "CASE "
        f"WHEN length({column_ref}) IN (5, 6) AND right({column_ref}, 1) = '0' "
        f"THEN left({column_ref}, length({column_ref}) - 1) "
        f"ELSE {column_ref} "
        "END"
    )
