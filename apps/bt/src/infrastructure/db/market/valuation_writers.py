"""Current-provider-basis adjusted fundamentals writer helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import math
from typing import Any

import pandas as pd

from src.infrastructure.db.market.market_mutations import MarketMutationStats
from src.infrastructure.db.market.market_schema import (
    DAILY_VALUATION_COLUMNS as _DAILY_VALUATION_COLUMNS,
    DAILY_VALUATION_TABLE_DDL as _DAILY_VALUATION_TABLE_DDL,
    STATEMENT_METRICS_ADJUSTED_COLUMNS as _STATEMENT_METRICS_ADJUSTED_COLUMNS,
)
from src.infrastructure.db.market.query_helpers import (
    normalize_stock_code,
    stock_code_query_candidates,
)


@dataclass(frozen=True)
class CurrentBasisFundamentalsSource:
    """One code's raw disclosures, event ledger, and current provider basis."""

    code: str
    fundamentals_adjustment_basis_date: str
    statement_rows: tuple[dict[str, Any], ...]
    adjustment_events: tuple[dict[str, Any], ...]
    fingerprint: str


@dataclass(frozen=True)
class AdjustedRelationPublishResult:
    stats: MarketMutationStats
    final_count: int


@dataclass(frozen=True, slots=True)
class DailyValuationMaterializationResult:
    """Semantic mutations and coverage after a valuation materialization."""

    stats: MarketMutationStats
    final_count: int
    latest_date: str | None


_DAILY_VALUATION_DESIRED = "__desired_daily_valuation"
_DAILY_VALUATION_CODES = "__daily_valuation_codes"
_DAILY_VALUATION_DATES = "__daily_valuation_dates"
_DAILY_VALUATION_KEY_COLUMNS = ("code", "date")
_DAILY_VALUATION_SEMANTIC_COLUMNS = tuple(
    column
    for column in _DAILY_VALUATION_COLUMNS
    if column not in {*_DAILY_VALUATION_KEY_COLUMNS, "created_at"}
)


def materialize_daily_valuation(
    conn: Any,
    lock: Any,
    *,
    full_rebuild: bool = False,
    rebuild_codes: frozenset[str] = frozenset(),
    changed_dates: frozenset[str] = frozenset(),
) -> DailyValuationMaterializationResult:
    """Reconcile the v5 current-provider-basis daily valuation table."""
    normalized_codes = frozenset(normalize_stock_code(code) for code in rebuild_codes)
    dates = frozenset(str(date) for date in changed_dates)
    with lock:
        relation_type = _daily_valuation_relation_type(conn)
        promote_view = relation_type == "VIEW"
        effective_full = full_rebuild or promote_view or relation_type is None
        if not effective_full and not normalized_codes and not dates:
            return _daily_valuation_result(conn, MarketMutationStats.empty())

        _register_scope_relation(conn, _DAILY_VALUATION_CODES, "code", normalized_codes)
        _register_scope_relation(conn, _DAILY_VALUATION_DATES, "date", dates)
        try:
            _materialize_desired_daily_valuation(
                conn,
                full_rebuild=effective_full,
            )
            if promote_view or relation_type is None:
                stats = MarketMutationStats(
                    input=_count_relation(conn, _DAILY_VALUATION_DESIRED),
                    inserted=_count_relation(conn, _DAILY_VALUATION_DESIRED),
                    updated=0,
                    unchanged=0,
                    deleted=0,
                )
                _replace_daily_valuation_relation(conn, relation_type)
            else:
                stats = _classify_daily_valuation_delta(
                    conn,
                    full_rebuild=effective_full,
                )
                if stats.mutated_rows:
                    _apply_daily_valuation_delta(
                        conn,
                        full_rebuild=effective_full,
                    )
            return _daily_valuation_result(conn, stats)
        finally:
            conn.execute(f"DROP TABLE IF EXISTS {_DAILY_VALUATION_DESIRED}")
            conn.execute(f"DROP TABLE IF EXISTS {_DAILY_VALUATION_CODES}")
            conn.execute(f"DROP TABLE IF EXISTS {_DAILY_VALUATION_DATES}")


def _register_scope_relation(
    conn: Any,
    relation: str,
    column: str,
    values: frozenset[str],
) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {relation}")
    conn.execute(f"CREATE TEMP TABLE {relation} ({column} TEXT PRIMARY KEY)")
    if values:
        conn.executemany(
            f"INSERT INTO {relation} VALUES (?)",
            [(value,) for value in sorted(values)],
        )


def _materialize_desired_daily_valuation(
    conn: Any,
    *,
    full_rebuild: bool,
) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {_DAILY_VALUATION_DESIRED}")
    scope = (
        "TRUE"
        if full_rebuild
        else f"""
        normalized_code IN (SELECT code FROM {_DAILY_VALUATION_CODES})
        OR (
            date IN (SELECT date FROM {_DAILY_VALUATION_DATES})
            AND normalized_code NOT IN (
                SELECT code FROM {_DAILY_VALUATION_CODES}
            )
        )
        """
    )
    created_at = datetime.now(UTC).isoformat()
    conn.execute(
        f"""
        CREATE TEMP TABLE {_DAILY_VALUATION_DESIRED} AS
        WITH statement_source AS (
            SELECT * EXCLUDE (alias_rank)
            FROM (
                SELECT
                    statements.*,
                    CASE
                        WHEN length(code) = 5 AND right(code, 1) = '0'
                        THEN left(code, 4)
                        ELSE code
                    END AS normalized_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            CASE
                                WHEN length(code) = 5 AND right(code, 1) = '0'
                                THEN left(code, 4)
                                ELSE code
                            END,
                            statement_id
                        ORDER BY
                            CASE WHEN length(code) = 4 THEN 0 ELSE 1 END,
                            code
                    ) AS alias_rank
                FROM statements
            )
            WHERE alias_rank = 1
        ),
        metric_source AS (
            SELECT
                metrics.*,
                source.type_of_document,
                source.sales,
                source.forecast_sales,
                source.next_year_forecast_sales,
                source.operating_profit,
                source.forecast_operating_profit,
                source.next_year_forecast_operating_profit,
                contains(
                    coalesce(source.type_of_document, ''),
                    'EarnForecastRevision'
                ) AS is_revision,
                (
                    upper(coalesce(metrics.period_type, '')) = 'FY'
                    AND NOT contains(
                        coalesce(source.type_of_document, ''),
                        'EarnForecastRevision'
                    )
                    AND (
                        metrics.adjusted_eps > 0
                        OR metrics.adjusted_bps > 0
                        OR source.sales > 0
                    )
                ) AS is_anchor
            FROM statement_metrics_adjusted AS metrics
            JOIN current_basis_fundamentals_state AS state USING (code)
            JOIN stock_provider_windows AS provider USING (code)
            LEFT JOIN statement_source AS source
              ON source.normalized_code = metrics.code
             AND source.statement_id = metrics.statement_id
            WHERE metrics.fundamentals_adjustment_basis_date
                      = state.fundamentals_adjustment_basis_date
              AND metrics.source_fingerprint = state.source_fingerprint
              AND state.fundamentals_adjustment_basis_date <= provider.coverage_end
        ),
        events AS (
            SELECT DISTINCT code, disclosed_at
            FROM metric_source
        ),
        actual_eps_stream AS (
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY code, disclosed_at
                    ORDER BY statement_id DESC
                ) AS rn
                FROM metric_source
                WHERE upper(coalesce(period_type, '')) = 'FY'
                  AND adjusted_eps IS NOT NULL
            ) WHERE rn = 1
        ),
        bps_stream AS (
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY code, disclosed_at
                    ORDER BY statement_id DESC
                ) AS rn
                FROM metric_source
                WHERE upper(coalesce(period_type, '')) = 'FY'
                  AND adjusted_bps IS NOT NULL
            ) WHERE rn = 1
        ),
        anchor_stream AS (
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY code, disclosed_at
                    ORDER BY statement_id DESC
                ) AS rn
                FROM metric_source WHERE is_anchor
            ) WHERE rn = 1
        ),
        share_stream AS (
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY code, disclosed_at
                    ORDER BY statement_id DESC
                ) AS rn
                FROM metric_source
                WHERE adjusted_shares_outstanding IS NOT NULL
            ) WHERE rn = 1
        ),
        forecast_stream AS (
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY code, disclosed_at
                    ORDER BY statement_id DESC
                ) AS rn
                FROM metric_source
                WHERE adjusted_forecast_eps IS NOT NULL
                  AND (
                      upper(coalesce(period_type, '')) <> 'FY'
                      OR is_anchor
                      OR is_revision
                  )
            ) WHERE rn = 1
        ),
        actual_sales_stream AS (
            SELECT * FROM anchor_stream WHERE sales IS NOT NULL
        ),
        actual_op_stream AS (
            SELECT * FROM anchor_stream WHERE operating_profit IS NOT NULL
        ),
        forward_stream AS (
            SELECT
                *,
                CASE
                    WHEN is_revision THEN COALESCE(
                        forecast_sales, next_year_forecast_sales
                    )
                    WHEN upper(coalesce(period_type, '')) = 'FY'
                    THEN COALESCE(next_year_forecast_sales, forecast_sales)
                    ELSE forecast_sales
                END AS selected_forward_sales,
                CASE
                    WHEN is_revision THEN COALESCE(
                        forecast_operating_profit,
                        next_year_forecast_operating_profit
                    )
                    WHEN upper(coalesce(period_type, '')) = 'FY'
                    THEN COALESCE(
                        next_year_forecast_operating_profit,
                        forecast_operating_profit
                    )
                    ELSE forecast_operating_profit
                END AS selected_forward_op,
                CASE
                    WHEN is_revision
                      OR upper(coalesce(period_type, '')) <> 'FY'
                    THEN 'revised'
                    ELSE 'fy'
                END AS forecast_source
            FROM metric_source
            WHERE is_revision
               OR upper(coalesce(period_type, '')) <> 'FY'
               OR is_anchor
        ),
        forward_sales_stream AS (
            SELECT * FROM forward_stream
            WHERE selected_forward_sales IS NOT NULL
        ),
        forward_op_stream AS (
            SELECT * FROM forward_stream
            WHERE selected_forward_op IS NOT NULL
        ),
        event_state AS (
            SELECT
                events.code,
                events.disclosed_at,
                actual.adjusted_eps,
                actual.disclosed_date AS eps_disclosed_date,
                actual.statement_id,
                actual.disclosed_at AS statement_disclosed_at,
                bps.adjusted_bps,
                bps.disclosed_date AS bps_disclosed_date,
                anchor.disclosed_date AS anchor_disclosed_date,
                shares.adjusted_shares_outstanding,
                shares.adjusted_treasury_shares,
                actual_sales.sales,
                actual_op.operating_profit,
                actual_op.disclosed_date AS actual_op_disclosed_date,
                forecast.adjusted_forecast_eps,
                forecast.disclosed_date AS forecast_disclosed_date,
                CASE
                    WHEN forecast.is_revision
                      OR upper(coalesce(forecast.period_type, '')) <> 'FY'
                    THEN 'revised'
                    ELSE 'fy'
                END AS forecast_source,
                forward_sales.selected_forward_sales,
                forward_sales.disclosed_date AS forward_sales_disclosed_date,
                forward_sales.forecast_source AS forward_sales_source,
                forward_op.selected_forward_op,
                forward_op.disclosed_date AS forward_op_disclosed_date,
                forward_op.forecast_source AS forward_op_source,
                state.fundamentals_adjustment_basis_date,
                state.source_fingerprint
            FROM events
            JOIN current_basis_fundamentals_state AS state USING (code)
            ASOF LEFT JOIN actual_eps_stream AS actual
              ON events.code = actual.code
             AND events.disclosed_at >= actual.disclosed_at
            ASOF LEFT JOIN bps_stream AS bps
              ON events.code = bps.code
             AND events.disclosed_at >= bps.disclosed_at
            ASOF LEFT JOIN anchor_stream AS anchor
              ON events.code = anchor.code
             AND events.disclosed_at >= anchor.disclosed_at
            ASOF LEFT JOIN share_stream AS shares
              ON events.code = shares.code
             AND events.disclosed_at >= shares.disclosed_at
            ASOF LEFT JOIN actual_sales_stream AS actual_sales
              ON events.code = actual_sales.code
             AND events.disclosed_at >= actual_sales.disclosed_at
            ASOF LEFT JOIN actual_op_stream AS actual_op
              ON events.code = actual_op.code
             AND events.disclosed_at >= actual_op.disclosed_at
            ASOF LEFT JOIN forecast_stream AS forecast
              ON events.code = forecast.code
             AND events.disclosed_at >= forecast.disclosed_at
            ASOF LEFT JOIN forward_sales_stream AS forward_sales
              ON events.code = forward_sales.code
             AND events.disclosed_at >= forward_sales.disclosed_at
            ASOF LEFT JOIN forward_op_stream AS forward_op
              ON events.code = forward_op.code
             AND events.disclosed_at >= forward_op.disclosed_at
        ),
        raw_prices AS (
            SELECT
                CASE
                    WHEN length(price.code) = 5 AND right(price.code, 1) = '0'
                    THEN left(price.code, 4)
                    ELSE price.code
                END AS normalized_code,
                price.date,
                price.close,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        CASE
                            WHEN length(price.code) = 5
                             AND right(price.code, 1) = '0'
                            THEN left(price.code, 4)
                            ELSE price.code
                        END,
                        price.date
                    ORDER BY
                        CASE WHEN length(price.code) = 4 THEN 0 ELSE 1 END,
                        price.code
                ) AS rn
            FROM stock_data AS price
        ),
        prices AS (
            SELECT
                normalized_code AS code,
                date,
                close
            FROM raw_prices
            WHERE rn = 1 AND ({scope})
        ),
        valued AS (
            SELECT
                prices.*,
                event_state.* EXCLUDE (code, disclosed_at),
                provider.coverage_start,
                provider.coverage_end,
                CASE
                    WHEN event_state.forecast_source = 'fy'
                     AND event_state.forecast_disclosed_date
                         = event_state.anchor_disclosed_date
                    THEN TRUE
                    WHEN event_state.forecast_source = 'revised'
                     AND event_state.forecast_disclosed_date
                         > event_state.anchor_disclosed_date
                    THEN TRUE
                    ELSE FALSE
                END AS forecast_valid,
                CASE
                    WHEN event_state.forward_sales_source = 'fy'
                     AND event_state.forward_sales_disclosed_date
                         = event_state.anchor_disclosed_date
                    THEN TRUE
                    WHEN event_state.forward_sales_source = 'revised'
                     AND event_state.forward_sales_disclosed_date
                         > event_state.anchor_disclosed_date
                    THEN TRUE
                    ELSE FALSE
                END AS forward_sales_valid,
                CASE
                    WHEN event_state.forward_op_source = 'fy'
                     AND event_state.forward_op_disclosed_date
                         = event_state.anchor_disclosed_date
                    THEN TRUE
                    WHEN event_state.forward_op_source = 'revised'
                     AND event_state.forward_op_disclosed_date
                         > event_state.anchor_disclosed_date
                    THEN TRUE
                    ELSE FALSE
                END AS forward_op_valid
            FROM prices
            JOIN stock_provider_windows AS provider
              ON provider.code = prices.code
             AND prices.date BETWEEN provider.coverage_start AND provider.coverage_end
            JOIN current_basis_fundamentals_state AS price_state
              ON price_state.code = provider.code
             AND price_state.fundamentals_adjustment_basis_date
                    <= provider.coverage_end
            ASOF LEFT JOIN event_state
              ON prices.code = event_state.code
             AND prices.date || 'T23:59:59+09:00'
                 >= event_state.disclosed_at
        )
        SELECT
            code,
            date,
            date AS price_basis_date,
            close,
            adjusted_eps AS eps,
            adjusted_bps AS bps,
            CASE WHEN forecast_valid THEN adjusted_forecast_eps END AS forward_eps,
            close / NULLIF(adjusted_eps, 0) AS per,
            CASE WHEN forecast_valid
                 THEN close / NULLIF(adjusted_forecast_eps, 0) END AS forward_per,
            sales,
            CASE WHEN forward_sales_valid
                 THEN selected_forward_sales END AS forward_sales,
            close * adjusted_shares_outstanding / NULLIF(sales, 0) AS psr,
            CASE WHEN forward_sales_valid THEN
                close * adjusted_shares_outstanding
                    / NULLIF(selected_forward_sales, 0)
            END AS forward_psr,
            CASE
                WHEN actual_op_disclosed_date = anchor_disclosed_date
                THEN close * adjusted_shares_outstanding
                    / NULLIF(operating_profit, 0)
            END AS p_op,
            CASE WHEN forward_op_valid THEN
                close * adjusted_shares_outstanding
                    / NULLIF(selected_forward_op, 0)
            END AS forward_p_op,
            close / NULLIF(adjusted_bps, 0) AS pbr,
            close * adjusted_shares_outstanding AS market_cap,
            CASE
                WHEN adjusted_shares_outstanding IS NULL THEN NULL
                ELSE close * GREATEST(
                    adjusted_shares_outstanding
                        - COALESCE(adjusted_treasury_shares, 0),
                    0
                )
            END AS free_float_market_cap,
            greatest(eps_disclosed_date, bps_disclosed_date)
                AS statement_disclosed_date,
            CASE WHEN forecast_valid THEN forecast_disclosed_date END
                AS forward_eps_disclosed_date,
            CASE WHEN forecast_valid THEN forecast_source END
                AS forward_eps_source,
            CASE WHEN forward_sales_valid THEN forward_sales_disclosed_date END
                AS forward_sales_disclosed_date,
            CASE WHEN forward_sales_valid THEN forward_sales_source END
                AS forward_sales_source,
            statement_id,
            statement_disclosed_at,
            fundamentals_adjustment_basis_date,
            source_fingerprint,
            ? AS created_at
        FROM valued
        """,
        [created_at],
    )


def _daily_valuation_relation_type(conn: Any) -> str | None:
    row = conn.execute(
        """
        SELECT table_type
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = 'daily_valuation'
        """
    ).fetchone()
    return str(row[0]).upper() if row else None


def _daily_valuation_table_ddl() -> str:
    return _DAILY_VALUATION_TABLE_DDL


def _replace_daily_valuation_relation(conn: Any, relation_type: str | None) -> None:
    columns = ", ".join(_DAILY_VALUATION_COLUMNS)
    conn.execute("BEGIN TRANSACTION")
    try:
        if relation_type == "VIEW":
            conn.execute("DROP VIEW daily_valuation")
        elif relation_type is not None:
            conn.execute("DROP TABLE daily_valuation")
        conn.execute(_daily_valuation_table_ddl())
        conn.execute(
            f"""
            INSERT INTO daily_valuation ({columns})
            SELECT {columns} FROM {_DAILY_VALUATION_DESIRED}
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_daily_valuation_date_code
            ON daily_valuation(date, code)
            """
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def _scope_predicate(alias: str, *, full_rebuild: bool) -> str:
    if full_rebuild:
        return "TRUE"
    return f"""
        {alias}.code IN (SELECT code FROM {_DAILY_VALUATION_CODES})
        OR (
            {alias}.date IN (SELECT date FROM {_DAILY_VALUATION_DATES})
            AND {alias}.code NOT IN (
                SELECT code FROM {_DAILY_VALUATION_CODES}
            )
        )
    """


def _classify_daily_valuation_delta(
    conn: Any,
    *,
    full_rebuild: bool,
) -> MarketMutationStats:
    distinct = " OR ".join(
        f"target.{column} IS DISTINCT FROM desired.{column}"
        for column in _DAILY_VALUATION_SEMANTIC_COLUMNS
    )
    scope = _scope_predicate("stale", full_rebuild=full_rebuild)
    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS input,
            COUNT(*) FILTER (WHERE target.code IS NULL) AS inserted,
            COUNT(*) FILTER (
                WHERE target.code IS NOT NULL AND ({distinct})
            ) AS updated,
            COUNT(*) FILTER (
                WHERE target.code IS NOT NULL AND NOT ({distinct})
            ) AS unchanged,
            (
                SELECT COUNT(*)
                FROM daily_valuation AS stale
                WHERE ({scope})
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {_DAILY_VALUATION_DESIRED} AS desired_stale
                      WHERE desired_stale.code = stale.code
                        AND desired_stale.date = stale.date
                  )
            ) AS deleted
        FROM {_DAILY_VALUATION_DESIRED} AS desired
        LEFT JOIN daily_valuation AS target USING (code, date)
        """
    ).fetchone()
    if row is None:
        return MarketMutationStats.empty()
    return MarketMutationStats(*(int(value or 0) for value in row))


def _apply_daily_valuation_delta(
    conn: Any,
    *,
    full_rebuild: bool,
) -> None:
    scope = _scope_predicate("target", full_rebuild=full_rebuild)
    distinct = " OR ".join(
        f"target.{column} IS DISTINCT FROM desired.{column}"
        for column in _DAILY_VALUATION_SEMANTIC_COLUMNS
    )
    assignments = ", ".join(
        [
            *(
                f"{column} = desired.{column}"
                for column in _DAILY_VALUATION_SEMANTIC_COLUMNS
            ),
            "created_at = desired.created_at",
        ]
    )
    columns = ", ".join(_DAILY_VALUATION_COLUMNS)
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            f"""
            DELETE FROM daily_valuation AS target
            WHERE ({scope})
              AND NOT EXISTS (
                  SELECT 1 FROM {_DAILY_VALUATION_DESIRED} AS desired
                  WHERE desired.code = target.code
                    AND desired.date = target.date
              )
            """
        )
        conn.execute(
            f"""
            UPDATE daily_valuation AS target
            SET {assignments}
            FROM {_DAILY_VALUATION_DESIRED} AS desired
            WHERE target.code = desired.code
              AND target.date = desired.date
              AND ({distinct})
            """
        )
        conn.execute(
            f"""
            INSERT INTO daily_valuation ({columns})
            SELECT {columns}
            FROM {_DAILY_VALUATION_DESIRED} AS desired
            WHERE NOT EXISTS (
                SELECT 1 FROM daily_valuation AS target
                WHERE target.code = desired.code
                  AND target.date = desired.date
            )
            """
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def _count_relation(conn: Any, relation: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0])


def _daily_valuation_result(
    conn: Any,
    stats: MarketMutationStats,
) -> DailyValuationMaterializationResult:
    row = conn.execute(
        "SELECT COUNT(*), MAX(date) FROM daily_valuation"
    ).fetchone()
    return DailyValuationMaterializationResult(
        stats=stats,
        final_count=int(row[0] or 0) if row else 0,
        latest_date=str(row[1]) if row and row[1] is not None else None,
    )


def load_current_basis_fundamentals_source(
    conn: Any,
    lock: Any,
    code: str,
) -> CurrentBasisFundamentalsSource | None:
    """Load only the requested code's current-basis fundamentals sources."""
    with lock:
        return _load_current_basis_fundamentals_source_unlocked(conn, code)


def publish_current_basis_statement_metrics(
    conn: Any,
    lock: Any,
    code: str,
    rows: Sequence[dict[str, Any]],
    *,
    expected_source_fingerprint: str,
) -> AdjustedRelationPublishResult:
    """Atomically reconcile current-basis metrics for exactly one code."""
    normalized = normalize_stock_code(code)
    desired_rows = [
        {column: row.get(column) for column in _STATEMENT_METRICS_ADJUSTED_COLUMNS}
        for row in rows
    ]
    now_iso = datetime.now().astimezone().isoformat()
    for row in desired_rows:
        row["code"] = normalized
        row["created_at"] = row.get("created_at") or now_iso

    relation = "__current_basis_statement_metrics"
    registered = False
    transaction_started = False
    with lock:
        existing = _fetch_dict_rows(
            conn,
            "SELECT * FROM statement_metrics_adjusted "
            "WHERE code = ? ORDER BY statement_id",
            [normalized],
        )
        stats = _semantic_stats(
            desired_rows,
            existing,
            key_columns=("code", "statement_id"),
            compare_columns=tuple(
                column
                for column in _STATEMENT_METRICS_ADJUSTED_COLUMNS
                if column != "created_at"
            ),
        )
        try:
            if desired_rows:
                conn.register(
                    relation,
                    pd.DataFrame.from_records(
                        desired_rows,
                        columns=_STATEMENT_METRICS_ADJUSTED_COLUMNS,
                    ),
                )
                registered = True
            conn.execute("BEGIN TRANSACTION")
            transaction_started = True
            current_source = _load_current_basis_fundamentals_source_unlocked(
                conn, normalized
            )
            if (
                current_source is None
                or current_source.fingerprint != expected_source_fingerprint
            ):
                raise RuntimeError(
                    "current-basis fundamentals sources drifted before publish "
                    f"for {normalized}"
                )

            if desired_rows:
                conn.execute(
                    f"""
                    DELETE FROM statement_metrics_adjusted AS target
                    WHERE target.code = ?
                      AND NOT EXISTS (
                          SELECT 1 FROM {relation} AS desired
                          WHERE desired.code = target.code
                            AND desired.statement_id = target.statement_id
                      )
                    """,
                    [normalized],
                )
                update_columns = tuple(
                    column
                    for column in _STATEMENT_METRICS_ADJUSTED_COLUMNS
                    if column not in {"code", "statement_id"}
                )
                semantic_columns = tuple(
                    column for column in update_columns if column != "created_at"
                )
                conn.execute(
                    f"""
                    INSERT INTO statement_metrics_adjusted
                        ({", ".join(_STATEMENT_METRICS_ADJUSTED_COLUMNS)})
                    SELECT {", ".join(_STATEMENT_METRICS_ADJUSTED_COLUMNS)}
                    FROM {relation}
                    ON CONFLICT (code, statement_id) DO UPDATE SET
                        {", ".join(f"{column} = excluded.{column}" for column in update_columns)}
                    WHERE {" OR ".join(f"statement_metrics_adjusted.{column} IS DISTINCT FROM excluded.{column}" for column in semantic_columns)}
                    """
                )
            else:
                conn.execute(
                    "DELETE FROM statement_metrics_adjusted WHERE code = ?",
                    [normalized],
                )
            final_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM statement_metrics_adjusted WHERE code = ?",
                    [normalized],
                ).fetchone()[0]
            )
            conn.execute(
                """
                INSERT INTO current_basis_fundamentals_state (
                    code, fundamentals_adjustment_basis_date,
                    source_fingerprint, statement_count, materialized_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (code) DO UPDATE SET
                    fundamentals_adjustment_basis_date =
                        excluded.fundamentals_adjustment_basis_date,
                    source_fingerprint = excluded.source_fingerprint,
                    statement_count = excluded.statement_count,
                    materialized_at = excluded.materialized_at
                """,
                [
                    normalized,
                    current_source.fundamentals_adjustment_basis_date,
                    expected_source_fingerprint,
                    final_count,
                    now_iso,
                ],
            )
            pending_codes = stock_code_query_candidates([normalized])
            conn.execute(
                "DELETE FROM current_basis_recompute_pending WHERE code IN ("
                + ", ".join("?" for _ in pending_codes)
                + ")",
                pending_codes,
            )
            conn.execute("COMMIT")
            transaction_started = False
        except Exception:
            if transaction_started:
                conn.execute("ROLLBACK")
            raise
        finally:
            if registered:
                conn.unregister(relation)
    return AdjustedRelationPublishResult(stats=stats, final_count=final_count)


def _load_current_basis_fundamentals_source_unlocked(
    conn: Any,
    code: str,
) -> CurrentBasisFundamentalsSource | None:
    normalized = normalize_stock_code(code)
    query_codes = stock_code_query_candidates([normalized])
    placeholders = ", ".join("?" for _ in query_codes)
    window_rows = _fetch_dict_rows(
        conn,
        f"""
        SELECT code, coverage_start, coverage_end, provider_as_of, source_fingerprint
        FROM stock_provider_windows
        WHERE code IN ({placeholders})
        ORDER BY CASE WHEN code = ? THEN 0 ELSE 1 END, coverage_end DESC
        LIMIT 1
        """,
        [*query_codes, normalized],
    )
    if not window_rows:
        return None
    window = window_rows[0]
    basis_date = str(window["coverage_end"])

    statement_candidates = _fetch_dict_rows(
        conn,
        f"""
        SELECT * FROM statements
        WHERE code IN ({placeholders})
        ORDER BY CASE WHEN code = ? THEN 0 ELSE 1 END,
                 disclosed_at, statement_id
        """,
        [*query_codes, normalized],
    )
    statements_by_id: dict[str, dict[str, Any]] = {}
    for row in statement_candidates:
        statements_by_id.setdefault(
            str(row["statement_id"]), {**row, "code": normalized}
        )
    statement_rows = tuple(
        sorted(
            statements_by_id.values(),
            key=lambda row: (str(row["disclosed_at"]), str(row["statement_id"])),
        )
    )

    event_candidates = _fetch_dict_rows(
        conn,
        f"""
        SELECT code, date, adjustment_factor, source_fingerprint
        FROM stock_adjustment_events
        WHERE code IN ({placeholders}) AND date <= ?
        ORDER BY CASE WHEN code = ? THEN 0 ELSE 1 END, date
        """,
        [*query_codes, basis_date, normalized],
    )
    events_by_date: dict[str, dict[str, Any]] = {}
    for row in event_candidates:
        events_by_date.setdefault(str(row["date"]), {**row, "code": normalized})
    adjustment_events = tuple(
        sorted(events_by_date.values(), key=lambda row: str(row["date"]))
    )

    event_fingerprint_rows = tuple(
        {
            "code": row["code"],
            "date": row["date"],
            "adjustment_factor": row["adjustment_factor"],
        }
        for row in adjustment_events
    )
    payload = (
        _canonical_dict_rows(
            statement_rows,
            tuple(sorted({key for row in statement_rows for key in row})),
        ),
        _canonical_dict_rows(
            event_fingerprint_rows,
            ("adjustment_factor", "code", "date"),
        ),
    )
    fingerprint = hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode()
    ).hexdigest()
    return CurrentBasisFundamentalsSource(
        code=normalized,
        fundamentals_adjustment_basis_date=basis_date,
        statement_rows=statement_rows,
        adjustment_events=adjustment_events,
        fingerprint=fingerprint,
    )


def _semantic_stats(
    desired_rows: Sequence[dict[str, Any]],
    existing_rows: Sequence[dict[str, Any]],
    *,
    key_columns: Sequence[str],
    compare_columns: Sequence[str],
) -> MarketMutationStats:
    desired = {
        tuple(row.get(column) for column in key_columns): row for row in desired_rows
    }
    existing = {
        tuple(row.get(column) for column in key_columns): row for row in existing_rows
    }
    inserted = sum(key not in existing for key in desired)
    updated = sum(
        key in existing
        and any(
            _values_distinct(existing[key].get(column), row.get(column))
            for column in compare_columns
        )
        for key, row in desired.items()
    )
    unchanged = len(desired) - inserted - updated
    deleted = sum(key not in desired for key in existing)
    return MarketMutationStats(len(desired), inserted, updated, unchanged, deleted)


def _values_distinct(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is not right
    if isinstance(left, float) and isinstance(right, float):
        if math.isnan(left) and math.isnan(right):
            return False
    return left != right


def _canonical_dict_rows(
    rows: Iterable[Mapping[str, Any]],
    columns: Sequence[str],
) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        sorted(
            (
                tuple(_fingerprint_scalar(row.get(column)) for column in columns)
                for row in rows
            ),
            key=repr,
        )
    )


def _fingerprint_scalar(value: Any) -> Any:
    if value is None:
        return ["null"]
    if isinstance(value, float):
        if math.isnan(value):
            return ["nan"]
        if math.isinf(value):
            return ["inf", 1 if value > 0 else -1]
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _fetch_dict_rows(
    conn: Any,
    query: str,
    params: Sequence[Any],
) -> tuple[dict[str, Any], ...]:
    cursor = conn.execute(query, params)
    columns = tuple(str(item[0]) for item in cursor.description)
    return tuple(
        dict(zip(columns, row, strict=True)) for row in cursor.fetchall()
    )
