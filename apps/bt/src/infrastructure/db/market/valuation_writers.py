"""Adjusted fundamentals and daily valuation writer helpers."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from src.infrastructure.db.market.market_schema import (
    DAILY_VALUATION_COLUMNS as _DAILY_VALUATION_COLUMNS,
    STATEMENT_METRICS_ADJUSTED_COLUMNS as _STATEMENT_METRICS_ADJUSTED_COLUMNS,
    STATEMENT_METRICS_ADJUSTED_RELATION as _STATEMENT_METRICS_ADJUSTED_RELATION,
)
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.domains.fundamentals.adjustment_basis import StockAdjustmentLineage
from src.infrastructure.db.market import adjustment_basis_writers


_ATOMIC_BASIS_RELATION = "__adjusted_publish_bases"
_ATOMIC_SEGMENT_RELATION = "__adjusted_publish_segments"
_ATOMIC_STATEMENT_RELATION = "__adjusted_publish_statements"
_ATOMIC_VALUATION_RELATION = "__adjusted_publish_valuations"


@dataclass(frozen=True)
class AdjustedBasisMaterializationPlan:
    """Complete, validated replacement payload for affected adjustment bases."""

    lineages: tuple[StockAdjustmentLineage, ...]
    adjusted_statement_rows: tuple[dict[str, Any], ...]
    daily_valuation_rows: tuple[dict[str, Any], ...]
    replace_basis_ids: Mapping[str, Sequence[str]]
    orphan_basis_ids: Mapping[str, Sequence[str]]


@dataclass(frozen=True)
class AdjustedBasisPublishResult:
    basis_rows: int
    segment_rows: int
    statement_rows: int
    daily_valuation_rows: int


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


def publish_adjusted_basis_materialization(
    conn: Any,
    lock: Any,
    plan: AdjustedBasisMaterializationPlan,
) -> AdjustedBasisPublishResult:
    """Atomically replace lineage and materialized rows for explicit bases."""
    adjustment_basis_writers._validate_lineages(plan.lineages)
    now_iso = datetime.now().isoformat()  # noqa: DTZ005
    basis_rows = [
        {
            "code": normalize_stock_code(basis.code),
            "basis_id": basis.basis_id,
            "valid_from": basis.valid_from,
            "valid_to_exclusive": basis.valid_to_exclusive,
            "adjustment_through_date": basis.adjustment_through_date,
            "source_fingerprint": basis.source_fingerprint,
            "materialized_through_date": basis.materialized_through_date,
            "status": basis.status,
            "created_at": now_iso,
            "updated_at": now_iso,
        }
        for lineage in plan.lineages
        for basis in lineage.bases
    ]
    segment_rows = [
        {
            "code": normalize_stock_code(segment.code),
            "basis_id": segment.basis_id,
            "source_date_from": segment.source_date_from,
            "source_date_to_exclusive": segment.source_date_to_exclusive,
            "cumulative_factor": segment.cumulative_factor,
        }
        for lineage in plan.lineages
        for segment in lineage.segments
    ]
    statement_rows = _rows_with_created_at(
        plan.adjusted_statement_rows,
        _STATEMENT_METRICS_ADJUSTED_COLUMNS,
        now_iso,
    )
    valuation_rows = _rows_with_created_at(
        plan.daily_valuation_rows,
        _DAILY_VALUATION_COLUMNS,
        now_iso,
    )
    replacements = _basis_keys(plan.replace_basis_ids)
    orphans = _basis_keys(plan.orphan_basis_ids)
    basis_columns = list(basis_rows[0]) if basis_rows else [
        "code", "basis_id", "valid_from", "valid_to_exclusive",
        "adjustment_through_date", "source_fingerprint",
        "materialized_through_date", "status", "created_at", "updated_at",
    ]
    segment_columns = [
        "code", "basis_id", "source_date_from", "source_date_to_exclusive",
        "cumulative_factor",
    ]
    frames = (
        (_ATOMIC_BASIS_RELATION, basis_rows, basis_columns),
        (_ATOMIC_SEGMENT_RELATION, segment_rows, segment_columns),
        (_ATOMIC_STATEMENT_RELATION, statement_rows, _STATEMENT_METRICS_ADJUSTED_COLUMNS),
        (_ATOMIC_VALUATION_RELATION, valuation_rows, _DAILY_VALUATION_COLUMNS),
    )
    registered: list[str] = []
    transaction_started = False
    with lock:
        try:
            for name, rows, columns in frames:
                if rows:
                    conn.register(name, pd.DataFrame.from_records(rows, columns=columns))
                    registered.append(name)
            conn.execute("BEGIN TRANSACTION")
            transaction_started = True
            _validate_materialization_payload(
                conn,
                basis_rows,
                segment_rows,
                statement_rows,
                valuation_rows,
                replacements,
                orphans,
            )
            adjustment_basis_writers._validate_final_catalog(conn, basis_rows, list(orphans))
            for code, basis_id in sorted(orphans | replacements):
                conn.execute(
                    "DELETE FROM daily_valuation WHERE code = ? AND basis_version = ?",
                    [code, basis_id],
                )
                conn.execute(
                    "DELETE FROM statement_metrics_adjusted WHERE code = ? AND basis_version = ?",
                    [code, basis_id],
                )
            for code, basis_id in sorted(orphans):
                conn.execute(
                    "DELETE FROM stock_adjustment_basis_segments WHERE code = ? AND basis_id = ?",
                    [code, basis_id],
                )
                conn.execute(
                    "DELETE FROM stock_adjustment_bases WHERE code = ? AND basis_id = ?",
                    [code, basis_id],
                )
            for row in basis_rows:
                conn.execute(
                    "DELETE FROM stock_adjustment_basis_segments WHERE code = ? AND basis_id = ?",
                    [row["code"], row["basis_id"]],
                )
            if basis_rows:
                conn.execute(
                    f"""
                    INSERT INTO stock_adjustment_bases ({", ".join(basis_columns)})
                    SELECT {", ".join(basis_columns)} FROM {_ATOMIC_BASIS_RELATION}
                    ON CONFLICT (code, basis_id) DO UPDATE SET
                        valid_from = excluded.valid_from,
                        valid_to_exclusive = excluded.valid_to_exclusive,
                        adjustment_through_date = excluded.adjustment_through_date,
                        source_fingerprint = excluded.source_fingerprint,
                        materialized_through_date = excluded.materialized_through_date,
                        status = excluded.status,
                        updated_at = excluded.updated_at
                    """
                )
            if segment_rows:
                conn.execute(
                    f"""
                    INSERT INTO stock_adjustment_basis_segments ({", ".join(segment_columns)})
                    SELECT {", ".join(segment_columns)} FROM {_ATOMIC_SEGMENT_RELATION}
                    """
                )
            if statement_rows:
                conn.execute(
                    f"""
                    INSERT INTO statement_metrics_adjusted
                    ({", ".join(_STATEMENT_METRICS_ADJUSTED_COLUMNS)})
                    SELECT {", ".join(_STATEMENT_METRICS_ADJUSTED_COLUMNS)}
                    FROM {_ATOMIC_STATEMENT_RELATION}
                    """
                )
            if valuation_rows:
                conn.execute(
                    f"""
                    INSERT INTO daily_valuation ({", ".join(_DAILY_VALUATION_COLUMNS)})
                    SELECT {", ".join(_DAILY_VALUATION_COLUMNS)}
                    FROM {_ATOMIC_VALUATION_RELATION}
                    """
                )
            conn.execute("COMMIT")
            transaction_started = False
        except Exception:
            if transaction_started:
                conn.execute("ROLLBACK")
            raise
        finally:
            for name in reversed(registered):
                conn.unregister(name)
    return AdjustedBasisPublishResult(
        basis_rows=len(basis_rows),
        segment_rows=len(segment_rows),
        statement_rows=len(statement_rows),
        daily_valuation_rows=len(valuation_rows),
    )


def _rows_with_created_at(
    rows: Sequence[dict[str, Any]],
    columns: Sequence[str],
    now_iso: str,
) -> list[dict[str, Any]]:
    return [
        {
            column: row.get(column) if column != "created_at" else row.get(column, now_iso)
            for column in columns
        }
        for row in rows
    ]


def _basis_keys(mapping: Mapping[str, Sequence[str]]) -> set[tuple[str, str]]:
    return {
        (normalize_stock_code(code), basis_id)
        for code, basis_ids in mapping.items()
        for basis_id in basis_ids
    }


def _validate_materialization_payload(
    conn: Any,
    basis_rows: Sequence[dict[str, Any]],
    segment_rows: Sequence[dict[str, Any]],
    statement_rows: Sequence[dict[str, Any]],
    valuation_rows: Sequence[dict[str, Any]],
    replacements: set[tuple[str, str]],
    orphans: set[tuple[str, str]],
) -> None:
    staged = {
        (str(row["code"]), str(row["basis_id"])): row for row in basis_rows
    }
    undeclared_staged_keys = set(staged) - replacements
    if undeclared_staged_keys:
        basis_ids = ", ".join(
            basis_id for _, basis_id in sorted(undeclared_staged_keys)
        )
        raise ValueError(
            f"every staged basis must be a declared replacement: {basis_ids}"
        )
    existing = {
        (str(row[0]), str(row[1])): {
            "valid_to_exclusive": str(row[2]) if row[2] is not None else None,
            "materialized_through_date": str(row[3]),
            "status": str(row[4]),
        }
        for row in conn.execute(
            "SELECT code, basis_id, valid_to_exclusive, materialized_through_date, status "
            "FROM stock_adjustment_bases"
        ).fetchall()
    }
    available = {**existing, **staged}
    segments_by_basis: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for segment in segment_rows:
        key = (normalize_stock_code(str(segment["code"])), str(segment["basis_id"]))
        segments_by_basis.setdefault(key, []).append(segment)
    for key, basis in staged.items():
        if basis["status"] != "ready":
            continue
        segments = sorted(
            segments_by_basis.get(key, []),
            key=lambda row: str(row["source_date_from"]),
        )
        if not segments or segments[-1].get("source_date_to_exclusive") is not None:
            raise ValueError(f"ready basis lacks segment coverage: {key[1]}")
        for current, following in zip(segments, segments[1:], strict=False):
            if current.get("source_date_to_exclusive") != following.get("source_date_from"):
                raise ValueError(f"ready basis has incomplete segment coverage: {key[1]}")
    for code, basis_id in replacements:
        basis = available.get((code, basis_id))
        if basis is None:
            raise ValueError(f"replacement basis does not exist: {basis_id}")
    for row in statement_rows:
        key = (normalize_stock_code(str(row["code"])), str(row["basis_version"]))
        basis = available.get(key)
        if key not in replacements or basis is None or basis["status"] != "ready":
            raise ValueError("adjusted statement references a non-replacement ready basis")
        interval_end = basis.get("valid_to_exclusive")
        if interval_end is not None and str(row["disclosed_date"]) >= str(interval_end):
            raise ValueError("adjusted statement disclosure is outside its basis interval")
    for row in valuation_rows:
        key = (normalize_stock_code(str(row["code"])), str(row["basis_version"]))
        basis = available.get(key)
        if key not in replacements or basis is None or basis["status"] != "ready":
            raise ValueError("valuation references a non-replacement ready basis")
        if str(row["date"]) > str(basis["materialized_through_date"]):
            raise ValueError("valuation exceeds basis coverage")
        for field in (
            "statement_disclosed_date",
            "forward_eps_disclosed_date",
            "forward_sales_disclosed_date",
        ):
            disclosed = row.get(field)
            if disclosed is not None and str(disclosed) > str(row["date"]):
                raise ValueError(f"valuation has future provenance: {field}")
    if replacements & orphans:
        raise ValueError("a basis cannot be both replacement and orphan")


_DAILY_VALUATION_REBUILD_SQL_TEMPLATE = """
        WITH
        stock_prices AS (
            SELECT code, date, close
            FROM stock_data
            {stock_filter}
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
                  OR s.sales > 0
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
        actual_sales_metrics AS (
            SELECT st.code, st.disclosed_date, st.sales
            FROM statements AS st
            JOIN fy_cycle_anchors AS fy
              ON fy.code = st.code
             AND fy.disclosed_date = st.disclosed_date
            WHERE upper(st.type_of_current_period) = 'FY'
              AND st.sales IS NOT NULL
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
        forward_sales_metrics AS (
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
                    THEN COALESCE(st.forecast_sales, st.next_year_forecast_sales)
                    WHEN upper(st.type_of_current_period) = 'FY'
                    THEN COALESCE(st.next_year_forecast_sales, st.forecast_sales)
                    ELSE st.forecast_sales
                END AS forecast_sales
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
                    st.forecast_sales IS NOT NULL
                    OR st.next_year_forecast_sales IS NOT NULL
                )
            )
               OR (
                   upper(st.type_of_current_period) != 'FY'
                   AND st.forecast_sales IS NOT NULL
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
            sales.sales,
            CASE
                WHEN fsales.forecast_sales IS NOT NULL
                 AND fy.disclosed_date IS NOT NULL
                 AND (
                     (
                         fsales.forward_source = 'fy'
                         AND fsales.disclosed_date = fy.disclosed_date
                     )
                     OR (
                         fsales.forward_source = 'revised'
                         AND fsales.disclosed_date > fy.disclosed_date
                     )
                 )
                THEN fsales.forecast_sales
                ELSE NULL
            END AS forward_sales,
            CASE
                WHEN s.close > 0
                 AND sh.adjusted_shares_outstanding > 0
                 AND sales.sales > 0
                 AND sales.disclosed_date = fy.disclosed_date
                THEN s.close * sh.adjusted_shares_outstanding / sales.sales
                ELSE NULL
            END AS psr,
            CASE
                WHEN s.close > 0
                 AND sh.adjusted_shares_outstanding > 0
                 AND fsales.forecast_sales > 0
                 AND fy.disclosed_date IS NOT NULL
                 AND (
                     (
                         fsales.forward_source = 'fy'
                         AND fsales.disclosed_date = fy.disclosed_date
                     )
                     OR (
                         fsales.forward_source = 'revised'
                         AND fsales.disclosed_date > fy.disclosed_date
                     )
                 )
                THEN s.close * sh.adjusted_shares_outstanding / fsales.forecast_sales
                ELSE NULL
            END AS forward_psr,
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
            CASE
                WHEN fsales.forecast_sales IS NOT NULL
                 AND fy.disclosed_date IS NOT NULL
                 AND (
                     (
                         fsales.forward_source = 'fy'
                         AND fsales.disclosed_date = fy.disclosed_date
                     )
                     OR (
                         fsales.forward_source = 'revised'
                         AND fsales.disclosed_date > fy.disclosed_date
                     )
                 )
                THEN fsales.disclosed_date
                ELSE NULL
            END AS forward_sales_disclosed_date,
            CASE
                WHEN fsales.forecast_sales IS NOT NULL
                 AND fsales.forward_source = 'fy'
                 AND fy.disclosed_date IS NOT NULL
                 AND fsales.disclosed_date = fy.disclosed_date
                THEN 'fy'
                WHEN fsales.forecast_sales IS NOT NULL
                 AND fy.disclosed_date IS NOT NULL
                 AND fsales.forward_source = 'revised'
                 AND fsales.disclosed_date > fy.disclosed_date
                THEN 'revised'
                ELSE NULL
            END AS forward_sales_source,
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
        ASOF LEFT JOIN actual_sales_metrics AS sales
          ON s.code = sales.code
         AND s.date >= sales.disclosed_date
        ASOF LEFT JOIN forward_operating_profit_metrics AS fop
          ON s.code = fop.code
         AND s.date >= fop.disclosed_date
        ASOF LEFT JOIN forward_sales_metrics AS fsales
          ON s.code = fsales.code
         AND s.date >= fsales.disclosed_date
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
           OR sales.disclosed_date IS NOT NULL
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
           OR (
               fsales.disclosed_date IS NOT NULL
               AND fy.disclosed_date IS NOT NULL
               AND (
                   (
                       fsales.forward_source = 'fy'
                       AND fsales.disclosed_date = fy.disclosed_date
                   )
                   OR (
                       fsales.forward_source = 'revised'
                       AND fsales.disclosed_date > fy.disclosed_date
                   )
               )
           )
           OR sh.disclosed_date IS NOT NULL
        ON CONFLICT (code, date, basis_version) DO UPDATE SET {daily_valuation_update_clause}
        """


def upsert_daily_valuation_from_adjusted_metrics(
    conn: Any,
    lock: Any,
    table_exists: Callable[[str], bool],
    basis_version: str,
    price_basis_date: str,
    codes: list[str] | None = None,
    start_date: str | None = None,
    start_date_inclusive: bool = True,
    replace_existing: bool = True,
) -> int:
    """Canonical daily valuation metrics を DuckDB relation で一括生成する。"""
    if (
        not table_exists("stock_data")
        or not table_exists("statement_metrics_adjusted")
    ):
        return 0

    normalized_codes = sorted({normalize_stock_code(code) for code in codes or [] if code})
    stock_conditions: list[str] = []
    stock_filter_params: list[Any] = []
    if normalized_codes:
        placeholders = ", ".join("?" for _ in normalized_codes)
        stock_conditions.append(f"code IN ({placeholders})")
        stock_filter_params.extend(normalized_codes)
    if start_date is not None:
        operator = ">=" if start_date_inclusive else ">"
        stock_conditions.append(f"date {operator} ?")
        stock_filter_params.append(start_date)
    stock_filter = (
        f"WHERE {' AND '.join(stock_conditions)}"
        if stock_conditions
        else ""
    )

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
        stock_filter=stock_filter,
        daily_valuation_columns=", ".join(_DAILY_VALUATION_COLUMNS),
        daily_valuation_update_clause=", ".join(
            f"{column} = excluded.{column}"
            for column in _DAILY_VALUATION_COLUMNS
            if column not in {"code", "date", "basis_version"}
        ),
    )
    insert_params = [
        *stock_filter_params,
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
            if replace_existing:
                conn.execute(delete_sql, target_params)
            conn.execute(insert_sql, insert_params)
            count_row = conn.execute(count_sql, target_params).fetchone()
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return int(count_row[0] or 0) if count_row else 0
