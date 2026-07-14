"""Queries for event-time corporate-action adjustment bases."""

from __future__ import annotations

from typing import Any

from src.infrastructure.db.market.query_helpers import normalize_stock_code


_NORMALIZED_CODE_SQL = """
CASE
    WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
    THEN left(code, length(code) - 1)
    ELSE code
END
"""


def load_raw_adjustment_points(
    fetchall_dicts: Any,
    codes: list[str] | None = None,
) -> list[dict[str, Any]]:
    normalized_codes = sorted({normalize_stock_code(code) for code in codes or []})
    where = ""
    params: list[Any] = []
    if normalized_codes:
        placeholders = ", ".join("?" for _ in normalized_codes)
        where = f"WHERE normalized_code IN ({placeholders})"
        params.extend(normalized_codes)
    return fetchall_dicts(
        f"""
        WITH raw_points AS (
            SELECT {_NORMALIZED_CODE_SQL} AS normalized_code,
                   date,
                   adjustment_factor
            FROM stock_data_raw
        )
        SELECT normalized_code AS code, date, adjustment_factor
        FROM raw_points
        {where}
        ORDER BY normalized_code, date
        """,
        params,
    )


def get_ready_adjustment_basis(
    fetchall_dicts: Any,
    code: str,
    effective_market_date: str,
) -> dict[str, Any] | None:
    rows = fetchall_dicts(
        """
        SELECT code, basis_id, valid_from, valid_to_exclusive,
               adjustment_through_date, source_fingerprint,
               materialized_through_date, status, created_at, updated_at
        FROM stock_adjustment_bases
        WHERE code = ?
          AND status = 'ready'
          AND valid_from <= ?
          AND (valid_to_exclusive IS NULL OR ? < valid_to_exclusive)
          AND materialized_through_date >= ?
        ORDER BY valid_from DESC
        LIMIT 1
        """,
        [
            normalize_stock_code(code),
            effective_market_date,
            effective_market_date,
            effective_market_date,
        ],
    )
    return rows[0] if rows else None


def get_adjustment_basis_segments(
    fetchall_dicts: Any,
    code: str,
    basis_id: str,
) -> list[dict[str, Any]]:
    return fetchall_dicts(
        """
        SELECT code, basis_id, source_date_from, source_date_to_exclusive,
               cumulative_factor
        FROM stock_adjustment_basis_segments
        WHERE code = ? AND basis_id = ?
        ORDER BY source_date_from
        """,
        [normalize_stock_code(code), basis_id],
    )


def get_basis_adjusted_stock_data(
    fetchall_dicts: Any,
    code: str,
    basis_id: str,
    *,
    start: str | None = None,
    end: str | None = None,
) -> list[dict[str, Any]]:
    normalized_code = normalize_stock_code(code)
    predicates = ["raw.normalized_code = ?", "basis.basis_id = ?"]
    params: list[Any] = [normalized_code, basis_id]
    if start is not None:
        predicates.append("raw.date >= ?")
        params.append(start)
    if end is not None:
        predicates.append("raw.date <= ?")
        params.append(end)
    where = " AND ".join(predicates)
    return fetchall_dicts(
        f"""
        WITH normalized_raw AS (
            SELECT
                {_NORMALIZED_CODE_SQL} AS normalized_code,
                code AS source_code,
                date, open, high, low, close, volume, adjustment_factor,
                ROW_NUMBER() OVER (
                    PARTITION BY {_NORMALIZED_CODE_SQL}, date
                    ORDER BY
                        CASE WHEN code = {_NORMALIZED_CODE_SQL} THEN 0 ELSE 1 END,
                        length(code), code
                ) AS alias_rank
            FROM stock_data_raw
        ),
        raw AS (
            SELECT * FROM normalized_raw WHERE alias_rank = 1
        )
        SELECT
            raw.normalized_code AS code,
            raw.date,
            raw.open * segment.cumulative_factor AS open,
            raw.high * segment.cumulative_factor AS high,
            raw.low * segment.cumulative_factor AS low,
            raw.close * segment.cumulative_factor AS close,
            CAST(ROUND(raw.volume / segment.cumulative_factor) AS BIGINT) AS volume,
            raw.adjustment_factor,
            segment.cumulative_factor,
            basis.basis_id
        FROM raw
        JOIN stock_adjustment_bases AS basis
          ON basis.code = raw.normalized_code
         AND basis.status = 'ready'
        JOIN stock_adjustment_basis_segments AS segment
          ON segment.code = basis.code
         AND segment.basis_id = basis.basis_id
         AND raw.date >= segment.source_date_from
         AND (
             segment.source_date_to_exclusive IS NULL
             OR raw.date < segment.source_date_to_exclusive
         )
        WHERE {where}
          AND raw.date <= basis.materialized_through_date
        ORDER BY raw.date
        """,
        params,
    )
