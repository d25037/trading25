"""Cutoff-normalized Dataset PIT adjustment lineage staging."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from src.domains.fundamentals.adjustment_basis import (
    RawAdjustmentPoint,
    StockAdjustmentLineage,
    build_stock_adjustment_lineage,
)


class DatasetPitLineageError(RuntimeError):
    """The source adjusted-metrics PIT lineage cannot prove the rebuilt graph."""


def _normalized_code(column: str) -> str:
    return (
        f"CASE WHEN length({column}) IN (5, 6) AND right({column}, 1) = '0' "
        f"THEN left({column}, length({column}) - 1) ELSE {column} END"
    )


def iter_cutoff_lineages(
    conn: Any,
    *,
    source_alias: str,
    target_code_table: str,
    cutoff: str,
) -> Iterator[StockAdjustmentLineage]:
    """Stream raw facts ordered by normalized code and build one code at a time."""

    sessions = [
        str(row[0])
        for row in conn.execute(
            f"SELECT DISTINCT date FROM {source_alias}.topix_data "
            "WHERE date <= ? ORDER BY date",
            [cutoff],
        ).fetchall()
    ]
    cursor = conn.execute(
        f"""
        SELECT {_normalized_code('code')} AS code, date, adjustment_factor
        FROM {source_alias}.stock_data_raw
        WHERE {_normalized_code('code')} IN (SELECT code FROM {target_code_table})
          AND date <= ?
        ORDER BY 1, date, code
        """,
        [cutoff],
    )
    current_code: str | None = None
    points: list[RawAdjustmentPoint] = []
    while rows := cursor.fetchmany(10_000):
        for code_value, date_value, factor in rows:
            code = str(code_value)
            if current_code is not None and code != current_code:
                yield build_stock_adjustment_lineage(
                    current_code, points, market_sessions=sessions
                )
                points = []
            current_code = code
            points.append(
                RawAdjustmentPoint(
                    code=code,
                    date=str(date_value),
                    adjustment_factor=(float(factor) if factor is not None else None),
                )
            )
    if current_code is not None:
        yield build_stock_adjustment_lineage(
            current_code, points, market_sessions=sessions
        )


def _fail(message: str) -> DatasetPitLineageError:
    return DatasetPitLineageError(
        f"adjusted_metrics_pit recovery required: {message}"
    )


def stage_cutoff_lineage(
    conn: Any,
    *,
    source_alias: str,
    target_code_table: str,
    cutoff: str,
) -> None:
    """Rebuild, prove against source materialization, and stage immutable lineage."""

    lineages = list(
        iter_cutoff_lineages(
            conn,
            source_alias=source_alias,
            target_code_table=target_code_table,
            cutoff=cutoff,
        )
    )
    rebuilt_bases = [basis for lineage in lineages for basis in lineage.bases]
    rebuilt_segments = [segment for lineage in lineages for segment in lineage.segments]
    source_basis_rows = conn.execute(
        f"""
        SELECT {_normalized_code('code')} AS code, basis_id, valid_from,
               valid_to_exclusive, adjustment_through_date, source_fingerprint,
               materialized_through_date, status
        FROM {source_alias}.stock_adjustment_bases
        WHERE {_normalized_code('code')} IN (SELECT code FROM {target_code_table})
        ORDER BY 1, valid_from, basis_id
        """
    ).fetchall()
    source_bases: dict[tuple[str, str], tuple[Any, ...]] = {}
    for row in source_basis_rows:
        key = (str(row[0]), str(row[1]))
        value = tuple(row[2:])
        if key in source_bases and source_bases[key] != value:
            raise _fail(f"conflicting source basis aliases for {key[0]} {key[1]}")
        source_bases[key] = value

    source_segment_rows = conn.execute(
        f"""
        SELECT {_normalized_code('code')} AS code, basis_id, source_date_from,
               source_date_to_exclusive, cumulative_factor
        FROM {source_alias}.stock_adjustment_basis_segments
        WHERE {_normalized_code('code')} IN (SELECT code FROM {target_code_table})
        ORDER BY 1, basis_id, source_date_from
        """
    ).fetchall()
    source_segments: dict[tuple[str, str], list[tuple[Any, ...]]] = {}
    for code, basis_id, source_from, source_to, factor in source_segment_rows:
        source_segments.setdefault((str(code), str(basis_id)), []).append(
            (str(source_from), str(source_to) if source_to is not None else None, float(factor))
        )

    for basis in rebuilt_bases:
        source = source_bases.get((basis.code, basis.basis_id))
        if source is None:
            raise _fail(f"missing source basis {basis.basis_id}")
        valid_from, valid_to, adjustment_through, _fingerprint, materialized, status = source
        if (
            str(status) != "ready"
            or str(valid_from) != basis.valid_from
            or str(adjustment_through) != basis.adjustment_through_date
            or str(materialized) < basis.materialized_through_date
        ):
            raise _fail(f"source basis proof mismatch for {basis.basis_id}")
        if basis.valid_to_exclusive is not None and (
            str(valid_to) != basis.valid_to_exclusive
            or str(materialized) != basis.materialized_through_date
        ):
            raise _fail(f"closed source basis mismatch for {basis.basis_id}")
        if basis.valid_to_exclusive is None and valid_to is not None and str(valid_to) <= cutoff:
            raise _fail(f"source basis closes before cutoff for {basis.basis_id}")
        expected_segments = [
            (
                segment.source_date_from,
                segment.source_date_to_exclusive,
                float(segment.cumulative_factor),
            )
            for segment in rebuilt_segments
            if segment.code == basis.code and segment.basis_id == basis.basis_id
        ]
        if source_segments.get((basis.code, basis.basis_id), []) != expected_segments:
            raise _fail(f"source segments mismatch for {basis.basis_id}")

    conn.execute("DROP TABLE IF EXISTS _dataset_pit_bases")
    conn.execute(
        """
        CREATE TEMP TABLE _dataset_pit_bases (
            code TEXT, basis_id TEXT, valid_from TEXT, valid_to_exclusive TEXT,
            adjustment_through_date TEXT, source_fingerprint TEXT,
            materialized_through_date TEXT, status TEXT,
            created_at TEXT, updated_at TEXT
        )
        """
    )
    if rebuilt_bases:
        conn.executemany(
            "INSERT INTO _dataset_pit_bases VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)",
            [
                (
                    basis.code,
                    basis.basis_id,
                    basis.valid_from,
                    basis.valid_to_exclusive,
                    basis.adjustment_through_date,
                    basis.source_fingerprint,
                    basis.materialized_through_date,
                    basis.status,
                )
                for basis in rebuilt_bases
            ],
        )
    conn.execute("DROP TABLE IF EXISTS _dataset_pit_segments")
    conn.execute(
        """
        CREATE TEMP TABLE _dataset_pit_segments (
            code TEXT, basis_id TEXT, source_date_from TEXT,
            source_date_to_exclusive TEXT, cumulative_factor DOUBLE
        )
        """
    )
    if rebuilt_segments:
        conn.executemany(
            "INSERT INTO _dataset_pit_segments VALUES (?, ?, ?, ?, ?)",
            [
                (
                    segment.code,
                    segment.basis_id,
                    segment.source_date_from,
                    segment.source_date_to_exclusive,
                    segment.cumulative_factor,
                )
                for segment in rebuilt_segments
            ],
        )
