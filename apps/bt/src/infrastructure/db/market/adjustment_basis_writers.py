"""Atomic writers for corporate-action adjustment basis lineages."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
import math
from typing import Any

import pandas as pd

from src.domains.fundamentals.adjustment_basis import StockAdjustmentLineage
from src.infrastructure.db.market.query_helpers import normalize_stock_code


_BASIS_RELATION = "__staged_stock_adjustment_bases"
_SEGMENT_RELATION = "__staged_stock_adjustment_basis_segments"


def _validate_lineages(lineages: Sequence[StockAdjustmentLineage]) -> None:
    grouped_bases: dict[str, list[Any]] = {}
    grouped_segments: dict[tuple[str, str], list[Any]] = {}
    for lineage in lineages:
        code = normalize_stock_code(lineage.code)
        for basis in lineage.bases:
            if normalize_stock_code(basis.code) != code:
                raise ValueError("basis code does not match lineage code")
            grouped_bases.setdefault(code, []).append(basis)

    basis_ids_by_code = {
        code: {basis.basis_id for basis in bases}
        for code, bases in grouped_bases.items()
    }
    for code, grouped in grouped_bases.items():
        bases = sorted(grouped, key=lambda basis: basis.valid_from)
        for index, basis in enumerate(bases):
            if (
                basis.valid_to_exclusive is not None
                and basis.valid_to_exclusive <= basis.valid_from
            ):
                raise ValueError(f"invalid basis interval for {basis.basis_id}")
            if index:
                previous_end = bases[index - 1].valid_to_exclusive
                if previous_end is None or previous_end > basis.valid_from:
                    raise ValueError(f"overlapping basis intervals for {code}")

    for lineage in lineages:
        code = normalize_stock_code(lineage.code)
        for segment in lineage.segments:
            if normalize_stock_code(segment.code) != code:
                raise ValueError("segment code does not match lineage code")
            if segment.basis_id not in basis_ids_by_code.get(code, set()):
                raise ValueError("segment references a basis outside its lineage")
            if not math.isfinite(segment.cumulative_factor) or segment.cumulative_factor <= 0:
                raise ValueError("segment cumulative factor must be finite and positive")
            if (
                segment.source_date_to_exclusive is not None
                and segment.source_date_to_exclusive <= segment.source_date_from
            ):
                raise ValueError(
                    f"invalid basis segment interval for {segment.basis_id}"
                )
            grouped_segments.setdefault((code, segment.basis_id), []).append(segment)

    for (_, basis_id), grouped in grouped_segments.items():
        ordered = sorted(grouped, key=lambda segment: segment.source_date_from)
        for index, segment in enumerate(ordered[:-1]):
            next_segment = ordered[index + 1]
            if (
                segment.source_date_to_exclusive is None
                or segment.source_date_to_exclusive > next_segment.source_date_from
            ):
                raise ValueError(f"overlapping basis segments for {basis_id}")


def publish_stock_adjustment_lineages(
    conn: Any,
    lock: Any,
    lineages: Sequence[StockAdjustmentLineage],
    *,
    remove_basis_ids: Mapping[str, Sequence[str]],
) -> None:
    _validate_lineages(lineages)
    now_iso = datetime.now().isoformat()  # noqa: DTZ005
    basis_records = [
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
        for lineage in lineages
        for basis in lineage.bases
    ]
    segment_records = [
        {
            "code": normalize_stock_code(segment.code),
            "basis_id": segment.basis_id,
            "source_date_from": segment.source_date_from,
            "source_date_to_exclusive": segment.source_date_to_exclusive,
            "cumulative_factor": segment.cumulative_factor,
        }
        for lineage in lineages
        for segment in lineage.segments
    ]
    removals = [
        (normalize_stock_code(code), basis_id)
        for code, basis_ids in remove_basis_ids.items()
        for basis_id in basis_ids
    ]
    basis_columns = [
        "code",
        "basis_id",
        "valid_from",
        "valid_to_exclusive",
        "adjustment_through_date",
        "source_fingerprint",
        "materialized_through_date",
        "status",
        "created_at",
        "updated_at",
    ]
    segment_columns = [
        "code",
        "basis_id",
        "source_date_from",
        "source_date_to_exclusive",
        "cumulative_factor",
    ]
    basis_frame = pd.DataFrame.from_records(basis_records, columns=basis_columns)
    segment_frame = pd.DataFrame.from_records(segment_records, columns=segment_columns)

    with lock:
        basis_registered = bool(basis_records)
        segments_registered = bool(segment_records)
        if basis_registered:
            conn.register(_BASIS_RELATION, basis_frame)
        if segments_registered:
            conn.register(_SEGMENT_RELATION, segment_frame)
        try:
            conn.execute("BEGIN TRANSACTION")
            for code, basis_id in removals:
                conn.execute(
                    "DELETE FROM stock_adjustment_basis_segments WHERE code = ? AND basis_id = ?",
                    [code, basis_id],
                )
                conn.execute(
                    "DELETE FROM stock_adjustment_bases WHERE code = ? AND basis_id = ?",
                    [code, basis_id],
                )
            if basis_registered:
                published_ids = [(row["code"], row["basis_id"]) for row in basis_records]
                for code, basis_id in published_ids:
                    conn.execute(
                        "DELETE FROM stock_adjustment_basis_segments WHERE code = ? AND basis_id = ?",
                        [code, basis_id],
                    )
                conn.execute(
                    f"""
                    INSERT INTO stock_adjustment_bases ({", ".join(basis_columns)})
                    SELECT {", ".join(basis_columns)} FROM {_BASIS_RELATION}
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
            if segments_registered:
                conn.execute(
                    f"""
                    INSERT INTO stock_adjustment_basis_segments ({", ".join(segment_columns)})
                    SELECT {", ".join(segment_columns)} FROM {_SEGMENT_RELATION}
                    ON CONFLICT (code, basis_id, source_date_from) DO UPDATE SET
                        source_date_to_exclusive = excluded.source_date_to_exclusive,
                        cumulative_factor = excluded.cumulative_factor
                    """
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            if segments_registered:
                conn.unregister(_SEGMENT_RELATION)
            if basis_registered:
                conn.unregister(_BASIS_RELATION)
