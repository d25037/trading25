"""Validation shared by atomic adjustment-basis publication paths."""

from __future__ import annotations

from collections.abc import Sequence
import math
from typing import Any

from src.domains.fundamentals.adjustment_basis import StockAdjustmentLineage
from src.infrastructure.db.market.query_helpers import normalize_stock_code


def validate_lineages(lineages: Sequence[StockAdjustmentLineage]) -> None:
    grouped_bases: dict[str, list[Any]] = {}
    grouped_segments: dict[tuple[str, str], list[Any]] = {}
    for lineage in lineages:
        code = normalize_stock_code(lineage.code)
        for basis in lineage.bases:
            if normalize_stock_code(basis.code) != code:
                raise ValueError("basis code does not match lineage code")
            expected_basis_id = f"event-pit-v1:{code}:{basis.valid_from}"
            if basis.basis_id != expected_basis_id:
                raise ValueError(
                    f"basis identity mismatch: expected {expected_basis_id}, "
                    f"got {basis.basis_id}"
                )
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
            if (
                not math.isfinite(segment.cumulative_factor)
                or segment.cumulative_factor <= 0
            ):
                raise ValueError(
                    "segment cumulative factor must be finite and positive"
                )
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


def validate_final_catalog(
    conn: Any,
    basis_records: Sequence[dict[str, Any]],
    removals: Sequence[tuple[str, str]],
) -> None:
    existing = conn.execute(
        """
        SELECT code, basis_id, valid_from, valid_to_exclusive
        FROM stock_adjustment_bases
        """
    ).fetchall()
    removed_keys = set(removals)
    published_keys = {
        (str(record["code"]), str(record["basis_id"])) for record in basis_records
    }
    final_rows = [
        {
            "code": str(row[0]),
            "basis_id": str(row[1]),
            "valid_from": str(row[2]),
            "valid_to_exclusive": str(row[3]) if row[3] is not None else None,
        }
        for row in existing
        if (str(row[0]), str(row[1])) not in removed_keys | published_keys
    ]
    final_rows.extend(
        {
            "code": str(record["code"]),
            "basis_id": str(record["basis_id"]),
            "valid_from": str(record["valid_from"]),
            "valid_to_exclusive": (
                str(record["valid_to_exclusive"])
                if record["valid_to_exclusive"] is not None
                else None
            ),
        }
        for record in basis_records
    )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in final_rows:
        grouped.setdefault(row["code"], []).append(row)
    for code, rows in grouped.items():
        ordered = sorted(rows, key=lambda row: row["valid_from"])
        for index, row in enumerate(ordered):
            expected_basis_id = f"event-pit-v1:{code}:{row['valid_from']}"
            if row["basis_id"] != expected_basis_id:
                raise ValueError(
                    f"basis identity mismatch: expected {expected_basis_id}, "
                    f"got {row['basis_id']}"
                )
            interval_end = row["valid_to_exclusive"]
            if interval_end is not None and interval_end <= row["valid_from"]:
                raise ValueError(f"invalid basis interval for {row['basis_id']}")
            if index:
                previous_end = ordered[index - 1]["valid_to_exclusive"]
                if previous_end is None or previous_end > row["valid_from"]:
                    raise ValueError(f"overlapping basis intervals for {code}")
