"""Atomic writers for corporate-action adjustment basis lineages."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

import pandas as pd

from src.domains.fundamentals.adjustment_basis import StockAdjustmentLineage
from src.infrastructure.db.market.adjustment_basis_validation import (
    validate_final_catalog,
    validate_lineages,
)
from src.infrastructure.db.market.query_helpers import normalize_stock_code


_BASIS_RELATION = "__staged_stock_adjustment_bases"
_SEGMENT_RELATION = "__staged_stock_adjustment_basis_segments"


def publish_stock_adjustment_lineages(
    conn: Any,
    lock: Any,
    lineages: Sequence[StockAdjustmentLineage],
    *,
    remove_basis_ids: Mapping[str, Sequence[str]],
) -> None:
    validate_lineages(lineages)
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
        basis_registered = False
        segments_registered = False
        transaction_started = False
        try:
            if basis_records:
                conn.register(_BASIS_RELATION, basis_frame)
                basis_registered = True
            if segment_records:
                conn.register(_SEGMENT_RELATION, segment_frame)
                segments_registered = True
            conn.execute("BEGIN TRANSACTION")
            transaction_started = True
            validate_final_catalog(conn, basis_records, removals)
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
            transaction_started = False
        except Exception:
            if transaction_started:
                conn.execute("ROLLBACK")
            raise
        finally:
            if segments_registered:
                conn.unregister(_SEGMENT_RELATION)
            if basis_registered:
                conn.unregister(_BASIS_RELATION)
