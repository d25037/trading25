"""Adjusted fundamentals and daily valuation writer helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from src.infrastructure.db.market.market_schema import (
    DAILY_VALUATION_COLUMNS as _DAILY_VALUATION_COLUMNS,
    STATEMENT_METRICS_ADJUSTED_COLUMNS as _STATEMENT_METRICS_ADJUSTED_COLUMNS,
)
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.domains.fundamentals.adjustment_basis import StockAdjustmentLineage
from src.infrastructure.db.market.adjustment_basis_validation import (
    validate_final_catalog,
    validate_lineages,
)


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


def publish_adjusted_basis_materialization(
    conn: Any,
    lock: Any,
    plan: AdjustedBasisMaterializationPlan,
) -> AdjustedBasisPublishResult:
    """Atomically replace lineage and materialized rows for explicit bases."""
    validate_lineages(plan.lineages)
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
            validate_final_catalog(conn, basis_rows, list(orphans))
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
