"""Adjusted fundamentals and daily valuation writer helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import math
from typing import Any, Literal, TypeAlias

import pandas as pd

from src.infrastructure.db.market.market_schema import (
    DAILY_VALUATION_COLUMNS as _DAILY_VALUATION_COLUMNS,
    STATEMENT_METRICS_ADJUSTED_COLUMNS as _STATEMENT_METRICS_ADJUSTED_COLUMNS,
)
from src.infrastructure.db.market.query_helpers import (
    normalize_stock_code,
    stock_code_query_candidates,
)
from src.infrastructure.db.market.market_mutations import MarketMutationStats
from src.domains.fundamentals.adjustment_basis import (
    StockAdjustmentBasis,
    StockAdjustmentBasisSegment,
    StockAdjustmentLineage,
)
from src.infrastructure.db.market.adjustment_basis_validation import (
    validate_final_catalog,
    validate_lineages,
)


_ATOMIC_BASIS_RELATION = "__adjusted_publish_bases"
_ATOMIC_SEGMENT_RELATION = "__adjusted_publish_segments"
_ATOMIC_STATEMENT_RELATION = "__adjusted_publish_statements"
_ATOMIC_VALUATION_RELATION = "__adjusted_publish_valuations"


@dataclass(frozen=True)
class BasisSnapshot:
    """Exact persisted graph and materialized rows for one named basis."""

    basis: dict[str, Any]
    segments: tuple[dict[str, Any], ...]
    statement_rows: tuple[dict[str, Any], ...]
    valuation_rows: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class AdjustedMaterializationSource:
    raw_rows: tuple[dict[str, Any], ...]
    statement_rows: tuple[dict[str, Any], ...]
    market_sessions: tuple[str, ...]
    fingerprint: str


@dataclass(frozen=True)
class AdjustedMarketSessions:
    sessions: tuple[str, ...]
    fingerprint: str


@dataclass(frozen=True)
class StructuralBasisPlan:
    kind: Literal["structural"]
    lineage: StockAdjustmentLineage
    adjusted_statement_rows: tuple[dict[str, Any], ...]
    daily_valuation_rows: tuple[dict[str, Any], ...]
    expected_snapshot: BasisSnapshot | None
    expected_source_fingerprint: str


@dataclass(frozen=True)
class FrontierExtensionBasisPlan:
    kind: Literal["frontier_extension"]
    basis: StockAdjustmentBasis
    segments: tuple[StockAdjustmentBasisSegment, ...]
    adjusted_statement_rows: tuple[dict[str, Any], ...]
    daily_valuation_rows: tuple[dict[str, Any], ...]
    expected_snapshot: BasisSnapshot
    expected_source_fingerprint: str


@dataclass(frozen=True)
class NoOpBasisPlan:
    kind: Literal["no_op"]
    code: str
    basis_id: str


BasisMaterializationPlan: TypeAlias = (
    StructuralBasisPlan | FrontierExtensionBasisPlan | NoOpBasisPlan
)


@dataclass(frozen=True)
class AdjustedBasisMaterializationPlan:
    """Discriminated per-basis mutation plans."""

    plans: tuple[BasisMaterializationPlan, ...]


@dataclass(frozen=True)
class AdjustedRelationPublishResult:
    stats: MarketMutationStats
    final_count: int


@dataclass(frozen=True)
class AdjustedBasisPublishResult:
    basis: AdjustedRelationPublishResult
    segments: AdjustedRelationPublishResult
    statements: AdjustedRelationPublishResult
    valuations: AdjustedRelationPublishResult
    plan_counts: Mapping[str, int]


def publish_adjusted_basis_materialization(
    conn: Any,
    lock: Any,
    plan: AdjustedBasisMaterializationPlan,
) -> AdjustedBasisPublishResult:
    """Apply structural replacements or proven append-only frontier extensions."""
    actionable = tuple(item for item in plan.plans if item.kind != "no_op")
    if not actionable:
        empty = AdjustedRelationPublishResult(MarketMutationStats.empty(), 0)
        return AdjustedBasisPublishResult(
            basis=empty,
            segments=empty,
            statements=empty,
            valuations=empty,
            plan_counts={"structural": 0, "frontier_extension": 0, "no_op": len(plan.plans)},
        )
    structural = tuple(item for item in actionable if item.kind == "structural")
    extensions = tuple(item for item in actionable if item.kind == "frontier_extension")
    for item in structural:
        if len(item.lineage.bases) != 1:
            raise ValueError("structural plan must name exactly one adjustment basis")
    for item in extensions:
        _validate_frontier_extension_plan(item)
    validate_lineages(tuple(item.lineage for item in structural))
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
        for item in structural
        for lineage in (item.lineage,)
        for basis in lineage.bases
    ]
    extension_basis_rows = [
        {
            **item.expected_snapshot.basis,
            "code": normalize_stock_code(item.basis.code),
            "basis_id": item.basis.basis_id,
            "valid_from": item.basis.valid_from,
            "valid_to_exclusive": item.basis.valid_to_exclusive,
            "adjustment_through_date": item.basis.adjustment_through_date,
            "source_fingerprint": item.basis.source_fingerprint,
            "materialized_through_date": item.basis.materialized_through_date,
            "status": item.basis.status,
        }
        for item in extensions
    ]
    validation_basis_rows = basis_rows + extension_basis_rows
    segment_rows = [
        {
            "code": normalize_stock_code(segment.code),
            "basis_id": segment.basis_id,
            "source_date_from": segment.source_date_from,
            "source_date_to_exclusive": segment.source_date_to_exclusive,
            "cumulative_factor": segment.cumulative_factor,
        }
        for item in structural
        for lineage in (item.lineage,)
        for segment in lineage.segments
    ]
    validation_segment_rows = segment_rows + [
        row for item in extensions for row in item.expected_snapshot.segments
    ]
    statement_rows = _rows_with_created_at(
        tuple(row for item in structural for row in item.adjusted_statement_rows)
        + tuple(row for item in extensions for row in item.adjusted_statement_rows),
        _STATEMENT_METRICS_ADJUSTED_COLUMNS,
        now_iso,
    )
    valuation_rows = _rows_with_created_at(
        tuple(row for item in structural for row in item.daily_valuation_rows)
        + tuple(row for item in extensions for row in item.daily_valuation_rows),
        _DAILY_VALUATION_COLUMNS,
        now_iso,
    )
    validation_basis_by_key = {
        (str(row["code"]), str(row["basis_id"])): row
        for row in validation_basis_rows
    }
    basis_stats = _sum_stats(
        _semantic_stats(
            (desired,),
            (() if item.expected_snapshot is None else (item.expected_snapshot.basis,)),
            key_columns=("code", "basis_id"),
            compare_columns=tuple(
                column for column in desired if column not in {"created_at", "updated_at"}
            ),
        )
        for item in actionable
        for basis in (
            item.lineage.bases[0] if item.kind == "structural" else item.basis,
        )
        for desired in (
            validation_basis_by_key[(normalize_stock_code(basis.code), basis.basis_id)],
        )
    )
    segment_stats = _sum_stats(
        _semantic_stats(
            tuple(
                {
                    "code": segment.code,
                    "basis_id": segment.basis_id,
                    "source_date_from": segment.source_date_from,
                    "source_date_to_exclusive": segment.source_date_to_exclusive,
                    "cumulative_factor": segment.cumulative_factor,
                }
                for segment in (
                    item.lineage.segments if item.kind == "structural" else item.segments
                )
            ),
            (() if item.expected_snapshot is None else item.expected_snapshot.segments),
            key_columns=("code", "basis_id", "source_date_from"),
            compare_columns=(
                "code", "basis_id", "source_date_from", "source_date_to_exclusive",
                "cumulative_factor",
            ),
        )
        for item in actionable
    )
    statement_stats = _sum_stats(
        _semantic_stats(
            item.adjusted_statement_rows,
            (
                ()
                if item.expected_snapshot is None
                else item.expected_snapshot.statement_rows
                if item.kind == "structural"
                else tuple(
                    row
                    for row in item.expected_snapshot.statement_rows
                    if any(
                        _statement_key(row) == _statement_key(candidate)
                        for candidate in item.adjusted_statement_rows
                    )
                )
            ),
            key_columns=("code", "disclosed_date", "period_end", "period_type", "basis_version"),
            compare_columns=tuple(
                column for column in _STATEMENT_METRICS_ADJUSTED_COLUMNS if column != "created_at"
            ),
            count_deletes=item.kind == "structural",
        )
        for item in actionable
    )
    valuation_stats = _sum_stats(
        _semantic_stats(
            item.daily_valuation_rows,
            (() if item.expected_snapshot is None else item.expected_snapshot.valuation_rows),
            key_columns=("code", "date", "basis_version"),
            compare_columns=tuple(
                column for column in _DAILY_VALUATION_COLUMNS if column != "created_at"
            ),
            count_deletes=item.kind == "structural",
        )
        for item in actionable
    )
    replacements = {
        (normalize_stock_code(basis.code), basis.basis_id)
        for item in actionable
        for basis in ((item.lineage.bases[0],) if item.kind == "structural" else (item.basis,))
    }
    affected = sorted(replacements)
    orphans: set[tuple[str, str]] = set()
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
    final_basis = final_segments = final_statements = final_valuations = 0
    with lock:
        try:
            for name, rows, columns in frames:
                if rows:
                    conn.register(name, pd.DataFrame.from_records(rows, columns=columns))
                    registered.append(name)
            conn.execute("BEGIN TRANSACTION")
            transaction_started = True
            expected_source_by_code: dict[str, str] = {}
            for item in actionable:
                basis = item.lineage.bases[0] if item.kind == "structural" else item.basis
                code = normalize_stock_code(basis.code)
                previous = expected_source_by_code.setdefault(
                    code, item.expected_source_fingerprint
                )
                if previous != item.expected_source_fingerprint:
                    raise ValueError("basis plans disagree on source fingerprint")
            for code, expected_fingerprint in expected_source_by_code.items():
                if load_adjusted_source_fingerprint(conn, lock, code) != expected_fingerprint:
                    raise RuntimeError("adjusted materialization sources drifted before publish")
            for item in actionable:
                current = _load_basis_snapshot_unlocked(
                    conn,
                    normalize_stock_code(
                        item.lineage.bases[0].code if item.kind == "structural" else item.basis.code
                    ),
                    item.lineage.bases[0].basis_id if item.kind == "structural" else item.basis.basis_id,
                )
                if not _basis_snapshots_equal(current, item.expected_snapshot):
                    raise RuntimeError("adjusted basis snapshot drifted before publish")
            _validate_materialization_payload(
                conn,
                validation_basis_rows,
                validation_segment_rows,
                statement_rows,
                valuation_rows,
                replacements,
                orphans,
            )
            validate_final_catalog(conn, validation_basis_rows, list(orphans))
            structural_keys = {
                (normalize_stock_code(item.lineage.bases[0].code), item.lineage.bases[0].basis_id)
                for item in structural
            }
            for code, basis_id in sorted(structural_keys):
                conn.execute(
                    "DELETE FROM daily_valuation WHERE code = ? AND basis_version = ?",
                    [code, basis_id],
                )
                conn.execute(
                    "DELETE FROM statement_metrics_adjusted WHERE code = ? AND basis_version = ?",
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
                statement_keys = {
                    "code", "disclosed_date", "period_end", "period_type", "basis_version"
                }
                statement_updates = [
                    column
                    for column in _STATEMENT_METRICS_ADJUSTED_COLUMNS
                    if column not in statement_keys
                ]
                conn.execute(
                    f"""
                    INSERT INTO statement_metrics_adjusted
                    ({", ".join(_STATEMENT_METRICS_ADJUSTED_COLUMNS)})
                    SELECT {", ".join(_STATEMENT_METRICS_ADJUSTED_COLUMNS)}
                    FROM {_ATOMIC_STATEMENT_RELATION}
                    ON CONFLICT (code, disclosed_date, period_end, period_type, basis_version)
                    DO UPDATE SET
                        {", ".join(f"{column} = excluded.{column}" for column in statement_updates)}
                    WHERE {" OR ".join(f"statement_metrics_adjusted.{column} IS DISTINCT FROM excluded.{column}" for column in statement_updates if column != "created_at")}
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
            for item in extensions:
                old_frontier = str(item.expected_snapshot.basis["materialized_through_date"])
                conn.execute(
                    """
                    UPDATE stock_adjustment_bases
                    SET materialized_through_date = ?, updated_at = ?
                    WHERE code = ? AND basis_id = ? AND materialized_through_date = ?
                    """,
                    [
                        item.basis.materialized_through_date,
                        now_iso,
                        normalize_stock_code(item.basis.code),
                        item.basis.basis_id,
                        old_frontier,
                    ],
                )
                observed = conn.execute(
                    "SELECT materialized_through_date FROM stock_adjustment_bases "
                    "WHERE code = ? AND basis_id = ?",
                    [normalize_stock_code(item.basis.code), item.basis.basis_id],
                ).fetchone()
                if observed != (item.basis.materialized_through_date,):
                    raise RuntimeError("adjusted basis frontier conditional update failed")
            final_basis = _count_affected(conn, "stock_adjustment_bases", "basis_id", affected)
            final_segments = _count_affected(
                conn, "stock_adjustment_basis_segments", "basis_id", affected
            )
            final_statements = _count_affected(
                conn, "statement_metrics_adjusted", "basis_version", affected
            )
            final_valuations = _count_affected(
                conn, "daily_valuation", "basis_version", affected
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
        basis=AdjustedRelationPublishResult(basis_stats, final_basis),
        segments=AdjustedRelationPublishResult(segment_stats, final_segments),
        statements=AdjustedRelationPublishResult(statement_stats, final_statements),
        valuations=AdjustedRelationPublishResult(valuation_stats, final_valuations),
        plan_counts={
            "structural": len(structural),
            "frontier_extension": len(extensions),
            "no_op": sum(item.kind == "no_op" for item in plan.plans),
        },
    )


def load_basis_snapshots(
    conn: Any,
    lock: Any,
    code: str,
) -> dict[str, BasisSnapshot]:
    """Load catalog plus exact dependent rows for every basis of one code."""
    normalized = normalize_stock_code(code)
    with lock:
        basis_ids = [
            str(row[0])
            for row in conn.execute(
                "SELECT basis_id FROM stock_adjustment_bases WHERE code = ? ORDER BY basis_id",
                [normalized],
            ).fetchall()
        ]
        return {
            basis_id: snapshot
            for basis_id in basis_ids
            if (snapshot := _load_basis_snapshot_unlocked(conn, normalized, basis_id))
            is not None
        }


def load_adjusted_source_fingerprint(conn: Any, lock: Any, code: str) -> str:
    normalized = normalize_stock_code(code)
    query_codes = stock_code_query_candidates([normalized])
    placeholders = ", ".join("?" for _ in query_codes)
    with lock:
        row = conn.execute(
            _source_digest_sql(placeholders),
            [*query_codes, *query_codes],
        ).fetchone()
        session_fingerprint = _load_market_sessions_fingerprint_unlocked(conn)
    return _combine_source_fingerprints(str(row[0]), str(row[1]), session_fingerprint)


def load_adjusted_materialization_source(
    conn: Any,
    lock: Any,
    code: str,
    *,
    market_sessions: Sequence[str] | None = None,
    market_sessions_fingerprint: str | None = None,
) -> AdjustedMaterializationSource:
    """Load one canonical per-code source snapshot with an exact semantic digest."""
    normalized = normalize_stock_code(code)
    query_codes = stock_code_query_candidates([normalized])
    placeholders = ", ".join("?" for _ in query_codes)
    with lock:
        raw_rows, raw_fingerprint = _fetch_dict_rows_with_digest(
            conn,
            f"""
            WITH source AS (
                SELECT * FROM stock_data_raw WHERE code IN ({placeholders})
            ), normalized AS (
                SELECT *,
                    CASE WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                         THEN left(code, length(code) - 1) ELSE code END AS normalized_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY CASE
                            WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                            THEN left(code, length(code) - 1) ELSE code END, date
                        ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                    ) AS alias_rank
                FROM source
            ), selected AS (
                SELECT normalized_code AS code, date, open, high, low, close, volume,
                       adjustment_factor
                FROM normalized WHERE alias_rank = 1
            )
            SELECT selected.*,
                   md5(string_agg(to_json(selected), '|' ORDER BY code, date) OVER ())
                       AS __source_digest
            FROM selected ORDER BY code, date
            """,
            list(query_codes),
        )
        statement_rows, statement_fingerprint = _fetch_dict_rows_with_digest(
            conn,
            f"""
            WITH source AS (
                SELECT * FROM statements WHERE code IN ({placeholders})
            ), normalized AS (
                SELECT *,
                    CASE WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                         THEN left(code, length(code) - 1) ELSE code END AS normalized_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY CASE
                            WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                            THEN left(code, length(code) - 1) ELSE code END, disclosed_date
                        ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                    ) AS alias_rank
                FROM source
            ), selected AS (
                SELECT * EXCLUDE (code, alias_rank, normalized_code), normalized_code AS code
                FROM normalized WHERE alias_rank = 1
            )
            SELECT selected.*,
                   md5(string_agg(to_json(selected), '|' ORDER BY code, disclosed_date) OVER ())
                       AS __source_digest
            FROM selected ORDER BY code, disclosed_date
            """,
            list(query_codes),
        )
        session_snapshot = (
            AdjustedMarketSessions(
                sessions=tuple(str(value) for value in market_sessions),
                fingerprint=market_sessions_fingerprint,
            )
            if market_sessions is not None and market_sessions_fingerprint is not None
            else _load_market_sessions_unlocked(conn)
        )
    return AdjustedMaterializationSource(
        raw_rows=raw_rows,
        statement_rows=statement_rows,
        market_sessions=session_snapshot.sessions,
        fingerprint=_combine_source_fingerprints(
            raw_fingerprint,
            statement_fingerprint,
            session_snapshot.fingerprint,
        ),
    )


def load_adjusted_market_sessions(conn: Any, lock: Any) -> AdjustedMarketSessions:
    with lock:
        return _load_market_sessions_unlocked(conn)


def _load_basis_snapshot_unlocked(
    conn: Any,
    code: str,
    basis_id: str,
) -> BasisSnapshot | None:
    basis_cursor = conn.execute(
        "SELECT * FROM stock_adjustment_bases WHERE code = ? AND basis_id = ?",
        [code, basis_id],
    )
    basis_row = basis_cursor.fetchone()
    if basis_row is None:
        return None
    basis_columns = [str(item[0]) for item in basis_cursor.description]

    def rows(query: str) -> tuple[dict[str, Any], ...]:
        cursor = conn.execute(query, [code, basis_id])
        columns = [str(item[0]) for item in cursor.description]
        return tuple(dict(zip(columns, row, strict=True)) for row in cursor.fetchall())

    return BasisSnapshot(
        basis=dict(zip(basis_columns, basis_row, strict=True)),
        segments=rows(
            "SELECT * FROM stock_adjustment_basis_segments "
            "WHERE code = ? AND basis_id = ? ORDER BY source_date_from"
        ),
        statement_rows=rows(
            "SELECT * FROM statement_metrics_adjusted "
            "WHERE code = ? AND basis_version = ? ORDER BY disclosed_date"
        ),
        valuation_rows=rows(
            "SELECT * FROM daily_valuation "
            "WHERE code = ? AND basis_version = ? ORDER BY date"
        ),
    )


def _count_affected(
    conn: Any,
    table: str,
    basis_column: str,
    affected: Sequence[tuple[str, str]],
) -> int:
    return sum(
        int(
            conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE code = ? AND {basis_column} = ?",
                [code, basis_id],
            ).fetchone()[0]
        )
        for code, basis_id in affected
    )


def _semantic_stats(
    desired_rows: Sequence[dict[str, Any]],
    existing_rows: Sequence[dict[str, Any]],
    *,
    key_columns: Sequence[str],
    compare_columns: Sequence[str],
    count_deletes: bool = True,
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
    deleted = sum(key not in desired for key in existing) if count_deletes else 0
    return MarketMutationStats(len(desired), inserted, updated, unchanged, deleted)


def _sum_stats(stats: Iterable[MarketMutationStats]) -> MarketMutationStats:
    items = tuple(stats)
    return MarketMutationStats(
        input=sum(item.input for item in items),
        inserted=sum(item.inserted for item in items),
        updated=sum(item.updated for item in items),
        unchanged=sum(item.unchanged for item in items),
        deleted=sum(item.deleted for item in items),
    )


def _statement_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(
        row.get(column)
        for column in ("code", "disclosed_date", "period_end", "period_type", "basis_version")
    )


def _validate_frontier_extension_plan(item: FrontierExtensionBasisPlan) -> None:
    expected = item.expected_snapshot
    old_frontier = str(expected.basis["materialized_through_date"])
    if item.basis.materialized_through_date <= old_frontier:
        raise ValueError("frontier extension must strictly advance")
    for column in (
        "code",
        "basis_id",
        "valid_from",
        "valid_to_exclusive",
        "adjustment_through_date",
        "source_fingerprint",
        "status",
    ):
        if expected.basis.get(column) != getattr(item.basis, column):
            raise ValueError(f"frontier extension changed structural field: {column}")
    desired_segments = tuple(
        {
            "code": segment.code,
            "basis_id": segment.basis_id,
            "source_date_from": segment.source_date_from,
            "source_date_to_exclusive": segment.source_date_to_exclusive,
            "cumulative_factor": segment.cumulative_factor,
        }
        for segment in item.segments
    )
    segment_columns = (
        "code", "basis_id", "source_date_from", "source_date_to_exclusive",
        "cumulative_factor",
    )
    if _canonical_dict_rows(expected.segments, segment_columns) != _canonical_dict_rows(
        desired_segments, segment_columns
    ):
        raise ValueError("frontier extension changed exact adjustment segments")
    if any(
        str(row["disclosed_date"]) <= old_frontier
        for row in item.adjusted_statement_rows
    ):
        raise ValueError("frontier extension contains a historical statement delta")
    if any(str(row["date"]) <= old_frontier for row in item.daily_valuation_rows):
        raise ValueError("frontier extension contains a historical valuation delta")


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


_EMPTY_MD5 = hashlib.md5(b"", usedforsecurity=False).hexdigest()


def _fetch_dict_rows_with_digest(
    conn: Any,
    query: str,
    params: Sequence[Any],
) -> tuple[tuple[dict[str, Any], ...], str]:
    rows = _fetch_dict_rows(conn, query, params)
    digest = str(rows[0]["__source_digest"]) if rows else _EMPTY_MD5
    return (
        tuple(
            {key: value for key, value in row.items() if key != "__source_digest"}
            for row in rows
        ),
        digest,
    )


def _load_market_sessions_unlocked(conn: Any) -> AdjustedMarketSessions:
    rows = conn.execute(
        """
        WITH selected AS (
            SELECT date FROM topix_data WHERE date IS NOT NULL ORDER BY date
        )
        SELECT date,
               md5(string_agg(to_json(selected), '|' ORDER BY date) OVER ())
                   AS __source_digest
        FROM selected ORDER BY date
        """
    ).fetchall()
    return AdjustedMarketSessions(
        sessions=tuple(str(row[0]) for row in rows),
        fingerprint=str(rows[0][1]) if rows else _EMPTY_MD5,
    )


def _load_market_sessions_fingerprint_unlocked(conn: Any) -> str:
    row = conn.execute(
        f"""
        WITH selected AS (
            SELECT date FROM topix_data WHERE date IS NOT NULL ORDER BY date
        )
        SELECT coalesce(md5(string_agg(to_json(selected), '|' ORDER BY date)),
                        '{_EMPTY_MD5}')
        FROM selected
        """
    ).fetchone()
    return str(row[0])


def _combine_source_fingerprints(*fingerprints: str) -> str:
    encoded = json.dumps(fingerprints, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _source_digest_sql(placeholders: str) -> str:
    return f"""
        WITH raw_source AS (
            SELECT * FROM stock_data_raw WHERE code IN ({placeholders})
        ), raw_normalized AS (
            SELECT *,
                CASE WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                     THEN left(code, length(code) - 1) ELSE code END AS normalized_code,
                ROW_NUMBER() OVER (
                    PARTITION BY CASE
                        WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                        THEN left(code, length(code) - 1) ELSE code END, date
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS alias_rank
            FROM raw_source
        ), raw_selected AS (
            SELECT normalized_code AS code, date, open, high, low, close, volume,
                   adjustment_factor
            FROM raw_normalized WHERE alias_rank = 1
        ), statement_source AS (
            SELECT * FROM statements WHERE code IN ({placeholders})
        ), statement_normalized AS (
            SELECT *,
                CASE WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                     THEN left(code, length(code) - 1) ELSE code END AS normalized_code,
                ROW_NUMBER() OVER (
                    PARTITION BY CASE
                        WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                        THEN left(code, length(code) - 1) ELSE code END, disclosed_date
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS alias_rank
            FROM statement_source
        ), statement_selected AS (
            SELECT * EXCLUDE (code, alias_rank, normalized_code), normalized_code AS code
            FROM statement_normalized WHERE alias_rank = 1
        )
        SELECT
            coalesce((
                SELECT md5(string_agg(to_json(raw_selected), '|' ORDER BY code, date))
                FROM raw_selected
            ), '{_EMPTY_MD5}'),
            coalesce((
                SELECT md5(string_agg(to_json(statement_selected), '|'
                                      ORDER BY code, disclosed_date))
                FROM statement_selected
            ), '{_EMPTY_MD5}')
    """


def _values_distinct(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is not right
    if isinstance(left, float) and isinstance(right, float):
        if math.isnan(left) and math.isnan(right):
            return False
    return left != right


def _basis_snapshots_equal(
    left: BasisSnapshot | None,
    right: BasisSnapshot | None,
) -> bool:
    if left is None or right is None:
        return left is right
    if set(left.basis) != set(right.basis) or any(
        _values_distinct(left.basis[key], right.basis[key]) for key in left.basis
    ):
        return False
    for left_rows, right_rows in (
        (left.segments, right.segments),
        (left.statement_rows, right.statement_rows),
        (left.valuation_rows, right.valuation_rows),
    ):
        columns = tuple(sorted({key for row in left_rows + right_rows for key in row}))
        if _canonical_dict_rows(left_rows, columns) != _canonical_dict_rows(
            right_rows, columns
        ):
            return False
    return True


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
