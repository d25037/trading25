"""Current-provider-basis adjusted fundamentals writer helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import math
from typing import Any

import pandas as pd

from src.infrastructure.db.market.market_mutations import MarketMutationStats
from src.infrastructure.db.market.market_schema import (
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

    payload = (
        tuple(
            _fingerprint_scalar(window.get(column))
            for column in (
                "coverage_start",
                "coverage_end",
                "provider_as_of",
                "source_fingerprint",
            )
        ),
        _canonical_dict_rows(
            statement_rows,
            tuple(sorted({key for row in statement_rows for key in row})),
        ),
        _canonical_dict_rows(
            adjustment_events,
            tuple(sorted({key for row in adjustment_events for key in row})),
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
