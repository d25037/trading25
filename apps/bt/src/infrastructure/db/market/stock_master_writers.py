"""Differential stock-master publication for MarketDb."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from src.infrastructure.db.market.market_mutations import (
    MarketMutationStats,
    SemanticDeltaResult,
    deterministic_last_wins,
)
from src.infrastructure.db.market.market_schema import (
    STOCK_MASTER_DAILY_COLUMNS,
    STOCK_MASTER_DAILY_RELATION,
)

_TOPIX500_INDEX_CODE = "TOPIX500"
_TOPIX500_SCALE_CATEGORIES = frozenset(
    {"TOPIX Core30", "TOPIX Large70", "TOPIX Mid400"}
)
_DAILY_SEMANTIC_COLUMNS = tuple(
    column
    for column in STOCK_MASTER_DAILY_COLUMNS
    if column not in {"date", "code", "created_at"}
)
_MASTER_VALUE_COLUMNS = (
    "company_name",
    "company_name_english",
    "market_code",
    "market_name",
    "sector_17_code",
    "sector_17_name",
    "sector_33_code",
    "sector_33_name",
    "scale_category",
    "listed_date",
)
_INTERVAL_COLUMNS = (
    "code",
    "valid_from",
    "valid_to",
    "fingerprint",
    *_MASTER_VALUE_COLUMNS,
    "created_at",
)
_LATEST_COLUMNS = (
    "code",
    *_MASTER_VALUE_COLUMNS,
    "source_date",
    "created_at",
    "updated_at",
)


@dataclass(frozen=True, slots=True)
class StockMasterPublicationResult:
    daily: SemanticDeltaResult
    membership: SemanticDeltaResult
    intervals: SemanticDeltaResult
    stocks_latest: SemanticDeltaResult
    stocks: SemanticDeltaResult
    derived_evaluated: bool

    @property
    def mutated_rows(self) -> int:
        return sum(
            result.mutated_rows
            for result in (
                self.daily,
                self.membership,
                self.intervals,
                self.stocks_latest,
                self.stocks,
            )
        )

    @classmethod
    def empty(cls, *, input_count: int = 0) -> StockMasterPublicationResult:
        return cls(
            daily=SemanticDeltaResult.empty(input_count=input_count),
            membership=SemanticDeltaResult.empty(),
            intervals=SemanticDeltaResult.empty(),
            stocks_latest=SemanticDeltaResult.empty(),
            stocks=SemanticDeltaResult.empty(),
            derived_evaluated=False,
        )


@dataclass(frozen=True, slots=True)
class StockMasterFrontierResult:
    stocks_latest: SemanticDeltaResult
    stocks: SemanticDeltaResult

    @property
    def mutated_rows(self) -> int:
        return self.stocks_latest.mutated_rows + self.stocks.mutated_rows

    @classmethod
    def empty(cls) -> StockMasterFrontierResult:
        return cls(SemanticDeltaResult.empty(), SemanticDeltaResult.empty())


def _stats(
    *, input_count: int, inserted: int, updated: int, unchanged: int, deleted: int = 0
) -> MarketMutationStats:
    return MarketMutationStats(input_count, inserted, updated, unchanged, deleted)


def _row_dict(columns: tuple[str, ...], row: tuple[Any, ...]) -> dict[str, Any]:
    return dict(zip(columns, row, strict=True))


def _same(
    left: dict[str, Any], right: dict[str, Any], columns: tuple[str, ...]
) -> bool:
    return all(left.get(column) == right.get(column) for column in columns)


def _placeholders(values: set[str] | frozenset[str]) -> str:
    return ", ".join("?" for _ in values)


def _daily_delta(
    conn: Any,
    rows: list[dict[str, Any]],
) -> tuple[SemanticDeltaResult, list[dict[str, Any]]]:
    valid = [
        dict(row, date=str(row.get("date") or ""), code=str(row.get("code") or ""))
        for row in rows
        if str(row.get("date") or "") and str(row.get("code") or "")
    ]
    if not valid:
        return SemanticDeltaResult.empty(), []
    deduped = deterministic_last_wins(valid, key_columns=("date", "code"))
    dataframe = pd.DataFrame.from_records(
        [
            {column: row.get(column) for column in STOCK_MASTER_DAILY_COLUMNS}
            for row in deduped
        ],
        columns=STOCK_MASTER_DAILY_COLUMNS,
    )
    conn.register(STOCK_MASTER_DAILY_RELATION, dataframe)
    try:
        existing_rows = conn.execute(
            f"""
            SELECT {", ".join(f"t.{column}" for column in STOCK_MASTER_DAILY_COLUMNS)}
            FROM stock_master_daily t
            JOIN {STOCK_MASTER_DAILY_RELATION} s USING (date, code)
            """
        ).fetchall()
    finally:
        conn.unregister(STOCK_MASTER_DAILY_RELATION)
    existing = {
        (str(row[0]), str(row[1])): _row_dict(STOCK_MASTER_DAILY_COLUMNS, row)
        for row in existing_rows
    }
    inserted: list[dict[str, Any]] = []
    updated: list[dict[str, Any]] = []
    unchanged = len(valid) - len(deduped)
    for row in deduped:
        current = existing.get((row["date"], row["code"]))
        if current is None:
            inserted.append(row)
        elif _same(current, row, _DAILY_SEMANTIC_COLUMNS):
            unchanged += 1
        else:
            updated.append(row)
    changed = inserted + updated
    result = SemanticDeltaResult(
        stats=_stats(
            input_count=len(valid),
            inserted=len(inserted),
            updated=len(updated),
            unchanged=unchanged,
        ),
        inserted_keys=tuple((row["date"], row["code"]) for row in inserted),
        updated_keys=tuple((row["date"], row["code"]) for row in updated),
        affected_dates=frozenset(str(row["date"]) for row in changed),
        affected_codes=frozenset(str(row["code"]) for row in changed),
    )
    return result, changed


def _apply_daily(conn: Any, rows: list[dict[str, Any]]) -> None:
    dataframe = pd.DataFrame.from_records(
        [
            {column: row.get(column) for column in STOCK_MASTER_DAILY_COLUMNS}
            for row in rows
        ],
        columns=STOCK_MASTER_DAILY_COLUMNS,
    )
    conn.register(STOCK_MASTER_DAILY_RELATION, dataframe)
    update_clause = ", ".join(
        f"{column} = excluded.{column}" for column in _DAILY_SEMANTIC_COLUMNS
    )
    try:
        conn.execute(
            f"""
            INSERT INTO stock_master_daily ({", ".join(STOCK_MASTER_DAILY_COLUMNS)})
            SELECT {", ".join(STOCK_MASTER_DAILY_COLUMNS)} FROM {STOCK_MASTER_DAILY_RELATION}
            ON CONFLICT (date, code) DO UPDATE SET {update_clause}
            """
        )
    finally:
        conn.unregister(STOCK_MASTER_DAILY_RELATION)


def _membership_delta(conn: Any, dates: frozenset[str]) -> SemanticDeltaResult:
    if not dates:
        return SemanticDeltaResult.empty()
    values = sorted(dates)
    placeholders = ", ".join("?" for _ in values)
    desired = {
        (str(row[0]), _TOPIX500_INDEX_CODE, str(row[1]))
        for row in conn.execute(
            f"""
            SELECT date, code FROM stock_master_daily
            WHERE date IN ({placeholders})
              AND coalesce(scale_category, '') IN ({", ".join("?" for _ in _TOPIX500_SCALE_CATEGORIES)})
            """,
            [*values, *_TOPIX500_SCALE_CATEGORIES],
        ).fetchall()
    }
    existing = {
        (str(row[0]), str(row[1]), str(row[2]))
        for row in conn.execute(
            f"""
            SELECT date, index_code, code FROM index_membership_daily
            WHERE index_code = ? AND date IN ({placeholders})
            """,
            [_TOPIX500_INDEX_CODE, *values],
        ).fetchall()
    }
    inserted = sorted(desired - existing)
    deleted = sorted(existing - desired)
    return SemanticDeltaResult(
        stats=_stats(
            input_count=len(desired),
            inserted=len(inserted),
            updated=0,
            unchanged=len(desired & existing),
            deleted=len(deleted),
        ),
        inserted_keys=tuple(inserted),
        deleted_keys=tuple(deleted),
        affected_dates=frozenset(key[0] for key in (*inserted, *deleted)),
        affected_codes=frozenset(key[2] for key in (*inserted, *deleted)),
    )


def _apply_membership(conn: Any, delta: SemanticDeltaResult) -> None:
    if delta.deleted_keys:
        conn.executemany(
            "DELETE FROM index_membership_daily WHERE date=? AND index_code=? AND code=?",
            list(delta.deleted_keys),
        )
    if delta.inserted_keys:
        now = datetime.now(UTC).isoformat()
        conn.executemany(
            "INSERT INTO index_membership_daily VALUES (?, ?, ?, ?)",
            [(*key, now) for key in delta.inserted_keys],
        )


def _desired_intervals(conn: Any, codes: frozenset[str]) -> list[dict[str, Any]]:
    values = sorted(codes)
    rows = conn.execute(
        f"""
        WITH cleaned AS (
            SELECT *, CASE WHEN listed_date IS NULL OR listed_date = date THEN '' ELSE listed_date END AS stable_listed_date
            FROM stock_master_daily WHERE code IN ({_placeholders(codes)})
        ), fingerprinted AS (
            SELECT *, md5(concat_ws('|', coalesce(company_name, ''), coalesce(company_name_english, ''),
                coalesce(market_code, ''), coalesce(market_name, ''), coalesce(sector_17_code, ''),
                coalesce(sector_17_name, ''), coalesce(sector_33_code, ''), coalesce(sector_33_name, ''),
                coalesce(scale_category, ''), stable_listed_date)) AS fingerprint
            FROM cleaned
        ), marked AS (
            SELECT *, CASE WHEN lag(fingerprint) OVER (PARTITION BY code ORDER BY date) = fingerprint THEN 0 ELSE 1 END AS starts_new_group
            FROM fingerprinted
        ), grouped AS (
            SELECT *, sum(starts_new_group) OVER (PARTITION BY code ORDER BY date) AS interval_group FROM marked
        )
        SELECT code, min(date), max(date), fingerprint, any_value(company_name),
            any_value(company_name_english), any_value(market_code), any_value(market_name),
            any_value(sector_17_code), any_value(sector_17_name), any_value(sector_33_code),
            any_value(sector_33_name), any_value(scale_category), any_value(stable_listed_date),
            max(created_at)
        FROM grouped GROUP BY code, interval_group, fingerprint
        """,
        values,
    ).fetchall()
    return [_row_dict(_INTERVAL_COLUMNS, row) for row in rows]


def _interval_delta(
    conn: Any, codes: frozenset[str]
) -> tuple[SemanticDeltaResult, list[dict[str, Any]]]:
    if not codes:
        return SemanticDeltaResult.empty(), []
    desired_rows = _desired_intervals(conn, codes)
    existing_rows = [
        _row_dict(_INTERVAL_COLUMNS, row)
        for row in conn.execute(
            f"SELECT {', '.join(_INTERVAL_COLUMNS)} FROM stock_master_intervals WHERE code IN ({_placeholders(codes)})",
            sorted(codes),
        ).fetchall()
    ]

    def key(row: dict[str, Any]) -> tuple[Any, Any, Any]:
        return row["code"], row["valid_from"], row["fingerprint"]

    desired = {key(row): row for row in desired_rows}
    existing = {key(row): row for row in existing_rows}
    inserted = [desired[value] for value in desired.keys() - existing.keys()]
    deleted = sorted(existing.keys() - desired.keys())
    updated = [
        desired[value]
        for value in desired.keys() & existing.keys()
        if not _same(
            existing[value],
            desired[value],
            tuple(c for c in _INTERVAL_COLUMNS if c != "created_at"),
        )
    ]
    unchanged = len(desired) - len(inserted) - len(updated)
    delta = SemanticDeltaResult(
        stats=_stats(
            input_count=len(desired),
            inserted=len(inserted),
            updated=len(updated),
            unchanged=unchanged,
            deleted=len(deleted),
        ),
        inserted_keys=tuple(key(row) for row in inserted),
        updated_keys=tuple(key(row) for row in updated),
        deleted_keys=tuple(deleted),
        affected_codes=codes,
    )
    return delta, inserted + updated


def _apply_intervals(
    conn: Any, delta: SemanticDeltaResult, changed: list[dict[str, Any]]
) -> None:
    if delta.deleted_keys:
        conn.executemany(
            "DELETE FROM stock_master_intervals WHERE code=? AND valid_from=? AND fingerprint=?",
            list(delta.deleted_keys),
        )
    if changed:
        update_columns = tuple(
            column
            for column in _INTERVAL_COLUMNS
            if column not in {"code", "valid_from", "fingerprint", "created_at"}
        )
        conn.executemany(
            f"""
            INSERT INTO stock_master_intervals ({", ".join(_INTERVAL_COLUMNS)})
            VALUES ({", ".join("?" for _ in _INTERVAL_COLUMNS)})
            ON CONFLICT (code, valid_from, fingerprint) DO UPDATE SET
            {", ".join(f"{column}=excluded.{column}" for column in update_columns)}
            """,
            [tuple(row.get(column) for column in _INTERVAL_COLUMNS) for row in changed],
        )


def _latest_desired(conn: Any, codes: frozenset[str]) -> list[dict[str, Any]]:
    latest = conn.execute("SELECT MAX(date) FROM stock_master_daily").fetchone()
    if not latest or latest[0] is None:
        return []
    rows = conn.execute(
        f"""
        SELECT code, {", ".join(_MASTER_VALUE_COLUMNS)}, date AS source_date, created_at, NULL AS updated_at
        FROM stock_master_daily WHERE date=? AND code IN ({_placeholders(codes)})
        """,
        [str(latest[0]), *sorted(codes)],
    ).fetchall()
    return [_row_dict(_LATEST_COLUMNS, row) for row in rows]


def _frontier_desired(conn: Any, snapshot_date: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT code, {", ".join(_MASTER_VALUE_COLUMNS)}, date AS source_date,
            created_at, NULL AS updated_at
        FROM stock_master_daily WHERE date=?
        """,
        [snapshot_date],
    ).fetchall()
    return [_row_dict(_LATEST_COLUMNS, row) for row in rows]


def _latest_delta(
    conn: Any,
    table: str,
    desired_rows: list[dict[str, Any]],
    *,
    complete_snapshot: bool = False,
) -> tuple[SemanticDeltaResult, list[dict[str, Any]]]:
    if not desired_rows and not complete_snapshot:
        return SemanticDeltaResult.empty(), []
    codes = frozenset(str(row["code"]) for row in desired_rows)
    table_columns = (
        _LATEST_COLUMNS
        if table == "stocks_latest"
        else ("code", *_MASTER_VALUE_COLUMNS, "created_at", "updated_at")
    )
    where_clause = ""
    params: list[str] = []
    if not complete_snapshot:
        where_clause = f" WHERE code IN ({_placeholders(codes)})"
        params = sorted(codes)
    existing_rows = [
        _row_dict(table_columns, row)
        for row in conn.execute(
            f"SELECT {', '.join(table_columns)} FROM {table}{where_clause}",
            params,
        ).fetchall()
    ]
    desired = {str(row["code"]): row for row in desired_rows}
    existing = {str(row["code"]): row for row in existing_rows}
    semantic = tuple(
        column for column in table_columns if column not in {"created_at", "updated_at"}
    )
    inserted = [row for code, row in desired.items() if code not in existing]
    updated = [
        row
        for code, row in desired.items()
        if code in existing and not _same(existing[code], row, semantic)
    ]
    deleted = sorted((code,) for code in existing.keys() - desired.keys())
    unchanged = len(desired) - len(inserted) - len(updated)
    delta = SemanticDeltaResult(
        stats=_stats(
            input_count=len(desired),
            inserted=len(inserted),
            updated=len(updated),
            unchanged=unchanged,
            deleted=len(deleted),
        ),
        inserted_keys=tuple((row["code"],) for row in inserted),
        updated_keys=tuple((row["code"],) for row in updated),
        deleted_keys=tuple(deleted),
        affected_codes=frozenset(
            [str(row["code"]) for row in (*inserted, *updated)]
            + [str(key[0]) for key in deleted]
        ),
    )
    return delta, inserted + updated


def _apply_latest(
    conn: Any,
    table: str,
    delta: SemanticDeltaResult,
    changed: list[dict[str, Any]],
) -> None:
    if delta.deleted_keys:
        conn.executemany(f"DELETE FROM {table} WHERE code=?", list(delta.deleted_keys))
    if not changed:
        return
    now = datetime.now(UTC).isoformat()
    columns = (
        _LATEST_COLUMNS
        if table == "stocks_latest"
        else ("code", *_MASTER_VALUE_COLUMNS, "created_at", "updated_at")
    )
    semantic = tuple(
        column
        for column in columns
        if column not in {"code", "created_at", "updated_at"}
    )
    conn.executemany(
        f"""
        INSERT INTO {table} ({", ".join(columns)}) VALUES ({", ".join("?" for _ in columns)})
        ON CONFLICT (code) DO UPDATE SET
        {", ".join(f"{column}=excluded.{column}" for column in semantic)}, updated_at=excluded.updated_at
        """,
        [
            tuple(
                now if column == "updated_at" else row.get(column) for column in columns
            )
            for row in changed
        ],
    )


def upsert_stocks(
    conn: Any, lock: Any, rows: list[dict[str, Any]]
) -> SemanticDeltaResult:
    """Differential direct stocks writer used by explicit metadata callers."""
    if not rows:
        return SemanticDeltaResult.empty()
    desired = [
        dict(row, source_date=None, updated_at=None) for row in rows if row.get("code")
    ]
    if not desired:
        return SemanticDeltaResult.empty()
    with lock:
        delta, changed = _latest_delta(conn, "stocks", desired)
        _apply_latest(conn, "stocks", delta, changed)
    return delta


def publish_stock_master_daily_rows(
    conn: Any,
    lock: Any,
    rows: list[dict[str, Any]],
    *,
    derive: bool = True,
) -> StockMasterPublicationResult:
    if not rows:
        return StockMasterPublicationResult.empty()
    with lock:
        daily, changed_rows = _daily_delta(conn, rows)
        candidate_dates = frozenset(
            str(row.get("date"))
            for row in rows
            if str(row.get("date") or "") and str(row.get("code") or "")
        )
        if daily.mutated_rows == 0:
            membership = _membership_delta(conn, candidate_dates)
            if membership.mutated_rows == 0:
                return StockMasterPublicationResult(
                    daily=daily,
                    membership=membership,
                    intervals=SemanticDeltaResult.empty(),
                    stocks_latest=SemanticDeltaResult.empty(),
                    stocks=SemanticDeltaResult.empty(),
                    derived_evaluated=False,
                )
            conn.execute("BEGIN TRANSACTION")
            try:
                _apply_membership(conn, membership)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            return StockMasterPublicationResult(
                daily=daily,
                membership=membership,
                intervals=SemanticDeltaResult.empty(),
                stocks_latest=SemanticDeltaResult.empty(),
                stocks=SemanticDeltaResult.empty(),
                derived_evaluated=False,
            )
        conn.execute("BEGIN TRANSACTION")
        try:
            _apply_daily(conn, changed_rows)
            membership = _membership_delta(conn, candidate_dates)
            _apply_membership(conn, membership)
            if not derive:
                now = datetime.now(UTC).isoformat()
                conn.executemany(
                    """
                    INSERT INTO stock_master_derivation_pending VALUES (?, ?)
                    ON CONFLICT (code) DO NOTHING
                    """,
                    [(code, now) for code in sorted(daily.affected_codes)],
                )
                frontier = conn.execute("SELECT MAX(date) FROM stock_master_daily").fetchone()
                frontier_date = str(frontier[0] or "") if frontier else ""
                if frontier_date in daily.affected_dates:
                    conn.execute(
                        """
                        INSERT INTO stock_master_frontier_pending VALUES (?, ?)
                        ON CONFLICT (date) DO NOTHING
                        """,
                        [frontier_date, now],
                    )
                conn.execute("COMMIT")
                return StockMasterPublicationResult(
                    daily=daily,
                    membership=membership,
                    intervals=SemanticDeltaResult.empty(),
                    stocks_latest=SemanticDeltaResult.empty(),
                    stocks=SemanticDeltaResult.empty(),
                    derived_evaluated=False,
                )
            intervals, interval_rows = _interval_delta(conn, daily.affected_codes)
            _apply_intervals(conn, intervals, interval_rows)
            desired_latest = _latest_desired(conn, daily.affected_codes)
            latest, latest_rows = _latest_delta(conn, "stocks_latest", desired_latest)
            _apply_latest(conn, "stocks_latest", latest, latest_rows)
            stocks, stock_rows = _latest_delta(conn, "stocks", desired_latest)
            _apply_latest(conn, "stocks", stocks, stock_rows)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return StockMasterPublicationResult(
        daily=daily,
        membership=membership,
        intervals=intervals,
        stocks_latest=latest,
        stocks=stocks,
        derived_evaluated=True,
    )


def get_stock_master_pending_derivation_codes(conn: Any, lock: Any) -> set[str]:
    with lock:
        return {
            str(row[0])
            for row in conn.execute(
                "SELECT code FROM stock_master_derivation_pending"
            ).fetchall()
        }


def get_stock_master_pending_frontier_dates(conn: Any, lock: Any) -> set[str]:
    with lock:
        return {
            str(row[0])
            for row in conn.execute(
                "SELECT date FROM stock_master_frontier_pending"
            ).fetchall()
        }


def reconcile_stock_master_derived_codes(
    conn: Any, lock: Any, codes: frozenset[str]
) -> StockMasterPublicationResult:
    if not codes:
        return StockMasterPublicationResult.empty()
    with lock:
        intervals, interval_rows = _interval_delta(conn, codes)
        desired_latest = _latest_desired(conn, codes)
        latest, latest_rows = _latest_delta(conn, "stocks_latest", desired_latest)
        stocks, stock_rows = _latest_delta(conn, "stocks", desired_latest)
        conn.execute("BEGIN TRANSACTION")
        try:
            _apply_intervals(conn, intervals, interval_rows)
            _apply_latest(conn, "stocks_latest", latest, latest_rows)
            _apply_latest(conn, "stocks", stocks, stock_rows)
            conn.executemany(
                "DELETE FROM stock_master_derivation_pending WHERE code=?",
                [(code,) for code in sorted(codes)],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return StockMasterPublicationResult(
        daily=SemanticDeltaResult.empty(),
        membership=SemanticDeltaResult.empty(),
        intervals=intervals,
        stocks_latest=latest,
        stocks=stocks,
        derived_evaluated=True,
    )


def reconcile_stock_master_frontier(
    conn: Any, lock: Any, snapshot_date: str
) -> StockMasterFrontierResult:
    """Delete stale latest rows only at a caller-proven complete frontier."""
    if not snapshot_date:
        return StockMasterFrontierResult.empty()
    with lock:
        frontier = conn.execute("SELECT MAX(date) FROM stock_master_daily").fetchone()
        if not frontier or str(frontier[0] or "") != snapshot_date:
            return StockMasterFrontierResult.empty()
        desired = _frontier_desired(conn, snapshot_date)
        latest, latest_rows = _latest_delta(
            conn, "stocks_latest", desired, complete_snapshot=True
        )
        stocks, stock_rows = _latest_delta(
            conn, "stocks", desired, complete_snapshot=True
        )
        conn.execute("BEGIN TRANSACTION")
        try:
            _apply_latest(conn, "stocks_latest", latest, latest_rows)
            _apply_latest(conn, "stocks", stocks, stock_rows)
            conn.execute(
                "DELETE FROM stock_master_frontier_pending WHERE date<=?",
                [snapshot_date],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return StockMasterFrontierResult(latest, stocks)
