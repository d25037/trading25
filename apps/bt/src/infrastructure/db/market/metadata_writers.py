"""Differential metadata and index-master writers for MarketDb."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from src.infrastructure.db.market.market_mutations import (
    MarketMutationStats,
    SemanticDeltaResult,
    deterministic_last_wins,
)

Execute = Callable[[str, list[Any] | tuple[Any, ...] | None], Any]

_INDEX_COLUMNS = (
    "code",
    "name",
    "name_english",
    "category",
    "data_start_date",
    "created_at",
    "updated_at",
)
_INDEX_SEMANTIC_COLUMNS = ("name", "name_english", "category", "data_start_date")


def set_sync_metadata(execute: Execute, key: str, value: str) -> None:
    execute(
        """
        INSERT INTO sync_metadata (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT (key) DO UPDATE
        SET value = excluded.value,
            updated_at = excluded.updated_at
        WHERE sync_metadata.value IS DISTINCT FROM excluded.value
        """,
        [key, value, datetime.now(UTC).isoformat()],
    )


def upsert_index_master(
    conn: Any, lock: Any, rows: list[dict[str, Any]]
) -> SemanticDeltaResult:
    valid = [
        dict(row, code=str(row.get("code") or ""))
        for row in rows
        if str(row.get("code") or "")
    ]
    if not valid:
        return SemanticDeltaResult.empty()
    deduped = deterministic_last_wins(valid, key_columns=("code",))
    codes = [str(row["code"]) for row in deduped]
    placeholders = ", ".join("?" for _ in codes)
    with lock:
        existing_rows = conn.execute(
            f"SELECT {', '.join(_INDEX_COLUMNS)} FROM index_master WHERE code IN ({placeholders})",
            codes,
        ).fetchall()
        existing = {
            str(row[0]): dict(zip(_INDEX_COLUMNS, row, strict=True))
            for row in existing_rows
        }
        inserted: list[dict[str, Any]] = []
        updated: list[dict[str, Any]] = []
        unchanged = len(valid) - len(deduped)
        for incoming in deduped:
            current = existing.get(str(incoming["code"]))
            if current is None:
                inserted.append(incoming)
                continue
            merged = dict(incoming)
            incoming_start = incoming.get("data_start_date")
            current_start = current.get("data_start_date")
            if current_start is not None and (
                incoming_start is None or current_start <= incoming_start
            ):
                merged["data_start_date"] = current_start
            if all(
                current.get(column) == merged.get(column)
                for column in _INDEX_SEMANTIC_COLUMNS
            ):
                unchanged += 1
            else:
                updated.append(merged)
        changed = inserted + updated
        if changed:
            now = datetime.now(UTC).isoformat()
            conn.executemany(
                f"""
                INSERT INTO index_master ({", ".join(_INDEX_COLUMNS)})
                VALUES ({", ".join("?" for _ in _INDEX_COLUMNS)})
                ON CONFLICT (code) DO UPDATE SET
                    name=excluded.name,
                    name_english=excluded.name_english,
                    category=excluded.category,
                    data_start_date=excluded.data_start_date,
                    updated_at=excluded.updated_at
                """,
                [
                    (
                        row.get("code"),
                        row.get("name"),
                        row.get("name_english"),
                        row.get("category"),
                        row.get("data_start_date"),
                        row.get("created_at"),
                        now,
                    )
                    for row in changed
                ],
            )
    return SemanticDeltaResult(
        stats=MarketMutationStats(
            input=len(valid),
            inserted=len(inserted),
            updated=len(updated),
            unchanged=unchanged,
            deleted=0,
        ),
        inserted_keys=tuple((row["code"],) for row in inserted),
        updated_keys=tuple((row["code"],) for row in updated),
        affected_codes=frozenset(str(row["code"]) for row in changed),
    )
