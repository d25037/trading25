"""Metadata and index-master writer helpers for MarketDb."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

Execute = Callable[[str, list[Any] | tuple[Any, ...] | None], Any]
ExecuteMany = Callable[[str, list[tuple[Any, ...]]], None]


def set_sync_metadata(execute: Execute, key: str, value: str) -> None:
    execute(
        """
        INSERT INTO sync_metadata (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT (key) DO UPDATE
        SET value = excluded.value,
            updated_at = excluded.updated_at
        """,
        [key, value, datetime.now().isoformat()],  # noqa: DTZ005
    )


def upsert_index_master(executemany: ExecuteMany, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    now_iso = datetime.now().isoformat()  # noqa: DTZ005
    params = [
        (
            row.get("code"),
            row.get("name"),
            row.get("name_english"),
            row.get("category"),
            row.get("data_start_date"),
            row.get("created_at"),
            now_iso,
        )
        for row in rows
    ]
    executemany(
        """
        INSERT INTO index_master (
            code, name, name_english, category, data_start_date, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (code) DO UPDATE
        SET name = excluded.name,
            name_english = excluded.name_english,
            category = excluded.category,
            data_start_date = CASE
                WHEN excluded.data_start_date IS NULL THEN index_master.data_start_date
                WHEN index_master.data_start_date IS NULL THEN excluded.data_start_date
                WHEN excluded.data_start_date < index_master.data_start_date THEN excluded.data_start_date
                ELSE index_master.data_start_date
            END,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at
        """,
        params,
    )
    return len(rows)
