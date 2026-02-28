"""Ingestion Pipeline Utilities.

Sync/Data build で共通の `fetch -> normalize -> validate -> publish -> index` ステージを
明示的に実行するための軽量ランナー。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from loguru import logger

Row = dict[str, Any]
TRow = TypeVar("TRow", bound=Row)


@dataclass(frozen=True)
class IngestionBatchResult:
    """単一バッチ実行結果。"""

    fetched_count: int
    normalized_count: int
    validated_count: int
    published_count: int
    rows: list[Row]


async def run_ingestion_batch(
    *,
    stage: str,
    fetch: Callable[[], Awaitable[list[Row]]],
    normalize: Callable[[list[Row]], list[Row]],
    validate: Callable[[list[Row]], list[Row]],
    publish: Callable[[list[Row]], Awaitable[int]],
    index: Callable[[list[Row]], Awaitable[None]] | None = None,
) -> IngestionBatchResult:
    """1バッチを5段階で処理する。"""
    fetched_rows = await fetch()
    normalized_rows = normalize(fetched_rows)
    validated_rows = validate(normalized_rows)

    published_count = 0
    if validated_rows:
        published_count = await publish(validated_rows)

    if index is not None:
        await index(validated_rows)

    return IngestionBatchResult(
        fetched_count=len(fetched_rows),
        normalized_count=len(normalized_rows),
        validated_count=len(validated_rows),
        published_count=published_count,
        rows=validated_rows,
    )


def validate_rows_required_fields(
    rows: list[TRow],
    *,
    required_fields: tuple[str, ...],
    dedupe_keys: tuple[str, ...] | None = None,
    stage: str,
) -> list[TRow]:
    """必須フィールド検証 + キー重複除去。"""
    valid_rows: list[TRow] = []
    missing_count = 0

    for row in rows:
        missing = [field for field in required_fields if _is_missing(row.get(field))]
        if missing:
            missing_count += 1
            continue
        valid_rows.append(row)

    if missing_count > 0:
        logger.warning(
            "Stage '{}' skipped {} rows with missing required fields {}",
            stage,
            missing_count,
            required_fields,
        )

    if dedupe_keys is None:
        return valid_rows

    deduped: list[TRow] = []
    seen: set[tuple[str, ...]] = set()
    duplicate_count = 0

    for row in valid_rows:
        row_key = _build_row_key(row, dedupe_keys)
        if row_key is None:
            # required_fields は通過しているが key が欠けるケースを防御
            duplicate_count += 1
            continue
        if row_key in seen:
            duplicate_count += 1
            continue
        seen.add(row_key)
        deduped.append(row)

    if duplicate_count > 0:
        logger.warning(
            "Stage '{}' removed {} duplicate rows for keys {}",
            stage,
            duplicate_count,
            dedupe_keys,
        )

    return deduped


def passthrough_rows(rows: list[TRow]) -> list[TRow]:
    return rows


def _build_row_key(row: dict[str, Any], keys: tuple[str, ...]) -> tuple[str, ...] | None:
    values: list[str] = []
    for key in keys:
        value = row.get(key)
        if _is_missing(value):
            return None
        values.append(str(value))
    return tuple(values)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False
