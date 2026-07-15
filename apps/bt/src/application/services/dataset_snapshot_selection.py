"""Select one immutable Dataset universe from the Market v4 snapshot."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from typing import Any, Protocol

from src.infrastructure.db.market.query_helpers import (
    expand_stock_code,
    normalize_stock_code,
)


class DatasetSelectionSource(Protocol):
    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[Any]: ...


class DatasetSnapshotSelectionError(RuntimeError):
    """The pinned Market snapshot cannot define a Dataset universe."""


def _canonical_date(value: object, *, field: str) -> str:
    text = str(value or "")
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise DatasetSnapshotSelectionError(
            f"{field} must be a canonical ISO YYYY-MM-DD date: {text or '<empty>'}"
        ) from exc
    if parsed.isoformat() != text:
        raise DatasetSnapshotSelectionError(
            f"{field} must be a canonical ISO YYYY-MM-DD date: {text}"
        )
    return text


def load_global_cutoff(source: DatasetSelectionSource) -> str:
    rows = source.query("SELECT max(date) AS cutoff FROM topix_data")
    cutoff = rows[0]["cutoff"] if rows else None
    if cutoff is None:
        raise DatasetSnapshotSelectionError(
            "Dataset global TOPIX frontier is missing; sync topix_data before dataset creation"
        )
    return _canonical_date(cutoff, field="Dataset global TOPIX frontier")


def _row_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    keys = getattr(row, "keys", None)
    if callable(keys):
        row_keys = keys()
        if isinstance(row_keys, Iterable):
            return {str(key): row[key] for key in row_keys}
    raise TypeError(f"Unsupported Dataset selection row: {type(row)!r}")


def load_cutoff_stock_master(
    source: DatasetSelectionSource, cutoff: str
) -> list[dict[str, Any]]:
    cutoff = _canonical_date(cutoff, field="Dataset cutoff")
    rows = source.query(
        """
        WITH ranked AS (
            SELECT
                CASE
                    WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                    THEN left(code, length(code) - 1)
                    ELSE code
                END AS normalized_code,
                company_name, company_name_english, market_code, market_name,
                sector_17_code, sector_17_name, sector_33_code, sector_33_name,
                scale_category, listed_date,
                row_number() OVER (
                    PARTITION BY CASE
                        WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                        THEN left(code, length(code) - 1)
                        ELSE code
                    END
                    ORDER BY CASE
                        WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                        THEN 1 ELSE 0 END, code
                ) AS source_rank
            FROM stock_master_daily
            WHERE date = ?
        )
        SELECT normalized_code, company_name, company_name_english,
               market_code, market_name, sector_17_code, sector_17_name,
               sector_33_code, sector_33_name, scale_category, listed_date
        FROM ranked
        WHERE source_rank = 1
        ORDER BY normalized_code
        """,
        (cutoff,),
    )
    if not rows:
        raise DatasetSnapshotSelectionError(
            f"No exact stock_master_daily rows exist at global cutoff {cutoff}; "
            "sync the cutoff-day stock master before dataset creation"
        )
    result: list[dict[str, Any]] = []
    for raw in rows:
        row = _row_dict(raw)
        listed = str(row.get("listed_date") or "")
        if listed:
            listed = _canonical_date(
                listed,
                field=f"stock_master_daily listed_date for {row['normalized_code']}",
            )
            if listed > cutoff:
                raise DatasetSnapshotSelectionError(
                    "stock_master_daily listed_date cannot be after Dataset cutoff: "
                    f"{row['normalized_code']} {listed} > {cutoff}"
                )
        result.append(
            {
                "Code": expand_stock_code(str(row["normalized_code"])),
                "CoName": str(row.get("company_name") or ""),
                "CoNameEn": row.get("company_name_english"),
                "Mkt": str(row.get("market_code") or ""),
                "MktNm": str(row.get("market_name") or ""),
                "S17": str(row.get("sector_17_code") or ""),
                "S17Nm": str(row.get("sector_17_name") or ""),
                "S33": str(row.get("sector_33_code") or ""),
                "S33Nm": str(row.get("sector_33_name") or ""),
                "ScaleCat": row.get("scale_category"),
                "Date": listed,
            }
        )
    return result


def load_selected_price_range(
    source: DatasetSelectionSource,
    normalized_codes: Sequence[str],
    cutoff: str,
) -> tuple[str, str]:
    cutoff = _canonical_date(cutoff, field="Dataset cutoff")
    codes = sorted(
        {normalize_stock_code(str(code)) for code in normalized_codes if str(code)}
    )
    if not codes:
        raise DatasetSnapshotSelectionError(
            "No selected stock codes are available for Dataset price coverage"
        )
    values = ", ".join("(?)" for _ in codes)
    rows = source.query(
        f"""
        WITH selected_codes(code) AS (VALUES {values}),
        complete_selected_prices AS (
            SELECT raw.date
            FROM stock_data_raw AS raw
            JOIN selected_codes AS selected
              ON CASE
                    WHEN length(raw.code) IN (5, 6) AND right(raw.code, 1) = '0'
                    THEN left(raw.code, length(raw.code) - 1)
                    ELSE raw.code
                 END = selected.code
            WHERE raw.date <= ?
              AND raw.open IS NOT NULL AND raw.high IS NOT NULL
              AND raw.low IS NOT NULL AND raw.close IS NOT NULL
              AND raw.volume IS NOT NULL
        )
        SELECT min(date) AS date_from FROM complete_selected_prices
        """,
        (*codes, cutoff),
    )
    date_from = _row_dict(rows[0]).get("date_from") if rows else None
    if date_from is None:
        raise DatasetSnapshotSelectionError(
            "No complete selected stock_data_raw prices exist through global cutoff "
            f"{cutoff}; sync or repair selected stock prices before dataset creation"
        )
    return (
        _canonical_date(date_from, field="Dataset selected price start"),
        cutoff,
    )
