"""Select one immutable Dataset universe from a pinned Market v5 vintage."""

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


def load_latest_stock_master_date(source: DatasetSelectionSource) -> str:
    rows = source.query("SELECT max(date) AS latest_date FROM stock_master_daily")
    latest = rows[0]["latest_date"] if rows else None
    if latest is None:
        raise DatasetSnapshotSelectionError(
            "Dataset stock universe is missing; sync stock_master_daily before creation"
        )
    return _canonical_date(latest, field="Dataset stock universe date")


def load_global_cutoff(
    source: DatasetSelectionSource, normalized_codes: Sequence[str]
) -> str:
    codes = sorted(
        {normalize_stock_code(str(code)) for code in normalized_codes if str(code)}
    )
    if not codes:
        raise DatasetSnapshotSelectionError(
            "No selected stock codes are available for Dataset provider coverage"
        )
    values = ", ".join("(?)" for _ in codes)
    rows = source.query(
        f"""
        WITH selected_codes(code) AS (VALUES {values}),
        selected_windows AS (
            SELECT selected.code, provider_window.coverage_start,
                   provider_window.coverage_end
            FROM selected_codes AS selected
            LEFT JOIN stock_provider_windows AS provider_window
              ON CASE
                    WHEN length(provider_window.code) IN (5, 6)
                     AND right(provider_window.code, 1) = '0'
                    THEN left(provider_window.code, length(provider_window.code) - 1)
                    ELSE provider_window.code
                 END = selected.code
        )
        SELECT min(coverage_end) AS cutoff, max(coverage_start) AS lower_bound,
               count(coverage_end) AS window_count
        FROM selected_windows
        """,
        tuple(codes),
    )
    cutoff = rows[0]["cutoff"] if rows else None
    lower_bound = rows[0]["lower_bound"] if rows else None
    window_count = int(rows[0]["window_count"] or 0) if rows else 0
    if cutoff is None or lower_bound is None or window_count != len(codes):
        raise DatasetSnapshotSelectionError(
            "Dataset provider vintage is missing; refresh provider stock windows before creation"
        )
    normalized_cutoff = _canonical_date(cutoff, field="Dataset provider coverage end")
    normalized_lower = _canonical_date(
        lower_bound, field="Dataset provider coverage start"
    )
    if normalized_lower > normalized_cutoff:
        raise DatasetSnapshotSelectionError(
            "Dataset provider windows have no common effective coverage"
        )
    return normalized_cutoff


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
        coverage AS (
            SELECT selected.code,
                   provider_window.coverage_start, provider_window.coverage_end,
                   provider_window.provider_plan, provider_window.provider_as_of,
                   provider_window.source_fingerprint,
                   basis_state.fundamentals_adjustment_basis_date,
                   basis_state.source_fingerprint AS fundamentals_source_fingerprint,
                   count(raw.date) AS quote_count
            FROM selected_codes AS selected
            LEFT JOIN stock_provider_windows AS provider_window
              ON CASE
                    WHEN length(provider_window.code) IN (5, 6)
                     AND right(provider_window.code, 1) = '0'
                    THEN left(provider_window.code, length(provider_window.code) - 1)
                    ELSE provider_window.code
                 END = selected.code
            LEFT JOIN current_basis_fundamentals_state AS basis_state
              ON CASE
                    WHEN length(basis_state.code) IN (5, 6)
                     AND right(basis_state.code, 1) = '0'
                    THEN left(basis_state.code, length(basis_state.code) - 1)
                    ELSE basis_state.code
                 END = selected.code
            LEFT JOIN stock_data_raw AS raw
              ON CASE
                    WHEN length(raw.code) IN (5, 6) AND right(raw.code, 1) = '0'
                    THEN left(raw.code, length(raw.code) - 1)
                    ELSE raw.code
                 END = selected.code
             AND raw.date >= provider_window.coverage_start
             AND raw.date <= provider_window.coverage_end
              AND raw.open IS NOT NULL AND raw.high IS NOT NULL
              AND raw.low IS NOT NULL AND raw.close IS NOT NULL
              AND raw.volume IS NOT NULL
              AND raw.adjusted_open IS NOT NULL AND raw.adjusted_high IS NOT NULL
              AND raw.adjusted_low IS NOT NULL AND raw.adjusted_close IS NOT NULL
              AND raw.adjusted_volume IS NOT NULL
            GROUP BY selected.code, provider_window.coverage_start,
                     provider_window.coverage_end, provider_window.provider_plan,
                     provider_window.provider_as_of,
                     provider_window.source_fingerprint,
                     basis_state.fundamentals_adjustment_basis_date,
                     basis_state.source_fingerprint
        )
        SELECT * FROM coverage ORDER BY code
        """,
        tuple(codes),
    )
    coverage = [_row_dict(row) for row in rows]
    missing_codes = [
        str(row["code"])
        for row in coverage
        if row.get("coverage_start") is None
        or row.get("coverage_end") is None
        or int(row.get("quote_count") or 0) == 0
        or not str(row.get("provider_plan") or "").strip()
        or not str(row.get("provider_as_of") or "").strip()
        or not str(row.get("source_fingerprint") or "").strip()
        or not str(row.get("fundamentals_source_fingerprint") or "").strip()
        or not str(row.get("fundamentals_adjustment_basis_date") or "").strip()
        or str(row.get("fundamentals_adjustment_basis_date"))
        > str(row.get("coverage_end"))
    ]
    if missing_codes:
        raise DatasetSnapshotSelectionError(
            "No complete provider/current-basis coverage exists through cutoff "
            f"{cutoff} for codes: {', '.join(missing_codes)}; "
            "sync or repair those stock prices before dataset creation"
        )
    date_from = max(str(row["coverage_start"]) for row in coverage)
    date_to = min(str(row["coverage_end"]) for row in coverage)
    provider_plan_values = {str(row["provider_plan"]) for row in coverage}
    if len(provider_plan_values) != 1:
        raise DatasetSnapshotSelectionError(
            "Selected stocks do not share one provider plan"
        )
    provider_as_of_values = {str(row["provider_as_of"]) for row in coverage}
    if len(provider_as_of_values) != 1:
        raise DatasetSnapshotSelectionError(
            "Selected stocks do not share one provider as-of vintage"
        )
    if date_from > date_to or date_to != cutoff:
        raise DatasetSnapshotSelectionError(
            "Selected stocks do not share the pinned effective provider coverage"
        )
    expected_rows = source.query(
        "SELECT date FROM topix_data WHERE date BETWEEN ? AND ? ORDER BY date",
        (date_from, date_to),
    )
    expected_sessions = {str(_row_dict(row)["date"]) for row in expected_rows}
    if not expected_sessions or min(expected_sessions) != date_from or max(expected_sessions) != date_to:
        raise DatasetSnapshotSelectionError(
            "Pinned provider coverage does not have exact market sessions at both bounds"
        )
    for code in codes:
        sessions_by_table: dict[str, set[str]] = {}
        for table in (
            "stock_master_daily",
            "stock_data_raw",
            "stock_data",
            "daily_valuation",
        ):
            table_rows = source.query(
                f"""
                SELECT DISTINCT date FROM {table}
                WHERE CASE
                        WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                        THEN left(code, length(code) - 1)
                        ELSE code
                      END = ?
                  AND date BETWEEN ? AND ?
                """,
                (code, date_from, date_to),
            )
            actual_sessions = {str(_row_dict(row)["date"]) for row in table_rows}
            sessions_by_table[table] = actual_sessions
        if sessions_by_table["stock_master_daily"] != expected_sessions:
            raise DatasetSnapshotSelectionError(
                "Selected provider coverage has an empty, gap, or bound mismatch: "
                f"{code} stock_master_daily"
            )
        quote_sessions = sessions_by_table["stock_data_raw"]
        if (
            not quote_sessions
            or not quote_sessions.issubset(expected_sessions)
            or sessions_by_table["stock_data"] != quote_sessions
            or sessions_by_table["daily_valuation"] != quote_sessions
        ):
            if not quote_sessions or not quote_sessions.issubset(expected_sessions):
                table = "stock_data_raw"
            elif sessions_by_table["stock_data"] != quote_sessions:
                table = "stock_data"
            else:
                table = "daily_valuation"
            raise DatasetSnapshotSelectionError(
                "Selected provider coverage has an empty, gap, or bound mismatch: "
                f"{code} {table}"
            )
    return (
        _canonical_date(date_from, field="Dataset selected price start"),
        _canonical_date(date_to, field="Dataset selected price end"),
    )
