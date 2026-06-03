"""Stock master writer and rebuild helpers for MarketDb."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

import pandas as pd

from src.infrastructure.db.market.market_schema import (
    STOCK_MASTER_DAILY_COLUMNS,
    STOCK_MASTER_DAILY_RELATION,
)

Execute = Callable[[str, list[Any] | tuple[Any, ...] | None], Any]
ExecuteMany = Callable[[str, list[tuple[Any, ...]]], None]
FetchAll = Callable[[str, list[Any] | tuple[Any, ...] | None], list[Any]]
CountRows = Callable[[str], int]
GetLatestDate = Callable[[], str | None]
TableExists = Callable[[str], bool]
UpsertStocks = Callable[[list[dict[str, Any]]], int]

_TOPIX500_INDEX_CODE = "TOPIX500"
_TOPIX500_SCALE_CATEGORIES = ("TOPIX Core30", "TOPIX Large70", "TOPIX Mid400")


def upsert_stocks(executemany: ExecuteMany, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    now_iso = datetime.now().isoformat()  # noqa: DTZ005
    params = [
        (
            row.get("code"),
            row.get("company_name"),
            row.get("company_name_english"),
            row.get("market_code"),
            row.get("market_name"),
            row.get("sector_17_code"),
            row.get("sector_17_name"),
            row.get("sector_33_code"),
            row.get("sector_33_name"),
            row.get("scale_category"),
            row.get("listed_date"),
            row.get("created_at"),
            now_iso,
        )
        for row in rows
    ]
    executemany(
        """
        INSERT INTO stocks (
            code, company_name, company_name_english, market_code, market_name,
            sector_17_code, sector_17_name, sector_33_code, sector_33_name,
            scale_category, listed_date, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (code) DO UPDATE
        SET company_name = excluded.company_name,
            company_name_english = excluded.company_name_english,
            market_code = excluded.market_code,
            market_name = excluded.market_name,
            sector_17_code = excluded.sector_17_code,
            sector_17_name = excluded.sector_17_name,
            sector_33_code = excluded.sector_33_code,
            sector_33_name = excluded.sector_33_name,
            scale_category = excluded.scale_category,
            listed_date = excluded.listed_date,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at
        """,
        params,
    )
    return len(rows)


def upsert_stock_master_daily(
    executemany: ExecuteMany,
    snapshot_date: str,
    rows: list[dict[str, Any]],
) -> int:
    if not rows:
        return 0
    params = [
        (
            snapshot_date,
            row.get("code"),
            row.get("company_name"),
            row.get("company_name_english"),
            row.get("market_code"),
            row.get("market_name"),
            row.get("sector_17_code"),
            row.get("sector_17_name"),
            row.get("sector_33_code"),
            row.get("sector_33_name"),
            row.get("scale_category"),
            row.get("listed_date"),
            row.get("created_at"),
        )
        for row in rows
    ]
    executemany(
        """
        INSERT INTO stock_master_daily (
            date, code, company_name, company_name_english, market_code, market_name,
            sector_17_code, sector_17_name, sector_33_code, sector_33_name,
            scale_category, listed_date, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (date, code) DO UPDATE
        SET company_name = excluded.company_name,
            company_name_english = excluded.company_name_english,
            market_code = excluded.market_code,
            market_name = excluded.market_name,
            sector_17_code = excluded.sector_17_code,
            sector_17_name = excluded.sector_17_name,
            sector_33_code = excluded.sector_33_code,
            sector_33_name = excluded.sector_33_name,
            scale_category = excluded.scale_category,
            listed_date = excluded.listed_date,
            created_at = excluded.created_at
        """,
        params,
    )
    return len(rows)


def upsert_stock_master_daily_rows(conn: Any, lock: Any, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        row_date = str(row.get("date") or "")
        code = str(row.get("code") or "")
        if not row_date or not code:
            continue
        deduped[(row_date, code)] = row
    if not deduped:
        return 0

    dataframe = pd.DataFrame.from_records(
        [
            {column: row.get(column) for column in STOCK_MASTER_DAILY_COLUMNS}
            for row in deduped.values()
        ],
        columns=STOCK_MASTER_DAILY_COLUMNS,
    )
    columns_sql = ", ".join(STOCK_MASTER_DAILY_COLUMNS)
    update_columns = [
        column for column in STOCK_MASTER_DAILY_COLUMNS if column not in {"date", "code"}
    ]
    update_clause = ", ".join(f"{column} = excluded.{column}" for column in update_columns)
    with lock:
        conn.register(STOCK_MASTER_DAILY_RELATION, dataframe)
        try:
            conn.execute(
                f"""
                INSERT INTO stock_master_daily ({columns_sql})
                SELECT {columns_sql} FROM {STOCK_MASTER_DAILY_RELATION}
                ON CONFLICT (date, code) DO UPDATE SET {update_clause}
                """
            )
        finally:
            conn.unregister(STOCK_MASTER_DAILY_RELATION)
    return len(deduped)


def rebuild_topix500_membership(
    conn: Any,
    lock: Any,
    *,
    dates: list[str] | None = None,
) -> int:
    date_values = sorted({str(date) for date in dates or [] if str(date)})
    date_filter = ""
    if date_values:
        date_filter = f" AND date IN ({', '.join('?' for _ in date_values)})"

    now_iso = datetime.now().isoformat()  # noqa: DTZ005
    category_placeholders = ", ".join("?" for _ in _TOPIX500_SCALE_CATEGORIES)
    with lock:
        conn.execute(
            f"""
            DELETE FROM index_membership_daily
            WHERE index_code = ?{date_filter}
            """,
            (_TOPIX500_INDEX_CODE, *date_values),
        )
        conn.execute(
            f"""
            INSERT INTO index_membership_daily (date, index_code, code, created_at)
            SELECT DISTINCT date, ? AS index_code, code, ? AS created_at
            FROM stock_master_daily
            WHERE coalesce(scale_category, '') IN ({category_placeholders}){date_filter}
            ON CONFLICT (date, index_code, code) DO UPDATE
            SET created_at = excluded.created_at
            """,
            (_TOPIX500_INDEX_CODE, now_iso, *_TOPIX500_SCALE_CATEGORIES, *date_values),
        )
        row = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM index_membership_daily
            WHERE index_code = ?{date_filter}
            """,
            (_TOPIX500_INDEX_CODE, *date_values),
        ).fetchone()
    return int(row[0]) if row is not None else 0


def rebuild_stock_master_intervals(conn: Any, lock: Any, table_exists: TableExists) -> int:
    if not table_exists("stock_master_daily"):
        return 0
    rebuild_table = "__stock_master_intervals_rebuild"
    with lock:
        conn.execute(f"DROP TABLE IF EXISTS {rebuild_table}")
        conn.execute(
            f"""
            CREATE TABLE {rebuild_table} (
                code TEXT,
                valid_from TEXT,
                valid_to TEXT,
                fingerprint TEXT,
                company_name TEXT NOT NULL,
                company_name_english TEXT,
                market_code TEXT NOT NULL,
                market_name TEXT NOT NULL,
                sector_17_code TEXT NOT NULL,
                sector_17_name TEXT NOT NULL,
                sector_33_code TEXT NOT NULL,
                sector_33_name TEXT NOT NULL,
                scale_category TEXT,
                listed_date TEXT,
                created_at TEXT,
                PRIMARY KEY (code, valid_from, fingerprint)
            )
            """
        )
        conn.execute(
            f"""
            INSERT INTO {rebuild_table} (
                code, valid_from, valid_to, fingerprint, company_name, company_name_english,
                market_code, market_name, sector_17_code, sector_17_name, sector_33_code,
                sector_33_name, scale_category, listed_date, created_at
            )
            WITH cleaned AS (
                SELECT
                    *,
                    CASE
                        WHEN listed_date IS NULL THEN ''
                        WHEN listed_date = date THEN ''
                        ELSE listed_date
                    END AS stable_listed_date
                FROM stock_master_daily
            ), fingerprinted AS (
                SELECT
                    *,
                    md5(concat_ws('|',
                        coalesce(company_name, ''), coalesce(company_name_english, ''),
                        coalesce(market_code, ''), coalesce(market_name, ''),
                        coalesce(sector_17_code, ''), coalesce(sector_17_name, ''),
                        coalesce(sector_33_code, ''), coalesce(sector_33_name, ''),
                        coalesce(scale_category, ''), stable_listed_date
                    )) AS fingerprint
                FROM cleaned
            ), marked AS (
                SELECT
                    *,
                    CASE
                        WHEN lag(fingerprint) OVER (PARTITION BY code ORDER BY date) = fingerprint
                        THEN 0 ELSE 1
                    END AS starts_new_group
                FROM fingerprinted
            ), grouped AS (
                SELECT
                    *,
                    sum(starts_new_group) OVER (PARTITION BY code ORDER BY date) AS interval_group
                FROM marked
            )
            SELECT
                code,
                min(date) AS valid_from,
                max(date) AS valid_to,
                fingerprint,
                any_value(company_name),
                any_value(company_name_english),
                any_value(market_code),
                any_value(market_name),
                any_value(sector_17_code),
                any_value(sector_17_name),
                any_value(sector_33_code),
                any_value(sector_33_name),
                any_value(scale_category),
                any_value(stable_listed_date),
                max(created_at)
            FROM grouped
            GROUP BY code, interval_group, fingerprint
            """
        )
        conn.execute("DROP TABLE stock_master_intervals")
        conn.execute(f"ALTER TABLE {rebuild_table} RENAME TO stock_master_intervals")
        row = conn.execute("SELECT COUNT(*) FROM stock_master_intervals").fetchone()
    return int(row[0]) if row is not None else 0


def rebuild_stocks_latest(
    execute: Execute,
    fetchall: FetchAll,
    count_rows: CountRows,
    get_latest_stock_master_date: GetLatestDate,
    upsert_stocks_fn: UpsertStocks,
) -> int:
    latest_date = get_latest_stock_master_date()
    if latest_date is None:
        return 0
    now_iso = datetime.now().isoformat()  # noqa: DTZ005
    execute("DELETE FROM stocks_latest", None)
    execute(
        """
        INSERT INTO stocks_latest (
            code, company_name, company_name_english, market_code, market_name,
            sector_17_code, sector_17_name, sector_33_code, sector_33_name,
            scale_category, listed_date, source_date, created_at, updated_at
        )
        SELECT
            code, company_name, company_name_english, market_code, market_name,
            sector_17_code, sector_17_name, sector_33_code, sector_33_name,
            scale_category, listed_date, date, created_at, ?
        FROM stock_master_daily
        WHERE date = ?
        """,
        [now_iso, latest_date],
    )
    rows = fetchall(
        """
        SELECT
            code, company_name, company_name_english, market_code, market_name,
            sector_17_code, sector_17_name, sector_33_code, sector_33_name,
            scale_category, listed_date, created_at
        FROM stocks_latest
        """,
        None,
    )
    upsert_stocks_fn(
        [
            {
                "code": row[0],
                "company_name": row[1],
                "company_name_english": row[2],
                "market_code": row[3],
                "market_name": row[4],
                "sector_17_code": row[5],
                "sector_17_name": row[6],
                "sector_33_code": row[7],
                "sector_33_name": row[8],
                "scale_category": row[9],
                "listed_date": row[10],
                "created_at": row[11],
            }
            for row in rows
        ]
    )
    return count_rows("stocks_latest")
