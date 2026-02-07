"""
Common Query Helpers

Phase 3B で 36 個の生 SQL に散在していた重複パターンを関数化。
新規コード（3D/3E）で使用。3B 既存コードは変更しない。
"""

from __future__ import annotations

import re

from sqlalchemy import Table, func, select
from sqlalchemy.sql import Select


def normalize_stock_code(code: str) -> str:
    """5桁→4桁変換（Drizzle stockCode() と同一ロジック）

    JQuants API は 5桁コード（末尾0）を使用するが、DB 格納は 4桁。
    """
    if len(code) == 5 and code.endswith("0"):
        return code[:4]
    return code


def expand_stock_code(code: str) -> str:
    """4桁→5桁変換（JQuants API 向け）"""
    if len(code) == 4:
        return f"{code}0"
    return code


def is_valid_stock_code(code: str) -> bool:
    """4桁銘柄コードバリデーション（Drizzle isValidStockCode と同一）"""
    return bool(re.match(r"^\d[0-9A-Z]\d[0-9A-Z]$", code))


def max_trading_date(table: Table) -> Select[tuple[str | None]]:
    """最新取引日を取得するサブクエリ"""
    return select(func.max(table.c.date)).scalar_subquery()  # type: ignore[return-value]


def trading_date_before(table: Table, target_date: str, offset: int) -> Select[tuple[str | None]]:
    """指定日から N 営業日前の日付を取得するサブクエリ

    target_date 以前の日付を降順に並べ、offset 番目を返す。
    """
    return (
        select(table.c.date)
        .where(table.c.date <= target_date)
        .order_by(table.c.date.desc())
        .offset(offset)
        .limit(1)
        .scalar_subquery()  # type: ignore[return-value]
    )


def ohlcv_query(table: Table, code: str, start: str | None = None, end: str | None = None) -> Select[tuple[object, ...]]:
    """OHLCV データの範囲クエリを構築"""
    stmt = select(table).where(table.c.code == code)
    if start:
        stmt = stmt.where(table.c.date >= start)
    if end:
        stmt = stmt.where(table.c.date <= end)
    return stmt.order_by(table.c.date)


def market_filter(table: Table, market_codes: list[str]) -> Select[tuple[object, ...]]:
    """マーケットコードでフィルタした銘柄一覧"""
    return select(table).where(table.c.market_code.in_(market_codes))


def stock_lookup(table: Table, code: str) -> Select[tuple[object, ...]]:
    """銘柄メタデータ取得"""
    return select(table).where(table.c.code == code)
