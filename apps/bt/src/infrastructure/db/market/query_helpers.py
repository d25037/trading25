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
    """JQuants API code を DB 格納向けの実コードへ正規化する。

    JQuants API は実コードの末尾に 0 を付けるため、
    4桁銘柄は 5桁 API code、5桁銘柄は 6桁 API code になる。
    """
    if len(code) in {5, 6} and code.endswith("0"):
        return code[:-1]
    return code


def expand_stock_code(code: str) -> str:
    """実コードを JQuants API code へ展開する。"""
    normalized = normalize_stock_code(code)
    if len(normalized) in {4, 5}:
        return f"{normalized}0"
    return normalized


def stock_code_candidates(code: str) -> tuple[str, ...]:
    """DB 検索向けの実コード/API code 候補を返す。"""
    code4 = normalize_stock_code(code)
    code5 = expand_stock_code(code4)
    if code4 == code5:
        return (code4,)
    return (code4, code5)


def is_valid_stock_code(code: str) -> bool:
    """4桁銘柄コードバリデーション（Drizzle isValidStockCode と同一）"""
    return bool(re.match(r"^\d[0-9A-Z]\d[0-9A-Z]$", code))


def max_trading_date(table: Table):
    """最新取引日を取得するサブクエリ"""
    return select(func.max(table.c.date)).scalar_subquery()


def trading_date_before(table: Table, target_date: str, offset: int):
    """指定日から N 営業日前の日付を取得するサブクエリ

    target_date 以前の日付を降順に並べ、offset 番目を返す。
    """
    return (
        select(table.c.date)
        .where(table.c.date <= target_date)
        .order_by(table.c.date.desc())
        .offset(offset)
        .limit(1)
        .scalar_subquery()
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
