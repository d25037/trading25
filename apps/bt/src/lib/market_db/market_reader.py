"""
Market Database Reader

market.db への読み取り専用アクセスを提供する。
SQLite read-only URI モードを使用し、Hono (ts/api) の書き込みと干渉しない。
WAL pragma は読み取り側で設定しない（書き込み側の ts/api が設定済み、read-only 接続は自動認識）。
"""

from __future__ import annotations

import sqlite3
from typing import Any

from src.lib.market_db.query_helpers import stock_code_candidates


class MarketDbReader:
    """market.db 読み取り専用リーダー"""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            uri = f"file:{self._db_path}?mode=ro"
            self._conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        """SQL クエリを実行して結果を返す"""
        cursor = self.conn.execute(sql, params)
        return cursor.fetchall()

    def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        """SQL クエリを実行して最初の 1 行を返す"""
        cursor = self.conn.execute(sql, params)
        return cursor.fetchone()

    def get_latest_price(self, code: str) -> float | None:
        """単一銘柄の最新終値を取得（4桁/5桁両対応）"""
        candidates = stock_code_candidates(code)
        placeholders = ",".join("?" for _ in candidates)
        row = self.query_one(
            f"""
            SELECT close FROM stock_data
            WHERE code IN ({placeholders})
            ORDER BY date DESC,
                CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
            LIMIT 1
            """,
            tuple(candidates),
        )
        if row is None:
            return None
        return row["close"]

    def get_stock_prices_by_date(self, code: str) -> list[tuple[str, float]]:
        """銘柄の日次終値を日付昇順で取得（4桁/5桁重複は4桁優先でマージ）"""
        candidates = stock_code_candidates(code)
        placeholders = ",".join("?" for _ in candidates)
        raw_rows = self.query(
            f"""
            SELECT date, close FROM stock_data
            WHERE code IN ({placeholders})
            ORDER BY date, CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
            """,
            tuple(candidates),
        )
        price_map: dict[str, float] = {}
        for row in raw_rows:
            price_map.setdefault(row["date"], row["close"])
        return sorted(price_map.items(), key=lambda x: x[0])
