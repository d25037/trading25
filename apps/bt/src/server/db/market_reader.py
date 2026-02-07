"""
Market Database Reader

market.db への読み取り専用アクセスを提供する。
SQLite read-only URI モードを使用し、Hono (ts/api) の書き込みと干渉しない。
WAL pragma は読み取り側で設定しない（書き込み側の ts/api が設定済み、read-only 接続は自動認識）。
"""

from __future__ import annotations

import sqlite3
from typing import Any


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
