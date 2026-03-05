"""
Market Database Reader

market time-series データ（DuckDB SoT）への読み取り専用アクセスを提供する。
"""

from __future__ import annotations

import importlib
import threading
from dataclasses import dataclass
from collections.abc import Iterator
from typing import Any, cast

from src.infrastructure.db.market.query_helpers import stock_code_candidates


@dataclass(frozen=True)
class _DuckDbRow:
    """DuckDB row adapter with mapping-style access patterns."""

    _columns: tuple[str, ...]
    _values: tuple[Any, ...]
    _index_map: dict[str, int]

    def __getitem__(self, key: str | int) -> Any:
        if isinstance(key, int):
            return self._values[key]
        idx = self._index_map[str(key)]
        return self._values[idx]

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        return iter(zip(self._columns, self._values))

    def keys(self) -> tuple[str, ...]:
        return self._columns

    def items(self):
        return zip(self._columns, self._values)

    def values(self) -> tuple[Any, ...]:
        return self._values


class MarketDbReader:
    """Market time-series 読み取り専用リーダー（DuckDB 専用）。"""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conns: dict[int, Any] = {}
        self._conn_lock = threading.Lock()

    def _create_connection(self) -> Any:
        duckdb = importlib.import_module("duckdb")

        # Keep the same default connection mode as the time-series store.
        # DuckDB rejects mixed configs (e.g. read_only + read_write) for one file in a process.
        return cast(Any, duckdb).connect(self._db_path)

    def _get_thread_connection(self) -> Any:
        thread_id = threading.get_ident()
        conn = self._conns.get(thread_id)
        if conn is not None:
            return conn

        with self._conn_lock:
            conn = self._conns.get(thread_id)
            if conn is None:
                conn = self._create_connection()
                self._conns[thread_id] = conn
        return conn

    @property
    def conn(self) -> Any:
        return self._get_thread_connection()

    def close(self) -> None:
        with self._conn_lock:
            conns = list(self._conns.values())
            self._conns.clear()
        for conn in conns:
            conn.close()

    def _adapt_duckdb_rows(self, cursor: Any, rows: list[tuple[Any, ...]]) -> list[_DuckDbRow]:
        description = getattr(cursor, "description", None) or []
        columns = tuple(str(column[0]) for column in description if column)
        if not columns:
            return []
        index_map = {column: idx for idx, column in enumerate(columns)}
        return [
            _DuckDbRow(_columns=columns, _values=tuple(row), _index_map=index_map)
            for row in rows
        ]

    @staticmethod
    def _assert_read_only_sql(sql: str) -> None:
        normalized = sql.lstrip().lower()
        if normalized.startswith(("select", "with", "pragma", "show", "describe", "explain")):
            return
        raise PermissionError("attempt to write through MarketDbReader")

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[Any]:
        """SQL クエリを実行して結果を返す"""
        self._assert_read_only_sql(sql)
        cursor = self.conn.execute(sql, params)
        rows = cursor.fetchall()
        return self._adapt_duckdb_rows(cursor, rows)

    def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> Any | None:
        """SQL クエリを実行して最初の 1 行を返す"""
        self._assert_read_only_sql(sql)
        cursor = self.conn.execute(sql, params)
        row = cursor.fetchone()
        if row is None:
            return None
        adapted = self._adapt_duckdb_rows(cursor, [tuple(row)])
        return adapted[0] if adapted else None

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
