"""
Market Database Access (SQLAlchemy Core)

market.db の読み取り + 書き込み操作を提供する。
Phase 3D（/api/db/* エンドポイント）の基盤。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import func, insert, select, text

from src.server.db.base import BaseDbAccess
from src.server.db.tables import (
    index_master,
    indices_data,
    market_meta,
    stock_data,
    stocks,
    sync_metadata,
    topix_data,
)


class MarketDb(BaseDbAccess):
    """market.db アクセス（read + write）"""

    def __init__(self, db_path: str, *, read_only: bool = False) -> None:
        super().__init__(db_path, read_only=read_only)

    # --- Read ---

    def get_stats(self) -> dict[str, Any]:
        """DB 統計情報を取得"""
        with self.engine.connect() as conn:
            result: dict[str, Any] = {}
            for table in [stocks, stock_data, topix_data, indices_data, sync_metadata, index_master]:
                count = conn.execute(select(func.count()).select_from(table)).scalar() or 0
                result[table.name] = count
            return result

    def validate_schema(self) -> dict[str, Any]:
        """スキーマ検証: 必要なテーブルが存在するか確認"""
        with self.engine.connect() as conn:
            existing = set(
                row[0]
                for row in conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table'")
                ).fetchall()
            )
            required = {t.name for t in market_meta.tables.values()}
            missing = required - existing
            return {
                "valid": len(missing) == 0,
                "required_tables": sorted(required),
                "existing_tables": sorted(existing & required),
                "missing_tables": sorted(missing),
            }

    def get_sync_metadata(self, key: str) -> str | None:
        """sync_metadata からキーの値を取得"""
        with self.engine.connect() as conn:
            row = conn.execute(
                select(sync_metadata.c.value).where(sync_metadata.c.key == key)
            ).fetchone()
            return row[0] if row else None

    def get_latest_trading_date(self) -> str | None:
        """topix_data の最新取引日を取得"""
        with self.engine.connect() as conn:
            row = conn.execute(select(func.max(topix_data.c.date))).fetchone()
            return row[0] if row else None

    # --- Write ---

    def upsert_stocks(self, rows: list[dict[str, Any]]) -> int:
        """stocks テーブルに upsert"""
        if not rows:
            return 0
        with self.engine.begin() as conn:
            for row in rows:
                row["updated_at"] = datetime.now().isoformat()  # noqa: DTZ005
                conn.execute(
                    insert(stocks)
                    .values(row)
                    .prefix_with("OR REPLACE")
                )
            return len(rows)

    def upsert_stock_data(self, rows: list[dict[str, Any]]) -> int:
        """stock_data テーブルに upsert"""
        if not rows:
            return 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(
                    insert(stock_data)
                    .values(row)
                    .prefix_with("OR REPLACE")
                )
            return len(rows)

    def upsert_topix_data(self, rows: list[dict[str, Any]]) -> int:
        """topix_data テーブルに upsert"""
        if not rows:
            return 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(
                    insert(topix_data)
                    .values(row)
                    .prefix_with("OR REPLACE")
                )
            return len(rows)

    def upsert_indices_data(self, rows: list[dict[str, Any]]) -> int:
        """indices_data テーブルに upsert"""
        if not rows:
            return 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(
                    insert(indices_data)
                    .values(row)
                    .prefix_with("OR REPLACE")
                )
            return len(rows)

    def set_sync_metadata(self, key: str, value: str) -> None:
        """sync_metadata にキーバリューを設定（upsert）"""
        with self.engine.begin() as conn:
            conn.execute(
                insert(sync_metadata)
                .values(key=key, value=value, updated_at=datetime.now().isoformat())  # noqa: DTZ005
                .prefix_with("OR REPLACE")
            )
