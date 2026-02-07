"""
Dataset Writer

データセット .db ファイルへの書き込み。
DatasetDb（read-only）の書き込み版。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, insert, select

from src.server.db.base import BaseDbAccess
from src.server.db.tables import (
    dataset_info,
    dataset_meta,
    ds_indices_data,
    ds_stock_data,
    ds_stocks,
    ds_topix_data,
    margin_data,
    statements,
)


class DatasetWriter(BaseDbAccess):
    """データセット .db ファイルへの書き込み"""

    def __init__(self, db_path: str) -> None:
        super().__init__(db_path, read_only=False)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """テーブルが存在しない場合は作成"""
        dataset_meta.create_all(self.engine, checkfirst=True)

    def upsert_stocks(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(insert(ds_stocks).values(row).prefix_with("OR REPLACE"))
        return len(rows)

    def upsert_stock_data(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(insert(ds_stock_data).values(row).prefix_with("OR REPLACE"))
        return len(rows)

    def upsert_topix_data(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(insert(ds_topix_data).values(row).prefix_with("OR REPLACE"))
        return len(rows)

    def upsert_indices_data(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(insert(ds_indices_data).values(row).prefix_with("OR REPLACE"))
        return len(rows)

    def upsert_margin_data(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(insert(margin_data).values(row).prefix_with("OR REPLACE"))
        return len(rows)

    def upsert_statements(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(insert(statements).values(row).prefix_with("OR REPLACE"))
        return len(rows)

    def set_dataset_info(self, key: str, value: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                insert(dataset_info)
                .values(key=key, value=value, updated_at=datetime.now(UTC).isoformat())
                .prefix_with("OR REPLACE")
            )

    def get_stock_count(self) -> int:
        with self.engine.connect() as conn:
            return conn.execute(select(func.count()).select_from(ds_stocks)).scalar() or 0

    def get_stock_data_count(self) -> int:
        with self.engine.connect() as conn:
            return conn.execute(select(func.count()).select_from(ds_stock_data)).scalar() or 0
