"""
Dataset Writer

データセット .db ファイルへの書き込み。
DatasetDb（read-only）の書き込み版。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, insert, select, text

from src.infrastructure.db.market.base import BaseDbAccess
from src.infrastructure.db.market.tables import (
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
        self._ensure_statements_columns()

    def _ensure_statements_columns(self) -> None:
        """既存 statements テーブルに不足カラムを追加する。"""
        additional_columns: tuple[tuple[str, str], ...] = (
            ("forecast_dividend_fy", "REAL"),
            ("next_year_forecast_dividend_fy", "REAL"),
            ("payout_ratio", "REAL"),
            ("forecast_payout_ratio", "REAL"),
            ("next_year_forecast_payout_ratio", "REAL"),
        )
        with self.engine.begin() as conn:
            existing_columns = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(statements)")).fetchall()
                if len(row) > 1
            }
            for column_name, column_type in additional_columns:
                if column_name in existing_columns:
                    continue
                conn.execute(
                    text(
                        f"ALTER TABLE statements ADD COLUMN {column_name} {column_type}"  # noqa: S608
                    )
                )

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

    def get_existing_stock_data_codes(self) -> set[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(select(ds_stock_data.c.code).distinct()).fetchall()
        return {str(row[0]) for row in rows if row and row[0] is not None}

    def has_topix_data(self) -> bool:
        with self.engine.connect() as conn:
            count = conn.execute(select(func.count()).select_from(ds_topix_data)).scalar() or 0
        return count > 0

    def get_existing_index_codes(self) -> set[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(select(ds_indices_data.c.code).distinct()).fetchall()
        return {str(row[0]) for row in rows if row and row[0] is not None}

    def get_existing_margin_codes(self) -> set[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(select(margin_data.c.code).distinct()).fetchall()
        return {str(row[0]) for row in rows if row and row[0] is not None}

    def get_existing_statement_codes(self) -> set[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(select(statements.c.code).distinct()).fetchall()
        return {str(row[0]) for row in rows if row and row[0] is not None}
