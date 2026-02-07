"""
Dataset Database Access (SQLAlchemy Core)

dataset.db（各スナップショット）の読み取り操作を提供する。
Phase 3D（/api/dataset/{name}/* エンドポイント）の基盤。
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import Row, func, select

from src.server.db.base import BaseDbAccess
from src.server.db.query_helpers import normalize_stock_code
from src.server.db.tables import (
    dataset_info,
    ds_indices_data,
    ds_stock_data,
    ds_stocks,
    ds_topix_data,
    margin_data,
    statements,
)


class DatasetDb(BaseDbAccess):
    """dataset.db 読み取り（各スナップショット）"""

    def __init__(self, db_path: str) -> None:
        super().__init__(db_path, read_only=True)

    # --- Stocks ---

    def get_stocks(
        self,
        sector: str | None = None,
        market: str | None = None,
    ) -> list[Row[Any]]:
        """銘柄一覧を取得"""
        stmt = select(ds_stocks)
        if sector:
            stmt = stmt.where(ds_stocks.c.sector_33_name == sector)
        if market:
            stmt = stmt.where(ds_stocks.c.market_code == market)
        with self.engine.connect() as conn:
            return list(conn.execute(stmt.order_by(ds_stocks.c.code)).fetchall())

    # --- OHLCV ---

    def get_stock_ohlcv(
        self,
        code: str,
        start: str | None = None,
        end: str | None = None,
    ) -> list[Row[Any]]:
        """個別銘柄の OHLCV データを取得"""
        code = normalize_stock_code(code)
        stmt = select(ds_stock_data).where(ds_stock_data.c.code == code)
        if start:
            stmt = stmt.where(ds_stock_data.c.date >= start)
        if end:
            stmt = stmt.where(ds_stock_data.c.date <= end)
        with self.engine.connect() as conn:
            return list(conn.execute(stmt.order_by(ds_stock_data.c.date)).fetchall())

    def get_ohlcv_batch(self, codes: list[str]) -> dict[str, list[Row[Any]]]:
        """複数銘柄の OHLCV データを一括取得"""
        normalized = [normalize_stock_code(c) for c in codes]
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(ds_stock_data)
                .where(ds_stock_data.c.code.in_(normalized))
                .order_by(ds_stock_data.c.code, ds_stock_data.c.date)
            ).fetchall()
        result: dict[str, list[Row[Any]]] = {c: [] for c in normalized}
        for row in rows:
            result.setdefault(row.code, []).append(row)
        return result

    # --- TOPIX ---

    def get_topix(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> list[Row[Any]]:
        """TOPIX データを取得"""
        stmt = select(ds_topix_data)
        if start:
            stmt = stmt.where(ds_topix_data.c.date >= start)
        if end:
            stmt = stmt.where(ds_topix_data.c.date <= end)
        with self.engine.connect() as conn:
            return list(conn.execute(stmt.order_by(ds_topix_data.c.date)).fetchall())

    # --- Indices ---

    def get_indices(self) -> list[Row[Any]]:
        """利用可能な指数コード一覧を取得"""
        with self.engine.connect() as conn:
            return list(
                conn.execute(
                    select(ds_indices_data.c.code, ds_indices_data.c.sector_name)
                    .distinct()
                    .order_by(ds_indices_data.c.code)
                ).fetchall()
            )

    def get_index_data(
        self,
        code: str,
        start: str | None = None,
        end: str | None = None,
    ) -> list[Row[Any]]:
        """指定指数の OHLC データを取得"""
        stmt = select(ds_indices_data).where(ds_indices_data.c.code == code)
        if start:
            stmt = stmt.where(ds_indices_data.c.date >= start)
        if end:
            stmt = stmt.where(ds_indices_data.c.date <= end)
        with self.engine.connect() as conn:
            return list(conn.execute(stmt.order_by(ds_indices_data.c.date)).fetchall())

    # --- Margin ---

    def get_margin(
        self,
        code: str | None = None,
        start: str | None = None,
        end: str | None = None,
    ) -> list[Row[Any]]:
        """信用取引データを取得"""
        stmt = select(margin_data)
        if code:
            stmt = stmt.where(margin_data.c.code == normalize_stock_code(code))
        if start:
            stmt = stmt.where(margin_data.c.date >= start)
        if end:
            stmt = stmt.where(margin_data.c.date <= end)
        with self.engine.connect() as conn:
            return list(conn.execute(stmt.order_by(margin_data.c.date)).fetchall())

    def get_margin_batch(self, codes: list[str]) -> dict[str, list[Row[Any]]]:
        """複数銘柄の信用取引データを一括取得"""
        normalized = [normalize_stock_code(c) for c in codes]
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(margin_data)
                .where(margin_data.c.code.in_(normalized))
                .order_by(margin_data.c.code, margin_data.c.date)
            ).fetchall()
        result: dict[str, list[Row[Any]]] = {c: [] for c in normalized}
        for row in rows:
            result.setdefault(row.code, []).append(row)
        return result

    # --- Statements ---

    def get_statements(self, code: str) -> list[Row[Any]]:
        """財務諸表データを取得"""
        code = normalize_stock_code(code)
        with self.engine.connect() as conn:
            return list(
                conn.execute(
                    select(statements)
                    .where(statements.c.code == code)
                    .order_by(statements.c.disclosed_date)
                ).fetchall()
            )

    def get_statements_batch(self, codes: list[str]) -> dict[str, list[Row[Any]]]:
        """複数銘柄の財務諸表データを一括取得"""
        normalized = [normalize_stock_code(c) for c in codes]
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(statements)
                .where(statements.c.code.in_(normalized))
                .order_by(statements.c.code, statements.c.disclosed_date)
            ).fetchall()
        result: dict[str, list[Row[Any]]] = {c: [] for c in normalized}
        for row in rows:
            result.setdefault(row.code, []).append(row)
        return result

    # --- Sectors ---

    def get_sectors(self) -> list[dict[str, str]]:
        """セクター一覧を取得"""
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(ds_stocks.c.sector_33_code, ds_stocks.c.sector_33_name)
                .distinct()
                .order_by(ds_stocks.c.sector_33_code)
            ).fetchall()
            return [{"code": row[0], "name": row[1]} for row in rows]

    def get_sector_mapping(self) -> dict[str, str]:
        """セクターコード → セクター名のマッピング"""
        sectors = self.get_sectors()
        return {s["code"]: s["name"] for s in sectors}

    def get_sector_stock_mapping(self) -> dict[str, list[str]]:
        """セクター名 → 銘柄コードリストのマッピング"""
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(ds_stocks.c.sector_33_name, ds_stocks.c.code)
                .order_by(ds_stocks.c.sector_33_name, ds_stocks.c.code)
            ).fetchall()
        result: dict[str, list[str]] = {}
        for row in rows:
            result.setdefault(row[0], []).append(row[1])
        return result

    def get_sector_stocks(self, sector_name: str) -> list[Row[Any]]:
        """指定セクターの銘柄一覧を取得"""
        with self.engine.connect() as conn:
            return list(
                conn.execute(
                    select(ds_stocks)
                    .where(ds_stocks.c.sector_33_name == sector_name)
                    .order_by(ds_stocks.c.code)
                ).fetchall()
            )

    # --- Dataset Info ---

    def get_dataset_info(self) -> dict[str, str]:
        """dataset_info テーブルの全エントリを取得"""
        with self.engine.connect() as conn:
            rows = conn.execute(select(dataset_info)).fetchall()
            return {row.key: row.value for row in rows}

    def get_stock_count(self) -> int:
        """銘柄数を取得"""
        with self.engine.connect() as conn:
            return conn.execute(select(func.count()).select_from(ds_stocks)).scalar() or 0
