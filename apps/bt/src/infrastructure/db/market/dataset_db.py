"""
Dataset Database Access (SQLAlchemy Core)

dataset.db（各スナップショット）の読み取り操作を提供する。
Phase 3D（/api/dataset/{name}/* エンドポイント）の基盤。
"""

from __future__ import annotations

import random
from typing import Any

from sqlalchemy import Row, func, literal, literal_column, or_, select, text

from src.infrastructure.db.market.base import BaseDbAccess
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.infrastructure.db.market.tables import (
    dataset_info,
    ds_indices_data,
    ds_stock_data,
    ds_stocks,
    ds_topix_data,
    margin_data,
    statements,
)
from src.shared.models.types import normalize_period_type

_LEGACY_PERIOD_TYPE_MAP = {
    "1Q": "Q1",
    "2Q": "Q2",
    "3Q": "Q3",
}
_REQUIRED_STATEMENTS_COLUMNS = {"code", "disclosed_date"}
_ACTUAL_ONLY_COLUMNS = (
    "earnings_per_share",
    "profit",
    "equity",
)


def _resolve_period_filter_values(period_type: str) -> list[str] | None:
    """Normalize period type and include legacy aliases when needed."""
    normalized_period = normalize_period_type(period_type)
    if normalized_period is None or normalized_period == "all":
        return None

    values = [normalized_period]
    legacy_value = _LEGACY_PERIOD_TYPE_MAP.get(normalized_period)
    if legacy_value is not None:
        values.append(legacy_value)
    return values


class DatasetDb(BaseDbAccess):
    """dataset.db 読み取り（各スナップショット）"""

    def __init__(self, db_path: str) -> None:
        super().__init__(db_path, read_only=True)
        self._statements_columns_cache: set[str] | None = None

    def _get_statements_columns(self) -> set[str]:
        """statements テーブルの実カラム一覧を取得（レガシーDB互換）。"""
        if self._statements_columns_cache is not None:
            return self._statements_columns_cache

        with self.engine.connect() as conn:
            rows = conn.execute(text("PRAGMA table_info(statements)")).fetchall()
        columns = {
            str(row[1])
            for row in rows
            if len(row) > 1 and row[1]
        }
        missing_required = _REQUIRED_STATEMENTS_COLUMNS - columns
        if missing_required:
            raise RuntimeError(
                "statements schema is missing required columns: "
                + ", ".join(sorted(missing_required))
            )
        self._statements_columns_cache = columns
        return columns

    def _select_statements_projection(self) -> Any:
        """
        statements の選択列を動的構築する。

        旧 dataset.db で未追加の列は NULL を同名 alias で補完し、
        呼び出し側の列マッピングを壊さない。
        """
        existing = self._get_statements_columns()
        projected_columns = [
            column if column.name in existing else literal(None).label(column.name)
            for column in statements.columns
        ]
        return select(*projected_columns)

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

    def _apply_statements_filters(
        self,
        stmt: Any,
        start: str | None = None,
        end: str | None = None,
        period_type: str = "all",
        actual_only: bool = True,
    ) -> Any:
        """Apply common filters for statements queries."""
        existing = self._get_statements_columns()

        if start and "disclosed_date" in existing:
            stmt = stmt.where(statements.c.disclosed_date >= start)
        if end and "disclosed_date" in existing:
            stmt = stmt.where(statements.c.disclosed_date <= end)

        period_values = _resolve_period_filter_values(period_type)
        if period_values and "type_of_current_period" in existing:
            stmt = stmt.where(statements.c.type_of_current_period.in_(period_values))

        if actual_only:
            actual_cols = [
                getattr(statements.c, column_name)
                for column_name in _ACTUAL_ONLY_COLUMNS
                if column_name in existing
            ]
            if actual_cols:
                stmt = stmt.where(or_(*(col.is_not(None) for col in actual_cols)))

        return stmt

    def get_statements(
        self,
        code: str,
        start: str | None = None,
        end: str | None = None,
        period_type: str = "all",
        actual_only: bool = True,
    ) -> list[Row[Any]]:
        """財務諸表データを取得"""
        code = normalize_stock_code(code)
        stmt = self._select_statements_projection().where(statements.c.code == code)
        stmt = self._apply_statements_filters(
            stmt,
            start=start,
            end=end,
            period_type=period_type,
            actual_only=actual_only,
        )
        stmt = stmt.order_by(statements.c.disclosed_date)
        with self.engine.connect() as conn:
            return list(conn.execute(stmt).fetchall())

    def get_statements_batch(
        self,
        codes: list[str],
        start: str | None = None,
        end: str | None = None,
        period_type: str = "all",
        actual_only: bool = True,
    ) -> dict[str, list[Row[Any]]]:
        """複数銘柄の財務諸表データを一括取得"""
        normalized = [normalize_stock_code(c) for c in codes]
        if not normalized:
            return {}

        stmt = self._select_statements_projection().where(statements.c.code.in_(normalized))
        stmt = self._apply_statements_filters(
            stmt,
            start=start,
            end=end,
            period_type=period_type,
            actual_only=actual_only,
        )
        stmt = stmt.order_by(statements.c.code, statements.c.disclosed_date)
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
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

    # --- Extended methods (Phase 3D-1) ---

    def get_stock_list_with_counts(self, min_records: int = 100) -> list[Row[Any]]:
        """銘柄一覧 + レコード数 + 日付範囲"""
        stmt = (
            select(
                ds_stocks.c.code.label("stockCode"),
                func.count(ds_stock_data.c.date).label("record_count"),
                func.min(ds_stock_data.c.date).label("start_date"),
                func.max(ds_stock_data.c.date).label("end_date"),
            )
            .outerjoin(ds_stock_data, ds_stocks.c.code == ds_stock_data.c.code)
            .group_by(ds_stocks.c.code)
            .having(func.count(ds_stock_data.c.date) >= min_records)
            .order_by(ds_stocks.c.code)
        )
        with self.engine.connect() as conn:
            return list(conn.execute(stmt).fetchall())

    def get_index_list_with_counts(self, min_records: int = 100) -> list[Row[Any]]:
        """指数一覧 + レコード数 + 日付範囲"""
        stmt = (
            select(
                ds_indices_data.c.code.label("indexCode"),
                func.min(ds_indices_data.c.sector_name).label("indexName"),
                func.count(ds_indices_data.c.date).label("record_count"),
                func.min(ds_indices_data.c.date).label("start_date"),
                func.max(ds_indices_data.c.date).label("end_date"),
            )
            .group_by(ds_indices_data.c.code)
            .having(func.count(ds_indices_data.c.date) >= min_records)
            .order_by(ds_indices_data.c.code)
        )
        with self.engine.connect() as conn:
            return list(conn.execute(stmt).fetchall())

    def get_margin_list(self, min_records: int = 10) -> list[Row[Any]]:
        """信用取引データ一覧 + レコード数 + 日付範囲 + 平均"""
        stmt = (
            select(
                margin_data.c.code.label("stockCode"),
                func.count(margin_data.c.date).label("record_count"),
                func.min(margin_data.c.date).label("start_date"),
                func.max(margin_data.c.date).label("end_date"),
                func.avg(margin_data.c.long_margin_volume).label("avg_long_margin"),
                func.avg(margin_data.c.short_margin_volume).label("avg_short_margin"),
            )
            .group_by(margin_data.c.code)
            .having(func.count(margin_data.c.date) >= min_records)
            .order_by(margin_data.c.code)
        )
        with self.engine.connect() as conn:
            return list(conn.execute(stmt).fetchall())

    def search_stocks(self, term: str, exact: bool = False, limit: int = 50) -> list[Row[Any]]:
        """銘柄検索（code/company_name）"""
        if exact:
            stmt = (
                select(
                    ds_stocks.c.code,
                    ds_stocks.c.company_name,
                    literal_column("'exact'").label("match_type"),
                )
                .where(
                    (ds_stocks.c.code == term)
                    | (ds_stocks.c.company_name == term)
                )
                .limit(limit)
            )
        else:
            pattern = f"%{term}%"
            stmt = (
                select(
                    ds_stocks.c.code,
                    ds_stocks.c.company_name,
                    literal_column("'partial'").label("match_type"),
                )
                .where(
                    (ds_stocks.c.code.like(pattern))
                    | (ds_stocks.c.company_name.like(pattern))
                )
                .order_by(ds_stocks.c.code)
                .limit(limit)
            )
        with self.engine.connect() as conn:
            return list(conn.execute(stmt).fetchall())

    def get_sample_codes(self, size: int = 10, seed: int | None = None) -> list[str]:
        """ランダムサンプリングで銘柄コードを取得"""
        with self.engine.connect() as conn:
            rows = conn.execute(select(ds_stocks.c.code).order_by(ds_stocks.c.code)).fetchall()
        codes = [row[0] for row in rows]
        if not codes:
            return []
        rng = random.Random(seed)  # noqa: S311
        return rng.sample(codes, min(size, len(codes)))

    def get_table_counts(self) -> dict[str, int]:
        """各テーブルの行数を取得"""
        tables = {
            "stocks": ds_stocks,
            "stock_data": ds_stock_data,
            "topix_data": ds_topix_data,
            "indices_data": ds_indices_data,
            "margin_data": margin_data,
            "statements": statements,
            "dataset_info": dataset_info,
        }
        result: dict[str, int] = {}
        with self.engine.connect() as conn:
            for name, table in tables.items():
                result[name] = conn.execute(select(func.count()).select_from(table)).scalar() or 0
        return result

    def get_date_range(self) -> dict[str, str] | None:
        """stock_data の日付範囲を取得"""
        with self.engine.connect() as conn:
            row = conn.execute(
                select(
                    func.min(ds_stock_data.c.date).label("min"),
                    func.max(ds_stock_data.c.date).label("max"),
                )
            ).fetchone()
        if row is None or row.min is None:
            return None
        return {"min": row.min, "max": row.max}

    def get_sectors_with_count(self) -> list[Row[Any]]:
        """セクター名 + 銘柄数"""
        stmt = (
            select(
                ds_stocks.c.sector_33_name.label("sectorName"),
                func.count(ds_stocks.c.code).label("count"),
            )
            .group_by(ds_stocks.c.sector_33_name)
            .order_by(ds_stocks.c.sector_33_name)
        )
        with self.engine.connect() as conn:
            return list(conn.execute(stmt).fetchall())

    def get_stocks_with_quotes_count(self) -> int:
        """OHLCV データを持つ銘柄数"""
        stmt = select(func.count(func.distinct(ds_stock_data.c.code)))
        with self.engine.connect() as conn:
            return conn.execute(stmt).scalar() or 0

    def get_stocks_with_margin_count(self) -> int:
        """信用取引データを持つ銘柄数"""
        stmt = select(func.count(func.distinct(margin_data.c.code)))
        with self.engine.connect() as conn:
            return conn.execute(stmt).scalar() or 0

    def get_stocks_with_statements_count(self) -> int:
        """財務データを持つ銘柄数"""
        stmt = select(func.count(func.distinct(statements.c.code)))
        with self.engine.connect() as conn:
            return conn.execute(stmt).scalar() or 0

    def get_fk_orphan_counts(self) -> dict[str, int]:
        """stocks に紐づかない参照データ件数を取得"""
        stock_data_join = ds_stock_data.outerjoin(ds_stocks, ds_stock_data.c.code == ds_stocks.c.code)
        margin_join = margin_data.outerjoin(ds_stocks, margin_data.c.code == ds_stocks.c.code)
        statements_join = statements.outerjoin(ds_stocks, statements.c.code == ds_stocks.c.code)

        with self.engine.connect() as conn:
            stock_data_orphans = (
                conn.execute(
                    select(func.count())
                    .select_from(stock_data_join)
                    .where(ds_stocks.c.code.is_(None))
                ).scalar()
                or 0
            )
            margin_data_orphans = (
                conn.execute(
                    select(func.count())
                    .select_from(margin_join)
                    .where(ds_stocks.c.code.is_(None))
                ).scalar()
                or 0
            )
            statements_orphans = (
                conn.execute(
                    select(func.count())
                    .select_from(statements_join)
                    .where(ds_stocks.c.code.is_(None))
                ).scalar()
                or 0
            )
        return {
            "stockDataOrphans": stock_data_orphans,
            "marginDataOrphans": margin_data_orphans,
            "statementsOrphans": statements_orphans,
        }

    def get_stocks_without_quotes_count(self) -> int:
        """stock_data が1件もない銘柄数を取得"""
        join_stmt = ds_stocks.outerjoin(ds_stock_data, ds_stocks.c.code == ds_stock_data.c.code)
        stmt = (
            select(func.count(func.distinct(ds_stocks.c.code)))
            .select_from(join_stmt)
            .where(ds_stock_data.c.code.is_(None))
        )
        with self.engine.connect() as conn:
            return conn.execute(stmt).scalar() or 0
