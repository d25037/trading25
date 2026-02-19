"""
Market Database Access (SQLAlchemy Core)

market.db の読み取り + 書き込み操作を提供する。
Phase 3D（/api/db/* エンドポイント）の基盤。
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from sqlalchemy import func, insert, select, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from src.lib.market_db.base import BaseDbAccess
from src.lib.market_db.tables import (
    index_master,
    indices_data,
    market_meta,
    market_statements,
    stock_data,
    stocks,
    sync_metadata,
    topix_data,
)

# Hono 互換 metadata キー
METADATA_KEYS = {
    "INIT_COMPLETED": "init_completed",
    "LAST_SYNC_DATE": "last_sync_date",
    "LAST_STOCKS_REFRESH": "last_stocks_refresh",
    "FAILED_DATES": "failed_dates",
    "REFETCHED_STOCKS": "refetched_stocks",
    "FUNDAMENTALS_LAST_SYNC_DATE": "fundamentals_last_sync_date",
    "FUNDAMENTALS_LAST_DISCLOSED_DATE": "fundamentals_last_disclosed_date",
    "FUNDAMENTALS_FAILED_DATES": "fundamentals_failed_dates",
    "FUNDAMENTALS_FAILED_CODES": "fundamentals_failed_codes",
}


class MarketDb(BaseDbAccess):
    """market.db アクセス（read + write）"""

    def __init__(self, db_path: str, *, read_only: bool = False) -> None:
        super().__init__(db_path, read_only=read_only)
        if not read_only:
            self.ensure_schema()

    def ensure_schema(self) -> None:
        """不足テーブルを補完する（既存DB互換）"""
        market_meta.create_all(self.engine, checkfirst=True)

    # --- Read ---

    def get_stats(self) -> dict[str, Any]:
        """DB 統計情報を取得"""
        with self.engine.connect() as conn:
            result: dict[str, Any] = {}
            for table in [
                stocks,
                stock_data,
                topix_data,
                indices_data,
                market_statements,
                sync_metadata,
                index_master,
            ]:
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

    def get_latest_stock_data_date(self) -> str | None:
        """stock_data の最新取引日を取得"""
        with self.engine.connect() as conn:
            row = conn.execute(select(func.max(stock_data.c.date))).fetchone()
            return row[0] if row else None

    def get_latest_indices_data_dates(self) -> dict[str, str]:
        """indices_data の銘柄コードごとの最新取引日を取得"""
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(
                    indices_data.c.code,
                    func.max(indices_data.c.date).label("max_date"),
                )
                .group_by(indices_data.c.code)
            ).fetchall()
        return {
            row.code: row.max_date
            for row in rows
            if row.code and row.max_date
        }

    def get_index_master_codes(self) -> set[str]:
        """index_master に存在する指数コード一覧を取得"""
        with self.engine.connect() as conn:
            rows = conn.execute(select(index_master.c.code)).fetchall()
        return {
            row.code
            for row in rows
            if row.code
        }

    def get_latest_statement_disclosed_date(self) -> str | None:
        """statements の最新開示日を取得"""
        with self.engine.connect() as conn:
            row = conn.execute(select(func.max(market_statements.c.disclosed_date))).fetchone()
            return row[0] if row else None

    def get_statement_codes(self) -> set[str]:
        """statements に存在する銘柄コード一覧を取得"""
        with self.engine.connect() as conn:
            rows = conn.execute(select(func.distinct(market_statements.c.code))).fetchall()
        return {
            str(row[0])
            for row in rows
            if row[0]
        }

    def get_prime_codes(self) -> set[str]:
        """stocks から Prime 銘柄コードを取得（legacy 表記も吸収）"""
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(stocks.c.code).where(
                    func.lower(func.trim(stocks.c.market_code)).in_(["0111", "prime"])
                )
            ).fetchall()
        return {
            str(row[0])
            for row in rows
            if row[0]
        }

    def get_prime_statement_coverage(
        self,
        *,
        limit_missing: int | None = 20,
    ) -> dict[str, Any]:
        """Prime 銘柄に対する statements カバレッジを集計"""
        prime_filter = func.lower(func.trim(stocks.c.market_code)).in_(["0111", "prime"])

        with self.engine.connect() as conn:
            prime_count = conn.execute(
                select(func.count(func.distinct(stocks.c.code))).where(prime_filter)
            ).scalar() or 0

            covered_count = conn.execute(
                select(func.count(func.distinct(stocks.c.code)))
                .select_from(
                    stocks.join(
                        market_statements,
                        market_statements.c.code == stocks.c.code,
                    )
                )
                .where(prime_filter)
            ).scalar() or 0

            missing_count = conn.execute(
                select(func.count(func.distinct(stocks.c.code)))
                .select_from(
                    stocks.outerjoin(
                        market_statements,
                        market_statements.c.code == stocks.c.code,
                    )
                )
                .where(prime_filter)
                .where(market_statements.c.code.is_(None))
            ).scalar() or 0

            missing_stmt = (
                select(stocks.c.code)
                .select_from(
                    stocks.outerjoin(
                        market_statements,
                        market_statements.c.code == stocks.c.code,
                    )
                )
                .where(prime_filter)
                .where(market_statements.c.code.is_(None))
                .order_by(stocks.c.code)
            )
            if limit_missing is not None and limit_missing >= 0:
                missing_stmt = missing_stmt.limit(limit_missing)
            missing_rows = conn.execute(missing_stmt).fetchall()

        coverage_ratio = 0.0
        if prime_count > 0:
            coverage_ratio = covered_count / prime_count

        return {
            "primeCount": int(prime_count),
            "coveredCount": int(covered_count),
            "missingCount": int(missing_count),
            "coverageRatio": round(coverage_ratio, 4),
            "missingCodes": [str(row[0]) for row in missing_rows if row[0]],
        }

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

    def upsert_statements(self, rows: list[dict[str, Any]]) -> int:
        """statements テーブルに upsert"""
        if not rows:
            return 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(
                    insert(market_statements)
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

    def upsert_index_master(self, rows: list[dict[str, Any]]) -> int:
        """index_master テーブルに upsert"""
        if not rows:
            return 0
        with self.engine.begin() as conn:
            for row in rows:
                payload = dict(row)
                payload["updated_at"] = datetime.now().isoformat()  # noqa: DTZ005
                stmt = sqlite_insert(index_master).values(payload)
                conn.execute(
                    stmt.on_conflict_do_update(
                        index_elements=[index_master.c.code],
                        set_={
                            "name": stmt.excluded.name,
                            "name_english": stmt.excluded.name_english,
                            "category": stmt.excluded.category,
                            "data_start_date": func.coalesce(
                                stmt.excluded.data_start_date,
                                index_master.c.data_start_date,
                            ),
                            "updated_at": stmt.excluded.updated_at,
                        },
                    )
                )
            return len(rows)

    # --- Stats (Phase 3D-2) ---

    def get_topix_date_range(self) -> dict[str, Any] | None:
        """TOPIX 日付範囲 + 件数"""
        with self.engine.connect() as conn:
            row = conn.execute(
                select(
                    func.count(topix_data.c.date).label("count"),
                    func.min(topix_data.c.date).label("min"),
                    func.max(topix_data.c.date).label("max"),
                )
            ).fetchone()
        if row is None or row.min is None:
            return None
        return {"count": row.count, "min": row.min, "max": row.max}

    def get_stock_data_date_range(self) -> dict[str, Any] | None:
        """stock_data 日付範囲 + 統計"""
        with self.engine.connect() as conn:
            row = conn.execute(
                select(
                    func.count().label("count"),
                    func.min(stock_data.c.date).label("min"),
                    func.max(stock_data.c.date).label("max"),
                    func.count(func.distinct(stock_data.c.date)).label("date_count"),
                )
            ).fetchone()
        if row is None or row.min is None:
            return None
        avg_per_day = row.count / row.date_count if row.date_count > 0 else 0
        return {
            "count": row.count,
            "min": row.min,
            "max": row.max,
            "dateCount": row.date_count,
            "averageStocksPerDay": round(avg_per_day, 1),
        }

    def get_stock_count_by_market(self) -> dict[str, int]:
        """市場別の銘柄数"""
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(
                    stocks.c.market_name,
                    func.count(stocks.c.code).label("count"),
                )
                .group_by(stocks.c.market_name)
            ).fetchall()
        return {row[0]: row[1] for row in rows}

    def get_indices_data_range(self) -> dict[str, Any] | None:
        """indices_data 統計"""
        with self.engine.connect() as conn:
            master_count = conn.execute(select(func.count()).select_from(index_master)).scalar() or 0
            row = conn.execute(
                select(
                    func.count().label("count"),
                    func.count(func.distinct(indices_data.c.date)).label("date_count"),
                    func.min(indices_data.c.date).label("min"),
                    func.max(indices_data.c.date).label("max"),
                )
            ).fetchone()
            cat_rows = conn.execute(
                select(
                    index_master.c.category,
                    func.count(index_master.c.code).label("count"),
                )
                .group_by(index_master.c.category)
            ).fetchall()
        by_category = {r.category: r.count for r in cat_rows}
        if row is None or row.min is None:
            return {"masterCount": master_count, "dataCount": 0, "dateCount": 0, "dateRange": None, "byCategory": by_category}
        return {
            "masterCount": master_count,
            "dataCount": row.count,
            "dateCount": row.date_count,
            "dateRange": {"min": row.min, "max": row.max},
            "byCategory": by_category,
        }

    def is_initialized(self) -> bool:
        """sync_metadata に init_completed があるか"""
        val = self.get_sync_metadata(METADATA_KEYS["INIT_COMPLETED"])
        return val == "true"

    def get_db_file_size(self) -> int:
        """DB ファイルサイズ"""
        # engine URL から取得
        url_str = str(self.engine.url)
        if url_str.startswith("sqlite:///"):
            path = url_str[len("sqlite:///"):]
        else:
            # creator モードの場合
            return 0
        try:
            return os.path.getsize(path)
        except OSError:
            return 0

    # --- Validate (Phase 3D-2) ---

    def get_missing_stock_data_dates(self) -> list[str]:
        """TOPIX 日付のうち stock_data に存在しない日付"""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT t.date FROM topix_data t
                    WHERE t.date NOT IN (SELECT DISTINCT date FROM stock_data)
                    ORDER BY t.date DESC
                    LIMIT 100
                """)
            ).fetchall()
        return [r[0] for r in rows]

    def get_adjustment_events(self, limit: int = 20) -> list[dict[str, Any]]:
        """adjustment_factor != 1.0 のイベント"""
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(
                    stock_data.c.code,
                    stock_data.c.date,
                    stock_data.c.adjustment_factor,
                    stock_data.c.close,
                )
                .where(stock_data.c.adjustment_factor != 1.0)
                .where(stock_data.c.adjustment_factor.isnot(None))
                .order_by(stock_data.c.date.desc())
                .limit(limit)
            ).fetchall()
        return [
            {
                "code": r.code,
                "date": r.date,
                "adjustmentFactor": r.adjustment_factor,
                "close": r.close,
                "eventType": "stock_split" if r.adjustment_factor < 1.0 else "reverse_split",
            }
            for r in rows
        ]

    def get_stocks_needing_refresh(self, limit: int = 20) -> list[str]:
        """調整イベントがある銘柄のうち再取得が必要なもの"""
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(func.distinct(stock_data.c.code))
                .where(stock_data.c.adjustment_factor != 1.0)
                .where(stock_data.c.adjustment_factor.isnot(None))
                .limit(limit)
            ).fetchall()
        return [r[0] for r in rows]

    def get_stock_data_unique_date_count(self) -> int:
        """stock_data のユニーク日付数"""
        with self.engine.connect() as conn:
            return conn.execute(select(func.count(func.distinct(stock_data.c.date)))).scalar() or 0
