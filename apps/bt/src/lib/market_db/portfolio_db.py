"""
Portfolio Database Access (SQLAlchemy Core)

portfolio.db の CRUD 操作を提供する。
Phase 3E（Portfolio/Watchlist エンドポイント）の基盤。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Row, delete, func, insert, outerjoin, select, update

from src.lib.market_db.base import BaseDbAccess
from src.lib.market_db.query_helpers import normalize_stock_code
from src.lib.market_db.tables import (
    portfolio_items,
    portfolio_meta,
    portfolio_metadata,
    portfolios,
    watchlist_items,
    watchlists,
)


class PortfolioDb(BaseDbAccess):
    """portfolio.db CRUD"""

    def __init__(self, db_path: str) -> None:
        super().__init__(db_path, read_only=False)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Drizzle 互換テーブル作成（IF NOT EXISTS）"""
        portfolio_meta.create_all(self.engine, checkfirst=True)
        self._set_metadata("schema_version", "1.1.0")

    def _now(self) -> str:
        return datetime.now().isoformat()  # noqa: DTZ005

    # --- Metadata ---

    def _get_metadata(self, key: str) -> str | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                select(portfolio_metadata.c.value).where(portfolio_metadata.c.key == key)
            ).fetchone()
            return row[0] if row else None

    def _set_metadata(self, key: str, value: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                insert(portfolio_metadata)
                .values(key=key, value=value, updated_at=self._now())
                .prefix_with("OR REPLACE")
            )

    def get_schema_version(self) -> str:
        return self._get_metadata("schema_version") or "unknown"

    # ===== Portfolio CRUD =====

    def list_portfolios(self) -> list[Row[Any]]:
        with self.engine.connect() as conn:
            return list(
                conn.execute(select(portfolios).order_by(portfolios.c.created_at.desc())).fetchall()
            )

    def get_portfolio(self, portfolio_id: int) -> Row[Any] | None:
        with self.engine.connect() as conn:
            return conn.execute(
                select(portfolios).where(portfolios.c.id == portfolio_id)
            ).fetchone()

    def get_portfolio_by_name(self, name: str) -> Row[Any] | None:
        with self.engine.connect() as conn:
            return conn.execute(
                select(portfolios).where(portfolios.c.name == name)
            ).fetchone()

    def create_portfolio(self, name: str, description: str | None = None) -> Row[Any]:
        now = self._now()
        with self.engine.begin() as conn:
            result = conn.execute(
                insert(portfolios).values(
                    name=name,
                    description=description,
                    created_at=now,
                    updated_at=now,
                )
            )
            portfolio_id = result.lastrowid
        return self.get_portfolio(portfolio_id)  # type: ignore[return-value]

    def update_portfolio(
        self,
        portfolio_id: int,
        *,
        name: str | None = None,
        description: str | None = ...,  # type: ignore[assignment]
    ) -> Row[Any] | None:
        values: dict[str, Any] = {"updated_at": self._now()}
        if name is not None:
            values["name"] = name
        if description is not ...:
            values["description"] = description
        with self.engine.begin() as conn:
            conn.execute(
                update(portfolios).where(portfolios.c.id == portfolio_id).values(**values)
            )
        return self.get_portfolio(portfolio_id)

    def delete_portfolio(self, portfolio_id: int) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(
                delete(portfolios).where(portfolios.c.id == portfolio_id)
            )
            return result.rowcount > 0  # type: ignore[union-attr]

    # ===== Portfolio Items CRUD =====

    def list_items(self, portfolio_id: int) -> list[Row[Any]]:
        with self.engine.connect() as conn:
            return list(
                conn.execute(
                    select(portfolio_items)
                    .where(portfolio_items.c.portfolio_id == portfolio_id)
                    .order_by(portfolio_items.c.purchase_date.desc())
                ).fetchall()
            )

    def get_item(self, item_id: int) -> Row[Any] | None:
        with self.engine.connect() as conn:
            return conn.execute(
                select(portfolio_items).where(portfolio_items.c.id == item_id)
            ).fetchone()

    def add_item(
        self,
        portfolio_id: int,
        code: str,
        company_name: str,
        quantity: int,
        purchase_price: float,
        purchase_date: str,
        account: str | None = None,
        notes: str | None = None,
    ) -> Row[Any]:
        code = normalize_stock_code(code)
        now = self._now()
        with self.engine.begin() as conn:
            result = conn.execute(
                insert(portfolio_items).values(
                    portfolio_id=portfolio_id,
                    code=code,
                    company_name=company_name,
                    quantity=quantity,
                    purchase_price=purchase_price,
                    purchase_date=purchase_date,
                    account=account,
                    notes=notes,
                    created_at=now,
                    updated_at=now,
                )
            )
            item_id = result.lastrowid
        return self.get_item(item_id)  # type: ignore[return-value]

    def update_item(self, item_id: int, **kwargs: Any) -> Row[Any] | None:
        kwargs["updated_at"] = self._now()
        with self.engine.begin() as conn:
            conn.execute(
                update(portfolio_items).where(portfolio_items.c.id == item_id).values(**kwargs)
            )
        return self.get_item(item_id)

    def delete_item(self, item_id: int) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(
                delete(portfolio_items).where(portfolio_items.c.id == item_id)
            )
            return result.rowcount > 0  # type: ignore[union-attr]

    def get_item_by_code(self, portfolio_id: int, code: str) -> Row[Any] | None:
        code = normalize_stock_code(code)
        with self.engine.connect() as conn:
            return conn.execute(
                select(portfolio_items).where(
                    portfolio_items.c.portfolio_id == portfolio_id,
                    portfolio_items.c.code == code,
                )
            ).fetchone()

    def get_portfolio_codes(self, portfolio_name: str) -> list[str]:
        """ポートフォリオ名から銘柄コード一覧を取得"""
        portfolio = self.get_portfolio_by_name(portfolio_name)
        if portfolio is None:
            return []
        items = self.list_items(portfolio.id)
        return [item.code for item in items]

    def upsert_stock(
        self,
        portfolio_name: str,
        code: str,
        company_name: str,
        quantity: int,
        purchase_price: float,
        purchase_date: str,
        **kwargs: Any,
    ) -> Row[Any]:
        """ポートフォリオ名＋銘柄コードで upsert"""
        portfolio = self.get_portfolio_by_name(portfolio_name)
        if portfolio is None:
            portfolio = self.create_portfolio(portfolio_name)
        existing = self.get_item_by_code(portfolio.id, code)
        if existing:
            return self.update_item(  # type: ignore[return-value]
                existing.id,
                company_name=company_name,
                quantity=quantity,
                purchase_price=purchase_price,
                purchase_date=purchase_date,
                **kwargs,
            )
        return self.add_item(
            portfolio.id,
            code,
            company_name=company_name,
            quantity=quantity,
            purchase_price=purchase_price,
            purchase_date=purchase_date,
            **kwargs,
        )

    def delete_stock(self, portfolio_name: str, code: str) -> bool:
        """ポートフォリオ名＋銘柄コードで削除"""
        portfolio = self.get_portfolio_by_name(portfolio_name)
        if portfolio is None:
            return False
        item = self.get_item_by_code(portfolio.id, code)
        if item is None:
            return False
        return self.delete_item(item.id)

    # ===== Portfolio Summary =====

    def get_portfolio_summary(self, portfolio_id: int) -> dict[str, Any] | None:
        """ポートフォリオのサマリーを取得"""
        portfolio = self.get_portfolio(portfolio_id)
        if portfolio is None:
            return None
        with self.engine.connect() as conn:
            row = conn.execute(
                select(
                    func.count().label("stock_count"),
                    func.coalesce(func.sum(portfolio_items.c.quantity), 0).label("total_shares"),
                ).where(portfolio_items.c.portfolio_id == portfolio_id)
            ).fetchone()
        return {
            "id": portfolio.id,
            "name": portfolio.name,
            "description": portfolio.description,
            "stock_count": row[0] if row else 0,
            "total_shares": row[1] if row else 0,
            "created_at": portfolio.created_at,
            "updated_at": portfolio.updated_at,
        }

    def list_portfolio_summaries(self) -> list[dict[str, Any]]:
        """LEFT JOIN で stockCount/totalShares 付きポートフォリオ一覧を返す"""
        j = outerjoin(portfolios, portfolio_items, portfolios.c.id == portfolio_items.c.portfolio_id)
        stmt = (
            select(
                portfolios,
                func.count(portfolio_items.c.id).label("stock_count"),
                func.coalesce(func.sum(portfolio_items.c.quantity), 0).label("total_shares"),
            )
            .select_from(j)
            .group_by(portfolios.c.id)
            .order_by(portfolios.c.created_at.desc())
        )
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
        return [
            {
                "id": r.id,
                "name": r.name,
                "description": r.description,
                "stock_count": r.stock_count,
                "total_shares": r.total_shares,
                "created_at": r.created_at,
                "updated_at": r.updated_at,
            }
            for r in rows
        ]

    def list_watchlist_summaries(self) -> list[dict[str, Any]]:
        """LEFT JOIN で stockCount 付きウォッチリスト一覧を返す"""
        j = outerjoin(watchlists, watchlist_items, watchlists.c.id == watchlist_items.c.watchlist_id)
        stmt = (
            select(
                watchlists,
                func.count(watchlist_items.c.id).label("stock_count"),
            )
            .select_from(j)
            .group_by(watchlists.c.id)
            .order_by(watchlists.c.created_at.desc())
        )
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
        return [
            {
                "id": r.id,
                "name": r.name,
                "description": r.description,
                "stock_count": r.stock_count,
                "created_at": r.created_at,
                "updated_at": r.updated_at,
            }
            for r in rows
        ]

    # ===== Watchlist CRUD =====

    def list_watchlists(self) -> list[Row[Any]]:
        with self.engine.connect() as conn:
            return list(
                conn.execute(select(watchlists).order_by(watchlists.c.created_at.desc())).fetchall()
            )

    def get_watchlist(self, watchlist_id: int) -> Row[Any] | None:
        with self.engine.connect() as conn:
            return conn.execute(
                select(watchlists).where(watchlists.c.id == watchlist_id)
            ).fetchone()

    def get_watchlist_by_name(self, name: str) -> Row[Any] | None:
        with self.engine.connect() as conn:
            return conn.execute(
                select(watchlists).where(watchlists.c.name == name)
            ).fetchone()

    def create_watchlist(self, name: str, description: str | None = None) -> Row[Any]:
        now = self._now()
        with self.engine.begin() as conn:
            result = conn.execute(
                insert(watchlists).values(
                    name=name,
                    description=description,
                    created_at=now,
                    updated_at=now,
                )
            )
            watchlist_id = result.lastrowid
        return self.get_watchlist(watchlist_id)  # type: ignore[return-value]

    def update_watchlist(self, watchlist_id: int, **kwargs: Any) -> Row[Any] | None:
        kwargs["updated_at"] = self._now()
        with self.engine.begin() as conn:
            conn.execute(
                update(watchlists).where(watchlists.c.id == watchlist_id).values(**kwargs)
            )
        return self.get_watchlist(watchlist_id)

    def delete_watchlist(self, watchlist_id: int) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(
                delete(watchlists).where(watchlists.c.id == watchlist_id)
            )
            return result.rowcount > 0  # type: ignore[union-attr]

    # ===== Watchlist Items =====

    def list_watchlist_items(self, watchlist_id: int) -> list[Row[Any]]:
        with self.engine.connect() as conn:
            return list(
                conn.execute(
                    select(watchlist_items)
                    .where(watchlist_items.c.watchlist_id == watchlist_id)
                    .order_by(watchlist_items.c.created_at.desc())
                ).fetchall()
            )

    def add_watchlist_item(
        self,
        watchlist_id: int,
        code: str,
        company_name: str,
        memo: str | None = None,
    ) -> Row[Any]:
        code = normalize_stock_code(code)
        now = self._now()
        with self.engine.begin() as conn:
            result = conn.execute(
                insert(watchlist_items).values(
                    watchlist_id=watchlist_id,
                    code=code,
                    company_name=company_name,
                    memo=memo,
                    created_at=now,
                )
            )
            item_id = result.lastrowid
        with self.engine.connect() as conn:
            return conn.execute(  # type: ignore[return-value]
                select(watchlist_items).where(watchlist_items.c.id == item_id)
            ).fetchone()

    def delete_watchlist_item(self, item_id: int) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(
                delete(watchlist_items).where(watchlist_items.c.id == item_id)
            )
            return result.rowcount > 0  # type: ignore[union-attr]
