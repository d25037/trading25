"""
Tests for PortfolioDb
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy.exc import IntegrityError

from src.server.db.portfolio_db import PortfolioDb


@pytest.fixture()
def pdb(tmp_path: Path) -> PortfolioDb:
    db = PortfolioDb(str(tmp_path / "portfolio.db"))
    yield db  # type: ignore[misc]
    db.close()


# ===== Schema =====

class TestPortfolioDbSchema:
    def test_schema_version(self, pdb: PortfolioDb) -> None:
        assert pdb.get_schema_version() == "1.1.0"

    def test_tables_created(self, pdb: PortfolioDb) -> None:
        from sqlalchemy import text

        with pdb.engine.connect() as conn:
            tables = [
                row[0]
                for row in conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table'")
                ).fetchall()
            ]
        assert "portfolios" in tables
        assert "portfolio_items" in tables
        assert "watchlists" in tables
        assert "watchlist_items" in tables
        assert "portfolio_metadata" in tables


# ===== Portfolio CRUD =====

class TestPortfolioCRUD:
    def test_create_portfolio(self, pdb: PortfolioDb) -> None:
        p = pdb.create_portfolio("test", "Test Portfolio")
        assert p.name == "test"
        assert p.description == "Test Portfolio"
        assert p.id == 1

    def test_create_portfolio_no_description(self, pdb: PortfolioDb) -> None:
        p = pdb.create_portfolio("test")
        assert p.description is None

    def test_get_portfolio(self, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("test")
        p = pdb.get_portfolio(1)
        assert p is not None
        assert p.name == "test"

    def test_get_portfolio_not_found(self, pdb: PortfolioDb) -> None:
        assert pdb.get_portfolio(999) is None

    def test_get_portfolio_by_name(self, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("test")
        p = pdb.get_portfolio_by_name("test")
        assert p is not None
        assert p.id == 1

    def test_list_portfolios(self, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("p1")
        pdb.create_portfolio("p2")
        result = pdb.list_portfolios()
        assert len(result) == 2

    def test_update_portfolio(self, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("test")
        updated = pdb.update_portfolio(1, name="updated")
        assert updated is not None
        assert updated.name == "updated"

    def test_delete_portfolio(self, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("test")
        assert pdb.delete_portfolio(1) is True
        assert pdb.get_portfolio(1) is None

    def test_delete_portfolio_not_found(self, pdb: PortfolioDb) -> None:
        assert pdb.delete_portfolio(999) is False

    def test_duplicate_name_raises(self, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("test")
        with pytest.raises(IntegrityError):
            pdb.create_portfolio("test")


# ===== Portfolio Items CRUD =====

class TestPortfolioItemsCRUD:
    def _create_portfolio_with_item(self, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("test")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-15")

    def test_add_item(self, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("test")
        item = pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-15")
        assert item.code == "7203"
        assert item.quantity == 100

    def test_add_item_5digit_code(self, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("test")
        item = pdb.add_item(1, "72030", "トヨタ", 100, 2500.0, "2024-01-15")
        assert item.code == "7203"  # 5桁→4桁自動変換

    def test_list_items(self, pdb: PortfolioDb) -> None:
        self._create_portfolio_with_item(pdb)
        items = pdb.list_items(1)
        assert len(items) == 1

    def test_get_item(self, pdb: PortfolioDb) -> None:
        self._create_portfolio_with_item(pdb)
        item = pdb.get_item(1)
        assert item is not None
        assert item.code == "7203"

    def test_update_item(self, pdb: PortfolioDb) -> None:
        self._create_portfolio_with_item(pdb)
        updated = pdb.update_item(1, quantity=200)
        assert updated is not None
        assert updated.quantity == 200

    def test_delete_item(self, pdb: PortfolioDb) -> None:
        self._create_portfolio_with_item(pdb)
        assert pdb.delete_item(1) is True
        assert pdb.get_item(1) is None

    def test_get_item_by_code(self, pdb: PortfolioDb) -> None:
        self._create_portfolio_with_item(pdb)
        item = pdb.get_item_by_code(1, "7203")
        assert item is not None
        assert item.quantity == 100

    def test_duplicate_stock_raises(self, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("test")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-15")
        with pytest.raises(IntegrityError):
            pdb.add_item(1, "7203", "トヨタ", 200, 3000.0, "2024-02-01")

    def test_fk_constraint(self, pdb: PortfolioDb) -> None:
        with pytest.raises(IntegrityError):
            pdb.add_item(999, "7203", "トヨタ", 100, 2500.0, "2024-01-15")


# ===== CASCADE Delete =====

class TestCascadeDelete:
    def test_cascade_delete_items(self, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("test")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-15")
        pdb.add_item(1, "6758", "ソニー", 50, 1500.0, "2024-01-15")
        assert len(pdb.list_items(1)) == 2

        pdb.delete_portfolio(1)
        assert len(pdb.list_items(1)) == 0


# ===== Portfolio Name-based Operations =====

class TestPortfolioNameOps:
    def test_get_portfolio_codes(self, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("test")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-15")
        pdb.add_item(1, "6758", "ソニー", 50, 1500.0, "2024-01-15")
        codes = pdb.get_portfolio_codes("test")
        assert set(codes) == {"7203", "6758"}

    def test_get_portfolio_codes_not_found(self, pdb: PortfolioDb) -> None:
        assert pdb.get_portfolio_codes("nonexistent") == []

    def test_upsert_stock_new(self, pdb: PortfolioDb) -> None:
        item = pdb.upsert_stock("test", "7203", "トヨタ", 100, 2500.0, "2024-01-15")
        assert item.code == "7203"
        assert item.quantity == 100
        # Portfolio should have been auto-created
        assert pdb.get_portfolio_by_name("test") is not None

    def test_upsert_stock_update(self, pdb: PortfolioDb) -> None:
        pdb.upsert_stock("test", "7203", "トヨタ", 100, 2500.0, "2024-01-15")
        item = pdb.upsert_stock("test", "7203", "トヨタ", 200, 3000.0, "2024-02-01")
        assert item.quantity == 200

    def test_delete_stock(self, pdb: PortfolioDb) -> None:
        pdb.upsert_stock("test", "7203", "トヨタ", 100, 2500.0, "2024-01-15")
        assert pdb.delete_stock("test", "7203") is True
        assert pdb.delete_stock("test", "7203") is False

    def test_delete_stock_nonexistent_portfolio(self, pdb: PortfolioDb) -> None:
        assert pdb.delete_stock("nonexistent", "7203") is False


# ===== Portfolio Summary =====

class TestPortfolioSummary:
    def test_summary(self, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("test")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-15")
        pdb.add_item(1, "6758", "ソニー", 50, 1500.0, "2024-01-15")
        summary = pdb.get_portfolio_summary(1)
        assert summary is not None
        assert summary["stock_count"] == 2
        assert summary["total_shares"] == 150

    def test_summary_empty(self, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("test")
        summary = pdb.get_portfolio_summary(1)
        assert summary is not None
        assert summary["stock_count"] == 0

    def test_summary_not_found(self, pdb: PortfolioDb) -> None:
        assert pdb.get_portfolio_summary(999) is None


# ===== Watchlist CRUD =====

class TestWatchlistCRUD:
    def test_create_watchlist(self, pdb: PortfolioDb) -> None:
        wl = pdb.create_watchlist("Tech Stocks", "Tech sector watchlist")
        assert wl.name == "Tech Stocks"
        assert wl.description == "Tech sector watchlist"

    def test_list_watchlists(self, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("wl1")
        pdb.create_watchlist("wl2")
        assert len(pdb.list_watchlists()) == 2

    def test_get_watchlist(self, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("test")
        wl = pdb.get_watchlist(1)
        assert wl is not None
        assert wl.name == "test"

    def test_get_watchlist_by_name(self, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("test")
        wl = pdb.get_watchlist_by_name("test")
        assert wl is not None

    def test_update_watchlist(self, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("test")
        updated = pdb.update_watchlist(1, name="updated")
        assert updated is not None
        assert updated.name == "updated"

    def test_delete_watchlist(self, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("test")
        assert pdb.delete_watchlist(1) is True
        assert pdb.get_watchlist(1) is None

    def test_duplicate_name_raises(self, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("test")
        with pytest.raises(IntegrityError):
            pdb.create_watchlist("test")


# ===== Watchlist Items =====

class TestWatchlistItems:
    def test_add_item(self, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("test")
        item = pdb.add_watchlist_item(1, "7203", "トヨタ", memo="注目")
        assert item.code == "7203"
        assert item.memo == "注目"

    def test_add_item_5digit(self, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("test")
        item = pdb.add_watchlist_item(1, "72030", "トヨタ")
        assert item.code == "7203"

    def test_list_items(self, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("test")
        pdb.add_watchlist_item(1, "7203", "トヨタ")
        pdb.add_watchlist_item(1, "6758", "ソニー")
        items = pdb.list_watchlist_items(1)
        assert len(items) == 2

    def test_delete_item(self, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("test")
        pdb.add_watchlist_item(1, "7203", "トヨタ")
        assert pdb.delete_watchlist_item(1) is True
        assert len(pdb.list_watchlist_items(1)) == 0

    def test_cascade_delete(self, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("test")
        pdb.add_watchlist_item(1, "7203", "トヨタ")
        pdb.delete_watchlist(1)
        assert len(pdb.list_watchlist_items(1)) == 0

    def test_duplicate_stock_raises(self, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("test")
        pdb.add_watchlist_item(1, "7203", "トヨタ")
        with pytest.raises(IntegrityError):
            pdb.add_watchlist_item(1, "7203", "トヨタ")

    def test_fk_constraint(self, pdb: PortfolioDb) -> None:
        with pytest.raises(IntegrityError):
            pdb.add_watchlist_item(999, "7203", "トヨタ")


# ===== Empty Database =====

class TestEmptyDatabase:
    def test_list_empty_portfolios(self, pdb: PortfolioDb) -> None:
        assert pdb.list_portfolios() == []

    def test_list_empty_watchlists(self, pdb: PortfolioDb) -> None:
        assert pdb.list_watchlists() == []

    def test_list_empty_items(self, pdb: PortfolioDb) -> None:
        assert pdb.list_items(1) == []

    def test_list_empty_watchlist_items(self, pdb: PortfolioDb) -> None:
        assert pdb.list_watchlist_items(1) == []
