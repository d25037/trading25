"""portfolio.py モデルのテスト"""

from datetime import date, datetime

import pytest
from pydantic import ValidationError

from src.shared.models.portfolio import Portfolio, PortfolioItem, PortfolioMetadata, PortfolioSummary


NOW = datetime(2025, 1, 1, 0, 0, 0)


class TestPortfolioMetadata:
    def test_creation(self):
        m = PortfolioMetadata(key="version", value="1.0", updated_at=NOW)
        assert m.key == "version"
        assert m.value == "1.0"


class TestPortfolio:
    def test_creation(self):
        p = Portfolio(id=1, name="test", created_at=NOW, updated_at=NOW)
        assert p.id == 1
        assert p.name == "test"
        assert p.description is None

    def test_with_description(self):
        p = Portfolio(id=1, name="test", description="desc", created_at=NOW, updated_at=NOW)
        assert p.description == "desc"


def _make_item(**kwargs) -> PortfolioItem:
    defaults = dict(
        id=1,
        portfolio_id=1,
        code="7203",
        company_name="トヨタ自動車",
        quantity=100,
        purchase_price=2500.0,
        purchase_date=date(2025, 1, 15),
        created_at=NOW,
        updated_at=NOW,
    )
    defaults.update(kwargs)
    return PortfolioItem(**defaults)


class TestPortfolioItem:
    def test_creation(self):
        item = _make_item()
        assert item.code == "7203"
        assert item.quantity == 100

    def test_total_cost(self):
        item = _make_item(quantity=100, purchase_price=2500.0)
        assert item.total_cost == 250000.0

    def test_total_cost_single_share(self):
        item = _make_item(quantity=1, purchase_price=100.0)
        assert item.total_cost == 100.0

    def test_code_stripped(self):
        item = _make_item(code="  7203  ")
        assert item.code == "7203"

    def test_optional_fields(self):
        item = _make_item(account="NISA", notes="長期保有")
        assert item.account == "NISA"
        assert item.notes == "長期保有"


class TestPortfolioItemValidation:
    def test_empty_code(self):
        with pytest.raises(ValidationError, match="銘柄コード"):
            _make_item(code="")

    def test_whitespace_only_code(self):
        with pytest.raises(ValidationError, match="銘柄コード"):
            _make_item(code="   ")

    def test_quantity_zero(self):
        with pytest.raises(ValidationError):
            _make_item(quantity=0)

    def test_quantity_negative(self):
        with pytest.raises(ValidationError):
            _make_item(quantity=-1)

    def test_purchase_price_zero(self):
        with pytest.raises(ValidationError):
            _make_item(purchase_price=0)

    def test_purchase_price_negative(self):
        with pytest.raises(ValidationError):
            _make_item(purchase_price=-100)


class TestPortfolioSummary:
    def test_creation(self):
        portfolio = Portfolio(id=1, name="test", created_at=NOW, updated_at=NOW)
        items = [_make_item(), _make_item(id=2, code="6758")]
        summary = PortfolioSummary(
            portfolio=portfolio, items=items, total_stocks=2, total_cost=500000.0
        )
        assert summary.total_stocks == 2
        assert len(summary.items) == 2
