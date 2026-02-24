"""
Tests for query_helpers
"""

from __future__ import annotations

from src.infrastructure.db.market.query_helpers import (
    expand_stock_code,
    is_valid_stock_code,
    normalize_stock_code,
)


class TestNormalizeStockCode:
    def test_5digit_to_4digit(self) -> None:
        assert normalize_stock_code("72030") == "7203"

    def test_4digit_unchanged(self) -> None:
        assert normalize_stock_code("7203") == "7203"

    def test_5digit_not_ending_0(self) -> None:
        assert normalize_stock_code("72031") == "72031"

    def test_3digit_unchanged(self) -> None:
        assert normalize_stock_code("720") == "720"


class TestExpandStockCode:
    def test_4digit_to_5digit(self) -> None:
        assert expand_stock_code("7203") == "72030"

    def test_5digit_unchanged(self) -> None:
        assert expand_stock_code("72030") == "72030"


class TestIsValidStockCode:
    def test_valid_4digit(self) -> None:
        assert is_valid_stock_code("7203") is True

    def test_valid_with_letter(self) -> None:
        assert is_valid_stock_code("1A2B") is True

    def test_invalid_5digit(self) -> None:
        assert is_valid_stock_code("72030") is False

    def test_invalid_3digit(self) -> None:
        assert is_valid_stock_code("720") is False

    def test_invalid_lowercase(self) -> None:
        assert is_valid_stock_code("1a2b") is False

    def test_invalid_empty(self) -> None:
        assert is_valid_stock_code("") is False
