"""
Tests for query_helpers
"""

from __future__ import annotations

from sqlalchemy import Column, MetaData, String, Table

from src.infrastructure.db.market.query_helpers import (
    expand_stock_code,
    is_valid_stock_code,
    market_filter,
    max_trading_date,
    normalize_stock_code,
    ohlcv_query,
    stock_lookup,
    stock_code_candidates,
    stock_code_query_candidates,
    trading_date_before,
)


def _build_test_table() -> Table:
    metadata = MetaData()
    return Table(
        "stock_data",
        metadata,
        Column("code", String),
        Column("date", String),
        Column("market_code", String),
    )


class TestNormalizeStockCode:
    def test_5digit_to_4digit(self) -> None:
        assert normalize_stock_code("72030") == "7203"

    def test_6digit_to_5digit(self) -> None:
        assert normalize_stock_code("259350") == "25935"

    def test_4digit_unchanged(self) -> None:
        assert normalize_stock_code("7203") == "7203"

    def test_5digit_not_ending_0(self) -> None:
        assert normalize_stock_code("72031") == "72031"

    def test_3digit_unchanged(self) -> None:
        assert normalize_stock_code("720") == "720"


class TestExpandStockCode:
    def test_4digit_to_5digit(self) -> None:
        assert expand_stock_code("7203") == "72030"

    def test_5digit_api_code_unchanged(self) -> None:
        assert expand_stock_code("72030") == "72030"

    def test_5digit_to_6digit(self) -> None:
        assert expand_stock_code("25935") == "259350"

    def test_6digit_api_code_unchanged(self) -> None:
        assert expand_stock_code("259350") == "259350"

    def test_non_stock_length_is_unchanged(self) -> None:
        assert expand_stock_code("720") == "720"


class TestStockCodeCandidates:
    def test_4digit_and_api_code_candidates(self) -> None:
        assert stock_code_candidates("7203") == ("7203", "72030")

    def test_5digit_and_api_code_candidates(self) -> None:
        assert stock_code_candidates("25935") == ("25935", "259350")

    def test_single_candidate_when_expansion_is_unnecessary(self) -> None:
        assert stock_code_candidates("720") == ("720",)


class TestStockCodeQueryCandidates:
    def test_expands_multiple_codes_preserving_first_seen_order(self) -> None:
        assert stock_code_query_candidates(["72030", "25935"]) == (
            "7203",
            "72030",
            "25935",
            "259350",
        )

    def test_dedupes_overlapping_code_forms(self) -> None:
        assert stock_code_query_candidates(["72030", "7203", "72030"]) == ("7203", "72030")


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


class TestQueryBuilders:
    def test_max_trading_date_builds_scalar_subquery(self) -> None:
        table = _build_test_table()

        compiled = str(max_trading_date(table).compile(compile_kwargs={"literal_binds": True}))

        assert "SELECT max(stock_data.date) AS max_1" in compiled
        assert "FROM stock_data" in compiled

    def test_trading_date_before_builds_offset_limit_subquery(self) -> None:
        table = _build_test_table()

        compiled = str(
            trading_date_before(table, "2026-03-06", 2).compile(compile_kwargs={"literal_binds": True})
        )

        assert "WHERE stock_data.date <= '2026-03-06'" in compiled
        assert "ORDER BY stock_data.date DESC" in compiled
        assert "LIMIT 1 OFFSET 2" in compiled

    def test_ohlcv_query_applies_optional_start_and_end_filters(self) -> None:
        table = _build_test_table()

        compiled = str(
            ohlcv_query(table, "7203", start="2026-01-01", end="2026-02-01").compile(
                compile_kwargs={"literal_binds": True}
            )
        )

        assert "WHERE stock_data.code = '7203'" in compiled
        assert "AND stock_data.date >= '2026-01-01'" in compiled
        assert "AND stock_data.date <= '2026-02-01'" in compiled
        assert compiled.endswith("ORDER BY stock_data.date")

    def test_market_filter_builds_market_code_in_clause(self) -> None:
        table = _build_test_table()

        compiled = str(
            market_filter(table, ["0111", "0112"]).compile(compile_kwargs={"literal_binds": True})
        )

        assert "WHERE stock_data.market_code IN ('0111', '0112')" in compiled

    def test_stock_lookup_builds_code_filter(self) -> None:
        table = _build_test_table()

        compiled = str(stock_lookup(table, "7203").compile(compile_kwargs={"literal_binds": True}))

        assert "WHERE stock_data.code = '7203'" in compiled
