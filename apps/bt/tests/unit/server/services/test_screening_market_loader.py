from __future__ import annotations

import sqlite3
from typing import Any

import pandas as pd
import pytest

from src.server.services.screening_market_loader import (
    _attach_statements,
    _group_statement_rows,
    _load_daily_by_code,
    _normalize_codes,
    _query_statements_rows,
    _resolve_period_filter_values,
    _rows_to_ohlc_df,
    _rows_to_ohlcv_df,
    load_market_multi_data,
    load_market_sector_indices,
    load_market_stock_sector_mapping,
    load_market_topix_data,
)


class DummyReader:
    def __init__(self, rows: list[dict[str, Any]] | None = None, error: Exception | None = None) -> None:
        self.rows = rows or []
        self.error = error
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        self.calls.append((sql, params))
        if self.error is not None:
            raise self.error
        return self.rows


def test_load_market_multi_data_returns_empty_for_empty_codes() -> None:
    reader = DummyReader()
    result, warnings = load_market_multi_data(reader, [])
    assert result == {}
    assert warnings == []


def test_load_market_multi_data_handles_daily_operational_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.server.services.screening_market_loader._load_daily_by_code",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(sqlite3.OperationalError("stock_data missing")),
    )
    result, warnings = load_market_multi_data(DummyReader(), ["7203"])
    assert result == {}
    assert any("market daily load failed" in warning for warning in warnings)


def test_load_market_multi_data_attaches_statements(monkeypatch: pytest.MonkeyPatch) -> None:
    index = pd.to_datetime(["2026-01-01", "2026-01-02"])
    daily = pd.DataFrame(
        {"Open": [1.0, 1.0], "High": [1.1, 1.2], "Low": [0.9, 1.0], "Close": [1.0, 1.1], "Volume": [100, 200]},
        index=index,
    )

    monkeypatch.setattr(
        "src.server.services.screening_market_loader._load_daily_by_code",
        lambda *_args, **_kwargs: {"7203": daily},
    )

    def _attach(_reader, result, _daily_index_by_code, **_kwargs):
        result["7203"]["statements_daily"] = pd.DataFrame({"dummy": [1, 2]}, index=index)
        return ["attached"]

    monkeypatch.setattr("src.server.services.screening_market_loader._attach_statements", _attach)

    result, warnings = load_market_multi_data(
        DummyReader(),
        ["72030"],
        include_statements_data=True,
        period_type="FY",
        include_forecast_revision=True,
    )

    assert "7203" in result
    assert "daily" in result["7203"]
    assert "statements_daily" in result["7203"]
    assert warnings == ["attached"]


def test_load_market_topix_data_builds_filtered_dataframe() -> None:
    reader = DummyReader(
        rows=[
            {"date": "2026-01-01", "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5},
            {"date": "2026-01-02", "open": 101.0, "high": 102.0, "low": 100.0, "close": 101.5},
        ]
    )
    df = load_market_topix_data(reader, start_date="2026-01-01", end_date="2026-01-31")
    sql, params = reader.calls[0]
    assert "WHERE date >= ? AND date <= ?" in sql
    assert params == ("2026-01-01", "2026-01-31")
    assert list(df.columns) == ["Open", "High", "Low", "Close"]
    assert len(df) == 2


def test_load_market_sector_indices_handles_operational_error() -> None:
    reader = DummyReader(error=sqlite3.OperationalError("missing index table"))
    assert load_market_sector_indices(reader) == {}


def test_load_market_sector_indices_filters_by_category_and_name() -> None:
    reader = DummyReader(
        rows=[
            {
                "code": "0010",
                "date": "2026-01-01",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "sector_name": "食料品",
                "category": "sector33",
            },
            {
                "code": "0000",
                "date": "2026-01-01",
                "open": 200.0,
                "high": 201.0,
                "low": 199.0,
                "close": 200.5,
                "sector_name": "TOPIX",
                "category": "topix",
            },
            {
                "code": "0099",
                "date": "2026-01-01",
                "open": 300.0,
                "high": 301.0,
                "low": 299.0,
                "close": 300.5,
                "sector_name": "",
                "category": "sector17",
            },
        ]
    )
    sector = load_market_sector_indices(reader)
    assert set(sector.keys()) == {"食料品"}
    assert len(sector["食料品"]) == 1


def test_load_market_stock_sector_mapping_normalizes_codes() -> None:
    reader = DummyReader(
        rows=[
            {"code": "72030", "sector_33_name": "輸送用機器"},
            {"code": "6758", "sector_33_name": "電気機器"},
            {"code": "", "sector_33_name": "invalid"},
            {"code": "13010", "sector_33_name": ""},
        ]
    )
    mapping = load_market_stock_sector_mapping(reader)
    assert mapping == {"7203": "輸送用機器", "6758": "電気機器"}


def test_normalize_codes_dedupes_and_skips_invalid() -> None:
    assert _normalize_codes(["72030", "7203", "", "abc", "67580"]) == ["7203", "abc", "6758"]


def test_load_daily_by_code_with_date_filters() -> None:
    reader = DummyReader(
        rows=[
            {"code": "7203", "date": "2026-01-01", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0, "volume": 100},
            {"code": "7203", "date": "2026-01-02", "open": 1.1, "high": 1.2, "low": 1.0, "close": 1.1, "volume": 200},
        ]
    )
    data = _load_daily_by_code(reader, ["7203"], start_date="2026-01-01", end_date="2026-01-31")
    sql, params = reader.calls[0]
    assert "date >= ?" in sql
    assert "date <= ?" in sql
    assert params == ("7203", "2026-01-01", "2026-01-31")
    assert "7203" in data
    assert len(data["7203"]) == 2


def test_attach_statements_missing_table_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    index = pd.to_datetime(["2026-01-01"])
    result = {"7203": {"daily": pd.DataFrame({"Close": [1.0]}, index=index)}}
    daily_index = {"7203": index}

    monkeypatch.setattr(
        "src.server.services.screening_market_loader._query_statements_rows",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(sqlite3.OperationalError("no such table: statements")),
    )

    warnings = _attach_statements(
        DummyReader(),
        result,
        daily_index,
        start_date=None,
        end_date=None,
        period_type="FY",
        include_forecast_revision=False,
    )
    assert any("statements table is missing" in warning for warning in warnings)


def test_attach_statements_merges_revision(monkeypatch: pytest.MonkeyPatch) -> None:
    index = pd.to_datetime(["2026-01-01", "2026-01-02"])
    result = {"7203": {"daily": pd.DataFrame({"Close": [1.0, 1.1]}, index=index)}}
    daily_index = {"7203": index}
    base_row = {
        "code": "7203",
        "disclosed_date": "2026-01-01",
        "earnings_per_share": 1.0,
        "profit": 1.0,
        "equity": 1.0,
        "type_of_current_period": "FY",
        "type_of_document": "Annual",
        "next_year_forecast_earnings_per_share": 2.0,
        "bps": 3.0,
        "sales": 4.0,
        "operating_profit": 5.0,
        "ordinary_profit": 6.0,
        "operating_cash_flow": 7.0,
        "dividend_fy": 8.0,
        "forecast_dividend_fy": 8.5,
        "next_year_forecast_dividend_fy": 9.0,
        "payout_ratio": 30.0,
        "forecast_payout_ratio": 32.0,
        "next_year_forecast_payout_ratio": 34.0,
        "forecast_eps": 9.0,
        "investing_cash_flow": 10.0,
        "financing_cash_flow": 11.0,
        "cash_and_equivalents": 12.0,
        "total_assets": 13.0,
        "shares_outstanding": 14.0,
        "treasury_shares": 15.0,
    }
    base_rows = [base_row]
    revision_rows = [{**base_row, "earnings_per_share": 2.0, "profit": 2.0, "equity": 2.0}]

    def _query(_reader, _codes, **kwargs):
        return revision_rows if kwargs.get("period_type") == "all" else base_rows

    monkeypatch.setattr("src.server.services.screening_market_loader._query_statements_rows", _query)
    monkeypatch.setattr("src.server.services.screening_market_loader.transform_statements_df", lambda df: df)
    monkeypatch.setattr(
        "src.server.services.screening_market_loader.merge_forward_forecast_revision",
        lambda base, _revision: base.assign(merged=True),
    )

    warnings = _attach_statements(
        DummyReader(),
        result,
        daily_index,
        start_date=None,
        end_date=None,
        period_type="FY",
        include_forecast_revision=True,
    )

    assert warnings == []
    assert "statements_daily" in result["7203"]
    assert bool(result["7203"]["statements_daily"]["merged"].iloc[-1]) is True


def test_attach_statements_collects_transform_error(monkeypatch: pytest.MonkeyPatch) -> None:
    index = pd.to_datetime(["2026-01-01"])
    result = {"7203": {"daily": pd.DataFrame({"Close": [1.0]}, index=index)}}
    daily_index = {"7203": index}
    rows = [
        {
            "code": "7203",
            "disclosed_date": "2026-01-01",
            "earnings_per_share": 1.0,
            "profit": 1.0,
            "equity": 1.0,
            "type_of_current_period": "FY",
            "type_of_document": "Annual",
            "next_year_forecast_earnings_per_share": 2.0,
            "bps": 3.0,
            "sales": 4.0,
            "operating_profit": 5.0,
            "ordinary_profit": 6.0,
            "operating_cash_flow": 7.0,
            "dividend_fy": 8.0,
            "forecast_dividend_fy": 8.5,
            "next_year_forecast_dividend_fy": 9.0,
            "payout_ratio": 30.0,
            "forecast_payout_ratio": 32.0,
            "next_year_forecast_payout_ratio": 34.0,
            "forecast_eps": 9.0,
            "investing_cash_flow": 10.0,
            "financing_cash_flow": 11.0,
            "cash_and_equivalents": 12.0,
            "total_assets": 13.0,
            "shares_outstanding": 14.0,
            "treasury_shares": 15.0,
        }
    ]

    monkeypatch.setattr("src.server.services.screening_market_loader._query_statements_rows", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(
        "src.server.services.screening_market_loader.transform_statements_df",
        lambda _df: (_ for _ in ()).throw(ValueError("transform failed")),
    )

    warnings = _attach_statements(
        DummyReader(),
        result,
        daily_index,
        start_date=None,
        end_date=None,
        period_type="FY",
        include_forecast_revision=False,
    )

    assert any("transform failed" in warning for warning in warnings)


def test_query_statements_rows_builds_filters() -> None:
    reader = DummyReader()
    _query_statements_rows(
        reader,
        ["7203"],
        start_date="2026-01-01",
        end_date="2026-01-31",
        period_type="1Q",
        actual_only=True,
    )
    sql, params = reader.calls[0]
    assert "disclosed_date >= ?" in sql
    assert "disclosed_date <= ?" in sql
    assert "type_of_current_period IN" in sql
    assert "earnings_per_share IS NOT NULL" in sql
    assert params[0:3] == ("7203", "2026-01-01", "2026-01-31")
    assert "1Q" in params and "Q1" in params


@pytest.mark.parametrize(
    ("period_type", "expected"),
    [
        ("all", None),
        ("FY", ["FY"]),
        ("1Q", ["1Q", "Q1"]),
        ("2Q", ["2Q", "Q2"]),
        ("3Q", ["3Q", "Q3"]),
    ],
)
def test_resolve_period_filter_values(period_type: str, expected: list[str] | None) -> None:
    assert _resolve_period_filter_values(period_type) == expected


def test_group_statement_rows_and_ohlc_helpers() -> None:
    grouped = _group_statement_rows(
        [
            {
                "code": "72030",
                "disclosed_date": "2026-01-01",
                "earnings_per_share": 1.0,
                "profit": 1.0,
                "equity": 1.0,
                "type_of_current_period": "FY",
                "type_of_document": "Annual",
                "next_year_forecast_earnings_per_share": 2.0,
                "bps": 3.0,
                "sales": 4.0,
                "operating_profit": 5.0,
                "ordinary_profit": 6.0,
                "operating_cash_flow": 7.0,
                "dividend_fy": 8.0,
                "forecast_dividend_fy": 8.5,
                "next_year_forecast_dividend_fy": 9.0,
                "payout_ratio": 30.0,
                "forecast_payout_ratio": 32.0,
                "next_year_forecast_payout_ratio": 34.0,
                "forecast_eps": 9.0,
                "investing_cash_flow": 10.0,
                "financing_cash_flow": 11.0,
                "cash_and_equivalents": 12.0,
                "total_assets": 13.0,
                "shares_outstanding": 14.0,
                "treasury_shares": 15.0,
            }
        ]
    )
    assert "7203" in grouped
    assert len(grouped["7203"]) == 1

    ohlcv = _rows_to_ohlcv_df(
        [{"date": "2026-01-01", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0, "volume": 100}]
    )
    assert list(ohlcv.columns) == ["Open", "High", "Low", "Close", "Volume"]

    ohlc = _rows_to_ohlc_df(
        [{"date": "2026-01-01", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0}]
    )
    assert list(ohlc.columns) == ["Open", "High", "Low", "Close"]

    assert _rows_to_ohlcv_df([]).empty
    assert _rows_to_ohlc_df([]).empty
