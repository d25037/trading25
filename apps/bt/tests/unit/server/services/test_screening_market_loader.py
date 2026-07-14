from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from src.application.services.screening_margin_loader import (
    attach_margin,
    group_margin_rows,
    query_margin_rows,
)
from src.application.services.screening_market_loader import (
    load_market_multi_data,
    load_market_sector_indices,
    load_market_stock_sector_mapping,
    load_market_topix_data,
)
from src.application.services.screening_price_loader import (
    load_daily_by_code,
    normalize_codes,
    rows_to_ohlc_df,
    rows_to_ohlcv_df,
)
from src.application.services.screening_statement_loader import (
    attach_statements,
    group_statement_rows,
    query_adjusted_statement_metric_rows,
    query_statements_rows,
    resolve_period_filter_values,
)
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.infrastructure.data_access.loaders.statements_loaders import transform_statements_df


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


def test_screening_adjusted_metrics_select_reference_date_basis(tmp_path) -> None:
    import duckdb

    db_path = tmp_path / "screening-bases.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE stock_adjustment_bases (
            code TEXT, basis_id TEXT, valid_from TEXT, valid_to_exclusive TEXT,
            adjustment_through_date TEXT, source_fingerprint TEXT,
            materialized_through_date TEXT, status TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE statement_metrics_adjusted (
            code TEXT, disclosed_date TEXT, period_end TEXT, period_type TEXT,
            adjusted_eps DOUBLE,
            adjusted_bps DOUBLE, adjusted_forecast_eps DOUBLE,
            adjusted_dividend_fy DOUBLE, basis_version TEXT
        )
    """)
    conn.executemany(
        "INSERT INTO stock_adjustment_bases VALUES (?, ?, ?, ?, ?, 'fp', ?, 'ready')",
        [
            ("7203", "event-pit-v1:7203:2024-01-04", "2024-01-04", "2024-06-28", "2024-01-04", "2024-06-27"),
            ("7203", "event-pit-v1:7203:2024-06-28", "2024-06-28", None, "2024-06-28", "2024-07-31"),
        ],
    )
    conn.executemany(
        "INSERT INTO statement_metrics_adjusted VALUES ('7203', '2024-05-10', '2024-03-31', 'FY', ?, 1000, 120, 30, ?)",
        [
            (100.0, "event-pit-v1:7203:2024-01-04"),
            (999.0, "event-pit-v1:7203:2024-06-28"),
        ],
    )
    conn.close()

    reader = MarketDbReader(str(db_path))
    try:
        rows = query_adjusted_statement_metric_rows(
            reader,
            ["72030"],
            start_date=None,
            end_date="2024-06-27",
        )
    finally:
        reader.close()

    assert {row["basis_version"] for row in rows} == {
        "event-pit-v1:7203:2024-01-04"
    }
    assert rows[0]["adjusted_eps"] == pytest.approx(100.0)


def test_screening_adjusted_metrics_preserve_same_day_materialization_keys(
    tmp_path,
) -> None:
    import duckdb

    db_path = tmp_path / "screening-same-day.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE stock_adjustment_bases (
            code TEXT, basis_id TEXT, valid_from TEXT, valid_to_exclusive TEXT,
            adjustment_through_date TEXT, source_fingerprint TEXT,
            materialized_through_date TEXT, status TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE statement_metrics_adjusted (
            code TEXT, disclosed_date TEXT, period_end TEXT, period_type TEXT,
            adjusted_eps DOUBLE, adjusted_bps DOUBLE,
            adjusted_forecast_eps DOUBLE, adjusted_dividend_fy DOUBLE,
            basis_version TEXT
        )
    """)
    basis_id = "event-pit-v1:7203:2024-01-04"
    conn.execute(
        "INSERT INTO stock_adjustment_bases VALUES ('7203', ?, '2024-01-04', NULL, '2024-01-04', 'fp', '2024-06-27', 'ready')",
        (basis_id,),
    )
    conn.executemany(
        "INSERT INTO statement_metrics_adjusted VALUES ('7203', '2024-05-10', ?, ?, ?, 1000, 120, 30, ?)",
        [
            ("2024-03-31", "FY", 100.0, basis_id),
            ("2024-06-30", "Q1", 25.0, basis_id),
        ],
    )
    conn.close()

    reader = MarketDbReader(str(db_path))
    try:
        rows = query_adjusted_statement_metric_rows(
            reader, ["7203"], start_date=None, end_date="2024-06-27"
        )
    finally:
        reader.close()

    assert {
        (row["disclosed_date"], row["period_end"], row["period_type"], row["adjusted_eps"])
        for row in rows
    } == {
        ("2024-05-10", "2024-03-31", "FY", 100.0),
        ("2024-05-10", "2024-06-30", "Q1", 25.0),
    }


def test_screening_adjusted_override_pairs_same_day_rows_by_full_key() -> None:
    disclosed = pd.Timestamp("2024-05-10")
    raw = pd.DataFrame(
        [
            {
                "typeOfCurrentPeriod": "FY", "periodEnd": "2024-03-31",
                "earningsPerShare": 1.0, "profit": 1.0, "equity": 10.0,
                "totalAssets": 20.0, "sales": 10.0, "operatingProfit": 1.0,
            },
            {
                "typeOfCurrentPeriod": "Q1", "periodEnd": "2024-06-30",
                "earningsPerShare": 2.0, "profit": 1.0, "equity": 10.0,
                "totalAssets": 20.0, "sales": 10.0, "operatingProfit": 1.0,
            },
        ],
        index=pd.DatetimeIndex([disclosed, disclosed]),
    )
    adjusted = pd.DataFrame(
        [
            {"periodEnd": "2024-03-31", "periodType": "FY", "adjustedEps": 100.0},
            {"periodEnd": "2024-06-30", "periodType": "Q1", "adjustedEps": 25.0},
        ],
        index=pd.DatetimeIndex([disclosed, disclosed]),
    )

    transformed = transform_statements_df(
        raw, adjusted_metrics_df=adjusted, require_adjusted_metrics=True
    )

    assert transformed["AdjustedEPS"].tolist() == [100.0, 25.0]


def test_load_market_multi_data_returns_empty_for_empty_codes() -> None:
    reader = DummyReader()
    result, warnings = load_market_multi_data(reader, [])
    assert result == {}
    assert warnings == []


def test_load_market_multi_data_handles_daily_operational_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.application.services.screening_price_loader.load_daily_by_code",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("stock_data missing")),
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
        "src.application.services.screening_price_loader.load_daily_by_code",
        lambda *_args, **_kwargs: {"7203": daily},
    )

    def _attach(_reader, result, _daily_index_by_code, **_kwargs):
        result["7203"]["statements_daily"] = pd.DataFrame({"dummy": [1, 2]}, index=index)
        return ["attached"]

    monkeypatch.setattr("src.application.services.screening_statement_loader.attach_statements", _attach)

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


def test_load_market_multi_data_attaches_margin(monkeypatch: pytest.MonkeyPatch) -> None:
    index = pd.to_datetime(["2026-01-01", "2026-01-02"])
    daily = pd.DataFrame(
        {"Open": [1.0, 1.0], "High": [1.1, 1.2], "Low": [0.9, 1.0], "Close": [1.0, 1.1], "Volume": [100, 200]},
        index=index,
    )

    monkeypatch.setattr(
        "src.application.services.screening_price_loader.load_daily_by_code",
        lambda *_args, **_kwargs: {"7203": daily},
    )

    def _attach(_reader, result, _daily_index_by_code, **_kwargs):
        result["7203"]["margin_daily"] = pd.DataFrame({"margin_balance": [1, 2]}, index=index)
        return ["margin attached"]

    monkeypatch.setattr("src.application.services.screening_margin_loader.attach_margin", _attach)

    result, warnings = load_market_multi_data(
        DummyReader(),
        ["72030"],
        include_margin_data=True,
    )

    assert "7203" in result
    assert "daily" in result["7203"]
    assert "margin_daily" in result["7203"]
    assert warnings == ["margin attached"]


def test_load_market_multi_data_keeps_daily_when_margin_attach_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index = pd.to_datetime(["2026-01-01", "2026-01-02"])
    daily = pd.DataFrame(
        {"Open": [1.0, 1.0], "High": [1.1, 1.2], "Low": [0.9, 1.0], "Close": [1.0, 1.1], "Volume": [100, 200]},
        index=index,
    )

    monkeypatch.setattr(
        "src.application.services.screening_price_loader.load_daily_by_code",
        lambda *_args, **_kwargs: {"7203": daily},
    )
    monkeypatch.setattr(
        "src.application.services.screening_margin_loader.attach_margin",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("margin query failed")),
    )

    result, warnings = load_market_multi_data(
        DummyReader(),
        ["72030"],
        include_margin_data=True,
    )

    assert "7203" in result
    assert "daily" in result["7203"]
    assert "margin_daily" not in result["7203"]
    assert warnings == ["market margin load failed (margin query failed)"]


def test_load_market_multi_data_skips_attach_when_daily_is_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.application.services.screening_price_loader.load_daily_by_code",
        lambda *_args, **_kwargs: {"7203": pd.DataFrame()},
    )

    called = {"attach": False}

    def _attach(*_args, **_kwargs):
        called["attach"] = True
        return []

    monkeypatch.setattr("src.application.services.screening_statement_loader.attach_statements", _attach)

    result, warnings = load_market_multi_data(
        DummyReader(),
        ["72030"],
        include_statements_data=True,
    )

    assert result == {}
    assert warnings == []
    assert called["attach"] is False


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


def test_load_market_topix_data_without_filters() -> None:
    reader = DummyReader(
        rows=[
            {"date": "2026-01-01", "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5},
        ]
    )
    _ = load_market_topix_data(reader)
    sql, params = reader.calls[0]
    assert "WHERE" not in sql
    assert params == ()


def test_load_market_sector_indices_handles_operational_error() -> None:
    reader = DummyReader(error=RuntimeError("missing index table"))
    assert load_market_sector_indices(reader) == {}


def test_load_market_sector_indices_without_date_filters() -> None:
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
                "category": "",
            },
        ]
    )
    result = load_market_sector_indices(reader)
    sql, params = reader.calls[0]
    assert "WHERE" not in sql
    assert params == ()
    assert "食料品" in result


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
    assert normalize_codes(["72030", "7203", "", "abc", "67580"]) == ["7203", "abc", "6758"]


def test_load_daily_by_code_with_date_filters() -> None:
    reader = DummyReader(
        rows=[
            {"code": "7203", "date": "2026-01-01", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0, "volume": 100},
            {"code": "7203", "date": "2026-01-02", "open": 1.1, "high": 1.2, "low": 1.0, "close": 1.1, "volume": 200},
        ]
    )
    data = load_daily_by_code(reader, ["7203"], start_date="2026-01-01", end_date="2026-01-31")
    sql, params = reader.calls[0]
    assert "date >= ?" in sql
    assert "date <= ?" in sql
    assert params == ("7203", "72030", "2026-01-01", "2026-01-31")
    assert "7203" in data
    assert len(data["7203"]) == 2


def test_load_daily_by_code_queries_alternate_code_forms_and_groups_normalized() -> None:
    reader = DummyReader(
        rows=[
            {
                "code": "72030",
                "date": "2026-01-01",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.0,
                "volume": 100,
            },
        ]
    )

    data = load_daily_by_code(reader, ["7203"], start_date=None, end_date=None)

    _sql, params = reader.calls[0]
    assert params == ("7203", "72030")
    assert list(data.keys()) == ["7203"]
    assert len(data["7203"]) == 1


def test_load_daily_by_code_without_date_filters() -> None:
    reader = DummyReader(
        rows=[
            {"code": "7203", "date": "2026-01-01", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0, "volume": 100},
        ]
    )
    _ = load_daily_by_code(reader, ["7203"], start_date=None, end_date=None)
    sql, params = reader.calls[0]
    assert "date >= ?" not in sql
    assert "date <= ?" not in sql
    assert params == ("7203", "72030")


def test_attach_statements_missing_table_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    index = pd.to_datetime(["2026-01-01"])
    result = {"7203": {"daily": pd.DataFrame({"Close": [1.0]}, index=index)}}
    daily_index = {"7203": index}

    monkeypatch.setattr(
        "src.application.services.screening_statement_loader.query_statements_rows",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("no such table: statements")),
    )

    warnings = attach_statements(
        DummyReader(),
        result,
        daily_index,
        start_date=None,
        end_date=None,
        period_type="FY",
        include_forecast_revision=False,
    )
    assert any("statements table is missing" in warning for warning in warnings)


def test_attach_statements_returns_empty_when_no_codes() -> None:
    warnings = attach_statements(
        DummyReader(),
        {},
        {},
        start_date=None,
        end_date=None,
        period_type="FY",
        include_forecast_revision=False,
    )
    assert warnings == []


def test_attach_margin_missing_table_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    index = pd.to_datetime(["2026-01-01"])
    result = {"7203": {"daily": pd.DataFrame({"Close": [1.0]}, index=index)}}
    daily_index = {"7203": index}

    monkeypatch.setattr(
        "src.application.services.screening_margin_loader.query_margin_rows",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("no such table: margin_data")),
    )

    warnings = attach_margin(
        DummyReader(),
        result,
        daily_index,
        start_date=None,
        end_date=None,
    )
    assert warnings == ["market margin_data table is missing; margin signals may be skipped"]


def test_attach_margin_returns_empty_when_no_codes() -> None:
    warnings = attach_margin(
        DummyReader(),
        {},
        {},
        start_date=None,
        end_date=None,
    )
    assert warnings == []


def test_attach_margin_reraises_unexpected_error(monkeypatch: pytest.MonkeyPatch) -> None:
    index = pd.to_datetime(["2026-01-01"])
    result = {"7203": {"daily": pd.DataFrame({"Close": [1.0]}, index=index)}}
    daily_index = {"7203": index}

    monkeypatch.setattr(
        "src.application.services.screening_margin_loader.query_margin_rows",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("permission denied")),
    )

    with pytest.raises(RuntimeError):
        attach_margin(
            DummyReader(),
            result,
            daily_index,
            start_date=None,
            end_date=None,
        )


def test_attach_margin_transform_failure_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    index = pd.to_datetime(["2026-01-01"])
    result = {"7203": {"daily": pd.DataFrame({"Close": [1.0]}, index=index)}}
    daily_index = {"7203": index}

    monkeypatch.setattr(
        "src.application.services.screening_margin_loader.query_margin_rows",
        lambda *_args, **_kwargs: [
            {
                "code": "7203",
                "date": "2026-01-01",
                "long_margin_volume": 100,
                "short_margin_volume": 40,
            }
        ],
    )
    monkeypatch.setattr(
        "src.application.services.screening_margin_loader.transform_margin_df",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("bad margin")),
    )

    warnings = attach_margin(
        DummyReader(),
        result,
        daily_index,
        start_date=None,
        end_date=None,
    )

    assert warnings == ["7203 margin transform failed (bad margin)"]


def test_query_margin_rows_with_date_filters() -> None:
    reader = DummyReader(
        rows=[
            {
                "code": "7203",
                "date": "2026-01-01",
                "long_margin_volume": 100,
                "short_margin_volume": 40,
            }
        ]
    )

    rows = query_margin_rows(
        reader,
        ["7203"],
        start_date="2026-01-01",
        end_date="2026-01-31",
    )

    sql, params = reader.calls[0]
    assert "date >= ?" in sql
    assert "date <= ?" in sql
    assert params == ("7203", "72030", "2026-01-01", "2026-01-31")
    assert rows[0]["code"] == "7203"


def test_query_margin_rows_queries_alternate_code_forms() -> None:
    reader = DummyReader(rows=[])

    query_margin_rows(reader, ["7203"], start_date=None, end_date=None)

    _sql, params = reader.calls[0]
    assert params == ("7203", "72030")


def test_group_margin_rows_normalizes_and_sorts_index() -> None:
    grouped = group_margin_rows(
        [
            {
                "code": "72030",
                "date": "2026-01-02",
                "long_margin_volume": 120,
                "short_margin_volume": 60,
            },
            {
                "code": "72030",
                "date": "2026-01-01",
                "long_margin_volume": 100,
                "short_margin_volume": 40,
            },
        ]
    )

    assert list(grouped.keys()) == ["7203"]
    assert list(grouped["7203"].index.strftime("%Y-%m-%d")) == ["2026-01-01", "2026-01-02"]


def test_attach_statements_reraises_unexpected_error(monkeypatch: pytest.MonkeyPatch) -> None:
    index = pd.to_datetime(["2026-01-01"])
    result = {"7203": {"daily": pd.DataFrame({"Close": [1.0]}, index=index)}}
    daily_index = {"7203": index}

    monkeypatch.setattr(
        "src.application.services.screening_statement_loader.query_statements_rows",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("permission denied")),
    )

    with pytest.raises(RuntimeError):
        attach_statements(
            DummyReader(),
            result,
            daily_index,
            start_date=None,
            end_date=None,
            period_type="FY",
            include_forecast_revision=False,
        )


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
    query_calls: list[dict[str, object]] = []

    def _query(_reader, _codes, **kwargs):
        query_calls.append(kwargs)
        return revision_rows if kwargs.get("period_type") == "all" else base_rows

    monkeypatch.setattr("src.application.services.screening_statement_loader.query_statements_rows", _query)
    monkeypatch.setattr(
        "src.application.services.screening_statement_loader.query_adjusted_statement_metric_rows",
        lambda *_args, **_kwargs: [{
            "code": "7203", "disclosed_date": "2026-01-01",
            "period_end": "2026-01-01", "period_type": "FY",
            "adjusted_eps": 1.0, "adjusted_bps": 3.0,
            "adjusted_forecast_eps": 2.0, "adjusted_dividend_fy": 8.0,
        }],
    )
    monkeypatch.setattr(
        "src.application.services.screening_statement_loader.transform_statements_df",
        lambda df, **_kwargs: df,
    )
    monkeypatch.setattr(
        "src.application.services.screening_statement_loader.merge_forward_forecast_revision",
        lambda base, _revision: base.assign(merged=True),
    )

    warnings = attach_statements(
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
    assert any(
        call.get("period_type") == "FY" and call.get("actual_only") is True
        for call in query_calls
    )
    assert any(
        call.get("period_type") == "all" and call.get("actual_only") is False
        for call in query_calls
    )


def test_attach_statements_skips_merge_when_revision_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    index = pd.to_datetime(["2026-01-01", "2026-01-02"])
    result = {"7203": {"daily": pd.DataFrame({"Close": [1.0, 1.1]}, index=index)}}
    daily_index = {"7203": index}
    base_rows = [
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

    def _query(_reader, _codes, **kwargs):
        if kwargs.get("period_type") == "all":
            return []
        return base_rows

    monkeypatch.setattr("src.application.services.screening_statement_loader.query_statements_rows", _query)
    monkeypatch.setattr(
        "src.application.services.screening_statement_loader.query_adjusted_statement_metric_rows",
        lambda *_args, **_kwargs: [{
            "code": "7203", "disclosed_date": "2026-01-01",
            "period_end": "2026-01-01", "period_type": "FY",
            "adjusted_eps": 1.0, "adjusted_bps": 3.0,
            "adjusted_forecast_eps": 2.0, "adjusted_dividend_fy": 8.0,
        }],
    )
    monkeypatch.setattr(
        "src.application.services.screening_statement_loader.merge_forward_forecast_revision",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not merge")),
    )

    warnings = attach_statements(
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


def test_attach_statements_fails_closed_when_exact_adjusted_row_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index = pd.to_datetime(["2026-01-01"])
    row = {
        "code": "7203",
        "disclosed_date": "2026-01-01",
        "earnings_per_share": 1.0,
        "profit": 1.0,
        "equity": 1.0,
        "type_of_current_period": "FY",
    }
    monkeypatch.setattr(
        "src.application.services.screening_statement_loader.query_statements_rows",
        lambda *_args, **_kwargs: [row],
    )
    monkeypatch.setattr(
        "src.application.services.screening_statement_loader.query_adjusted_statement_metric_rows",
        lambda *_args, **_kwargs: [],
    )

    with pytest.raises(ValueError, match="adjusted_metrics_pit"):
        attach_statements(
            DummyReader(),
            {"7203": {}},
            {"7203": index},
            start_date=None,
            end_date=None,
            period_type="FY",
            include_forecast_revision=False,
        )


def test_attach_statements_fails_closed_when_same_day_period_sibling_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index = pd.to_datetime(["2026-01-01"])
    base = {
        "code": "7203",
        "disclosed_date": "2026-01-01",
        "earnings_per_share": 1.0,
        "profit": 1.0,
        "equity": 1.0,
        "type_of_document": "Annual",
    }
    monkeypatch.setattr(
        "src.application.services.screening_statement_loader.query_statements_rows",
        lambda *_args, **_kwargs: [
            {**base, "type_of_current_period": "FY"},
            {**base, "type_of_current_period": "Q1"},
        ],
    )
    monkeypatch.setattr(
        "src.application.services.screening_statement_loader.query_adjusted_statement_metric_rows",
        lambda *_args, **_kwargs: [{
            "code": "7203",
            "disclosed_date": "2026-01-01",
            "period_end": "2026-01-01",
            "period_type": "FY",
            "adjusted_eps": 1.0,
            "adjusted_bps": 3.0,
            "adjusted_forecast_eps": 2.0,
            "adjusted_dividend_fy": 8.0,
            "basis_version": "event-pit-v1:7203:2025-01-01",
        }],
    )

    with pytest.raises(ValueError, match="adjusted_metrics_pit"):
        attach_statements(
            DummyReader(),
            {"7203": {}},
            {"7203": index},
            start_date=None,
            end_date=None,
            period_type="all",
            include_forecast_revision=False,
        )


def test_attach_statements_overlays_adjusted_metric_sot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index = pd.to_datetime(["2026-01-01", "2026-01-02"])
    result = {"7203": {"daily": pd.DataFrame({"Close": [1.0, 1.1]}, index=index)}}
    daily_index = {"7203": index}
    rows = [
        {
            "code": "7203",
            "disclosed_date": "2026-01-01",
            "earnings_per_share": 100.0,
            "profit": 1.0,
            "equity": 1.0,
            "type_of_current_period": "FY",
            "type_of_document": "Annual",
            "next_year_forecast_earnings_per_share": 120.0,
            "bps": 1000.0,
            "sales": 4.0,
            "operating_profit": 5.0,
            "ordinary_profit": 6.0,
            "operating_cash_flow": 7.0,
            "dividend_fy": 30.0,
            "forecast_dividend_fy": 8.5,
            "next_year_forecast_dividend_fy": 9.0,
            "payout_ratio": 30.0,
            "forecast_payout_ratio": 32.0,
            "next_year_forecast_payout_ratio": 34.0,
            "forecast_eps": None,
            "investing_cash_flow": 10.0,
            "financing_cash_flow": 11.0,
            "cash_and_equivalents": 12.0,
            "total_assets": 13.0,
            "shares_outstanding": 100.0,
            "treasury_shares": 15.0,
        }
    ]
    adjusted_rows = [
        {
            "code": "7203",
            "disclosed_date": "2026-01-01",
            "period_end": "2026-01-01",
            "period_type": "FY",
            "adjusted_eps": 50.0,
            "adjusted_bps": 500.0,
            "adjusted_forecast_eps": 60.0,
            "adjusted_dividend_fy": 15.0,
        }
    ]

    monkeypatch.setattr(
        "src.application.services.screening_statement_loader.query_statements_rows",
        lambda *_args, **_kwargs: rows,
    )
    monkeypatch.setattr(
        "src.application.services.screening_statement_loader.query_adjusted_statement_metric_rows",
        lambda *_args, **_kwargs: adjusted_rows,
    )

    warnings = attach_statements(
        DummyReader(),
        result,
        daily_index,
        start_date=None,
        end_date=None,
        period_type="FY",
        include_forecast_revision=False,
    )

    statements = result["7203"]["statements_daily"]
    assert warnings == []
    assert statements["EPS"].iloc[-1] == pytest.approx(100.0)
    assert statements["AdjustedEPS"].iloc[-1] == pytest.approx(50.0)
    assert statements["AdjustedBPS"].iloc[-1] == pytest.approx(500.0)
    assert statements["AdjustedForwardForecastEPS"].iloc[-1] == pytest.approx(60.0)


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

    monkeypatch.setattr("src.application.services.screening_statement_loader.query_statements_rows", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(
        "src.application.services.screening_statement_loader.query_adjusted_statement_metric_rows",
        lambda *_args, **_kwargs: [{
            "code": "7203", "disclosed_date": "2026-01-01",
            "period_end": "2026-01-01", "period_type": "FY",
            "adjusted_eps": 1.0, "adjusted_bps": 3.0,
            "adjusted_forecast_eps": 2.0, "adjusted_dividend_fy": 8.0,
        }],
    )
    monkeypatch.setattr(
        "src.application.services.screening_statement_loader.transform_statements_df",
        lambda _df, **_kwargs: (_ for _ in ()).throw(ValueError("transform failed")),
    )

    warnings = attach_statements(
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
    query_statements_rows(
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
    assert params[0:4] == ("7203", "72030", "2026-01-01", "2026-01-31")
    assert "1Q" in params and "Q1" in params


def test_query_statements_rows_queries_alternate_code_forms() -> None:
    reader = DummyReader()

    query_statements_rows(
        reader,
        ["7203"],
        start_date=None,
        end_date=None,
        period_type="all",
        actual_only=False,
    )

    _sql, params = reader.calls[0]
    assert params == ("7203", "72030")


def test_query_statements_rows_without_optional_filters() -> None:
    reader = DummyReader()
    query_statements_rows(
        reader,
        ["7203"],
        start_date=None,
        end_date=None,
        period_type="all",
        actual_only=False,
    )
    sql, params = reader.calls[0]
    assert "disclosed_date >= ?" not in sql
    assert "disclosed_date <= ?" not in sql
    assert "type_of_current_period IN" not in sql
    assert "earnings_per_share IS NOT NULL" not in sql
    assert params == ("7203", "72030")


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
    assert resolve_period_filter_values(period_type) == expected


def test_group_statement_rows_and_ohlc_helpers() -> None:
    grouped = group_statement_rows(
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

    ohlcv = rows_to_ohlcv_df(
        [{"date": "2026-01-01", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0, "volume": 100}]
    )
    assert list(ohlcv.columns) == ["Open", "High", "Low", "Close", "Volume"]

    ohlc = rows_to_ohlc_df(
        [{"date": "2026-01-01", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0}]
    )
    assert list(ohlc.columns) == ["Open", "High", "Low", "Close"]

    assert rows_to_ohlcv_df([]).empty
    assert rows_to_ohlc_df([]).empty
