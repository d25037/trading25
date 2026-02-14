"""Unit tests for src.data.access.mode and src.data.access.clients."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, Generator

import pandas as pd
import pytest

from src.data.access import clients, mode


@pytest.fixture(autouse=True)
def _reset_access_caches() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]
    clients._dataset_db_cache.clear()
    clients._market_reader = None
    yield
    clients._dataset_db_cache.clear()
    clients._market_reader = None


def _ns(**kwargs: Any) -> SimpleNamespace:
    return SimpleNamespace(**kwargs)


def _patch_settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        clients,
        "get_settings",
        lambda: _ns(
            dataset_base_path=str(tmp_path),
            market_db_path=str(tmp_path / "market.db"),
        ),
    )


def test_mode_normalization_and_context(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(mode.DATA_ACCESS_MODE_ENV, raising=False)

    assert mode.normalize_data_access_mode(None) == "http"
    assert mode.normalize_data_access_mode("DIRECT") == "direct"
    assert mode.get_data_access_mode() == "http"

    monkeypatch.setenv(mode.DATA_ACCESS_MODE_ENV, "direct")
    assert mode.get_data_access_mode() == "direct"
    assert mode.should_use_direct_db() is True

    with mode.data_access_mode_context("http"):
        assert mode.get_data_access_mode() == "http"
        assert mode.should_use_direct_db() is False

    with mode.data_access_mode_context(None):
        assert mode.get_data_access_mode() == "direct"

    assert mode.get_data_access_mode() == "direct"


def test_resolve_dataset_db_raises_when_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _patch_settings(monkeypatch, tmp_path)

    with pytest.raises(FileNotFoundError, match="Dataset not found"):
        clients._resolve_dataset_db("missing")


def test_resolve_dataset_db_uses_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _patch_settings(monkeypatch, tmp_path)
    (tmp_path / "sample.db").write_text("", encoding="utf-8")

    init_calls: list[str] = []

    class _FakeDatasetDb:
        def __init__(self, db_path: str) -> None:
            init_calls.append(db_path)

    monkeypatch.setattr(clients, "DatasetDb", _FakeDatasetDb)

    first = clients._resolve_dataset_db("sample")
    second = clients._resolve_dataset_db("sample.db")

    assert first is second
    assert len(init_calls) == 1


def test_resolve_market_reader_raises_when_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _patch_settings(monkeypatch, tmp_path)

    with pytest.raises(FileNotFoundError, match="market.db not found"):
        clients._resolve_market_reader()


def test_resolve_market_reader_uses_singleton(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _patch_settings(monkeypatch, tmp_path)
    (tmp_path / "market.db").write_text("", encoding="utf-8")

    init_calls: list[str] = []

    class _FakeMarketReader:
        def __init__(self, db_path: str) -> None:
            init_calls.append(db_path)

    monkeypatch.setattr(clients, "MarketDbReader", _FakeMarketReader)

    first = clients._resolve_market_reader()
    second = clients._resolve_market_reader()

    assert first is second
    assert len(init_calls) == 1


def test_conversion_helpers_empty_rows() -> None:
    assert clients._to_ohlcv_df([]).empty
    assert clients._to_ohlc_df([]).empty
    assert clients._to_margin_df([]).empty
    assert clients._to_statements_df([]).empty


def test_direct_dataset_client_methods(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeDatasetDb:
        def get_stock_ohlcv(self, code: str, start=None, end=None):  # noqa: ANN001, ANN202
            assert code == "7203"
            assert start == "2024-01-01"
            assert end == "2024-12-31"
            return [
                _ns(
                    date="2024-01-04",
                    open=100.0,
                    high=101.0,
                    low=99.0,
                    close=100.5,
                    volume=1000,
                )
            ]

        def get_ohlcv_batch(self, codes: list[str]) -> dict[str, list[SimpleNamespace]]:
            assert codes == ["7203", "6501"]
            return {
                "7203": [
                    _ns(
                        date="2024-01-04",
                        open=100.0,
                        high=101.0,
                        low=99.0,
                        close=100.5,
                        volume=1000,
                    )
                ],
                "6501": [],
            }

        def get_stock_list_with_counts(self, min_records: int = 100) -> list[SimpleNamespace]:
            assert min_records in {100, 50}
            return [
                _ns(
                    stockCode="7203",
                    record_count=200,
                    start_date="2020-01-01",
                    end_date="2024-12-31",
                ),
                _ns(
                    stockCode="6501",
                    record_count=180,
                    start_date="2020-01-01",
                    end_date="2024-12-31",
                ),
            ]

        def get_topix(self, start=None, end=None):  # noqa: ANN001, ANN202
            assert start == "2024-01-01"
            assert end == "2024-12-31"
            return [
                _ns(date="2024-01-04", open=2000.0, high=2010.0, low=1990.0, close=2005.0)
            ]

        def get_index_data(self, code: str, start=None, end=None):  # noqa: ANN001, ANN202
            assert code == "IDX-1"
            assert start == "2024-01-01"
            assert end == "2024-12-31"
            return [
                _ns(date="2024-01-04", open=3000.0, high=3010.0, low=2990.0, close=3005.0)
            ]

        def get_index_list_with_counts(self, min_records: int = 100) -> list[SimpleNamespace]:
            assert min_records == 100
            return [
                _ns(
                    indexCode="IDX-1",
                    indexName="電気機器",
                    record_count=200,
                    start_date="2020-01-01",
                    end_date="2024-12-31",
                ),
                _ns(
                    indexCode="IDX-2",
                    indexName="化学",
                    record_count=180,
                    start_date="2020-01-01",
                    end_date="2024-12-31",
                ),
            ]

        def get_margin(self, code: str, start=None, end=None):  # noqa: ANN001, ANN202
            assert code == "7203"
            assert start == "2024-01-01"
            assert end == "2024-12-31"
            return [
                _ns(
                    date="2024-01-04",
                    long_margin_volume=100.0,
                    short_margin_volume=50.0,
                )
            ]

        def get_margin_batch(self, codes: list[str]) -> dict[str, list[SimpleNamespace]]:
            assert codes == ["7203", "6501"]
            return {
                "7203": [
                    _ns(
                        date="2024-01-04",
                        long_margin_volume=100.0,
                        short_margin_volume=50.0,
                    )
                ],
                "6501": [],
            }

        def get_margin_list(self, min_records: int = 10) -> list[SimpleNamespace]:
            assert min_records == 10
            return [
                _ns(
                    stockCode="7203",
                    record_count=20,
                    start_date="2024-01-01",
                    end_date="2024-12-31",
                    avg_long_margin=100.0,
                    avg_short_margin=50.0,
                )
            ]

        def get_statements(self, code: str) -> list[SimpleNamespace]:
            assert code == "7203"
            return [
                _ns(
                    code="7203",
                    disclosed_date="2024-03-31",
                    earnings_per_share=10.0,
                    profit=1000.0,
                    equity=5000.0,
                    type_of_current_period="FY",
                    type_of_document="Q1",
                    next_year_forecast_earnings_per_share=12.0,
                    bps=100.0,
                    sales=20000.0,
                    operating_profit=3000.0,
                    ordinary_profit=2500.0,
                    operating_cash_flow=1500.0,
                    dividend_fy=40.0,
                    forecast_eps=11.0,
                    investing_cash_flow=-400.0,
                    financing_cash_flow=-300.0,
                    cash_and_equivalents=1200.0,
                    total_assets=60000.0,
                    shares_outstanding=100.0,
                    treasury_shares=2.0,
                )
            ]

        def get_statements_batch(self, codes: list[str]) -> dict[str, list[SimpleNamespace]]:
            assert codes == ["7203", "6501"]
            return {"7203": self.get_statements("7203"), "6501": []}

        def get_sectors(self) -> list[dict[str, str]]:
            return [{"code": "3250", "name": "電気機器"}]

        def get_indices(self) -> list[SimpleNamespace]:
            return [_ns(code="IDX-1", sector_name="電気機器")]

        def get_sector_stock_mapping(self) -> dict[str, list[str]]:
            return {"電気機器": ["7203", "6501"]}

        def get_sector_stocks(self, sector_name: str) -> list[SimpleNamespace]:
            assert sector_name == "電気機器"
            return [_ns(code="7203"), _ns(code="6501")]

        def get_sectors_with_count(self) -> list[SimpleNamespace]:
            return [_ns(sectorName="電気機器", count=2)]

    fake_db = _FakeDatasetDb()
    monkeypatch.setattr(clients, "_resolve_dataset_db", lambda _dataset_name: fake_db)

    client = clients.DirectDatasetClient("sample.db")
    assert client.dataset_name == "sample"
    assert client.__enter__() is client
    assert client.__exit__(None, None, None) is None

    stock_df = client.get_stock_ohlcv("7203", "2024-01-01", "2024-12-31")
    assert list(stock_df.columns) == ["Open", "High", "Low", "Close", "Volume"]

    stock_batch = client.get_stocks_ohlcv_batch(["7203", "6501"])
    assert list(stock_batch.keys()) == ["7203"]

    stock_list = client.get_stock_list(min_records=50)
    assert len(stock_list) == 2
    stock_list_limited = client.get_stock_list(limit=1)
    assert len(stock_list_limited) == 1
    assert len(client.get_available_stocks(min_records=50)) == 2

    topix_df = client.get_topix("2024-01-01", "2024-12-31")
    assert list(topix_df.columns) == ["Open", "High", "Low", "Close"]

    index_df = client.get_index("IDX-1", "2024-01-01", "2024-12-31")
    assert list(index_df.columns) == ["Open", "High", "Low", "Close"]
    assert len(client.get_index_list(codes=["IDX-1"])) == 1
    assert client.get_index_list(codes=["UNKNOWN"]).empty

    margin_df = client.get_margin("7203", "2024-01-01", "2024-12-31")
    assert list(margin_df.columns) == ["longMarginVolume", "shortMarginVolume"]
    assert list(client.get_margin_batch(["7203", "6501"]).keys()) == ["7203"]
    assert len(client.get_margin_list(codes=["7203"])) == 1
    assert client.get_margin_list(codes=["UNKNOWN"]).empty

    statements_df = client.get_statements("7203")
    assert "dividendFY" in statements_df.columns
    assert list(client.get_statements_batch(["7203", "6501"]).keys()) == ["7203"]

    sector_mapping = client.get_sector_mapping()
    assert list(sector_mapping.columns) == [
        "sector_code",
        "sector_name",
        "index_code",
        "index_name",
    ]
    assert client.get_stock_sector_mapping() == {"7203": "電気機器", "6501": "電気機器"}
    assert client.get_sector_stocks("電気機器") == ["7203", "6501"]

    all_sectors = client.get_all_sectors()
    assert list(all_sectors["stock_count"]) == [2]

    monkeypatch.setattr(client, "get_sector_mapping", lambda: pd.DataFrame())
    assert client.get_all_sectors().empty


def test_direct_market_client_get_topix(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class _FakeMarketReader:
        def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
            captured["sql"] = sql
            captured["params"] = params
            return [
                {
                    "date": "2024-01-04",
                    "open": 2000.0,
                    "high": 2010.0,
                    "low": 1990.0,
                    "close": 2005.0,
                }
            ]

    monkeypatch.setattr(clients, "_resolve_market_reader", lambda: _FakeMarketReader())

    market_client = clients.DirectMarketClient()
    assert market_client.__enter__() is market_client
    assert market_client.__exit__(None, None, None) is None

    df = market_client.get_topix("2024-01-01", "2024-12-31")
    assert list(df.columns) == ["Open", "High", "Low", "Close"]
    assert "WHERE date >= ? AND date <= ?" in captured["sql"]
    assert captured["params"] == ("2024-01-01", "2024-12-31")


def test_direct_market_client_get_topix_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeMarketReader:
        def query(self, _sql: str, _params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
            return []

    monkeypatch.setattr(clients, "_resolve_market_reader", lambda: _FakeMarketReader())

    market_client = clients.DirectMarketClient()
    assert market_client.get_topix().empty


def test_client_factories_switch_by_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    class _HTTPDatasetClient:
        def __init__(self, dataset_name: str) -> None:
            self.dataset_name = dataset_name

    class _HTTPMarketClient:
        def __init__(self) -> None:
            self.kind = "http"

    class _DirectDatasetClient:
        def __init__(self, dataset_name: str) -> None:
            self.dataset_name = dataset_name

    class _DirectMarketClient:
        def __init__(self) -> None:
            self.kind = "direct"

    monkeypatch.setattr(clients, "DirectDatasetClient", _DirectDatasetClient)
    monkeypatch.setattr(clients, "DirectMarketClient", _DirectMarketClient)
    monkeypatch.setattr(
        clients,
        "_create_http_dataset_client",
        lambda dataset_name: _HTTPDatasetClient(dataset_name),
    )
    monkeypatch.setattr(
        clients,
        "_create_http_market_client",
        lambda: _HTTPMarketClient(),
    )

    monkeypatch.setattr(clients, "should_use_direct_db", lambda: False)
    http_dataset = clients.get_dataset_client("sample")
    http_market = clients.get_market_client()
    assert isinstance(http_dataset, _HTTPDatasetClient)
    assert isinstance(http_market, _HTTPMarketClient)

    monkeypatch.setattr(clients, "should_use_direct_db", lambda: True)
    direct_dataset = clients.get_dataset_client("sample")
    direct_market = clients.get_market_client()
    assert isinstance(direct_dataset, _DirectDatasetClient)
    assert isinstance(direct_market, _DirectMarketClient)
