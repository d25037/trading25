"""Tests for direct data-access mode (no internal HTTP self-call)."""

from __future__ import annotations

from types import SimpleNamespace

from src.api.client import BaseAPIClient
from src.data.access.mode import data_access_mode_context
from src.data.loaders.index_loaders import load_topix_data_from_market_db
from src.data.loaders.stock_loaders import load_stock_data


def test_load_stock_data_direct_mode_bypasses_http(monkeypatch):
    class _FakeDatasetDb:
        def get_stock_ohlcv(self, _code, start=None, end=None):  # noqa: ANN001, ANN202
            _ = (start, end)
            return [
                SimpleNamespace(
                    date="2024-01-04",
                    open=100.0,
                    high=101.0,
                    low=99.0,
                    close=100.5,
                    volume=12345,
                )
            ]

    monkeypatch.setattr(
        "src.data.access.clients._resolve_dataset_db",
        lambda _dataset_name: _FakeDatasetDb(),
    )

    def _fail_http_request(*args, **kwargs):  # noqa: ANN001, ANN002, ARG001
        raise AssertionError("HTTP client must not be used in direct mode")

    monkeypatch.setattr(BaseAPIClient, "_request", _fail_http_request)

    with data_access_mode_context("direct"):
        df = load_stock_data("sample", "7203")

    assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]
    assert not df.empty


def test_load_topix_market_direct_mode_bypasses_http(monkeypatch):
    class _FakeMarketReader:
        def query(self, _sql, _params=()):  # noqa: ANN001, ANN202
            return [
                {
                    "date": "2024-01-04",
                    "open": 1000.0,
                    "high": 1010.0,
                    "low": 990.0,
                    "close": 1005.0,
                }
            ]

    monkeypatch.setattr(
        "src.data.access.clients._resolve_market_reader",
        lambda: _FakeMarketReader(),
    )

    def _fail_http_request(*args, **kwargs):  # noqa: ANN001, ANN002, ARG001
        raise AssertionError("HTTP client must not be used in direct mode")

    monkeypatch.setattr(BaseAPIClient, "_request", _fail_http_request)

    with data_access_mode_context("direct"):
        df = load_topix_data_from_market_db()

    assert list(df.columns) == ["Open", "High", "Low", "Close"]
    assert not df.empty
