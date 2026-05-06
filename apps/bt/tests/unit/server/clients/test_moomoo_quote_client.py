from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from src.infrastructure.external_api.clients.moomoo_quote_client import (
    MoomooOpenDConfig,
    MoomooOpenDError,
    MoomooQuoteClient,
    symbol_from_us_code,
    normalize_us_code,
)


class _FakeFrame:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def to_dict(self, orient: str) -> list[dict[str, Any]]:
        assert orient == "records"
        return self._rows


class _FakeQuoteContext:
    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def get_stock_basicinfo(self, *_args: Any) -> tuple[int, _FakeFrame]:
        return (
            0,
            _FakeFrame(
                [
                    {"code": "US.AAPL", "name": "Apple Inc.", "lot_size": 1},
                    {"code": "US.MSFT", "name": "Microsoft Corporation", "lot_size": 1},
                ]
            ),
        )

    def request_history_kline(self, *_args: Any, **_kwargs: Any) -> tuple[int, _FakeFrame, None]:
        return (
            0,
            _FakeFrame(
                [
                    {
                        "code": "US.AAPL",
                        "name": "Apple Inc.",
                        "time_key": "2025-01-02 00:00:00",
                        "open": 100,
                        "close": 101,
                        "high": 102,
                        "low": 99,
                        "volume": 12345,
                    }
                ]
            ),
            None,
        )

    def get_market_snapshot(self, *_args: Any) -> tuple[int, _FakeFrame]:
        return (
            0,
            _FakeFrame(
                [
                    {
                        "code": "US.AAPL",
                        "name": "Apple Inc.",
                        "update_time": "2025-01-02 16:00:00",
                        "last_price": 101,
                    }
                ]
            ),
        )


def _config() -> MoomooOpenDConfig:
    return MoomooOpenDConfig(
        host="127.0.0.1",
        port=11111,
        is_encrypt=False,
        enabled=True,
        max_history_rows=5000,
    )


def _fake_sdk() -> SimpleNamespace:
    return SimpleNamespace(
        RET_OK=0,
        OpenQuoteContext=_FakeQuoteContext,
        Market=SimpleNamespace(US="US"),
        SecurityType=SimpleNamespace(STOCK="STOCK"),
        KLType=SimpleNamespace(K_DAY="K_DAY"),
        AuType=SimpleNamespace(QFQ="QFQ"),
    )


def test_normalize_us_code_accepts_bare_and_us_prefixed_symbols() -> None:
    assert normalize_us_code("aapl") == "US.AAPL"
    assert normalize_us_code("US.msft") == "US.MSFT"


def test_normalize_us_code_rejects_non_us_market() -> None:
    with pytest.raises(MoomooOpenDError):
        normalize_us_code("HK.00700")


def test_normalize_us_code_rejects_empty_symbol() -> None:
    with pytest.raises(MoomooOpenDError):
        normalize_us_code(" ")


def test_symbol_from_us_code_accepts_bare_symbol() -> None:
    assert symbol_from_us_code("AAPL") == "AAPL"


def test_status_reports_sdk_missing_without_raising() -> None:
    client = MoomooQuoteClient(_config())
    with patch("importlib.import_module", side_effect=ImportError("missing")):
        status = client.status()
    assert status["sdkInstalled"] is False
    assert status["openDReachable"] is False


def test_status_reports_sdk_import_side_effect_failure_without_raising() -> None:
    client = MoomooQuoteClient(_config())
    with patch("importlib.import_module", side_effect=PermissionError("log denied")):
        status = client.status()
    assert status["sdkInstalled"] is False
    assert status["openDReachable"] is False
    assert "import failed" in str(status["message"])


def test_status_reports_disabled_integration_without_opening_context() -> None:
    client = MoomooQuoteClient(
        MoomooOpenDConfig(
            host="127.0.0.1",
            port=11111,
            is_encrypt=False,
            enabled=False,
            max_history_rows=5000,
        )
    )
    with patch("importlib.import_module", return_value=_fake_sdk()):
        status = client.status()
    assert status["sdkInstalled"] is True
    assert status["openDReachable"] is False
    assert status["quoteContextReady"] is False


def test_read_only_fetches_normalize_us_market_rows() -> None:
    client = MoomooQuoteClient(_config())
    with patch("importlib.import_module", return_value=_fake_sdk()):
        search = client.search_us_stocks("apple", limit=10)
        history = client.get_us_history("AAPL", start="2025-01-01", end="2025-01-31")
        snapshot = client.get_us_snapshot(["AAPL"])

    assert search["items"][0]["symbol"] == "AAPL"
    assert history["code"] == "US.AAPL"
    assert history["rows"][0]["close"] == 101.0
    assert snapshot["items"][0]["symbol"] == "AAPL"


def test_snapshot_rejects_empty_symbol_list() -> None:
    client = MoomooQuoteClient(_config())
    with pytest.raises(MoomooOpenDError):
        client.get_us_snapshot([])
