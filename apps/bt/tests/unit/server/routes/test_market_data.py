"""
Market Data Routes Unit Tests

sync_client を使用してルートの E2E テスト。
"""

import os
from pathlib import Path

import pytest

from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app
from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.market_schema import (
    MARKET_SCHEMA_VERSION,
    METADATA_KEYS,
    PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE,
)
from src.shared.provider_stock_window import ProviderStockStage
from tests.unit.server.db.market_writer_test_support import (
    open_market_db,
    open_time_series_store,
)


def _build_market_timeseries_dir(base_dir: Path) -> str:
    base_dir.mkdir(parents=True, exist_ok=True)
    duckdb_path = base_dir / "market.duckdb"
    market_db = open_market_db(str(duckdb_path))
    try:
        market_db.upsert_stocks(
            [
                {
                    "code": "72030",
                    "company_name": "トヨタ自動車",
                    "company_name_english": "TOYOTA MOTOR",
                    "market_code": "prime",
                    "market_name": "プライム",
                    "sector_17_code": "S17_1",
                    "sector_17_name": "輸送用機器",
                    "sector_33_code": "S33_1",
                    "sector_33_name": "輸送用機器",
                    "scale_category": "TOPIX Large70",
                    "listed_date": "1949-05-16",
                },
                {
                    "code": "67580",
                    "company_name": "ソニーグループ",
                    "company_name_english": "SONY GROUP",
                    "market_code": "prime",
                    "market_name": "プライム",
                    "sector_17_code": "S17_2",
                    "sector_17_name": "電気機器",
                    "sector_33_code": "S33_2",
                    "sector_33_name": "電気機器",
                    "scale_category": "TOPIX Large70",
                    "listed_date": "1958-12-01",
                },
                {
                    "code": "99840",
                    "company_name": "テスト銘柄",
                    "company_name_english": "TEST STOCK",
                    "market_code": "standard",
                    "market_name": "スタンダード",
                    "sector_17_code": "S17_3",
                    "sector_17_name": "情報通信",
                    "sector_33_code": "S33_3",
                    "sector_33_name": "情報通信",
                    "scale_category": None,
                    "listed_date": "2020-01-01",
                },
            ]
        )
        market_db.publish_stock_master_daily_rows(
            [
                {
                    "date": "2024-01-15",
                    "code": "72030",
                    "company_name": "トヨタ自動車（履歴）",
                    "company_name_english": "TOYOTA MOTOR HIST",
                    "market_code": "prime",
                    "market_name": "プライム",
                    "sector_17_code": "S17_1",
                    "sector_17_name": "輸送用機器",
                    "sector_33_code": "S33_1",
                    "sector_33_name": "輸送用機器",
                    "scale_category": "TOPIX Core30",
                    "listed_date": "1949-05-16",
                }
            ],
            derive=False,
        )
        market_db.upsert_index_master(
            [
                {
                    "code": "0000",
                    "name": "TOPIX",
                    "name_english": "TOPIX",
                    "category": "topix",
                    "data_start_date": "2008-05-07",
                },
                {
                    "code": "0001",
                    "name": "電気機器",
                    "name_english": "Electric Appliances",
                    "category": "sector33",
                    "data_start_date": "2010-01-04",
                },
                {
                    "code": "N225_UNDERPX",
                    "name": "日経平均",
                    "name_english": "Nikkei 225 (UnderPx derived)",
                    "category": "synthetic",
                    "data_start_date": "2024-01-16",
                },
            ]
        )

        store = open_time_series_store(
            duckdb_path=str(duckdb_path),
            parquet_dir=str(base_dir / "parquet"),
        )
        try:
            stock_rows = []
            for code in ("72030", "67580"):
                for offset, date_value in enumerate(
                    ("2024-01-15", "2024-01-16", "2024-01-17")
                ):
                    base = (
                        2500.0 + offset * 10
                        if code == "72030"
                        else 13000.0 + offset * 50
                    )
                    volume = 1_000_000 + offset * 100
                    stock_rows.append(
                        {
                            "code": code,
                            "date": date_value,
                            "open": base,
                            "high": base + 20,
                            "low": base - 10,
                            "close": base + 5,
                            "volume": volume,
                            "turnover_value": (base + 5) * volume,
                            "adjustment_factor": 1.0,
                            "adjusted_open": base,
                            "adjusted_high": base + 20,
                            "adjusted_low": base - 10,
                            "adjusted_close": base + 5,
                            "adjusted_volume": volume,
                        }
                    )
            store.publish_stock_data(
                stock_rows,
                stage=ProviderStockStage(
                    provider_plan="premium",
                    provider_as_of=max(str(row["date"]) for row in stock_rows),
                    provider_codes=frozenset(str(row["code"]) for row in stock_rows),
                ),
            )
            store.publish_stock_minute_data(
                [
                    {
                        "code": "7203",
                        "date": "2024-01-16",
                        "time": "09:00",
                        "open": 2500.0,
                        "high": 2505.0,
                        "low": 2495.0,
                        "close": 2502.0,
                        "volume": 1000,
                        "turnover_value": 2502000.0,
                    },
                    {
                        "code": "72030",
                        "date": "2024-01-16",
                        "time": "09:00",
                        "open": 2500.0,
                        "high": 2505.0,
                        "low": 2495.0,
                        "close": 2502.0,
                        "volume": 1000,
                        "turnover_value": 2502000.0,
                    },
                    {
                        "code": "7203",
                        "date": "2024-01-16",
                        "time": "09:01",
                        "open": 2502.0,
                        "high": 2508.0,
                        "low": 2500.0,
                        "close": 2506.0,
                        "volume": 800,
                        "turnover_value": 2004800.0,
                    },
                    {
                        "code": "9984",
                        "date": "2024-01-16",
                        "time": "15:30",
                        "open": 7000.0,
                        "high": 7010.0,
                        "low": 6990.0,
                        "close": 7005.0,
                        "volume": 500,
                        "turnover_value": 3502500.0,
                    },
                ]
            )
            store.publish_topix_data(
                [
                    {
                        "date": date_value,
                        "open": 2500.0,
                        "high": 2520.0,
                        "low": 2480.0,
                        "close": 2510.0,
                    }
                    for date_value in (
                        "2024-01-15",
                        "2024-01-16",
                        "2024-01-17",
                    )
                ]
            )
            store.publish_indices_data(
                [
                    *[
                        {
                            "code": "0000",
                            "date": date_value,
                            "open": 2500.0,
                            "high": 2520.0,
                            "low": 2480.0,
                            "close": 2510.0,
                            "sector_name": None,
                        }
                        for date_value in (
                            "2024-01-15",
                            "2024-01-16",
                            "2024-01-17",
                        )
                    ],
                    *[
                        {
                            "code": "0001",
                            "date": date_value,
                            "open": 1200.0,
                            "high": 1220.0,
                            "low": 1190.0,
                            "close": 1210.0,
                            "sector_name": "電気機器",
                        }
                        for date_value in (
                            "2024-01-15",
                            "2024-01-16",
                            "2024-01-17",
                        )
                    ],
                    {
                        "code": "N225_UNDERPX",
                        "date": "2024-01-16",
                        "open": 36100.0,
                        "high": 36100.0,
                        "low": 36100.0,
                        "close": 36100.0,
                        "sector_name": "日経平均",
                    },
                    {
                        "code": "N225_UNDERPX",
                        "date": "2024-01-17",
                        "open": 36250.0,
                        "high": 36250.0,
                        "low": 36250.0,
                        "close": 36250.0,
                        "sector_name": "日経平均",
                    },
                ]
            )
            store.publish_options_225_data(
                [
                    {
                        "code": "131040018",
                        "date": "2024-01-16",
                        "whole_day_open": 10.0,
                        "whole_day_high": 12.0,
                        "whole_day_low": 9.0,
                        "whole_day_close": 11.0,
                        "night_session_open": 9.0,
                        "night_session_high": 11.0,
                        "night_session_low": 8.0,
                        "night_session_close": 10.0,
                        "day_session_open": 10.0,
                        "day_session_high": 12.0,
                        "day_session_low": 9.0,
                        "day_session_close": 11.0,
                        "volume": 100,
                        "open_interest": 250,
                        "turnover_value": 110000.0,
                        "contract_month": "2024-04",
                        "strike_price": 32000.0,
                        "only_auction_volume": 0,
                        "emergency_margin_trigger_division": None,
                        "put_call_division": "1",
                        "last_trading_day": "2024-04-11",
                        "special_quotation_day": "2024-04-12",
                        "settlement_price": 11.0,
                        "theoretical_price": 10.5,
                        "base_volatility": 18.0,
                        "underlying_price": 36100.0,
                        "implied_volatility": 22.0,
                        "interest_rate": 0.5,
                    },
                    {
                        "code": "131040018",
                        "date": "2024-01-17",
                        "whole_day_open": 12.0,
                        "whole_day_high": 13.0,
                        "whole_day_low": 11.0,
                        "whole_day_close": 12.5,
                        "night_session_open": 11.0,
                        "night_session_high": 12.0,
                        "night_session_low": 10.0,
                        "night_session_close": 11.0,
                        "day_session_open": 12.0,
                        "day_session_high": 13.0,
                        "day_session_low": 11.0,
                        "day_session_close": 12.5,
                        "volume": 120,
                        "open_interest": 260,
                        "turnover_value": 130000.0,
                        "contract_month": "2024-04",
                        "strike_price": 32000.0,
                        "only_auction_volume": 0,
                        "emergency_margin_trigger_division": None,
                        "put_call_division": "1",
                        "last_trading_day": "2024-04-11",
                        "special_quotation_day": "2024-04-12",
                        "settlement_price": 12.0,
                        "theoretical_price": 11.5,
                        "base_volatility": 18.5,
                        "underlying_price": 36250.0,
                        "implied_volatility": 23.0,
                        "interest_rate": 0.5,
                    },
                    {
                        "code": "141040018",
                        "date": "2024-01-17",
                        "whole_day_open": 20.0,
                        "whole_day_high": 21.0,
                        "whole_day_low": 18.0,
                        "whole_day_close": 19.5,
                        "night_session_open": 19.0,
                        "night_session_high": 20.0,
                        "night_session_low": 18.0,
                        "night_session_close": 19.0,
                        "day_session_open": 20.0,
                        "day_session_high": 21.0,
                        "day_session_low": 18.0,
                        "day_session_close": 19.5,
                        "volume": 90,
                        "open_interest": 180,
                        "turnover_value": 175000.0,
                        "contract_month": "2024-04",
                        "strike_price": 36000.0,
                        "only_auction_volume": 0,
                        "emergency_margin_trigger_division": None,
                        "put_call_division": "2",
                        "last_trading_day": "2024-04-11",
                        "special_quotation_day": "2024-04-12",
                        "settlement_price": 19.0,
                        "theoretical_price": 19.2,
                        "base_volatility": 17.5,
                        "underlying_price": 36250.0,
                        "implied_volatility": 19.0,
                        "interest_rate": 0.5,
                    },
                ]
            )
            store.index_stock_data()
            store.index_stock_minute_data()
            store.index_topix_data()
            store.index_indices_data()
            store.index_options_225_data()
        finally:
            store.close()
    finally:
        market_db.close()
    return str(base_dir)

@pytest.fixture(scope="module")
def market_data_timeseries_dir(tmp_path_factory):
    return _build_market_timeseries_dir(tmp_path_factory.mktemp("market-data-routes") / "market-timeseries")


def test_market_data_fixture_uses_v5_provider_adjusted_contract(
    market_data_timeseries_dir: str,
) -> None:
    market_db = open_market_db(
        str(Path(market_data_timeseries_dir) / "market.duckdb"),
        read_only=True,
    )
    try:
        assert market_db.get_market_schema_version() == MARKET_SCHEMA_VERSION
        assert market_db.get_stock_price_adjustment_mode() == (
            PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE
        )
        assert market_db.get_sync_metadata(METADATA_KEYS["PROVIDER_PLAN"]) == (
            "premium"
        )
        assert market_db._count_rows("stock_data_raw") == 6
        assert market_db._count_rows("stock_data") == 6
        assert market_db._count_rows("stock_provider_windows") == 2
        assert market_db._fetchone(
            """
            SELECT COUNT(*)
            FROM stock_data_raw AS raw
            JOIN stock_data AS consumer USING (code, date)
            WHERE raw.adjusted_open IS DISTINCT FROM consumer.open
               OR raw.adjusted_high IS DISTINCT FROM consumer.high
               OR raw.adjusted_low IS DISTINCT FROM consumer.low
               OR raw.adjusted_close IS DISTINCT FROM consumer.close
               OR raw.adjusted_volume IS DISTINCT FROM consumer.volume
            """
        ) == (0,)
    finally:
        market_db.close()


def test_market_data_fixture_releases_writer_after_setup_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_root = tmp_path / "market-timeseries"

    def fail_seed(_self: MarketDb, _rows: list[dict[str, object]]) -> None:
        raise RuntimeError("injected fixture seed failure")

    with monkeypatch.context() as scoped:
        scoped.setattr(MarketDb, "upsert_stocks", fail_seed)
        with pytest.raises(RuntimeError, match="injected fixture seed failure"):
            _build_market_timeseries_dir(market_root)

    probe = open_market_db(str(market_root / "market.duckdb"))
    probe.close()


@pytest.fixture(scope="module")
def client_with_market_db(market_data_timeseries_dir):
    """market.duckdb 付きテストクライアント"""
    from src.shared.config.settings import reload_settings

    env_updates = {
        "MARKET_TIMESERIES_DIR": market_data_timeseries_dir,
        "MARKET_DB_PATH": str(Path(market_data_timeseries_dir) / "market.duckdb"),
        "JQUANTS_API_KEY": "dummy_token_value_0000",
        "JQUANTS_PLAN": "free",
    }
    original_env = {key: os.environ.get(key) for key in env_updates}

    for key, value in env_updates.items():
        os.environ[key] = value
    reload_settings()
    app = create_app()
    try:
        with TestClient(app) as client:
            yield client
    finally:
        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        reload_settings()


class TestGetAllStocks:
    def test_200_prime(self, client_with_market_db):
        """プライム市場の全銘柄データ"""
        resp = client_with_market_db.get("/api/market/stocks")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 2
        assert "code" in data[0]
        assert "company_name" in data[0]
        assert "data" in data[0]

    def test_200_standard(self, client_with_market_db):
        """スタンダード市場"""
        resp = client_with_market_db.get("/api/market/stocks?market=standard")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        # standard にはデータなし
        assert len(data) == 0

    def test_200_numeric_prime_alias_matches_legacy(self, client_with_market_db):
        """current market code 0111 は legacy prime と同義"""
        legacy = client_with_market_db.get("/api/market/stocks?market=prime")
        numeric = client_with_market_db.get("/api/market/stocks?market=0111")
        assert legacy.status_code == 200
        assert numeric.status_code == 200
        assert numeric.json() == legacy.json()

    def test_200_numeric_standard_alias_matches_legacy(self, client_with_market_db):
        """current market code 0112 は legacy standard と同義"""
        legacy = client_with_market_db.get("/api/market/stocks?market=standard")
        numeric = client_with_market_db.get("/api/market/stocks?market=0112")
        assert legacy.status_code == 200
        assert numeric.status_code == 200
        assert numeric.json() == legacy.json()

    def test_growth_aliases_are_accepted(self, client_with_market_db):
        """growth / 0113 も API 入力境界で同義に扱われる"""
        legacy = client_with_market_db.get("/api/market/stocks?market=growth")
        numeric = client_with_market_db.get("/api/market/stocks?market=0113")
        assert legacy.status_code == numeric.status_code
        assert legacy.status_code in (200, 404)
        if legacy.status_code == 404:
            legacy_payload = legacy.json()
            numeric_payload = numeric.json()
            assert numeric_payload["status"] == legacy_payload["status"] == "error"
            assert numeric_payload["error"] == legacy_payload["error"]
            assert numeric_payload["message"] == legacy_payload["message"]
        else:
            assert numeric.json() == legacy.json()

    def test_422_invalid_market(self, client_with_market_db):
        """無効な market パラメータ"""
        resp = client_with_market_db.get("/api/market/stocks?market=invalid")
        assert resp.status_code == 422

    def test_422_invalid_history_days(self, client_with_market_db):
        """無効な history_days"""
        resp = client_with_market_db.get("/api/market/stocks?history_days=0")
        assert resp.status_code == 422

    def test_default_params(self, client_with_market_db):
        """デフォルトパラメータ (market=prime, history_days=300)"""
        resp = client_with_market_db.get("/api/market/stocks")
        assert resp.status_code == 200


class TestGetStockInfo:
    def test_200(self, client_with_market_db):
        """銘柄情報取得"""
        resp = client_with_market_db.get("/api/market/stocks/7203")
        assert resp.status_code == 200
        data = resp.json()
        assert data["code"] == "72030"
        assert data["companyName"] == "トヨタ自動車"
        assert data["companyNameEnglish"] == "TOYOTA MOTOR"
        assert data["marketCode"] == "prime"

    def test_200_5digit(self, client_with_market_db):
        """5桁コードでも取得可能"""
        resp = client_with_market_db.get("/api/market/stocks/72030")
        assert resp.status_code == 200
        assert resp.json()["code"] == "72030"

    def test_200_as_of_date_uses_stock_master_daily(self, client_with_market_db):
        """asOfDate 指定時は日次マスターを参照する"""
        resp = client_with_market_db.get("/api/market/stocks/72030?asOfDate=2024-01-15")
        assert resp.status_code == 200
        data = resp.json()
        assert data["companyName"] == "トヨタ自動車（履歴）"
        assert data["scaleCategory"] == "TOPIX Core30"

    def test_404(self, client_with_market_db):
        """存在しない銘柄"""
        resp = client_with_market_db.get("/api/market/stocks/0000")
        assert resp.status_code == 404
        data = resp.json()
        assert data["status"] == "error"
        assert "Stock not found" in data["message"]


class TestGetStockOhlcv:
    def test_200(self, client_with_market_db):
        """OHLCV データ取得"""
        resp = client_with_market_db.get("/api/market/stocks/7203/ohlcv")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 3
        assert data[0]["date"] == "2024-01-15"
        assert "open" in data[0]
        assert "volume" in data[0]
        assert isinstance(data[0]["volume"], int)

    def test_200_with_date_range(self, client_with_market_db):
        """日付範囲指定"""
        resp = client_with_market_db.get("/api/market/stocks/7203/ohlcv?start_date=2024-01-16&end_date=2024-01-16")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1

    def test_404(self, client_with_market_db):
        """存在しない銘柄"""
        resp = client_with_market_db.get("/api/market/stocks/0000/ohlcv")
        assert resp.status_code == 404

    def test_response_shape(self, client_with_market_db):
        """レスポンス形状が public API contract と一致"""
        resp = client_with_market_db.get("/api/market/stocks/7203/ohlcv")
        data = resp.json()
        record = data[0]
        assert set(record.keys()) == {"date", "open", "high", "low", "close", "volume"}


class TestGetStockMinuteBars:
    def test_200(self, client_with_market_db):
        resp = client_with_market_db.get(
            "/api/market/stocks/7203/minute-bars?date=2024-01-16"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["time"] == "09:00"
        assert data[0]["volume"] == 1000
        assert data[0]["turnoverValue"] == 2502000.0
        assert data[1]["time"] == "09:01"

    def test_200_with_time_range(self, client_with_market_db):
        resp = client_with_market_db.get(
            "/api/market/stocks/7203/minute-bars?date=2024-01-16&start_time=09:01&end_time=09:01"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["time"] == "09:01"

    def test_404(self, client_with_market_db):
        resp = client_with_market_db.get(
            "/api/market/stocks/0000/minute-bars?date=2024-01-16"
        )
        assert resp.status_code == 404

    def test_422_invalid_time_range(self, client_with_market_db):
        resp = client_with_market_db.get(
            "/api/market/stocks/7203/minute-bars?date=2024-01-16&start_time=15:30&end_time=09:00"
        )
        assert resp.status_code == 422


class TestGetTopix:
    def test_200(self, client_with_market_db):
        """TOPIX データ取得"""
        resp = client_with_market_db.get("/api/market/topix")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 3
        assert data[0]["date"] == "2024-01-15"
        # TOPIX には volume がない
        assert "volume" not in data[0]

    def test_200_with_date_range(self, client_with_market_db):
        """日付範囲指定"""
        resp = client_with_market_db.get("/api/market/topix?start_date=2024-01-16")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2

    def test_response_shape(self, client_with_market_db):
        """レスポンス形状が public API contract と一致（volume なし）"""
        resp = client_with_market_db.get("/api/market/topix")
        data = resp.json()
        record = data[0]
        assert set(record.keys()) == {"date", "open", "high", "low", "close"}


class TestGetOptions225:
    def test_200_latest(self, client_with_market_db):
        resp = client_with_market_db.get("/api/market/options/225")

        assert resp.status_code == 200
        data = resp.json()
        assert data["requestedDate"] is None
        assert data["resolvedDate"] == "2024-01-17"
        assert data["sourceCallCount"] == 0
        assert len(data["items"]) == 2

    def test_200_requested_date(self, client_with_market_db):
        resp = client_with_market_db.get("/api/market/options/225?date=2024-01-16")

        assert resp.status_code == 200
        data = resp.json()
        assert data["requestedDate"] == "2024-01-16"
        assert data["resolvedDate"] == "2024-01-16"
        assert len(data["items"]) == 1
        assert data["items"][0]["underlyingPrice"] == 36100.0

    def test_404_missing_date(self, client_with_market_db):
        resp = client_with_market_db.get("/api/market/options/225?date=2024-01-18")

        assert resp.status_code == 404
        data = resp.json()
        assert data["status"] == "error"
        assert data["details"] == [
            {"field": "reason", "message": "options_225_data_missing"},
            {"field": "recovery", "message": "market_db_sync"},
        ]

    def test_422_invalid_date(self, client_with_market_db):
        resp = client_with_market_db.get("/api/market/options/225?date=202401")

        assert resp.status_code == 422
