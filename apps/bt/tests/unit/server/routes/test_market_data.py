"""
Market Data Routes Unit Tests

sync_client を使用してルートの E2E テスト。
"""

import os
from pathlib import Path

import pytest
import duckdb

from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app


def _build_market_timeseries_dir(base_dir: Path) -> str:
    base_dir.mkdir(parents=True, exist_ok=True)
    duckdb_path = base_dir / "market.duckdb"
    conn = duckdb.connect(str(duckdb_path))

    conn.execute("""
        CREATE TABLE stocks (
            code TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            company_name_english TEXT,
            market_code TEXT NOT NULL,
            market_name TEXT NOT NULL,
            sector_17_code TEXT NOT NULL,
            sector_17_name TEXT NOT NULL,
            sector_33_code TEXT NOT NULL,
            sector_33_name TEXT NOT NULL,
            scale_category TEXT,
            listed_date TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE stock_master_daily (
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            company_name TEXT NOT NULL,
            company_name_english TEXT,
            market_code TEXT NOT NULL,
            market_name TEXT NOT NULL,
            sector_17_code TEXT NOT NULL,
            sector_17_name TEXT NOT NULL,
            sector_33_code TEXT NOT NULL,
            sector_33_name TEXT NOT NULL,
            scale_category TEXT,
            listed_date TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE stock_data (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open DOUBLE NOT NULL,
            high DOUBLE NOT NULL,
            low DOUBLE NOT NULL,
            close DOUBLE NOT NULL,
            volume BIGINT NOT NULL,
            adjustment_factor DOUBLE,
            created_at TEXT,
            PRIMARY KEY (code, date)
        )
    """)
    conn.execute("""
        CREATE TABLE stock_data_minute_raw (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            open DOUBLE NOT NULL,
            high DOUBLE NOT NULL,
            low DOUBLE NOT NULL,
            close DOUBLE NOT NULL,
            volume BIGINT NOT NULL,
            turnover_value DOUBLE,
            created_at TEXT,
            PRIMARY KEY (code, date, time)
        )
    """)
    conn.execute("""
        CREATE TABLE topix_data (
            date TEXT PRIMARY KEY,
            open DOUBLE NOT NULL,
            high DOUBLE NOT NULL,
            low DOUBLE NOT NULL,
            close DOUBLE NOT NULL,
            created_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE index_master (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            name_english TEXT,
            category TEXT NOT NULL,
            data_start_date TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE indices_data (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            sector_name TEXT,
            created_at TEXT,
            PRIMARY KEY (code, date)
        )
    """)
    conn.execute("""
        CREATE TABLE options_225_data (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            whole_day_open DOUBLE,
            whole_day_high DOUBLE,
            whole_day_low DOUBLE,
            whole_day_close DOUBLE,
            night_session_open DOUBLE,
            night_session_high DOUBLE,
            night_session_low DOUBLE,
            night_session_close DOUBLE,
            day_session_open DOUBLE,
            day_session_high DOUBLE,
            day_session_low DOUBLE,
            day_session_close DOUBLE,
            volume DOUBLE,
            open_interest DOUBLE,
            turnover_value DOUBLE,
            contract_month TEXT,
            strike_price DOUBLE,
            only_auction_volume DOUBLE,
            emergency_margin_trigger_division TEXT,
            put_call_division TEXT,
            last_trading_day TEXT,
            special_quotation_day TEXT,
            settlement_price DOUBLE,
            theoretical_price DOUBLE,
            base_volatility DOUBLE,
            underlying_price DOUBLE,
            implied_volatility DOUBLE,
            interest_rate DOUBLE,
            created_at TEXT,
            PRIMARY KEY (code, date)
        )
    """)

    conn.execute(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("72030", "トヨタ自動車", "TOYOTA MOTOR", "prime", "プライム", "S17_1", "輸送用機器", "S33_1", "輸送用機器", "TOPIX Large70", "1949-05-16", None, None),
    )
    conn.execute(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("2024-01-15", "72030", "トヨタ自動車（履歴）", "TOYOTA MOTOR HIST", "prime", "プライム", "S17_1", "輸送用機器", "S33_1", "輸送用機器", "TOPIX Core30", "1949-05-16", None, None),
    )
    conn.execute(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("67580", "ソニーグループ", "SONY GROUP", "prime", "プライム", "S17_2", "電気機器", "S33_2", "電気機器", "TOPIX Large70", "1958-12-01", None, None),
    )
    conn.execute(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("99840", "テスト銘柄", "TEST STOCK", "standard", "スタンダード", "S17_3", "情報通信", "S33_3", "情報通信", None, "2020-01-01", None, None),
    )

    for code in ("72030", "67580"):
        for i, date_value in enumerate(("2024-01-15", "2024-01-16", "2024-01-17")):
            base = 2500.0 + i * 10 if code == "72030" else 13000.0 + i * 50
            conn.execute(
                "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (code, date_value, base, base + 20, base - 10, base + 5, 1000000 + i * 100, 1.0, None),
            )

    conn.executemany(
        "INSERT INTO stock_data_minute_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("7203", "2024-01-16", "09:00", 2500.0, 2505.0, 2495.0, 2502.0, 1000, 2502000.0, None),
            ("72030", "2024-01-16", "09:00", 2500.0, 2505.0, 2495.0, 2502.0, 1000, 2502000.0, None),
            ("7203", "2024-01-16", "09:01", 2502.0, 2508.0, 2500.0, 2506.0, 800, 2004800.0, None),
            ("9984", "2024-01-16", "15:30", 7000.0, 7010.0, 6990.0, 7005.0, 500, 3502500.0, None),
        ],
    )

    for date_value in ("2024-01-15", "2024-01-16", "2024-01-17"):
        conn.execute(
            "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)",
            (date_value, 2500.0, 2520.0, 2480.0, 2510.0, None),
        )

    conn.execute(
        "INSERT INTO index_master VALUES (?, ?, ?, ?, ?)",
        ("0000", "TOPIX", "TOPIX", "topix", "2008-05-07"),
    )
    conn.execute(
        "INSERT INTO index_master VALUES (?, ?, ?, ?, ?)",
        ("0001", "電気機器", "Electric Appliances", "sector33", "2010-01-04"),
    )
    conn.execute(
        "INSERT INTO index_master VALUES (?, ?, ?, ?, ?)",
        ("N225_UNDERPX", "日経平均", "Nikkei 225 (UnderPx derived)", "synthetic", "2024-01-16"),
    )

    for date_value in ("2024-01-15", "2024-01-16", "2024-01-17"):
        conn.execute(
            "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("0000", date_value, 2500.0, 2520.0, 2480.0, 2510.0, None, None),
        )
        conn.execute(
            "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("0001", date_value, 1200.0, 1220.0, 1190.0, 1210.0, "電気機器", None),
        )
    conn.execute(
        "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("N225_UNDERPX", "2024-01-16", 36100.0, 36100.0, 36100.0, 36100.0, "日経平均", None),
    )
    conn.execute(
        "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("N225_UNDERPX", "2024-01-17", 36250.0, 36250.0, 36250.0, 36250.0, "日経平均", None),
    )

    conn.executemany(
        """
        INSERT INTO options_225_data VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "131040018",
                "2024-01-16",
                10.0,
                12.0,
                9.0,
                11.0,
                9.0,
                11.0,
                8.0,
                10.0,
                10.0,
                12.0,
                9.0,
                11.0,
                100.0,
                250.0,
                110000.0,
                "2024-04",
                32000.0,
                0.0,
                None,
                "1",
                "2024-04-11",
                "2024-04-12",
                11.0,
                10.5,
                18.0,
                36100.0,
                22.0,
                0.5,
                None,
            ),
            (
                "131040018",
                "2024-01-17",
                12.0,
                13.0,
                11.0,
                12.5,
                11.0,
                12.0,
                10.0,
                11.0,
                12.0,
                13.0,
                11.0,
                12.5,
                120.0,
                260.0,
                130000.0,
                "2024-04",
                32000.0,
                0.0,
                None,
                "1",
                "2024-04-11",
                "2024-04-12",
                12.0,
                11.5,
                18.5,
                36250.0,
                23.0,
                0.5,
                None,
            ),
            (
                "141040018",
                "2024-01-17",
                20.0,
                21.0,
                18.0,
                19.5,
                19.0,
                20.0,
                18.0,
                19.0,
                20.0,
                21.0,
                18.0,
                19.5,
                90.0,
                180.0,
                175000.0,
                "2024-04",
                36000.0,
                0.0,
                None,
                "2",
                "2024-04-11",
                "2024-04-12",
                19.0,
                19.2,
                17.5,
                36250.0,
                19.0,
                0.5,
                None,
            ),
        ],
    )

    conn.close()
    return str(base_dir)


@pytest.fixture(scope="module")
def market_data_timeseries_dir(tmp_path_factory):
    return _build_market_timeseries_dir(tmp_path_factory.mktemp("market-data-routes") / "market-timeseries")


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
        """レスポンス形状が Hono 互換"""
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
        """レスポンス形状が Hono 互換（volume なし）"""
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
