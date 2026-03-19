"""
PyTest設定ファイル

テストに使用する共通フィクスチャやモックデータを定義します。
"""

import httpx
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import Mock
from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app


@pytest.fixture
def test_app():
    """テスト用 FastAPI アプリ（lifespan 含む）"""
    return create_app()


@pytest.fixture
def sync_client(test_app):
    """同期テストクライアント（既存パターン、lifespan 自動処理）"""
    with TestClient(test_app) as client:
        yield client


@pytest.fixture
async def async_client(test_app):
    """非同期テストクライアント（ASGITransport）"""
    transport = httpx.ASGITransport(app=test_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
def sample_ohlcv_data():
    """サンプルOHLCVデータを生成"""
    np.random.seed(42)  # 再現可能な結果のために固定シード

    dates = pd.date_range(start="2023-01-01", end="2023-12-31", freq="D")
    n_days = len(dates)

    # ランダムウォークで価格データを生成
    base_price = 1000
    returns = np.random.normal(0, 0.02, n_days)
    prices = [base_price]

    for r in returns[1:]:
        prices.append(prices[-1] * (1 + r))

    # OHLCV生成
    close = pd.Series(prices, index=dates, name="Close")
    open_prices = close.shift(1).fillna(close.iloc[0])

    # 高値・安値を生成（適度なボラティリティ）
    high_factor = np.random.uniform(1.0, 1.02, n_days)
    low_factor = np.random.uniform(0.98, 1.0, n_days)

    high = pd.Series(close * high_factor, index=dates, name="High")
    low = pd.Series(close * low_factor, index=dates, name="Low")
    volume = pd.Series(
        np.random.randint(1000, 10000, n_days), index=dates, name="Volume"
    )

    return pd.DataFrame(
        {
            "Open": open_prices,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": volume,
        }
    )


@pytest.fixture
def sample_portfolio():
    """サンプルVectorBTポートフォリオを生成"""
    import vectorbt as vbt

    data = pd.DataFrame(
        {"Close": [100, 101, 99, 102, 98, 105, 103]},
        index=pd.date_range("2023-01-01", periods=7),
    )

    entries = pd.Series(
        [True, False, False, True, False, False, False], index=data.index
    )
    exits = pd.Series([False, False, True, False, False, True, False], index=data.index)

    portfolio = vbt.Portfolio.from_signals(
        close=data["Close"],
        entries=entries,
        exits=exits,
        init_cash=10000,
        fees=0.001,
        freq="1D",
    )

    return portfolio


@pytest.fixture
def mock_vbt_logger():
    """VBTLoggerのモック"""
    logger = Mock()
    logger.debug = Mock()
    logger.info = Mock()
    logger.warning = Mock()
    logger.error = Mock()
    return logger


@pytest.fixture
def mock_prepare_vbt_data():
    """prepare_vbt_dataのモック"""

    def _mock_prepare_data(
        db_path, stock_code, start_date=None, end_date=None, use_relative_strength=False
    ):
        dates = pd.date_range("2023-01-01", "2023-12-31", freq="D")
        n_days = len(dates)

        # 基本データ
        close = pd.Series(np.random.uniform(950, 1050, n_days), index=dates)
        data = pd.DataFrame(
            {
                "Open": close.shift(1).fillna(close.iloc[0]),
                "High": close * np.random.uniform(1.0, 1.02, n_days),
                "Low": close * np.random.uniform(0.98, 1.0, n_days),
                "Close": close,
                "Volume": np.random.randint(1000, 10000, n_days),
            }
        )

        return {"daily": data}

    return _mock_prepare_data


@pytest.fixture
def disable_warnings():
    """テスト実行時に警告を無効化"""
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        yield


# --- Phase 3B-1: JQuants mock fixtures ---


@pytest.fixture
def mock_jquants_env(monkeypatch):
    """JQuants 環境変数をセット（テスト用）"""
    monkeypatch.setenv("JQUANTS_API_KEY", "dummy_token_value_0000")
    monkeypatch.setenv("JQUANTS_PLAN", "free")


# --- Phase 3B-2a: market db fixtures (DuckDB SoT) ---


@pytest.fixture
def market_db_path(market_duckdb_path: str) -> str:
    """後方互換 fixture 名。実体は DuckDB market path を返す。"""
    return market_duckdb_path


@pytest.fixture
def market_timeseries_dir(tmp_path: Path) -> str:
    """テスト用 DuckDB time-series ディレクトリを作成"""
    import duckdb

    base_dir = tmp_path / "market-timeseries"
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
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("67580", "ソニーグループ", "SONY GROUP", "prime", "プライム", "S17_2", "電気機器", "S33_2", "電気機器", "TOPIX Large70", "1958-12-01", None, None),
    )
    conn.execute(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("99840", "テスト銘柄", "TEST STOCK", "standard", "スタンダード", "S17_3", "情報通信", "S33_3", "情報通信", None, "2020-01-01", None, None),
    )

    for code in ("72030", "67580"):
        for i, d in enumerate(("2024-01-15", "2024-01-16", "2024-01-17")):
            base = 2500.0 + i * 10 if code == "72030" else 13000.0 + i * 50
            conn.execute(
                "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (code, d, base, base + 20, base - 10, base + 5, 1000000 + i * 100, 1.0, None),
            )

    for d in ("2024-01-15", "2024-01-16", "2024-01-17"):
        conn.execute(
            "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)",
            (d, 2500.0, 2520.0, 2480.0, 2510.0, None),
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

    for d in ("2024-01-15", "2024-01-16", "2024-01-17"):
        conn.execute(
            "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("0000", d, 2500.0, 2520.0, 2480.0, 2510.0, None, None),
        )
        conn.execute(
            "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("0001", d, 1200.0, 1220.0, 1190.0, 1210.0, "電気機器", None),
        )
    conn.execute(
        "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("N225_UNDERPX", "2024-01-16", 36100.0, 36100.0, 36100.0, 36100.0, "日経平均", None),
    )
    conn.execute(
        "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("N225_UNDERPX", "2024-01-17", 36250.0, 36250.0, 36250.0, 36250.0, "日経平均", None),
    )

    options_rows = [
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
    ]
    conn.executemany(
        """
        INSERT INTO options_225_data VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        options_rows,
    )

    conn.close()
    return str(base_dir)


@pytest.fixture
def market_duckdb_path(market_timeseries_dir: str) -> str:
    return str(Path(market_timeseries_dir) / "market.duckdb")
