"""
PyTest設定ファイル

テストに使用する共通フィクスチャやモックデータを定義します。
"""

import httpx
import pytest
import pandas as pd
import numpy as np
import vectorbt as vbt
from unittest.mock import Mock
from fastapi.testclient import TestClient

from src.server.app import create_app


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
    monkeypatch.setenv("JQUANTS_API_KEY", "test-api-key-12345678")
    monkeypatch.setenv("JQUANTS_PLAN", "free")


# --- Phase 3B-2a: market.db fixtures ---


@pytest.fixture
def market_db_path(tmp_path):
    """テスト用 in-memory 風 market.db を tmp_path に作成"""
    import sqlite3

    db_path = str(tmp_path / "market.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    # stocks テーブル
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

    # stock_data テーブル
    conn.execute("""
        CREATE TABLE stock_data (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER NOT NULL,
            adjustment_factor REAL,
            created_at TEXT,
            PRIMARY KEY (code, date)
        )
    """)

    # topix_data テーブル
    conn.execute("""
        CREATE TABLE topix_data (
            date TEXT PRIMARY KEY,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            created_at TEXT
        )
    """)

    # テストデータ挿入
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

    # OHLCV データ
    for code in ("72030", "67580"):
        for i, d in enumerate(("2024-01-15", "2024-01-16", "2024-01-17")):
            base = 2500.0 + i * 10 if code == "72030" else 13000.0 + i * 50
            conn.execute(
                "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (code, d, base, base + 20, base - 10, base + 5, 1000000 + i * 100, 1.0, None),
            )

    # TOPIX データ
    for d in ("2024-01-15", "2024-01-16", "2024-01-17"):
        conn.execute(
            "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)",
            (d, 2500.0, 2520.0, 2480.0, 2510.0, None),
        )

    # --- Phase 3B-2b: index_master + indices_data ---

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
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            sector_name TEXT,
            created_at TEXT,
            PRIMARY KEY (code, date)
        )
    """)

    conn.execute(
        "INSERT INTO index_master VALUES (?, ?, ?, ?, ?)",
        ("0000", "TOPIX", "TOPIX", "topix", "2008-05-07"),
    )
    conn.execute(
        "INSERT INTO index_master VALUES (?, ?, ?, ?, ?)",
        ("0001", "電気機器", "Electric Appliances", "sector33", "2010-01-04"),
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

    conn.commit()
    conn.close()
    return db_path
