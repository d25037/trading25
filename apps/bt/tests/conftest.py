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
