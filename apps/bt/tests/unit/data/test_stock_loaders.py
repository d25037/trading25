"""stock_loaders.py のテスト"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def _ohlcv_df(n=5):
    idx = pd.date_range("2025-01-01", periods=n, freq="D")
    return pd.DataFrame(
        {"Open": 100.0, "High": 105.0, "Low": 95.0, "Close": 102.0, "Volume": 1000},
        index=idx,
    )


def _mock_api_client():
    """DatasetAPIClient のコンテキストマネージャモックを生成"""
    client = MagicMock()
    client.__enter__ = MagicMock(return_value=client)
    client.__exit__ = MagicMock(return_value=False)
    return client


def _mock_cache_disabled():
    """キャッシュ無効モックを生成"""
    cache = MagicMock()
    cache.is_enabled.return_value = False
    return cache


class TestGetStockList:
    @patch("src.data.loaders.stock_loaders.DataCache")
    @patch("src.data.loaders.stock_loaders.extract_dataset_name")
    @patch("src.data.loaders.stock_loaders.DatasetAPIClient")
    def test_returns_list(self, mock_client_cls, mock_extract, mock_cache_cls):
        from src.data.loaders.stock_loaders import get_stock_list

        mock_extract.return_value = "testds"
        mock_cache_cls.get_instance.return_value = _mock_cache_disabled()
        client = _mock_api_client()
        client.get_stock_list.return_value = pd.DataFrame({"stockCode": ["7203", "6758"]})
        mock_client_cls.return_value = client

        result = get_stock_list("testds")
        assert result == ["7203", "6758"]

    @patch("src.data.loaders.stock_loaders.DataCache")
    @patch("src.data.loaders.stock_loaders.extract_dataset_name")
    @patch("src.data.loaders.stock_loaders.DatasetAPIClient")
    def test_empty_returns_empty_list(self, mock_client_cls, mock_extract, mock_cache_cls):
        from src.data.loaders.stock_loaders import get_stock_list

        mock_extract.return_value = "testds"
        mock_cache_cls.get_instance.return_value = _mock_cache_disabled()
        client = _mock_api_client()
        client.get_stock_list.return_value = pd.DataFrame()
        mock_client_cls.return_value = client

        result = get_stock_list("testds")
        assert result == []


class TestLoadStockData:
    @patch("src.data.loaders.stock_loaders.DataCache")
    @patch("src.data.loaders.stock_loaders.extract_dataset_name")
    @patch("src.data.loaders.stock_loaders.DatasetAPIClient")
    def test_returns_dataframe(self, mock_client_cls, mock_extract, mock_cache_cls):
        from src.data.loaders.stock_loaders import load_stock_data

        mock_extract.return_value = "testds"
        mock_cache_cls.get_instance.return_value = _mock_cache_disabled()
        client = _mock_api_client()
        client.get_stock_ohlcv.return_value = _ohlcv_df()
        mock_client_cls.return_value = client

        result = load_stock_data("testds", "7203")
        assert len(result) == 5
        assert "Close" in result.columns

    @patch("src.data.loaders.stock_loaders.DataCache")
    @patch("src.data.loaders.stock_loaders.extract_dataset_name")
    @patch("src.data.loaders.stock_loaders.DatasetAPIClient")
    def test_empty_raises_value_error(self, mock_client_cls, mock_extract, mock_cache_cls):
        from src.data.loaders.stock_loaders import load_stock_data

        mock_extract.return_value = "testds"
        mock_cache_cls.get_instance.return_value = _mock_cache_disabled()
        client = _mock_api_client()
        client.get_stock_ohlcv.return_value = pd.DataFrame()
        mock_client_cls.return_value = client

        with pytest.raises(ValueError, match="No data found"):
            load_stock_data("testds", "9999")


class TestGetAvailableStocks:
    @patch("src.data.loaders.stock_loaders.extract_dataset_name")
    @patch("src.data.loaders.stock_loaders.DatasetAPIClient")
    def test_returns_dataframe(self, mock_client_cls, mock_extract):
        from src.data.loaders.stock_loaders import get_available_stocks

        mock_extract.return_value = "testds"
        expected = pd.DataFrame({"stockCode": ["7203"], "recordCount": [500]})
        client = _mock_api_client()
        client.get_available_stocks.return_value = expected
        mock_client_cls.return_value = client

        result = get_available_stocks("testds")
        assert len(result) == 1
        assert result["stockCode"].iloc[0] == "7203"
