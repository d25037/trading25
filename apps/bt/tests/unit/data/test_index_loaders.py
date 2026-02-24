"""index_loaders.py のテスト"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def _index_df(n=5):
    idx = pd.date_range("2025-01-01", periods=n, freq="D")
    return pd.DataFrame(
        {"Open": 2000.0, "High": 2050.0, "Low": 1980.0, "Close": 2020.0, "Volume": 0},
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


class TestLoadTopixData:
    @patch("src.infrastructure.data_access.loaders.cache.DataCache")
    @patch("src.infrastructure.data_access.loaders.index_loaders.extract_dataset_name")
    @patch("src.infrastructure.data_access.loaders.index_loaders.DatasetAPIClient")
    def test_basic(self, mock_client_cls, mock_extract, mock_cache_cls):
        from src.infrastructure.data_access.loaders.index_loaders import load_topix_data

        mock_extract.return_value = "testds"
        mock_cache_cls.get_instance.return_value = _mock_cache_disabled()
        client = _mock_api_client()
        client.get_topix.return_value = _index_df()
        mock_client_cls.return_value = client

        result = load_topix_data("testds")
        assert len(result) == 5


class TestLoadIndexData:
    @patch("src.infrastructure.data_access.loaders.cache.DataCache")
    @patch("src.infrastructure.data_access.loaders.index_loaders.extract_dataset_name")
    @patch("src.infrastructure.data_access.loaders.index_loaders.DatasetAPIClient")
    def test_basic(self, mock_client_cls, mock_extract, mock_cache_cls):
        from src.infrastructure.data_access.loaders.index_loaders import load_index_data

        mock_extract.return_value = "testds"
        mock_cache_cls.get_instance.return_value = _mock_cache_disabled()
        client = _mock_api_client()
        client.get_index.return_value = _index_df()
        mock_client_cls.return_value = client

        result = load_index_data("testds", "topix")
        assert len(result) == 5

    @patch("src.infrastructure.data_access.loaders.cache.DataCache")
    @patch("src.infrastructure.data_access.loaders.index_loaders.extract_dataset_name")
    @patch("src.infrastructure.data_access.loaders.index_loaders.DatasetAPIClient")
    def test_empty_raises(self, mock_client_cls, mock_extract, mock_cache_cls):
        from src.infrastructure.data_access.loaders.index_loaders import load_index_data

        mock_extract.return_value = "testds"
        mock_cache_cls.get_instance.return_value = _mock_cache_disabled()
        client = _mock_api_client()
        client.get_index.return_value = pd.DataFrame()
        mock_client_cls.return_value = client

        with pytest.raises(ValueError, match="No.*data"):
            load_index_data("testds", "unknown_index")


class TestGetIndexList:
    @patch("src.infrastructure.data_access.loaders.index_loaders.extract_dataset_name")
    @patch("src.infrastructure.data_access.loaders.index_loaders.DatasetAPIClient")
    def test_returns_list(self, mock_client_cls, mock_extract):
        from src.infrastructure.data_access.loaders.index_loaders import get_index_list

        mock_extract.return_value = "testds"
        client = _mock_api_client()
        client.get_index_list.return_value = pd.DataFrame(
            {"indexCode": ["topix", "nikkei225"]}
        )
        mock_client_cls.return_value = client

        result = get_index_list("testds")
        assert "topix" in result

    @patch("src.infrastructure.data_access.loaders.index_loaders.extract_dataset_name")
    @patch("src.infrastructure.data_access.loaders.index_loaders.DatasetAPIClient")
    def test_empty_returns_empty_list(self, mock_client_cls, mock_extract):
        from src.infrastructure.data_access.loaders.index_loaders import get_index_list

        mock_extract.return_value = "testds"
        client = _mock_api_client()
        client.get_index_list.return_value = pd.DataFrame()
        mock_client_cls.return_value = client

        result = get_index_list("testds")
        assert result == []


class TestLoadTopixDataFromMarketDb:
    @patch("src.infrastructure.data_access.loaders.index_loaders.MarketAPIClient")
    def test_basic(self, mock_client_cls):
        from src.infrastructure.data_access.loaders.index_loaders import load_topix_data_from_market_db

        client = _mock_api_client()
        client.get_topix.return_value = _index_df()
        mock_client_cls.return_value = client

        result = load_topix_data_from_market_db()
        assert len(result) == 5

    @patch("src.infrastructure.data_access.loaders.index_loaders.MarketAPIClient")
    def test_empty_raises(self, mock_client_cls):
        from src.infrastructure.data_access.loaders.index_loaders import load_topix_data_from_market_db

        client = _mock_api_client()
        client.get_topix.return_value = pd.DataFrame()
        mock_client_cls.return_value = client

        with pytest.raises(ValueError, match="No TOPIX data"):
            load_topix_data_from_market_db()


class TestLoadTopixDataEmpty:
    @patch("src.infrastructure.data_access.loaders.cache.DataCache")
    @patch("src.infrastructure.data_access.loaders.index_loaders.extract_dataset_name")
    @patch("src.infrastructure.data_access.loaders.index_loaders.DatasetAPIClient")
    def test_empty_raises(self, mock_client_cls, mock_extract, mock_cache_cls):
        from src.infrastructure.data_access.loaders.index_loaders import load_topix_data

        mock_extract.return_value = "testds"
        mock_cache_cls.get_instance.return_value = _mock_cache_disabled()
        client = _mock_api_client()
        client.get_topix.return_value = pd.DataFrame()
        mock_client_cls.return_value = client

        with pytest.raises(ValueError, match="No TOPIX data"):
            load_topix_data("testds")


class TestGetAvailableIndices:
    @patch("src.infrastructure.data_access.loaders.index_loaders.extract_dataset_name")
    @patch("src.infrastructure.data_access.loaders.index_loaders.DatasetAPIClient")
    def test_returns_dataframe(self, mock_client_cls, mock_extract):
        from src.infrastructure.data_access.loaders.index_loaders import get_available_indices

        mock_extract.return_value = "testds"
        client = _mock_api_client()
        expected = pd.DataFrame({"indexCode": ["topix"], "record_count": [500]})
        client.get_index_list.return_value = expected
        mock_client_cls.return_value = client

        result = get_available_indices("testds")
        assert len(result) == 1
        assert "indexCode" in result.columns
