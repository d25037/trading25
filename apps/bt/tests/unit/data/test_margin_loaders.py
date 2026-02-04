"""margin_loaders.py のテスト"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.data.loaders.margin_loaders import get_margin_available_stocks, transform_margin_df


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


class TestTransformMarginDf:
    def test_basic_transform(self):
        df = pd.DataFrame({
            "longMarginVolume": [1000.0, 2000.0],
            "shortMarginVolume": [500.0, 300.0],
        })
        result = transform_margin_df(df)
        assert "LongMargin" in result.columns
        assert "ShortMargin" in result.columns
        assert "TotalMargin" in result.columns
        assert "MarginRatio" in result.columns
        assert result["TotalMargin"].iloc[0] == 1500.0
        assert result["MarginRatio"].iloc[0] == pytest.approx(500.0 / 1500.0)

    def test_nan_handling(self):
        df = pd.DataFrame({
            "longMarginVolume": [float("nan"), 1000.0],
            "shortMarginVolume": [500.0, float("nan")],
        })
        result = transform_margin_df(df)
        assert result["LongMargin"].iloc[0] == 0.0
        assert result["ShortMargin"].iloc[1] == 0.0
        assert result["TotalMargin"].iloc[0] == 500.0

    def test_zero_total_margin_ratio(self):
        df = pd.DataFrame({
            "longMarginVolume": [0.0],
            "shortMarginVolume": [0.0],
        })
        result = transform_margin_df(df)
        assert result["MarginRatio"].iloc[0] == 0.0
        assert result["TotalMargin"].iloc[0] == 0.0


class TestLoadMarginData:
    @patch("src.data.loaders.margin_loaders.DataCache")
    @patch("src.data.loaders.margin_loaders.extract_dataset_name")
    @patch("src.data.loaders.margin_loaders.DatasetAPIClient")
    def test_basic_load(self, mock_client_cls, mock_extract, mock_cache_cls):
        from src.data.loaders.margin_loaders import load_margin_data

        mock_extract.return_value = "testds"
        mock_cache_cls.get_instance.return_value = _mock_cache_disabled()
        client = _mock_api_client()
        idx = pd.date_range("2025-01-01", periods=3, freq="W")
        client.get_margin.return_value = pd.DataFrame(
            {"longMarginVolume": [100, 200, 300], "shortMarginVolume": [50, 100, 150]},
            index=idx,
        )
        mock_client_cls.return_value = client

        result = load_margin_data("testds", "7203")
        assert "TotalMargin" in result.columns
        assert len(result) == 3

    @patch("src.data.loaders.margin_loaders.DataCache")
    @patch("src.data.loaders.margin_loaders.extract_dataset_name")
    @patch("src.data.loaders.margin_loaders.DatasetAPIClient")
    def test_empty_raises(self, mock_client_cls, mock_extract, mock_cache_cls):
        from src.data.loaders.margin_loaders import load_margin_data

        mock_extract.return_value = "testds"
        mock_cache_cls.get_instance.return_value = _mock_cache_disabled()
        client = _mock_api_client()
        client.get_margin.return_value = pd.DataFrame()
        mock_client_cls.return_value = client

        with pytest.raises(ValueError, match="No margin data"):
            load_margin_data("testds", "9999")

    @patch("src.data.loaders.margin_loaders.DataCache")
    @patch("src.data.loaders.margin_loaders.extract_dataset_name")
    @patch("src.data.loaders.margin_loaders.DatasetAPIClient")
    def test_daily_index_reindex(self, mock_client_cls, mock_extract, mock_cache_cls):
        from src.data.loaders.margin_loaders import load_margin_data

        mock_extract.return_value = "testds"
        mock_cache_cls.get_instance.return_value = _mock_cache_disabled()
        client = _mock_api_client()
        weekly_idx = pd.date_range("2025-01-06", periods=2, freq="W-MON")
        client.get_margin.return_value = pd.DataFrame(
            {"longMarginVolume": [100, 200], "shortMarginVolume": [50, 100]},
            index=weekly_idx,
        )
        mock_client_cls.return_value = client

        daily_idx = pd.date_range("2025-01-06", periods=10, freq="D")
        result = load_margin_data("testds", "7203", daily_index=daily_idx)
        assert len(result) == 10
        assert result.isna().sum().sum() == 0


class TestGetMarginAvailableStocks:
    @patch("src.data.loaders.margin_loaders.extract_dataset_name")
    @patch("src.data.loaders.margin_loaders.DatasetAPIClient")
    def test_success(self, mock_client_cls, mock_extract):
        mock_extract.return_value = "testds"
        client = _mock_api_client()
        expected = pd.DataFrame({"stockCode": ["7203"], "record_count": [100]})
        client.get_margin_list.return_value = expected
        mock_client_cls.return_value = client

        result = get_margin_available_stocks("testds")
        assert len(result) == 1
        assert "stockCode" in result.columns
