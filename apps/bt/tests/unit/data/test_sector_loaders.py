"""data/loaders/sector_loaders.py のテスト"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.data.loaders import sector_loaders
from src.data.loaders.sector_loaders import (
    get_sector_mapping,
    get_stock_sector_mapping,
    load_all_sector_indices,
    prepare_sector_data,
)
from src.exceptions import SectorDataLoadError


@pytest.fixture(autouse=True)
def clear_caches():
    """テストごとにモジュールキャッシュをクリア"""
    sector_loaders._sector_indices_cache.clear()
    sector_loaders._stock_sector_mapping_cache.clear()
    yield
    sector_loaders._sector_indices_cache.clear()
    sector_loaders._stock_sector_mapping_cache.clear()


def _make_mapping_df():
    return pd.DataFrame(
        {
            "sector_code": ["3050", "3100"],
            "sector_name": ["化学", "医薬品"],
            "index_code": ["I301", "I302"],
            "index_name": ["化学指数", "医薬品指数"],
        }
    )


class TestGetSectorMapping:
    def test_success(self):
        mock_client = MagicMock()
        mock_client.get_sector_mapping.return_value = _make_mapping_df()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        with patch("src.data.loaders.sector_loaders.DatasetAPIClient", return_value=mock_client):
            result = get_sector_mapping("test_dataset")
        assert len(result) == 2
        assert "sector_name" in result.columns


class TestPrepareSectorData:
    def test_success(self):
        mapping_df = _make_mapping_df()
        index_data = pd.DataFrame({"Close": [100, 101]}, index=pd.date_range("2025-01-01", periods=2))
        with (
            patch("src.data.loaders.sector_loaders.get_sector_mapping", return_value=mapping_df),
            patch("src.data.loaders.sector_loaders.load_index_data", return_value=index_data),
        ):
            result = prepare_sector_data("test", "化学")
        assert "sector_index" in result
        assert "sector_mapping" in result

    def test_sector_not_found(self):
        mapping_df = _make_mapping_df()
        with patch("src.data.loaders.sector_loaders.get_sector_mapping", return_value=mapping_df):
            with pytest.raises(SectorDataLoadError, match="セクター名が見つかりません"):
                prepare_sector_data("test", "存在しない業種")

    def test_index_load_error(self):
        mapping_df = _make_mapping_df()
        with (
            patch("src.data.loaders.sector_loaders.get_sector_mapping", return_value=mapping_df),
            patch("src.data.loaders.sector_loaders.load_index_data", side_effect=ValueError("error")),
        ):
            with pytest.raises(SectorDataLoadError):
                prepare_sector_data("test", "化学")


class TestLoadAllSectorIndices:
    def test_success(self):
        mapping_df = _make_mapping_df()
        index_data = pd.DataFrame({"Close": [100]}, index=pd.date_range("2025-01-01", periods=1))
        with (
            patch("src.data.loaders.sector_loaders.get_sector_mapping", return_value=mapping_df),
            patch("src.data.loaders.sector_loaders.load_index_data", return_value=index_data),
        ):
            result = load_all_sector_indices("test")
        assert len(result) == 2
        assert "化学" in result
        assert "医薬品" in result

    def test_cache_hit(self):
        mapping_df = _make_mapping_df()
        index_data = pd.DataFrame({"Close": [100]}, index=pd.date_range("2025-01-01", periods=1))
        with (
            patch("src.data.loaders.sector_loaders.get_sector_mapping", return_value=mapping_df),
            patch("src.data.loaders.sector_loaders.load_index_data", return_value=index_data) as mock_load,
        ):
            result1 = load_all_sector_indices("test")
            result2 = load_all_sector_indices("test")
        assert result1 is result2
        # load_index_data should only be called for first invocation
        assert mock_load.call_count == 2  # 2 sectors, first call only

    def test_partial_failure(self):
        mapping_df = _make_mapping_df()
        index_data = pd.DataFrame({"Close": [100]}, index=pd.date_range("2025-01-01", periods=1))

        def side_effect(dataset, index_code, start_date=None, end_date=None):
            if index_code == "I301":
                return index_data
            raise Exception("load error")

        with (
            patch("src.data.loaders.sector_loaders.get_sector_mapping", return_value=mapping_df),
            patch("src.data.loaders.sector_loaders.load_index_data", side_effect=side_effect),
        ):
            result = load_all_sector_indices("test_partial")
        assert len(result) == 1
        assert "化学" in result

    def test_empty_mapping(self):
        empty_df = pd.DataFrame(columns=["sector_code", "sector_name", "index_code", "index_name"])
        with patch("src.data.loaders.sector_loaders.get_sector_mapping", return_value=empty_df):
            result = load_all_sector_indices("test_empty")
        assert result == {}


class TestGetStockSectorMapping:
    def test_success(self):
        mock_client = MagicMock()
        mock_client.get_stock_sector_mapping.return_value = {"1234": "化学", "5678": "医薬品"}
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        with patch("src.data.loaders.sector_loaders.DatasetAPIClient", return_value=mock_client):
            result = get_stock_sector_mapping("test_dataset")
        assert result == {"1234": "化学", "5678": "医薬品"}

    def test_cache_hit(self):
        mock_client = MagicMock()
        mock_client.get_stock_sector_mapping.return_value = {"1234": "化学"}
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        with patch("src.data.loaders.sector_loaders.DatasetAPIClient", return_value=mock_client):
            result1 = get_stock_sector_mapping("test_cache")
            result2 = get_stock_sector_mapping("test_cache")
        assert result1 is result2
        assert mock_client.get_stock_sector_mapping.call_count == 1

    def test_exception_returns_empty(self):
        mock_client = MagicMock()
        mock_client.get_stock_sector_mapping.side_effect = Exception("API error")
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        with patch("src.data.loaders.sector_loaders.DatasetAPIClient", return_value=mock_client):
            result = get_stock_sector_mapping("test_error")
        assert result == {}
