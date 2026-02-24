"""strategies/signals/sector.py のテスト"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.domains.strategy.signals.sector import (
    get_all_sectors,
    get_sector_correlation_matrix,
    get_sector_index_code,
    get_sector_stocks,
    validate_sector_name,
)


def _make_mapping_df():
    return pd.DataFrame(
        {
            "sector_code": ["3050", "3100"],
            "sector_name": ["化学", "医薬品"],
            "index_code": ["I301", "I302"],
            "index_name": ["化学指数", "医薬品指数"],
        }
    )


class TestGetSectorIndexCode:
    def test_found(self):
        with patch("src.domains.strategy.signals.sector.get_sector_mapping", return_value=_make_mapping_df()):
            result = get_sector_index_code("test", "化学")
        assert result == "I301"

    def test_not_found(self):
        with patch("src.domains.strategy.signals.sector.get_sector_mapping", return_value=_make_mapping_df()):
            with pytest.raises(ValueError, match="セクター名が見つかりません"):
                get_sector_index_code("test", "存在しない")


class TestGetSectorStocks:
    def test_success(self):
        mock_client = MagicMock()
        mock_client.get_sector_stocks.return_value = ["1234", "5678"]
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        with patch("src.domains.strategy.signals.sector.DatasetAPIClient", return_value=mock_client):
            result = get_sector_stocks("test", "化学")
        assert result == ["1234", "5678"]

    def test_empty_raises(self):
        mock_client = MagicMock()
        mock_client.get_sector_stocks.return_value = []
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        with patch("src.domains.strategy.signals.sector.DatasetAPIClient", return_value=mock_client):
            with pytest.raises(ValueError, match="銘柄が見つかりません"):
                get_sector_stocks("test", "化学")


class TestGetAllSectors:
    def test_success(self):
        mock_client = MagicMock()
        mock_client.get_all_sectors.return_value = _make_mapping_df()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        with patch("src.domains.strategy.signals.sector.DatasetAPIClient", return_value=mock_client):
            result = get_all_sectors("test")
        assert len(result) == 2


class TestValidateSectorName:
    def test_valid(self):
        with patch("src.domains.strategy.signals.sector.get_sector_mapping", return_value=_make_mapping_df()):
            assert validate_sector_name("test", "化学") is True

    def test_invalid(self):
        with patch("src.domains.strategy.signals.sector.get_sector_mapping", return_value=_make_mapping_df()):
            assert validate_sector_name("test", "存在しない") is False

    def test_exception_returns_false(self):
        with patch("src.domains.strategy.signals.sector.get_sector_mapping", side_effect=Exception("error")):
            assert validate_sector_name("test", "化学") is False


class TestGetSectorCorrelationMatrix:
    def test_success(self):
        sectors_df = _make_mapping_df()
        index_data_1 = pd.DataFrame(
            {"Close": [100.0, 101.0, 102.0]},
            index=pd.date_range("2025-01-01", periods=3),
        )
        index_data_2 = pd.DataFrame(
            {"Close": [200.0, 202.0, 201.0]},
            index=pd.date_range("2025-01-01", periods=3),
        )

        call_count = [0]
        def mock_load(dataset, index_code, start_date=None, end_date=None):
            call_count[0] += 1
            return index_data_1 if call_count[0] == 1 else index_data_2

        with (
            patch("src.domains.strategy.signals.sector.get_all_sectors", return_value=sectors_df),
            patch("src.domains.strategy.signals.sector.load_index_data", side_effect=mock_load),
        ):
            result = get_sector_correlation_matrix("test")
        assert result.shape == (2, 2)
        assert "化学" in result.columns
        assert "医薬品" in result.columns

    def test_empty_raises(self):
        sectors_df = _make_mapping_df()
        with (
            patch("src.domains.strategy.signals.sector.get_all_sectors", return_value=sectors_df),
            patch("src.domains.strategy.signals.sector.load_index_data", side_effect=Exception("error")),
        ):
            with pytest.raises(ValueError, match="セクターデータが取得できません"):
                get_sector_correlation_matrix("test")
