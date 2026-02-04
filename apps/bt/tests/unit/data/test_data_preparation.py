"""data_preparation.py のテスト"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.exceptions import DataPreparationError, NoValidDataError


def _ohlcv_df(n=5):
    idx = pd.date_range("2025-01-01", periods=n, freq="D")
    return pd.DataFrame(
        {"Open": 100.0, "High": 105.0, "Low": 95.0, "Close": 102.0, "Volume": 1000},
        index=idx,
    )


def _available_stocks_df():
    return pd.DataFrame({"stockCode": ["7203", "6758"], "recordCount": [500, 400]})


def _mock_api_client():
    """DatasetAPIClient のコンテキストマネージャモックを生成"""
    client = MagicMock()
    client.__enter__ = MagicMock(return_value=client)
    client.__exit__ = MagicMock(return_value=False)
    return client


class TestPrepareData:
    @patch("src.data.loaders.data_preparation.load_stock_data")
    def test_single_stock_basic(self, mock_load):
        from src.data.loaders.data_preparation import prepare_data

        mock_load.return_value = _ohlcv_df()
        result = prepare_data("testds", "7203")
        assert "daily" in result
        assert len(result["daily"]) == 5
        mock_load.assert_called_once()

    @patch("src.data.loaders.data_preparation.load_margin_data")
    @patch("src.data.loaders.data_preparation.load_stock_data")
    def test_with_margin_data(self, mock_stock, mock_margin):
        from src.data.loaders.data_preparation import prepare_data

        mock_stock.return_value = _ohlcv_df()
        mock_margin.return_value = pd.DataFrame(
            {"LongMargin": [100], "ShortMargin": [50]},
            index=pd.date_range("2025-01-01", periods=1, freq="D"),
        )
        result = prepare_data("testds", "7203", include_margin_data=True)
        assert "daily" in result
        assert "margin_daily" in result

    @patch("src.data.loaders.data_preparation.load_margin_data")
    @patch("src.data.loaders.data_preparation.load_stock_data")
    def test_margin_error_graceful(self, mock_stock, mock_margin):
        from src.data.loaders.data_preparation import prepare_data

        mock_stock.return_value = _ohlcv_df()
        mock_margin.side_effect = ValueError("no margin")
        result = prepare_data("testds", "7203", include_margin_data=True)
        assert "daily" in result
        assert "margin_daily" not in result

    @patch("src.data.loaders.data_preparation.load_statements_data")
    @patch("src.data.loaders.data_preparation.load_stock_data")
    def test_with_statements_data(self, mock_stock, mock_stmt):
        from src.data.loaders.data_preparation import prepare_data

        mock_stock.return_value = _ohlcv_df()
        mock_stmt.return_value = pd.DataFrame(
            {"EPS": [100.0]},
            index=pd.date_range("2025-01-01", periods=1, freq="D"),
        )
        result = prepare_data("testds", "7203", include_statements_data=True)
        assert "statements_daily" in result

    @patch("src.data.loaders.data_preparation.prepare_all_stocks_data")
    def test_all_delegates(self, mock_all):
        from src.data.loaders.data_preparation import prepare_data

        mock_all.return_value = {"daily": _ohlcv_df()}
        result = prepare_data("testds", "all")
        mock_all.assert_called_once()
        assert "daily" in result


class TestPrepareAllStocksData:
    @patch("src.data.loaders.data_preparation.load_multiple_stocks")
    @patch("src.data.loaders.data_preparation.get_available_stocks")
    def test_basic(self, mock_avail, mock_multi):
        from src.data.loaders.data_preparation import prepare_all_stocks_data

        mock_avail.return_value = _available_stocks_df()
        mock_multi.return_value = _ohlcv_df()
        result = prepare_all_stocks_data("testds")
        assert "daily" in result

    @patch("src.data.loaders.data_preparation.load_multiple_stocks")
    @patch("src.data.loaders.data_preparation.get_available_stocks")
    def test_value_error_raises_data_preparation_error(self, mock_avail, mock_multi):
        from src.data.loaders.data_preparation import prepare_all_stocks_data

        mock_avail.return_value = _available_stocks_df()
        mock_multi.side_effect = ValueError("no data")
        with pytest.raises(DataPreparationError):
            prepare_all_stocks_data("testds")


class TestPrepareMultiData:
    @patch("src.data.loaders.data_preparation.extract_dataset_name")
    @patch("src.data.loaders.data_preparation.DatasetAPIClient")
    def test_basic(self, mock_client_cls, mock_extract):
        from src.data.loaders.data_preparation import prepare_multi_data

        mock_extract.return_value = "testds"
        client = _mock_api_client()
        client.get_stocks_ohlcv_batch.return_value = {"7203": _ohlcv_df()}
        mock_client_cls.return_value = client

        result = prepare_multi_data("testds", ["7203"])
        assert "7203" in result
        assert "daily" in result["7203"]

    def test_empty_stock_codes(self):
        from src.data.loaders.data_preparation import prepare_multi_data

        result = prepare_multi_data("testds", [])
        assert result == {}

    @patch("src.data.loaders.data_preparation.load_stock_data")
    @patch("src.data.loaders.data_preparation.extract_dataset_name")
    @patch("src.data.loaders.data_preparation.DatasetAPIClient")
    def test_batch_fallback(self, mock_client_cls, mock_extract, mock_load):
        from src.data.loaders.data_preparation import prepare_multi_data
        from src.exceptions import BatchAPIError

        mock_extract.return_value = "testds"
        client = _mock_api_client()
        client.get_stocks_ohlcv_batch.side_effect = BatchAPIError("fail")
        mock_client_cls.return_value = client

        mock_load.return_value = _ohlcv_df()
        result = prepare_multi_data("testds", ["7203"])
        assert "7203" in result

    @patch("src.data.loaders.data_preparation.extract_dataset_name")
    @patch("src.data.loaders.data_preparation.DatasetAPIClient")
    def test_no_valid_data_raises(self, mock_client_cls, mock_extract):
        from src.data.loaders.data_preparation import prepare_multi_data

        mock_extract.return_value = "testds"
        client = _mock_api_client()
        client.get_stocks_ohlcv_batch.return_value = {}
        mock_client_cls.return_value = client

        with pytest.raises(NoValidDataError):
            prepare_multi_data("testds", ["7203"])
