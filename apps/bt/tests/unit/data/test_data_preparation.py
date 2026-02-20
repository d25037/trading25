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

    @patch("src.data.loaders.data_preparation.load_statements_data")
    @patch("src.data.loaders.data_preparation.load_stock_data")
    def test_statements_error_graceful(self, mock_stock, mock_stmt):
        from src.data.loaders.data_preparation import prepare_data

        mock_stock.return_value = _ohlcv_df()
        mock_stmt.side_effect = ValueError("no statements")

        result = prepare_data("testds", "7203", include_statements_data=True)

        assert "daily" in result
        assert "statements_daily" not in result

    @patch("src.data.loaders.data_preparation.load_statements_data")
    @patch("src.data.loaders.data_preparation.load_stock_data")
    def test_with_statements_data_propagates_forecast_revision_flag(self, mock_stock, mock_stmt):
        from src.data.loaders.data_preparation import prepare_data

        mock_stock.return_value = _ohlcv_df()
        mock_stmt.return_value = pd.DataFrame(
            {"EPS": [100.0]},
            index=pd.date_range("2025-01-01", periods=1, freq="D"),
        )

        prepare_data(
            "testds",
            "7203",
            include_statements_data=True,
            include_forecast_revision=True,
        )

        assert mock_stmt.call_args.kwargs["include_forecast_revision"] is True

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

    @patch("src.data.loaders.data_preparation.load_multiple_statements_data")
    @patch("src.data.loaders.data_preparation.load_multiple_margin_data")
    @patch("src.data.loaders.data_preparation.load_multiple_stocks")
    @patch("src.data.loaders.data_preparation.get_available_stocks")
    def test_with_margin_and_statements_data(
        self,
        mock_avail,
        mock_multi_stocks,
        mock_multi_margin,
        mock_multi_statements,
    ):
        from src.data.loaders.data_preparation import prepare_all_stocks_data

        mock_avail.return_value = _available_stocks_df()
        combined = _ohlcv_df()
        mock_multi_stocks.return_value = combined
        mock_multi_margin.return_value = pd.DataFrame(
            {"7203": [1.0] * len(combined), "6758": [2.0] * len(combined)},
            index=combined.index,
        )
        mock_multi_statements.return_value = pd.DataFrame(
            {"7203": [100.0] * len(combined), "6758": [120.0] * len(combined)},
            index=combined.index,
        )

        result = prepare_all_stocks_data(
            "testds",
            include_margin_data=True,
            include_statements_data=True,
            include_forecast_revision=True,
        )

        assert "daily" in result
        assert "margin_daily" in result
        assert "statements_daily" in result
        assert mock_multi_statements.call_args.kwargs["include_forecast_revision"] is True

    @patch("src.data.loaders.data_preparation.load_multiple_statements_data")
    @patch("src.data.loaders.data_preparation.load_multiple_margin_data")
    @patch("src.data.loaders.data_preparation.load_multiple_stocks")
    @patch("src.data.loaders.data_preparation.get_available_stocks")
    def test_margin_and_statements_errors_are_ignored(
        self,
        mock_avail,
        mock_multi_stocks,
        mock_multi_margin,
        mock_multi_statements,
    ):
        from src.data.loaders.data_preparation import prepare_all_stocks_data

        mock_avail.return_value = _available_stocks_df()
        mock_multi_stocks.return_value = _ohlcv_df()
        mock_multi_margin.side_effect = ValueError("margin unavailable")
        mock_multi_statements.side_effect = ValueError("statements unavailable")

        result = prepare_all_stocks_data(
            "testds",
            include_margin_data=True,
            include_statements_data=True,
        )

        assert "daily" in result
        assert "margin_daily" not in result
        assert "statements_daily" not in result


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

    @patch("src.data.loaders.data_preparation.load_stock_data")
    @patch("src.data.loaders.data_preparation.extract_dataset_name")
    @patch("src.data.loaders.data_preparation.DatasetAPIClient")
    def test_batch_fallback_skips_invalid_stock_code(self, mock_client_cls, mock_extract, mock_load):
        from src.data.loaders.data_preparation import prepare_multi_data
        from src.exceptions import BatchAPIError

        mock_extract.return_value = "testds"
        client = _mock_api_client()
        client.get_stocks_ohlcv_batch.side_effect = BatchAPIError("fail")
        mock_client_cls.return_value = client
        mock_load.side_effect = [ValueError("missing"), _ohlcv_df()]

        result = prepare_multi_data("testds", ["7203", "6758"])

        assert "7203" not in result
        assert "6758" in result

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

    @patch("src.data.loaders.data_preparation.extract_dataset_name")
    @patch("src.data.loaders.data_preparation.DatasetAPIClient")
    def test_include_forecast_revision_fetches_all_period_batch(self, mock_client_cls, mock_extract):
        from src.data.loaders.data_preparation import prepare_multi_data

        mock_extract.return_value = "testds"
        client = _mock_api_client()
        client.get_stocks_ohlcv_batch.return_value = {"7203": _ohlcv_df()}
        client.get_statements_batch.side_effect = [
            {
                "7203": pd.DataFrame(
                    {
                        "disclosedDate": [pd.Timestamp("2025-01-01")],
                        "typeOfCurrentPeriod": ["FY"],
                        "earningsPerShare": [80.0],
                        "nextYearForecastEarningsPerShare": [100.0],
                        "profit": [1000.0],
                        "equity": [5000.0],
                    }
                ).set_index("disclosedDate")
            },
            {
                "7203": pd.DataFrame(
                    {
                        "disclosedDate": [pd.Timestamp("2025-01-03")],
                        "typeOfCurrentPeriod": ["1Q"],
                        "earningsPerShare": [20.0],
                        "forecastEps": [120.0],
                        "profit": [300.0],
                        "equity": [5100.0],
                    }
                ).set_index("disclosedDate")
            },
        ]
        mock_client_cls.return_value = client

        result = prepare_multi_data(
            "testds",
            ["7203"],
            include_statements_data=True,
            period_type="FY",
            include_forecast_revision=True,
        )

        assert "statements_daily" in result["7203"]
        merged = result["7203"]["statements_daily"]
        assert merged.loc["2025-01-02", "ForwardForecastEPS"] == 100.0
        assert merged.loc["2025-01-05", "ForwardForecastEPS"] == 120.0
        assert client.get_statements_batch.call_count == 2
        assert client.get_statements_batch.call_args_list[0].kwargs["period_type"] == "FY"
        assert client.get_statements_batch.call_args_list[1].kwargs["period_type"] == "all"
        assert client.get_statements_batch.call_args_list[1].kwargs["actual_only"] is False

    @patch("src.data.loaders.data_preparation.extract_dataset_name")
    @patch("src.data.loaders.data_preparation.DatasetAPIClient")
    def test_forecast_revision_skipped_when_period_type_not_fy(self, mock_client_cls, mock_extract):
        from src.data.loaders.data_preparation import prepare_multi_data

        mock_extract.return_value = "testds"
        client = _mock_api_client()
        client.get_stocks_ohlcv_batch.return_value = {"7203": _ohlcv_df()}
        client.get_statements_batch.return_value = {
            "7203": pd.DataFrame(
                {
                    "disclosedDate": [pd.Timestamp("2025-01-03")],
                    "typeOfCurrentPeriod": ["1Q"],
                    "earningsPerShare": [20.0],
                    "forecastEps": [120.0],
                    "profit": [300.0],
                    "equity": [5100.0],
                }
            ).set_index("disclosedDate")
        }
        mock_client_cls.return_value = client

        prepare_multi_data(
            "testds",
            ["7203"],
            include_statements_data=True,
            period_type="all",
            include_forecast_revision=True,
        )

        assert client.get_statements_batch.call_count == 1

    @patch("src.data.loaders.data_preparation.transform_margin_df")
    @patch("src.data.loaders.data_preparation.extract_dataset_name")
    @patch("src.data.loaders.data_preparation.DatasetAPIClient")
    def test_include_margin_data_batch_success(
        self,
        mock_client_cls,
        mock_extract,
        mock_transform_margin_df,
    ):
        from src.data.loaders.data_preparation import prepare_multi_data

        mock_extract.return_value = "testds"
        mock_transform_margin_df.side_effect = lambda df: df

        client = _mock_api_client()
        stock_df = _ohlcv_df()
        client.get_stocks_ohlcv_batch.return_value = {"7203": stock_df}
        client.get_margin_batch.return_value = {
            "7203": pd.DataFrame(
                {"LongMargin": [10.0]},
                index=[stock_df.index[0]],
            )
        }
        mock_client_cls.return_value = client

        result = prepare_multi_data("testds", ["7203"], include_margin_data=True)

        assert "margin_daily" in result["7203"]
        assert result["7203"]["margin_daily"].iloc[0]["LongMargin"] == 10.0

    @patch("src.data.loaders.data_preparation.load_margin_data")
    @patch("src.data.loaders.data_preparation.extract_dataset_name")
    @patch("src.data.loaders.data_preparation.DatasetAPIClient")
    def test_include_margin_data_batch_failure_fallback(
        self,
        mock_client_cls,
        mock_extract,
        mock_load_margin_data,
    ):
        from src.data.loaders.data_preparation import prepare_multi_data
        from src.exceptions import BatchAPIError

        mock_extract.return_value = "testds"
        client = _mock_api_client()
        stock_df = _ohlcv_df()
        client.get_stocks_ohlcv_batch.return_value = {"7203": stock_df, "6758": stock_df}
        client.get_margin_batch.side_effect = BatchAPIError("margin batch failed")
        mock_client_cls.return_value = client
        mock_load_margin_data.side_effect = [
            pd.DataFrame({"LongMargin": [20.0]}, index=[stock_df.index[0]]),
            ValueError("missing"),
        ]

        result = prepare_multi_data(
            "testds",
            ["7203", "6758"],
            include_margin_data=True,
        )

        assert "margin_daily" in result["7203"]
        assert "margin_daily" not in result["6758"]

    @patch("src.data.loaders.data_preparation.logger")
    @patch("src.data.loaders.data_preparation.extract_dataset_name")
    @patch("src.data.loaders.data_preparation.DatasetAPIClient")
    def test_revision_batch_failure_continues_with_fy_data(
        self,
        mock_client_cls,
        mock_extract,
        mock_logger,
    ):
        from src.data.loaders.data_preparation import prepare_multi_data
        from src.exceptions import BatchAPIError

        mock_extract.return_value = "testds"
        client = _mock_api_client()
        client.get_stocks_ohlcv_batch.return_value = {"7203": _ohlcv_df()}
        client.get_statements_batch.side_effect = [
            {
                "7203": pd.DataFrame(
                    {
                        "disclosedDate": [pd.Timestamp("2025-01-01")],
                        "typeOfCurrentPeriod": ["FY"],
                        "earningsPerShare": [80.0],
                        "nextYearForecastEarningsPerShare": [100.0],
                        "profit": [1000.0],
                        "equity": [5000.0],
                    }
                ).set_index("disclosedDate")
            },
            BatchAPIError("revision fetch failed"),
        ]
        mock_client_cls.return_value = client

        result = prepare_multi_data(
            "testds",
            ["7203"],
            include_statements_data=True,
            period_type="FY",
            include_forecast_revision=True,
        )

        assert "statements_daily" in result["7203"]
        mock_logger.warning.assert_called_once()

    @patch("src.data.loaders.data_preparation.load_statements_data")
    @patch("src.data.loaders.data_preparation.extract_dataset_name")
    @patch("src.data.loaders.data_preparation.DatasetAPIClient")
    def test_statements_batch_failure_fallback_to_individual(
        self,
        mock_client_cls,
        mock_extract,
        mock_load_statements_data,
    ):
        from src.data.loaders.data_preparation import prepare_multi_data
        from src.exceptions import BatchAPIError

        mock_extract.return_value = "testds"
        client = _mock_api_client()
        stock_df = _ohlcv_df()
        client.get_stocks_ohlcv_batch.return_value = {"7203": stock_df, "6758": stock_df}
        client.get_statements_batch.side_effect = BatchAPIError("batch failed")
        mock_client_cls.return_value = client
        mock_load_statements_data.side_effect = [
            pd.DataFrame({"EPS": [100.0]}, index=[stock_df.index[0]]),
            ValueError("missing"),
        ]

        result = prepare_multi_data(
            "testds",
            ["7203", "6758"],
            include_statements_data=True,
            include_forecast_revision=True,
            period_type="FY",
        )

        assert "statements_daily" in result["7203"]
        assert "statements_daily" not in result["6758"]
        assert mock_load_statements_data.call_args_list[0].kwargs["include_forecast_revision"] is True
