"""multi_asset_loaders.py のテスト"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.data.loaders.multi_asset_loaders import (
    load_multiple_indices,
    load_multiple_margin_data,
    load_multiple_statements_data,
    load_multiple_stocks,
)
from src.exceptions import BatchAPIError, NoValidDataError


def _mock_context_manager(client: MagicMock) -> MagicMock:
    manager = MagicMock()
    manager.__enter__ = MagicMock(return_value=client)
    manager.__exit__ = MagicMock(return_value=False)
    return manager


def _price_df(values: list[float] | None = None, column: str = "Close") -> pd.DataFrame:
    if values is None:
        values = [100.0, 101.0, 102.0]
    return pd.DataFrame(
        {column: values},
        index=pd.date_range("2025-01-01", periods=len(values), freq="D"),
    )


def _margin_df(values: list[float] | None = None, column: str = "TotalMargin") -> pd.DataFrame:
    if values is None:
        values = [1000.0, 1100.0, 1200.0]
    return pd.DataFrame(
        {column: values},
        index=pd.date_range("2025-01-01", periods=len(values), freq="D"),
    )


class TestLoadMultipleStocks:
    @patch("src.data.loaders.multi_asset_loaders.DataCache.get_instance")
    @patch("src.data.loaders.multi_asset_loaders.extract_dataset_name")
    @patch("src.data.loaders.multi_asset_loaders.get_dataset_client")
    def test_batch_api_uses_valid_price_columns_only(
        self,
        mock_get_client,
        mock_extract,
        mock_cache_get_instance,
    ):
        cache = MagicMock()
        cache.is_enabled.return_value = False
        mock_cache_get_instance.return_value = cache
        mock_extract.return_value = "testds"

        client = MagicMock()
        client.get_stocks_ohlcv_batch.return_value = {
            "7203": _price_df([100.0, 101.0, 102.0], "Close"),
            "6758": _price_df([90.0, 91.0, 92.0], "Open"),
        }
        mock_get_client.return_value = _mock_context_manager(client)

        result = load_multiple_stocks(
            dataset="dataset.db",
            stock_codes=["7203", "6758"],
            price_column="Close",
        )

        assert list(result.columns) == ["7203"]
        assert len(result.index) == 3

    @patch("src.data.loaders.multi_asset_loaders.load_stock_data")
    @patch("src.data.loaders.multi_asset_loaders.DataCache.get_instance")
    @patch("src.data.loaders.multi_asset_loaders.extract_dataset_name")
    @patch("src.data.loaders.multi_asset_loaders.get_dataset_client")
    def test_batch_failure_falls_back_to_individual_load(
        self,
        mock_get_client,
        mock_extract,
        mock_cache_get_instance,
        mock_load_stock_data,
    ):
        cache = MagicMock()
        cache.is_enabled.return_value = False
        mock_cache_get_instance.return_value = cache
        mock_extract.return_value = "testds"

        client = MagicMock()
        client.get_stocks_ohlcv_batch.side_effect = BatchAPIError("batch failed")
        mock_get_client.return_value = _mock_context_manager(client)
        mock_load_stock_data.side_effect = [
            _price_df([10.0, 11.0, 12.0], "Close"),
            ValueError("missing"),
        ]

        result = load_multiple_stocks(
            dataset="dataset.db",
            stock_codes=["7203", "6758"],
            price_column="Close",
        )

        assert list(result.columns) == ["7203"]
        assert mock_load_stock_data.call_count == 2

    @patch("src.data.loaders.multi_asset_loaders.load_stock_data")
    @patch("src.data.loaders.multi_asset_loaders.DataCache.get_instance")
    def test_raises_when_no_valid_stock_data(self, mock_cache_get_instance, mock_load_stock_data):
        cache = MagicMock()
        cache.is_enabled.return_value = True
        mock_cache_get_instance.return_value = cache
        mock_load_stock_data.side_effect = ValueError("missing")

        with pytest.raises(NoValidDataError):
            load_multiple_stocks(
                dataset="dataset.db",
                stock_codes=["7203"],
                price_column="Close",
            )


class TestLoadMultipleIndices:
    @patch("src.data.loaders.multi_asset_loaders.load_index_data")
    def test_partial_failure_is_skipped(self, mock_load_index_data):
        mock_load_index_data.side_effect = [
            _price_df([3000.0, 3010.0, 3020.0], "Close"),
            ValueError("missing"),
        ]

        result = load_multiple_indices(
            dataset="dataset.db",
            index_codes=["TOPIX", "JPX-NIKKEI"],
            price_column="Close",
        )

        assert list(result.columns) == ["TOPIX"]
        assert len(result.index) == 3

    @patch("src.data.loaders.multi_asset_loaders.load_index_data")
    def test_raises_when_no_valid_indices(self, mock_load_index_data):
        mock_load_index_data.side_effect = ValueError("missing")

        with pytest.raises(ValueError, match="No valid index data"):
            load_multiple_indices(
                dataset="dataset.db",
                index_codes=["TOPIX"],
                price_column="Close",
            )


class TestLoadMultipleMarginData:
    @patch("src.data.loaders.multi_asset_loaders.extract_dataset_name")
    @patch("src.data.loaders.multi_asset_loaders.get_dataset_client")
    @patch("src.data.loaders.multi_asset_loaders.transform_margin_df")
    def test_batch_api_margin_success(
        self,
        mock_transform_margin_df,
        mock_get_client,
        mock_extract,
    ):
        mock_extract.return_value = "testds"
        mock_transform_margin_df.side_effect = lambda df: df

        client = MagicMock()
        client.get_margin_batch.return_value = {
            "7203": _margin_df([1000.0, 1100.0, 1200.0], "TotalMargin")
        }
        mock_get_client.return_value = _mock_context_manager(client)

        result = load_multiple_margin_data(
            dataset="dataset.db",
            stock_codes=["7203"],
            margin_column="TotalMargin",
        )

        assert list(result.columns) == ["7203"]
        assert result.iloc[-1, 0] == 1200.0

    @patch("src.data.loaders.multi_asset_loaders.load_margin_data")
    @patch("src.data.loaders.multi_asset_loaders.extract_dataset_name")
    @patch("src.data.loaders.multi_asset_loaders.get_dataset_client")
    def test_batch_failure_margin_fallback(
        self,
        mock_get_client,
        mock_extract,
        mock_load_margin_data,
    ):
        mock_extract.return_value = "testds"

        client = MagicMock()
        client.get_margin_batch.side_effect = BatchAPIError("batch failed")
        mock_get_client.return_value = _mock_context_manager(client)
        mock_load_margin_data.side_effect = [
            _margin_df([11.0, 12.0, 13.0], "LongMargin"),
            ValueError("missing"),
        ]

        result = load_multiple_margin_data(
            dataset="dataset.db",
            stock_codes=["7203", "6758"],
            margin_column="LongMargin",
        )

        assert list(result.columns) == ["7203"]

    @patch("src.data.loaders.multi_asset_loaders.load_margin_data")
    @patch("src.data.loaders.multi_asset_loaders.extract_dataset_name")
    @patch("src.data.loaders.multi_asset_loaders.get_dataset_client")
    def test_raises_when_no_valid_margin_data(
        self,
        mock_get_client,
        mock_extract,
        mock_load_margin_data,
    ):
        mock_extract.return_value = "testds"
        client = MagicMock()
        client.get_margin_batch.return_value = {}
        mock_get_client.return_value = _mock_context_manager(client)
        mock_load_margin_data.side_effect = ValueError("missing")

        with pytest.raises(ValueError, match="No valid margin data"):
            load_multiple_margin_data(
                dataset="dataset.db",
                stock_codes=["7203"],
                margin_column="TotalMargin",
            )


class TestLoadMultipleStatementsData:
    @patch("src.data.loaders.multi_asset_loaders.extract_dataset_name")
    @patch("src.data.loaders.multi_asset_loaders.get_dataset_client")
    def test_include_forecast_revision_merges_batch_all_period(
        self,
        mock_get_client,
        mock_extract,
    ):
        mock_extract.return_value = "testds"
        daily_index = pd.date_range("2025-01-01", "2025-01-05")

        client = MagicMock()
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
        mock_get_client.return_value = _mock_context_manager(client)

        result = load_multiple_statements_data(
            dataset="testds",
            stock_codes=["7203"],
            daily_index=daily_index,
            statements_column="ForwardForecastEPS",
            period_type="FY",
            include_forecast_revision=True,
        )

        assert result.loc["2025-01-02", "7203"] == 100.0
        assert result.loc["2025-01-05", "7203"] == 120.0
        assert client.get_statements_batch.call_count == 2
        assert client.get_statements_batch.call_args_list[0].kwargs["period_type"] == "FY"
        assert client.get_statements_batch.call_args_list[1].kwargs["period_type"] == "all"
        assert client.get_statements_batch.call_args_list[1].kwargs["actual_only"] is False

    @patch("src.data.loaders.multi_asset_loaders.logger")
    @patch("src.data.loaders.multi_asset_loaders.extract_dataset_name")
    @patch("src.data.loaders.multi_asset_loaders.get_dataset_client")
    def test_include_forecast_revision_fallback_on_all_period_error(
        self,
        mock_get_client,
        mock_extract,
        mock_logger,
    ):
        mock_extract.return_value = "testds"
        daily_index = pd.date_range("2025-01-01", "2025-01-05")

        client = MagicMock()
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
            RuntimeError("boom"),
        ]
        mock_get_client.return_value = _mock_context_manager(client)

        result = load_multiple_statements_data(
            dataset="testds",
            stock_codes=["7203"],
            daily_index=daily_index,
            statements_column="ForwardForecastEPS",
            period_type="FY",
            include_forecast_revision=True,
        )

        assert result.loc["2025-01-05", "7203"] == 100.0
        mock_logger.warning.assert_called_once()

    @patch("src.data.loaders.multi_asset_loaders.extract_dataset_name")
    @patch("src.data.loaders.multi_asset_loaders.get_dataset_client")
    def test_revision_fetch_skipped_when_period_type_not_fy(
        self,
        mock_get_client,
        mock_extract,
    ):
        mock_extract.return_value = "testds"
        daily_index = pd.date_range("2025-01-01", "2025-01-05")

        client = MagicMock()
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
        mock_get_client.return_value = _mock_context_manager(client)

        load_multiple_statements_data(
            dataset="testds",
            stock_codes=["7203"],
            daily_index=daily_index,
            statements_column="ForwardForecastEPS",
            period_type="all",
            include_forecast_revision=True,
        )

        assert client.get_statements_batch.call_count == 1

    @patch("src.data.loaders.multi_asset_loaders.extract_dataset_name")
    @patch("src.data.loaders.multi_asset_loaders.get_dataset_client")
    def test_revision_fetch_skipped_when_non_forward_column_requested(
        self,
        mock_get_client,
        mock_extract,
    ):
        mock_extract.return_value = "testds"
        daily_index = pd.date_range("2025-01-01", "2025-01-05")

        client = MagicMock()
        client.get_statements_batch.return_value = {
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
        }
        mock_get_client.return_value = _mock_context_manager(client)

        load_multiple_statements_data(
            dataset="testds",
            stock_codes=["7203"],
            daily_index=daily_index,
            statements_column="eps",
            period_type="FY",
            include_forecast_revision=True,
        )

        assert client.get_statements_batch.call_count == 1

    @patch("src.data.loaders.multi_asset_loaders.extract_dataset_name")
    @patch("src.data.loaders.multi_asset_loaders.get_dataset_client")
    def test_lowercase_alias_column_is_resolved(
        self,
        mock_get_client,
        mock_extract,
    ):
        mock_extract.return_value = "testds"
        daily_index = pd.date_range("2025-01-01", "2025-01-05")

        client = MagicMock()
        client.get_statements_batch.return_value = {
            "7203": pd.DataFrame(
                {
                    "disclosedDate": [pd.Timestamp("2025-01-01")],
                    "typeOfCurrentPeriod": ["FY"],
                    "earningsPerShare": [80.0],
                    "profit": [1000.0],
                    "equity": [5000.0],
                }
            ).set_index("disclosedDate")
        }
        mock_get_client.return_value = _mock_context_manager(client)

        result = load_multiple_statements_data(
            dataset="testds",
            stock_codes=["7203"],
            daily_index=daily_index,
            statements_column="eps",
            period_type="FY",
        )

        assert result.loc["2025-01-05", "7203"] == 80.0

    @patch("src.data.loaders.multi_asset_loaders.load_statements_data")
    @patch("src.data.loaders.multi_asset_loaders.extract_dataset_name")
    @patch("src.data.loaders.multi_asset_loaders.get_dataset_client")
    def test_batch_missing_requested_column_falls_back_to_individual(
        self,
        mock_get_client,
        mock_extract,
        mock_load_statements_data,
    ):
        mock_extract.return_value = "testds"
        daily_index = pd.date_range("2025-01-01", "2025-01-05")

        client = MagicMock()
        client.get_statements_batch.return_value = {
            "7203": pd.DataFrame(
                {
                    "disclosedDate": [pd.Timestamp("2025-01-01")],
                    "typeOfCurrentPeriod": ["FY"],
                    "earningsPerShare": [80.0],
                    "profit": [1000.0],
                    "equity": [5000.0],
                }
            ).set_index("disclosedDate")
        }
        mock_get_client.return_value = _mock_context_manager(client)
        mock_load_statements_data.return_value = pd.DataFrame(
            {"CustomMetric": [42.0]},
            index=[pd.Timestamp("2025-01-01")],
        ).reindex(daily_index, method="ffill")

        result = load_multiple_statements_data(
            dataset="testds",
            stock_codes=["7203"],
            daily_index=daily_index,
            statements_column="CustomMetric",
            period_type="FY",
        )

        assert result.loc["2025-01-05", "7203"] == 42.0
        mock_load_statements_data.assert_called_once()

    @patch("src.data.loaders.multi_asset_loaders.logger")
    @patch("src.data.loaders.multi_asset_loaders.load_statements_data")
    @patch("src.data.loaders.multi_asset_loaders.extract_dataset_name")
    @patch("src.data.loaders.multi_asset_loaders.get_dataset_client")
    def test_batch_failure_and_invalid_fallback_rows_raise(
        self,
        mock_get_client,
        mock_extract,
        mock_load_statements_data,
        mock_logger,
    ):
        mock_extract.return_value = "testds"
        daily_index = pd.date_range("2025-01-01", "2025-01-05")
        client = MagicMock()
        client.get_statements_batch.side_effect = BatchAPIError("batch failed")
        mock_get_client.return_value = _mock_context_manager(client)
        mock_load_statements_data.side_effect = [
            pd.DataFrame({"Other": [1.0]}, index=[pd.Timestamp("2025-01-01")]),
            ValueError("invalid"),
        ]

        with pytest.raises(ValueError, match="No valid statements data"):
            load_multiple_statements_data(
                dataset="testds",
                stock_codes=["7203", "6758"],
                daily_index=daily_index,
                statements_column="CustomMetric",
                period_type="FY",
            )

        assert mock_logger.warning.call_count >= 2
