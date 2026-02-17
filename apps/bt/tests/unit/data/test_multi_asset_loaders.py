"""multi_asset_loaders.py の財務諸表読み込みテスト"""

from unittest.mock import MagicMock, patch

import pandas as pd

from src.data.loaders.multi_asset_loaders import load_multiple_statements_data


def _mock_context_manager(client: MagicMock) -> MagicMock:
    manager = MagicMock()
    manager.__enter__ = MagicMock(return_value=client)
    manager.__exit__ = MagicMock(return_value=False)
    return manager


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
