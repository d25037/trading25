"""
財務諸表ローダーユニットテスト

statements_loaders.pyのperiod_typeパラメータ機能をテスト
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from src.data.loaders.statements_loaders import (
    load_statements_data,
    transform_statements_df,
)


class TestLoadStatementsDataPeriodType:
    """load_statements_dataのperiod_typeテスト"""

    def setup_method(self):
        """テストデータ作成"""
        # 2024年全体をカバーするインデックス
        self.daily_index = pd.date_range("2024-01-01", "2024-12-31")

        # モック用の財務諸表データ（四半期別）
        self.mock_statements_df = pd.DataFrame(
            {
                "disclosedDate": [
                    pd.Timestamp("2024-01-15"),  # 3Q
                    pd.Timestamp("2024-04-28"),  # FY
                    pd.Timestamp("2024-07-30"),  # 1Q
                ],
                "typeOfCurrentPeriod": ["3Q", "FY", "1Q"],
                "earningsPerShare": [100.0, 200.0, 50.0],
                "profit": [1000000, 2000000, 500000],
                "equity": [5000000, 5500000, 5600000],
            }
        ).set_index("disclosedDate")

    @patch("src.data.loaders.statements_loaders.DatasetAPIClient")
    @patch("src.data.loaders.statements_loaders.extract_dataset_name")
    def test_period_type_fy_calls_api_with_fy(
        self, mock_extract, mock_client_class
    ):
        """FYフィルタがAPI側でFYパラメータを使用することを確認"""
        mock_extract.return_value = "test_dataset"

        # FYのみのデータを返す（API側でフィルタリング済み想定）
        fy_only_df = self.mock_statements_df[
            self.mock_statements_df["typeOfCurrentPeriod"] == "FY"
        ].copy()

        mock_client = MagicMock()
        mock_client.get_statements.return_value = fy_only_df
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client

        result = load_statements_data(
            dataset="test.db",
            stock_code="7203",
            daily_index=self.daily_index,
            period_type="FY",
        )

        # API呼び出しパラメータを確認
        mock_client.get_statements.assert_called_once_with(
            "7203", None, None, period_type="FY", actual_only=True
        )
        assert result is not None
        # FYのEPS=200が適用されていることを確認
        assert result.loc["2024-04-28", "EPS"] == 200.0

    @patch("src.data.loaders.statements_loaders.DatasetAPIClient")
    @patch("src.data.loaders.statements_loaders.extract_dataset_name")
    def test_period_type_all_calls_api_with_all(
        self, mock_extract, mock_client_class
    ):
        """allフィルタがAPI側でallパラメータを使用することを確認"""
        mock_extract.return_value = "test_dataset"

        mock_client = MagicMock()
        mock_client.get_statements.return_value = self.mock_statements_df.copy()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client

        result = load_statements_data(
            dataset="test.db",
            stock_code="7203",
            daily_index=self.daily_index,
            period_type="all",
        )

        # API呼び出しパラメータを確認
        mock_client.get_statements.assert_called_once_with(
            "7203", None, None, period_type="all", actual_only=True
        )
        # allの場合は全てのデータが含まれる
        assert result is not None
        # 3Q（2024-01-15以降）のEPS=100が適用されていることを確認
        assert result.loc["2024-01-15", "EPS"] == 100.0

    @patch("src.data.loaders.statements_loaders.DatasetAPIClient")
    @patch("src.data.loaders.statements_loaders.extract_dataset_name")
    def test_period_type_default_is_fy(self, mock_extract, mock_client_class):
        """デフォルトのperiod_typeがFYであることを確認"""
        mock_extract.return_value = "test_dataset"

        mock_client = MagicMock()
        # FYのみのデータを返す
        fy_only_df = self.mock_statements_df[
            self.mock_statements_df["typeOfCurrentPeriod"] == "FY"
        ].copy()
        mock_client.get_statements.return_value = fy_only_df
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client

        # period_typeを指定しない場合
        result = load_statements_data(
            dataset="test.db",
            stock_code="7203",
            daily_index=self.daily_index,
        )

        # デフォルトでFYが使用されることを確認
        mock_client.get_statements.assert_called_once_with(
            "7203", None, None, period_type="FY", actual_only=True
        )
        assert result is not None

    @patch("src.data.loaders.statements_loaders.DatasetAPIClient")
    @patch("src.data.loaders.statements_loaders.extract_dataset_name")
    def test_period_type_2q_calls_api_with_2q(
        self, mock_extract, mock_client_class
    ):
        """2QフィルタがAPI側で2Qパラメータを使用することを確認"""
        mock_extract.return_value = "test_dataset"

        # 2Qのみのデータを返す（API側でフィルタリング済み想定）
        statements_2q = pd.DataFrame(
            {
                "disclosedDate": [
                    pd.Timestamp("2024-02-15"),  # 2Q
                ],
                "typeOfCurrentPeriod": ["2Q"],
                "earningsPerShare": [120.0],
                "profit": [1200000],
                "equity": [5200000],
            }
        ).set_index("disclosedDate")

        mock_client = MagicMock()
        mock_client.get_statements.return_value = statements_2q
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client

        result = load_statements_data(
            dataset="test.db",
            stock_code="7203",
            daily_index=self.daily_index,
            period_type="2Q",
        )

        # API呼び出しパラメータを確認（2Qで取得）
        mock_client.get_statements.assert_called_once_with(
            "7203", None, None, period_type="2Q", actual_only=True
        )
        assert result is not None
        # 2Q（2024-02-15以降）のEPS=120が適用されていることを確認
        assert result.loc["2024-02-15", "EPS"] == 120.0

    @patch("src.data.loaders.statements_loaders.DatasetAPIClient")
    @patch("src.data.loaders.statements_loaders.extract_dataset_name")
    def test_actual_only_default_is_true(self, mock_extract, mock_client_class):
        """デフォルトでactual_only=Trueであることを確認"""
        mock_extract.return_value = "test_dataset"

        mock_client = MagicMock()
        fy_only_df = self.mock_statements_df[
            self.mock_statements_df["typeOfCurrentPeriod"] == "FY"
        ].copy()
        mock_client.get_statements.return_value = fy_only_df
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client

        load_statements_data(
            dataset="test.db",
            stock_code="7203",
            daily_index=self.daily_index,
        )

        # actual_only=Trueがデフォルトで使用されることを確認
        mock_client.get_statements.assert_called_once_with(
            "7203", None, None, period_type="FY", actual_only=True
        )

    @patch("src.data.loaders.statements_loaders.DatasetAPIClient")
    @patch("src.data.loaders.statements_loaders.extract_dataset_name")
    def test_actual_only_can_be_disabled(self, mock_extract, mock_client_class):
        """actual_only=Falseを指定できることを確認"""
        mock_extract.return_value = "test_dataset"

        mock_client = MagicMock()
        fy_only_df = self.mock_statements_df[
            self.mock_statements_df["typeOfCurrentPeriod"] == "FY"
        ].copy()
        mock_client.get_statements.return_value = fy_only_df
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client

        load_statements_data(
            dataset="test.db",
            stock_code="7203",
            daily_index=self.daily_index,
            actual_only=False,
        )

        # actual_only=Falseが渡されることを確認
        mock_client.get_statements.assert_called_once_with(
            "7203", None, None, period_type="FY", actual_only=False
        )

    @patch("src.data.loaders.statements_loaders.DatasetAPIClient")
    @patch("src.data.loaders.statements_loaders.extract_dataset_name")
    def test_include_forecast_revision_merges_quarterly_forecast(
        self, mock_extract, mock_client_class
    ):
        """period_type=FY時に四半期修正forecastがForwardForecastへ反映されること"""
        mock_extract.return_value = "test_dataset"

        fy_only_df = pd.DataFrame(
            {
                "disclosedDate": [pd.Timestamp("2024-04-28")],
                "typeOfCurrentPeriod": ["FY"],
                "earningsPerShare": [80.0],
                "nextYearForecastEarningsPerShare": [100.0],
                "profit": [2000000],
                "equity": [5500000],
            }
        ).set_index("disclosedDate")
        all_period_df = pd.DataFrame(
            {
                "disclosedDate": [pd.Timestamp("2024-07-30")],
                "typeOfCurrentPeriod": ["1Q"],
                "earningsPerShare": [20.0],
                "forecastEps": [120.0],
                "profit": [500000],
                "equity": [5600000],
            }
        ).set_index("disclosedDate")

        mock_client = MagicMock()
        mock_client.get_statements.side_effect = [fy_only_df, all_period_df]
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client

        result = load_statements_data(
            dataset="test.db",
            stock_code="7203",
            daily_index=self.daily_index,
            period_type="FY",
            include_forecast_revision=True,
        )

        assert mock_client.get_statements.call_count == 2
        assert mock_client.get_statements.call_args_list[0].kwargs["period_type"] == "FY"
        assert mock_client.get_statements.call_args_list[1].kwargs["period_type"] == "all"
        assert mock_client.get_statements.call_args_list[1].kwargs["actual_only"] is False
        assert result.loc["2024-07-29", "ForwardForecastEPS"] == 100.0
        assert result.loc["2024-07-30", "ForwardForecastEPS"] == 120.0
        # 分母はFY実績EPSを維持
        assert result.loc["2024-07-30", "ForwardBaseEPS"] == 80.0

    @patch("src.data.loaders.statements_loaders.logger")
    @patch("src.data.loaders.statements_loaders.DatasetAPIClient")
    @patch("src.data.loaders.statements_loaders.extract_dataset_name")
    def test_include_forecast_revision_fallback_on_revision_error(
        self, mock_extract, mock_client_class, mock_logger
    ):
        """四半期修正取得失敗時はFYのみで継続すること"""
        mock_extract.return_value = "test_dataset"
        fy_only_df = pd.DataFrame(
            {
                "disclosedDate": [pd.Timestamp("2024-04-28")],
                "typeOfCurrentPeriod": ["FY"],
                "earningsPerShare": [80.0],
                "nextYearForecastEarningsPerShare": [100.0],
                "profit": [2000000],
                "equity": [5500000],
            }
        ).set_index("disclosedDate")

        mock_client = MagicMock()
        mock_client.get_statements.side_effect = [fy_only_df, RuntimeError("boom")]
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client

        result = load_statements_data(
            dataset="test.db",
            stock_code="7203",
            daily_index=self.daily_index,
            period_type="FY",
            include_forecast_revision=True,
        )

        assert result.loc["2024-07-30", "ForwardForecastEPS"] == 100.0
        mock_logger.warning.assert_called_once()


class TestTransformStatementsAdjusted:
    def test_adjusted_uses_latest_quarter_shares(self):
        df = pd.DataFrame(
            {
                "disclosedDate": [
                    pd.Timestamp("2024-01-15"),  # 3Q
                    pd.Timestamp("2024-04-28"),  # FY
                    pd.Timestamp("2024-07-30"),  # 1Q (latest quarter)
                ],
                "typeOfCurrentPeriod": ["3Q", "FY", "1Q"],
                "earningsPerShare": [100.0, 200.0, 50.0],
                "bps": [1000.0, 2000.0, 900.0],
                "forecastEps": [110.0, 210.0, 60.0],
                "nextYearForecastEarningsPerShare": [120.0, 220.0, 70.0],
                "sharesOutstanding": [1500.0, 1000.0, 2000.0],
                "profit": [1000000, 2000000, 500000],
                "equity": [5000000, 5500000, 5600000],
            }
        ).set_index("disclosedDate")

        result = transform_statements_df(df)

        # Baseline should be the latest quarter shares = 2000
        assert result.loc["2024-04-28", "AdjustedEPS"] == 100.0  # 200 * 1000 / 2000
        assert result.loc["2024-01-15", "AdjustedEPS"] == 75.0  # 100 * 1500 / 2000
        assert result.loc["2024-07-30", "AdjustedEPS"] == 50.0  # 50 * 2000 / 2000
        assert result.loc["2024-04-28", "AdjustedBPS"] == 1000.0  # 2000 * 1000 / 2000
        assert (
            result.loc["2024-04-28", "AdjustedNextYearForecastEPS"] == 110.0
        )  # 220 * 1000 / 2000

    def test_adjusted_fallback_without_shares(self):
        df = pd.DataFrame(
            {
                "disclosedDate": [pd.Timestamp("2024-04-28")],
                "typeOfCurrentPeriod": ["FY"],
                "earningsPerShare": [200.0],
                "bps": [2000.0],
                "forecastEps": [210.0],
                "nextYearForecastEarningsPerShare": [220.0],
                "profit": [2000000],
                "equity": [5500000],
            }
        ).set_index("disclosedDate")

        result = transform_statements_df(df)

        assert result.loc["2024-04-28", "AdjustedEPS"] == 200.0
        assert result.loc["2024-04-28", "AdjustedBPS"] == 2000.0
        assert result.loc["2024-04-28", "AdjustedForecastEPS"] == 210.0
        assert result.loc["2024-04-28", "AdjustedNextYearForecastEPS"] == 220.0


    def test_adjusted_with_nan_shares(self):
        """SharesOutstandingがNaNの場合、rawにフォールバック"""
        df = pd.DataFrame(
            {
                "disclosedDate": [pd.Timestamp("2024-04-28")],
                "typeOfCurrentPeriod": ["FY"],
                "earningsPerShare": [200.0],
                "bps": [2000.0],
                "forecastEps": [210.0],
                "nextYearForecastEarningsPerShare": [220.0],
                "sharesOutstanding": [float("nan")],
                "profit": [2000000],
                "equity": [5500000],
            }
        ).set_index("disclosedDate")

        result = transform_statements_df(df)

        # NaN shares → baseline is None → fallback to raw
        assert result.loc["2024-04-28", "AdjustedEPS"] == 200.0
        assert result.loc["2024-04-28", "AdjustedBPS"] == 2000.0

    def test_adjusted_with_zero_shares(self):
        """SharesOutstandingが0の場合、rawにフォールバック"""
        df = pd.DataFrame(
            {
                "disclosedDate": [pd.Timestamp("2024-04-28")],
                "typeOfCurrentPeriod": ["FY"],
                "earningsPerShare": [200.0],
                "bps": [2000.0],
                "forecastEps": [210.0],
                "nextYearForecastEarningsPerShare": [220.0],
                "sharesOutstanding": [0.0],
                "profit": [2000000],
                "equity": [5500000],
            }
        ).set_index("disclosedDate")

        result = transform_statements_df(df)

        assert result.loc["2024-04-28", "AdjustedEPS"] == 200.0
        assert result.loc["2024-04-28", "AdjustedBPS"] == 2000.0

    def test_adjusted_single_period_identity(self):
        """株式数が同一の場合、Adjusted == Raw"""
        df = pd.DataFrame(
            {
                "disclosedDate": [pd.Timestamp("2024-04-28")],
                "typeOfCurrentPeriod": ["1Q"],
                "earningsPerShare": [50.0],
                "bps": [900.0],
                "forecastEps": [60.0],
                "nextYearForecastEarningsPerShare": [70.0],
                "sharesOutstanding": [1000.0],
                "profit": [500000],
                "equity": [5000000],
            }
        ).set_index("disclosedDate")

        result = transform_statements_df(df)

        assert result.loc["2024-04-28", "AdjustedEPS"] == 50.0
        assert result.loc["2024-04-28", "AdjustedBPS"] == 900.0
        assert result.loc["2024-04-28", "AdjustedForecastEPS"] == 60.0

    def test_transform_builds_forward_columns_with_fy_base_and_quarter_revision(self):
        df = pd.DataFrame(
            {
                "disclosedDate": [pd.Timestamp("2024-04-28"), pd.Timestamp("2024-07-30")],
                "typeOfCurrentPeriod": ["FY", "1Q"],
                "earningsPerShare": [200.0, 50.0],
                "forecastEps": [210.0, 60.0],
                "nextYearForecastEarningsPerShare": [220.0, 70.0],
                "sharesOutstanding": [1000.0, 1000.0],
                "profit": [2000000, 500000],
                "equity": [5500000, 5600000],
            }
        ).set_index("disclosedDate")

        result = transform_statements_df(df)

        # FY実績EPSを分母に固定
        assert result.loc["2024-04-28", "ForwardBaseEPS"] == 200.0
        assert result.loc["2024-07-30", "ForwardBaseEPS"] == 200.0
        # FYはNxFEPS、QはFEPS
        assert result.loc["2024-04-28", "ForwardForecastEPS"] == 220.0
        assert result.loc["2024-07-30", "ForwardForecastEPS"] == 60.0


class TestTransformStatementsRoa:
    def test_transform_calculates_roa_from_total_assets(self):
        df = pd.DataFrame(
            {
                "disclosedDate": [pd.Timestamp("2024-04-28"), pd.Timestamp("2024-07-30")],
                "profit": [2_000_000, 1_200_000],
                "equity": [5_000_000, 5_200_000],
                "totalAssets": [20_000_000, 24_000_000],
            }
        ).set_index("disclosedDate")

        result = transform_statements_df(df)

        assert "ROA" in result.columns
        assert result.loc["2024-04-28", "ROA"] == pytest.approx(10.0)
        assert result.loc["2024-07-30", "ROA"] == pytest.approx(5.0)

    def test_transform_roa_is_nan_when_total_assets_invalid(self):
        df = pd.DataFrame(
            {
                "disclosedDate": [pd.Timestamp("2024-04-28"), pd.Timestamp("2024-07-30")],
                "profit": [2_000_000, 1_200_000],
                "equity": [5_000_000, 5_200_000],
                "totalAssets": [0, None],
            }
        ).set_index("disclosedDate")

        result = transform_statements_df(df)

        assert pd.isna(result.loc["2024-04-28", "ROA"])
        assert pd.isna(result.loc["2024-07-30", "ROA"])


if __name__ == "__main__":
    pytest.main([__file__])
