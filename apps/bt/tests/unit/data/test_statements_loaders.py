"""
財務諸表ローダーユニットテスト

statements_loaders.pyのperiod_typeパラメータ機能をテスト
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from src.data.loaders.statements_loaders import load_statements_data


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


if __name__ == "__main__":
    pytest.main([__file__])
