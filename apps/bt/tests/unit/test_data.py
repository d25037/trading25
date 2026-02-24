"""
データローダーのユニットテスト
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from src.infrastructure.data_access.loaders import (
    load_stock_data,
    load_multiple_stocks,
    get_available_stocks,
    prepare_data,
)
from src.domains.strategy.transforms import create_relative_ohlc_data


@pytest.fixture
def sample_daily_data():
    """サンプル日足データを作成"""
    dates = pd.date_range("2023-01-01", periods=100, freq="D")
    return pd.DataFrame(
        {
            "Open": np.random.randn(100).cumsum() + 100,
            "High": np.random.randn(100).cumsum() + 105,
            "Low": np.random.randn(100).cumsum() + 95,
            "Close": np.random.randn(100).cumsum() + 100,
            "Volume": np.random.randint(1000, 10000, 100),
        },
        index=dates,
    )


class TestLoadStockData:
    """load_stock_data関数のテストクラス"""

    @patch("src.infrastructure.data_access.loaders.stock_loaders.DatasetAPIClient")
    def test_load_stock_data_success(self, mock_client_class):
        """データ読み込み成功のテスト"""
        # モックデータ作成（VectorBT形式：DatetimeIndex + OHLCV列）
        dates = pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"])
        mock_df = pd.DataFrame(
            {
                "Open": [100.0, 101.0, 102.0],
                "High": [105.0, 106.0, 107.0],
                "Low": [95.0, 96.0, 97.0],
                "Close": [100.0, 101.0, 102.0],
                "Volume": [1000, 1100, 1200],
            },
            index=dates,
        )

        # コンテキストマネージャのモック設定
        mock_client = MagicMock()
        mock_client.get_stock_ohlcv.return_value = mock_df
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client_class.return_value.__exit__.return_value = None

        # テスト実行
        result = load_stock_data("test.db", "1234")

        # 検証
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["Open", "High", "Low", "Close", "Volume"]
        assert isinstance(result.index, pd.DatetimeIndex)
        mock_client.get_stock_ohlcv.assert_called_once_with("1234", None, None, "daily")

    @patch("src.infrastructure.data_access.loaders.stock_loaders.DatasetAPIClient")
    def test_load_stock_data_empty_result(self, mock_client_class):
        """空のデータの場合のテスト"""
        # 空のDataFrameを返すモック
        mock_client = MagicMock()
        mock_client.get_stock_ohlcv.return_value = pd.DataFrame()
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client_class.return_value.__exit__.return_value = None

        with pytest.raises(ValueError, match="No data found for stock code"):
            load_stock_data("test.db", "NONEXISTENT")

    @patch("src.infrastructure.data_access.loaders.stock_loaders.DatasetAPIClient")
    def test_load_stock_data_with_date_range(self, mock_client_class):
        """日付範囲指定のテスト"""
        dates = pd.to_datetime(["2023-01-01", "2023-01-02"])
        mock_df = pd.DataFrame(
            {
                "Open": [100.0, 101.0],
                "High": [105.0, 106.0],
                "Low": [95.0, 96.0],
                "Close": [100.0, 101.0],
                "Volume": [1000, 1100],
            },
            index=dates,
        )

        mock_client = MagicMock()
        mock_client.get_stock_ohlcv.return_value = mock_df
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client_class.return_value.__exit__.return_value = None

        load_stock_data("test.db", "1234", "2023-01-01", "2023-01-31")

        # 日付パラメータが渡されることを確認
        mock_client.get_stock_ohlcv.assert_called_once_with(
            "1234", "2023-01-01", "2023-01-31", "daily"
        )


class TestMultipleStocks:
    """複数銘柄読み込みのテストクラス"""

    @patch("src.infrastructure.data_access.loaders.multi_asset_loaders.load_stock_data")
    def test_load_multiple_stocks_success(self, mock_load_stock):
        """複数銘柄読み込み成功のテスト"""
        # モックデータ作成
        dates = pd.date_range("2023-01-01", periods=5, freq="D")

        def mock_load_side_effect(dataset, stock_code, start_date=None, end_date=None, timeframe="daily"):
            return pd.DataFrame(
                {
                    "Open": [100.0] * 5,
                    "High": [105.0] * 5,
                    "Low": [95.0] * 5,
                    "Close": [100.0 + int(stock_code)] * 5,  # 銘柄ごとに異なる価格
                    "Volume": [1000] * 5,
                },
                index=dates,
            )

        mock_load_stock.side_effect = mock_load_side_effect

        result = load_multiple_stocks("test.db", ["1234", "5678"])

        # 検証
        assert isinstance(result, pd.DataFrame)
        assert len(result.columns) == 2
        assert "1234" in result.columns
        assert "5678" in result.columns
        assert len(result) == 5

    @patch("src.infrastructure.data_access.loaders.multi_asset_loaders.load_stock_data")
    @patch("src.infrastructure.data_access.loaders.multi_asset_loaders.logger")
    def test_load_multiple_stocks_with_errors(self, mock_logger, mock_load_stock):
        """一部銘柄でエラーが発生する場合のテスト"""

        def mock_load_side_effect(dataset, stock_code, start_date=None, end_date=None, timeframe="daily"):
            if stock_code == "ERROR":
                raise ValueError("Data not found")

            dates = pd.date_range("2023-01-01", periods=5, freq="D")
            return pd.DataFrame(
                {
                    "Open": [100.0] * 5,
                    "High": [105.0] * 5,
                    "Low": [95.0] * 5,
                    "Close": [100.0] * 5,
                    "Volume": [1000] * 5,
                },
                index=dates,
            )

        mock_load_stock.side_effect = mock_load_side_effect

        result = load_multiple_stocks("test.db", ["1234", "ERROR", "5678"])

        # 検証
        assert len(result.columns) == 2  # エラー銘柄は除外される
        assert "1234" in result.columns
        assert "5678" in result.columns
        mock_logger.warning.assert_called_once()


class TestRelativeStrength:
    """相対強度のテストクラス"""

    def test_create_relative_ohlc_data(self, sample_daily_data):
        """相対OHLCデータ作成のテスト"""
        # ベンチマークデータ作成（株価の半分程度の動き）
        benchmark_data = sample_daily_data.copy()
        for col in ["Open", "High", "Low", "Close"]:
            benchmark_data[col] = benchmark_data[col] * 0.5

        # 相対OHLCデータ作成
        relative_data = create_relative_ohlc_data(sample_daily_data, benchmark_data)

        # 検証
        assert isinstance(relative_data, pd.DataFrame)
        assert list(relative_data.columns) == ["Open", "High", "Low", "Close", "Volume"]

        # 相対強度の計算が正しいかチェック（概ね2.0付近になる）
        if len(relative_data) > 0:
            first_close_ratio = relative_data.iloc[0]["Close"]
            assert 1.8 <= first_close_ratio <= 2.2  # 許容範囲


class TestGetAvailableStocks:
    """利用可能銘柄取得のテストクラス"""

    @patch("src.infrastructure.data_access.loaders.stock_loaders.DatasetAPIClient")
    def test_get_available_stocks(self, mock_client_class):
        """利用可能銘柄取得のテスト"""
        mock_df = pd.DataFrame(
            {
                "stockCode": ["1234", "5678", "9012"],
                "record_count": [1500, 2000, 1200],
                "start_date": ["2022-01-01", "2021-01-01", "2022-06-01"],
                "end_date": ["2023-12-31", "2023-12-31", "2023-12-31"],
            }
        )

        mock_client = MagicMock()
        mock_client.get_available_stocks.return_value = mock_df
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client_class.return_value.__exit__.return_value = None

        result = get_available_stocks("test.db", min_records=1000)

        # 検証
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert "stockCode" in result.columns
        assert "record_count" in result.columns


class TestPrepareData:
    """データ準備関数のテストクラス"""

    @patch("src.infrastructure.data_access.loaders.data_preparation.load_stock_data")
    def test_prepare_data_basic(self, mock_load_stock):
        """基本的なデータ準備のテスト"""
        # モックデータ作成（十分な期間）
        dates = pd.date_range("2023-01-01", periods=300, freq="D")
        mock_daily_data = pd.DataFrame(
            {
                "Open": np.random.randn(300).cumsum() + 100,
                "High": np.random.randn(300).cumsum() + 105,
                "Low": np.random.randn(300).cumsum() + 95,
                "Close": np.random.randn(300).cumsum() + 100,
                "Volume": np.random.randint(1000, 10000, 300),
            },
            index=dates,
        )

        mock_load_stock.return_value = mock_daily_data

        result = prepare_data("test.db", "1234")

        # 検証
        assert isinstance(result, dict)
        assert "daily" in result

        # データ形式の確認
        assert isinstance(result["daily"], pd.DataFrame)

    # 注: use_relative_strength引数は廃止されたため、関連テストは削除しました
    # 現在のprepare_data()APIは include_margin_data, include_statements_data を使用します


if __name__ == "__main__":
    pytest.main([__file__])
