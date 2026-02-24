"""
市場データ分析モジュールのテスト

market_analysis.pyはsignal_screeningのre-exportモジュール。
ランキング・スクリーニング実行機能はts/cliに移行済み（bt-020）。
"""

import pytest
import pandas as pd

from src.domains.analytics.market_analysis import (
    is_signal_available_in_market_db,
    calculate_signal_for_stock,
)


@pytest.fixture
def mock_stock_data_for_screening():
    """スクリーニング用株価データのモックデータ"""
    dates = pd.to_datetime(["2025-10-20", "2025-10-21", "2025-10-22", "2025-10-23"])
    return {
        "1001": (
            pd.DataFrame(
                {
                    "Open": [980.0, 1080.0, 1180.0, 1280.0],
                    "High": [1020.0, 1120.0, 1220.0, 1320.0],
                    "Low": [970.0, 1070.0, 1170.0, 1270.0],
                    "Close": [1000.0, 1100.0, 1200.0, 1300.0],
                    "Volume": [1000000, 1200000, 1100000, 1000000],
                },
                index=dates,
            ),
            "Test Company A",
        ),
        "1002": (
            pd.DataFrame(
                {
                    "Open": [490.0, 470.0, 450.0, 430.0],
                    "High": [510.0, 490.0, 470.0, 450.0],
                    "Low": [480.0, 460.0, 440.0, 420.0],
                    "Close": [500.0, 480.0, 460.0, 440.0],
                    "Volume": [500000, 550000, 520000, 500000],
                },
                index=dates,
            ),
            "Test Company B",
        ),
        "1003": (
            pd.DataFrame(
                {
                    "Open": [195.0, 200.0, 197.0, 198.0],
                    "High": [205.0, 210.0, 207.0, 208.0],
                    "Low": [190.0, 195.0, 192.0, 193.0],
                    "Close": [200.0, 205.0, 202.0, 203.0],
                    "Volume": [100000, 110000, 105000, 100000],
                },
                index=dates,
            ),
            "Test Company C",
        ),
    }


@pytest.fixture
def mock_topix_data():
    """TOPIXデータのモックデータ"""
    dates = pd.to_datetime(["2025-10-20", "2025-10-21", "2025-10-22", "2025-10-23"])
    return pd.DataFrame(
        {
            "Open": [2450.0, 2480.0, 2510.0, 2540.0],
            "High": [2480.0, 2510.0, 2540.0, 2570.0],
            "Low": [2440.0, 2470.0, 2500.0, 2530.0],
            "Close": [2470.0, 2500.0, 2530.0, 2560.0],
        },
        index=dates,
    )


# ===== スクリーニング機能テスト（re-export経由） =====


class TestIsSignalAvailableInMarketDb:
    """シグナル利用可否判定テスト（純粋ロジックテスト - APIモック不要）"""

    def test_available_signals(self):
        """利用可能シグナル判定"""
        # OHLCV系シグナル
        assert is_signal_available_in_market_db("volume") is True
        assert is_signal_available_in_market_db("trading_value") is True
        assert is_signal_available_in_market_db("period_breakout") is True
        assert is_signal_available_in_market_db("bollinger_bands") is True
        assert is_signal_available_in_market_db("crossover") is True
        assert is_signal_available_in_market_db("rsi_threshold") is True

    def test_unavailable_signals(self):
        """利用不可シグナル判定"""
        # 外部データ必要シグナル（β値は topix テーブルがあるため利用可能）
        assert is_signal_available_in_market_db("fundamental") is False
        assert is_signal_available_in_market_db("margin") is False
        assert is_signal_available_in_market_db("index_daily_change") is False

    def test_unknown_signal(self):
        """未知のシグナル判定"""
        assert is_signal_available_in_market_db("unknown_signal") is False


class TestCalculateSignalForStock:
    """シグナル計算テスト（純粋ロジックテスト - APIモック不要）"""

    def test_volume_signal(self, mock_stock_data_for_screening):
        """出来高シグナル計算"""
        stock_df = mock_stock_data_for_screening["1001"][0]

        signal, beta_value = calculate_signal_for_stock(
            stock_df,
            signal_name="volume",
            signal_params={
                "direction": "surge",
                "threshold": 1.5,
                "short_period": 2,
                "long_period": 3,
                "ma_type": "sma",
            },
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert beta_value is None  # volume signal doesn't return beta

    def test_trading_value_signal(self, mock_stock_data_for_screening):
        """売買代金シグナル計算"""
        stock_df = mock_stock_data_for_screening["1001"][0]

        signal, trading_value_avg = calculate_signal_for_stock(
            stock_df,
            signal_name="trading_value",
            signal_params={
                "direction": "above",
                "period": 2,  # テストデータは4日分なので period=2 で計算可能
                "threshold_value": 1.0,  # 1億円
            },
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # trading_value シグナルは売買代金平均を返す
        assert trading_value_avg is not None
        assert isinstance(trading_value_avg, float)
        assert trading_value_avg >= 0.0

    def test_unsupported_signal(self, mock_stock_data_for_screening):
        """サポート外シグナルはFalseを返す"""
        stock_df = mock_stock_data_for_screening["1001"][0]

        signal, beta_value = calculate_signal_for_stock(
            stock_df, signal_name="unsupported_signal", signal_params={}
        )

        assert isinstance(signal, pd.Series)
        assert (~signal).all()  # 全てFalse
        assert beta_value is None


class TestPydanticValidation:
    """Pydanticパラメータ検証テスト（Phase 2 問題2・3対応）"""

    def test_volume_signal_validation_success(self, mock_stock_data_for_screening):
        """出来高シグナル正常パラメータ検証"""
        stock_df = mock_stock_data_for_screening["1001"][0]

        # 正常パラメータ
        signal, beta_value = calculate_signal_for_stock(
            stock_df,
            signal_name="volume",
            signal_params={
                "direction": "surge",
                "threshold": 2.0,
                "short_period": 2,
                "long_period": 3,
                "ma_type": "sma",
            },
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert beta_value is None

    def test_volume_signal_validation_invalid_direction(
        self, mock_stock_data_for_screening
    ):
        """出来高シグナル不正direction検証"""
        stock_df = mock_stock_data_for_screening["1001"][0]

        # 不正なdirection（ValidationError発生 → 全False）
        signal, beta_value = calculate_signal_for_stock(
            stock_df,
            signal_name="volume",
            signal_params={
                "direction": "invalid_direction",  # 不正値
                "threshold": 2.0,
                "short_period": 2,
                "long_period": 3,
                "ma_type": "sma",
            },
        )

        # エラー時は全Falseを返す
        assert isinstance(signal, pd.Series)
        assert (~signal).all()
        assert beta_value is None

    def test_volume_signal_validation_out_of_range_threshold(
        self, mock_stock_data_for_screening
    ):
        """出来高シグナル範囲外threshold検証"""
        stock_df = mock_stock_data_for_screening["1001"][0]

        # threshold範囲外（1.0〜10.0外）
        signal, beta_value = calculate_signal_for_stock(
            stock_df,
            signal_name="volume",
            signal_params={
                "direction": "surge",
                "threshold": 20.0,  # 範囲外（10.0超過）
                "short_period": 2,
                "long_period": 3,
                "ma_type": "sma",
            },
        )

        # エラー時は全Falseを返す
        assert isinstance(signal, pd.Series)
        assert (~signal).all()
        assert beta_value is None

    def test_bollinger_bands_validation_success(self, mock_stock_data_for_screening):
        """ボリンジャーバンド正常パラメータ検証"""
        stock_df = mock_stock_data_for_screening["1001"][0]

        # 正常パラメータ
        signal, beta_value = calculate_signal_for_stock(
            stock_df,
            signal_name="bollinger_bands",
            signal_params={
                "window": 2,  # テストデータが4日分なので小さいwindowを使用
                "std_dev": 2.0,
                "position": "below_upper",
            },
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert beta_value is None

    def test_bollinger_bands_validation_invalid_position(
        self, mock_stock_data_for_screening
    ):
        """ボリンジャーバンド不正position検証"""
        stock_df = mock_stock_data_for_screening["1001"][0]

        # 不正なposition
        signal, beta_value = calculate_signal_for_stock(
            stock_df,
            signal_name="bollinger_bands",
            signal_params={
                "window": 2,
                "std_dev": 2.0,
                "position": "invalid_position",  # 不正値
            },
        )

        # エラー時は全Falseを返す
        assert isinstance(signal, pd.Series)
        assert (~signal).all()
        assert beta_value is None

    def test_crossover_validation_success(self, mock_stock_data_for_screening):
        """クロスオーバー正常パラメータ検証"""
        stock_df = mock_stock_data_for_screening["1001"][0]

        # 正常パラメータ（テストデータが少ないので期間を短く）
        signal, beta_value = calculate_signal_for_stock(
            stock_df,
            signal_name="crossover",
            signal_params={
                "type": "sma",
                "fast_period": 2,
                "slow_period": 3,
                "direction": "golden",
                "signal_period": 9,
                "lookback_days": 1,
            },
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert beta_value is None

    def test_rsi_threshold_validation_success(self, mock_stock_data_for_screening):
        """RSI閾値シグナル正常パラメータ検証"""
        stock_df = mock_stock_data_for_screening["1001"][0]

        # 正常パラメータ（テストデータが少ないので期間を短く）
        signal, beta_value = calculate_signal_for_stock(
            stock_df,
            signal_name="rsi_threshold",
            signal_params={
                "period": 2,
                "threshold": 30.0,
                "condition": "below",
            },
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert beta_value is None

    def test_rsi_threshold_validation_invalid_condition(
        self, mock_stock_data_for_screening
    ):
        """RSI閾値シグナル不正condition検証"""
        stock_df = mock_stock_data_for_screening["1001"][0]

        # 不正なcondition（ValidationError発生 → 全False）
        signal, beta_value = calculate_signal_for_stock(
            stock_df,
            signal_name="rsi_threshold",
            signal_params={
                "period": 2,
                "threshold": 30.0,
                "condition": "invalid_condition",  # 不正値
            },
        )

        # エラー時は全Falseを返す
        assert isinstance(signal, pd.Series)
        assert (~signal).all()
        assert beta_value is None

    def test_rsi_threshold_validation_out_of_range_threshold(
        self, mock_stock_data_for_screening
    ):
        """RSI閾値シグナル範囲外threshold検証"""
        stock_df = mock_stock_data_for_screening["1001"][0]

        # threshold範囲外（0〜100外）
        signal, beta_value = calculate_signal_for_stock(
            stock_df,
            signal_name="rsi_threshold",
            signal_params={
                "period": 2,
                "threshold": 150.0,  # 範囲外（100超過）
                "condition": "below",
            },
        )

        # エラー時は全Falseを返す
        assert isinstance(signal, pd.Series)
        assert (~signal).all()
        assert beta_value is None


class TestIsSignalAvailableInMarketDB:
    """シグナル利用可否判定テスト"""

    def test_beta_signal_is_available(self):
        """β値シグナルが利用可能として判定されること"""
        assert is_signal_available_in_market_db("beta") is True

    def test_volume_signal_is_available(self):
        """出来高シグナルが利用可能として判定されること"""
        assert is_signal_available_in_market_db("volume") is True

    def test_fundamental_signal_is_not_available(self):
        """財務シグナルが利用不可として判定されること"""
        assert is_signal_available_in_market_db("fundamental") is False

    def test_margin_signal_is_not_available(self):
        """信用残高シグナルが利用不可として判定されること"""
        assert is_signal_available_in_market_db("margin") is False


class TestBetaSignalCalculation:
    """β値シグナル計算テスト"""

    def test_beta_signal_with_benchmark_data(
        self, mock_stock_data_for_screening, mock_topix_data
    ):
        """β値シグナル正常計算（ベンチマークデータあり）"""
        stock_df = mock_stock_data_for_screening["1001"][0]

        # β値シグナル計算（テストデータが4日分なのでlookback_periodを小さくする必要がある）
        # しかし、lookback_periodの最小値は20なので、実際のデータ不足で計算できない
        signal, beta_value = calculate_signal_for_stock(
            stock_df,
            signal_name="beta",
            signal_params={
                "min_beta": 0.5,
                "max_beta": 1.5,
                "lookback_period": 20,  # 最小値20（validation要件）
            },
            benchmark_data=mock_topix_data,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # テストデータが少ないためβ値はNoneの可能性がある（データ不足）
        assert beta_value is None or isinstance(beta_value, float)

    def test_beta_signal_without_benchmark_data(self, mock_stock_data_for_screening):
        """β値シグナル計算エラー（ベンチマークデータなし）"""
        stock_df = mock_stock_data_for_screening["1001"][0]

        # ベンチマークデータなしで呼び出し → エラー時全False
        signal, beta_value = calculate_signal_for_stock(
            stock_df,
            signal_name="beta",
            signal_params={
                "min_beta": 0.5,
                "max_beta": 1.5,
                "lookback_period": 3,
            },
            benchmark_data=None,  # ベンチマークデータなし
        )

        # エラー時は全Falseを返す
        assert isinstance(signal, pd.Series)
        assert (~signal).all()
        assert beta_value is None

    def test_beta_signal_validation_out_of_range(
        self, mock_stock_data_for_screening, mock_topix_data
    ):
        """β値シグナル範囲外パラメータ検証"""
        stock_df = mock_stock_data_for_screening["1001"][0]

        # min_beta範囲外（-2.0〜5.0外）
        signal, beta_value = calculate_signal_for_stock(
            stock_df,
            signal_name="beta",
            signal_params={
                "min_beta": -3.0,  # 範囲外（-2.0未満）
                "max_beta": 1.5,
                "lookback_period": 20,
            },
            benchmark_data=mock_topix_data,
        )

        # エラー時は全Falseを返す
        assert isinstance(signal, pd.Series)
        assert (~signal).all()
        assert beta_value is None
