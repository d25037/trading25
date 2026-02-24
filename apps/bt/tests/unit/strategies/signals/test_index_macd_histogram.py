"""
INDEXヒストグラムシグナルのユニットテスト

INDEXのMACDヒストグラム（MACD線 - Signal線）の符号に基づくシグナル生成機能のテスト
"""

import pandas as pd
import pytest

from src.domains.strategy.signals.index_macd_histogram import (
    index_macd_histogram_signal,
    index_macd_histogram_multi_signal,
)


@pytest.fixture
def sample_index_data() -> pd.DataFrame:
    """テスト用INDEXデータ（TOPIXを模擬）"""
    import numpy as np

    dates = pd.date_range("2023-01-01", periods=100, freq="D")

    # 加速する上昇トレンド（MACDヒストグラムが正になる）
    # 指数関数的な成長でモメンタムを作る
    x = np.arange(100)
    uptrend_prices = pd.Series(
        100 + 50 * (np.exp(x / 50) - 1),  # 加速する上昇
        index=dates,
        dtype=float,
    )

    return pd.DataFrame(
        {
            "Open": uptrend_prices,
            "High": uptrend_prices + 2,
            "Low": uptrend_prices - 2,
            "Close": uptrend_prices,
            "Volume": 1000000,
        }
    )


@pytest.fixture
def downtrend_index_data() -> pd.DataFrame:
    """下降トレンドINDEXデータ"""
    import numpy as np

    dates = pd.date_range("2023-01-01", periods=100, freq="D")

    # 加速する下降トレンド（MACDヒストグラムが負になる）
    x = np.arange(100)
    downtrend_prices = pd.Series(
        200 - 50 * (np.exp(x / 50) - 1),  # 加速する下降
        index=dates,
        dtype=float,
    )

    return pd.DataFrame(
        {
            "Open": downtrend_prices,
            "High": downtrend_prices + 2,
            "Low": downtrend_prices - 2,
            "Close": downtrend_prices,
            "Volume": 1000000,
        }
    )


class TestIndexMACDHistogramSignal:
    """INDEXヒストグラムシグナル基本テスト"""

    def test_positive_direction_uptrend(self, sample_index_data: pd.DataFrame):
        """上昇トレンドでpositive方向シグナル生成"""
        signal = index_macd_histogram_signal(
            index_data=sample_index_data,
            fast_period=12,
            slow_period=26,
            signal_period=9,
            direction="positive",
        )

        # 戻り値の型チェック
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

        # 上昇トレンドではヒストグラム正（True）が多いはず
        true_count = signal.sum()
        total_count = len(signal)
        true_ratio = true_count / total_count

        # 少なくとも30%以上はTrueになるはず（初期期間はNaN→Falseになる）
        assert true_ratio > 0.3, f"True比率が低すぎます: {true_ratio:.2%}"

    def test_negative_direction_downtrend(self, downtrend_index_data: pd.DataFrame):
        """下降トレンドでnegative方向シグナル生成"""
        signal = index_macd_histogram_signal(
            index_data=downtrend_index_data,
            fast_period=12,
            slow_period=26,
            signal_period=9,
            direction="negative",
        )

        # 戻り値の型チェック
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

        # 下降トレンドではヒストグラム負（True）が多いはず
        true_count = signal.sum()
        total_count = len(signal)
        true_ratio = true_count / total_count

        # 少なくとも30%以上はTrueになるはず
        assert true_ratio > 0.3, f"True比率が低すぎます: {true_ratio:.2%}"

    def test_invalid_direction(self, sample_index_data: pd.DataFrame):
        """不正なdirectionパラメータでエラー"""
        with pytest.raises(
            ValueError, match="direction は 'positive' または 'negative'"
        ):
            index_macd_histogram_signal(
                index_data=sample_index_data,
                fast_period=12,
                slow_period=26,
                signal_period=9,
                direction="invalid",
            )

    def test_empty_index_data(self):
        """空のINDEXデータでエラー"""
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="index_data が空またはNoneです"):
            index_macd_histogram_signal(
                index_data=empty_df,
                fast_period=12,
                slow_period=26,
                signal_period=9,
                direction="positive",
            )

    def test_none_index_data(self):
        """None INDEXデータでエラー"""
        with pytest.raises(ValueError, match="index_data が空またはNoneです"):
            index_macd_histogram_signal(
                index_data=None,
                fast_period=12,
                slow_period=26,
                signal_period=9,
                direction="positive",
            )

    def test_missing_close_column(self, sample_index_data: pd.DataFrame):
        """Closeカラム欠如でエラー"""
        invalid_data = sample_index_data.drop(columns=["Close"])

        with pytest.raises(ValueError, match="index_data に 'Close' カラムが必要です"):
            index_macd_histogram_signal(
                index_data=invalid_data,
                fast_period=12,
                slow_period=26,
                signal_period=9,
                direction="positive",
            )

    def test_custom_macd_periods(self, sample_index_data: pd.DataFrame):
        """カスタムMACD期間パラメータ"""
        signal = index_macd_histogram_signal(
            index_data=sample_index_data,
            fast_period=8,
            slow_period=21,
            signal_period=5,
            direction="positive",
        )

        # 正常に生成されることを確認
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(sample_index_data)

    def test_signal_index_matches_input(self, sample_index_data: pd.DataFrame):
        """シグナルのインデックスが入力データと一致"""
        signal = index_macd_histogram_signal(
            index_data=sample_index_data,
            fast_period=12,
            slow_period=26,
            signal_period=9,
            direction="positive",
        )

        # インデックスが完全一致
        assert signal.index.equals(sample_index_data.index)

    def test_nan_handling(self):
        """NaN値を含むデータの処理"""
        dates = pd.date_range("2023-01-01", periods=50, freq="D")
        data = pd.DataFrame(
            {
                "Open": range(100, 150),
                "High": range(102, 152),
                "Low": range(98, 148),
                "Close": range(100, 150),
                "Volume": 1000000,
            },
            index=dates,
        )

        signal = index_macd_histogram_signal(
            index_data=data,
            fast_period=12,
            slow_period=26,
            signal_period=9,
            direction="positive",
        )

        # NaNはFalseに変換されているはず
        assert not signal.isna().any(), "シグナルにNaN値が残っています"
        assert signal.dtype == bool


class TestIndexMACDHistogramMultiSignal:
    """複数銘柄向けINDEXヒストグラムシグナルテスト"""

    def test_multi_signal_generation(self, sample_index_data: pd.DataFrame):
        """複数銘柄向けシグナル生成"""
        stock_count = 10
        multi_signal = index_macd_histogram_multi_signal(
            index_data=sample_index_data,
            stock_count=stock_count,
            fast_period=12,
            slow_period=26,
            signal_period=9,
            direction="positive",
        )

        # DataFrameの形状チェック
        assert isinstance(multi_signal, pd.DataFrame)
        assert multi_signal.shape[0] == len(sample_index_data)
        assert multi_signal.shape[1] == stock_count

        # カラム名チェック
        expected_columns = [f"stock_{i}" for i in range(stock_count)]
        assert list(multi_signal.columns) == expected_columns

        # 全銘柄で同一シグナル
        base_signal = multi_signal["stock_0"]
        for col in multi_signal.columns[1:]:
            assert multi_signal[col].equals(base_signal), f"{col} のシグナルが不一致"

    def test_multi_signal_negative_direction(self, downtrend_index_data: pd.DataFrame):
        """negative方向の複数銘柄シグナル"""
        stock_count = 5
        multi_signal = index_macd_histogram_multi_signal(
            index_data=downtrend_index_data,
            stock_count=stock_count,
            fast_period=12,
            slow_period=26,
            signal_period=9,
            direction="negative",
        )

        # 基本チェック
        assert isinstance(multi_signal, pd.DataFrame)
        assert multi_signal.shape == (len(downtrend_index_data), stock_count)

        # 下降トレンドでnegativeシグナルが多いはず
        true_ratio = multi_signal.sum().sum() / (
            multi_signal.shape[0] * multi_signal.shape[1]
        )
        assert true_ratio > 0.3, f"True比率が低すぎます: {true_ratio:.2%}"

    def test_multi_signal_custom_periods(self, sample_index_data: pd.DataFrame):
        """カスタムMACD期間の複数銘柄シグナル"""
        multi_signal = index_macd_histogram_multi_signal(
            index_data=sample_index_data,
            stock_count=3,
            fast_period=8,
            slow_period=21,
            signal_period=5,
            direction="positive",
        )

        # 正常に生成されることを確認
        assert isinstance(multi_signal, pd.DataFrame)
        assert multi_signal.shape[1] == 3


class TestEdgeCases:
    """エッジケーステスト"""

    def test_very_short_data(self):
        """非常に短いデータ期間"""
        dates = pd.date_range("2023-01-01", periods=30, freq="D")
        short_data = pd.DataFrame(
            {
                "Open": range(100, 130),
                "High": range(102, 132),
                "Low": range(98, 128),
                "Close": range(100, 130),
                "Volume": 1000000,
            },
            index=dates,
        )

        # MACD期間がデータ長に近い場合でも動作する
        signal = index_macd_histogram_signal(
            index_data=short_data,
            fast_period=12,
            slow_period=26,
            signal_period=9,
            direction="positive",
        )

        assert isinstance(signal, pd.Series)
        assert len(signal) == len(short_data)
        # 初期期間はNaN→Falseになる
        assert signal.dtype == bool

    def test_flat_price_data(self):
        """価格変動なしのフラットデータ"""
        dates = pd.date_range("2023-01-01", periods=100, freq="D")
        flat_data = pd.DataFrame(
            {
                "Open": 100.0,
                "High": 100.0,
                "Low": 100.0,
                "Close": 100.0,
                "Volume": 1000000,
            },
            index=dates,
        )

        signal = index_macd_histogram_signal(
            index_data=flat_data,
            fast_period=12,
            slow_period=26,
            signal_period=9,
            direction="positive",
        )

        # フラット価格ではヒストグラムは0に近いはず
        # direction="positive"では histogram > 0 なので、ほぼ全てFalseになる
        assert isinstance(signal, pd.Series)
        assert signal.sum() < len(signal) * 0.1  # True比率が10%未満
