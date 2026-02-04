"""
指数前日比シグナルのテスト

指数の前日比に基づくシグナル生成機能のユニットテスト
"""

import pandas as pd
import pytest

from src.strategies.signals.index_daily_change import (
    index_daily_change_signal,
    index_daily_change_multi_signal,
    calculate_index_statistics,
)


class TestIndexDailyChangeSignal:
    """指数前日比シグナルの基本テスト"""

    @pytest.fixture
    def sample_index_data(self):
        """サンプル指数データ（TOPIX風）"""
        # 10日間のシンプルなデータ
        data = {
            "Close": [
                1000.0,  # Day 0 (基準)
                1010.0,  # Day 1: +1.0%
                1005.0,  # Day 2: -0.5%
                1015.0,  # Day 3: +1.0%
                1030.0,  # Day 4: +1.5%
                1025.0,  # Day 5: -0.5%
                1040.0,  # Day 6: +1.5%
                1035.0,  # Day 7: -0.5%
                1045.0,  # Day 8: +1.0%
                1050.0,  # Day 9: +0.5%
            ]
        }
        index = pd.date_range("2023-01-01", periods=10, freq="D")
        return pd.DataFrame(data, index=index)

    def test_basic_signal_below_threshold(self, sample_index_data):
        """基本機能: direction="below" のテスト"""
        signal = index_daily_change_signal(
            sample_index_data, max_daily_change_pct=1.0, direction="below"
        )

        # 型チェック
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

        # インデックスの一致チェック
        assert signal.index.equals(sample_index_data.index)

        # 初日はNaNのためFalse
        assert not signal.iloc[0]

        # 前日比が+1.0%以下の日はTrue（Day 1, 2, 3, 5, 7, 8, 9）
        # Day 1: +1.0% → True
        assert signal.iloc[1]
        # Day 2: -0.5% → True
        assert signal.iloc[2]
        # Day 3: +1.0% → True
        assert signal.iloc[3]
        # Day 4: +1.5% → False（閾値超）
        assert not signal.iloc[4]
        # Day 5: -0.5% → True
        assert signal.iloc[5]
        # Day 6: +1.5% → False（閾値超）
        assert not signal.iloc[6]
        # Day 7: -0.5% → True
        assert signal.iloc[7]
        # Day 8: +1.0% → True
        assert signal.iloc[8]
        # Day 9: +0.5% → True
        assert signal.iloc[9]

    def test_basic_signal_above_threshold(self, sample_index_data):
        """基本機能: direction="above" のテスト"""
        signal = index_daily_change_signal(
            sample_index_data, max_daily_change_pct=1.0, direction="above"
        )

        # 前日比が+1.0%を超える日はTrue（Day 4, 6）
        assert not signal.iloc[0]  # 初日はFalse
        assert not signal.iloc[1]  # +1.0%（閾値以下）
        assert not signal.iloc[2]  # -0.5%
        assert not signal.iloc[3]  # +1.0%（閾値以下）
        assert signal.iloc[4]  # +1.5%（閾値超）
        assert not signal.iloc[5]  # -0.5%
        assert signal.iloc[6]  # +1.5%（閾値超）
        assert not signal.iloc[7]  # -0.5%
        assert not signal.iloc[8]  # +1.0%（閾値以下）
        assert not signal.iloc[9]  # +0.5%

    def test_different_thresholds(self, sample_index_data):
        """異なる閾値でのテスト"""
        # 閾値0.5%の場合
        signal_05 = index_daily_change_signal(
            sample_index_data, max_daily_change_pct=0.5, direction="below"
        )

        # Day 1: +1.0% → False（閾値超）
        assert not signal_05.iloc[1]
        # Day 2: -0.5% → True（閾値以下）
        assert signal_05.iloc[2]
        # Day 9: +0.5% → True（閾値以下）
        assert signal_05.iloc[9]

        # 閾値2.0%の場合
        signal_20 = index_daily_change_signal(
            sample_index_data, max_daily_change_pct=2.0, direction="below"
        )

        # Day 4: +1.5% → True（閾値以下）
        assert signal_20.iloc[4]
        # Day 6: +1.5% → True（閾値以下）
        assert signal_20.iloc[6]

    def test_empty_dataframe_raises_error(self):
        """空のDataFrameの場合はエラー"""
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="index_data が空またはNoneです"):
            index_daily_change_signal(empty_df, max_daily_change_pct=1.0)

    def test_missing_close_column_raises_error(self):
        """Closeカラムがない場合はエラー"""
        invalid_df = pd.DataFrame({"Open": [100, 110, 105]})

        with pytest.raises(ValueError, match="'Close' カラムが必要です"):
            index_daily_change_signal(invalid_df, max_daily_change_pct=1.0)

    def test_invalid_direction_raises_error(self, sample_index_data):
        """不正なdirectionの場合はエラー"""
        with pytest.raises(
            ValueError, match="direction は 'below' または 'above' である必要があります"
        ):
            index_daily_change_signal(
                sample_index_data, max_daily_change_pct=1.0, direction="invalid"
            )

    def test_negative_threshold(self, sample_index_data):
        """負の閾値のテスト（下落相場フィルター）"""
        # 閾値-0.3%: 前日比が-0.3%以下（下落）の日にTrue
        signal = index_daily_change_signal(
            sample_index_data, max_daily_change_pct=-0.3, direction="below"
        )

        # Day 2: -0.5% → True（閾値以下）
        assert signal.iloc[2]
        # Day 5: -0.5% → True（閾値以下）
        assert signal.iloc[5]
        # Day 7: -0.5% → True（閾値以下）
        assert signal.iloc[7]

        # Day 1: +1.0% → False（閾値超）
        assert not signal.iloc[1]
        # Day 9: +0.5% → False（閾値超）
        assert not signal.iloc[9]


class TestIndexDailyChangeMultiSignal:
    """複数銘柄向けシグナルのテスト"""

    @pytest.fixture
    def sample_index_data(self):
        """サンプル指数データ"""
        data = {"Close": [1000.0, 1010.0, 1005.0, 1015.0, 1030.0]}
        index = pd.date_range("2023-01-01", periods=5, freq="D")
        return pd.DataFrame(data, index=index)

    def test_multi_signal_basic(self, sample_index_data):
        """複数銘柄シグナルの基本テスト"""
        multi_signal = index_daily_change_multi_signal(
            sample_index_data,
            stock_count=3,
            max_daily_change_pct=1.0,
            direction="below",
        )

        # 型チェック
        assert isinstance(multi_signal, pd.DataFrame)
        assert multi_signal.shape == (5, 3)

        # 全銘柄に同一シグナル適用
        assert (multi_signal["stock_0"] == multi_signal["stock_1"]).all()
        assert (multi_signal["stock_1"] == multi_signal["stock_2"]).all()


class TestCalculateIndexStatistics:
    """指数統計情報計算のテスト"""

    @pytest.fixture
    def sample_index_data(self):
        """サンプル指数データ"""
        data = {"Close": [1000.0, 1010.0, 1005.0, 1015.0, 1030.0, 1025.0]}
        index = pd.date_range("2023-01-01", periods=6, freq="D")
        return pd.DataFrame(data, index=index)

    def test_statistics_calculation(self, sample_index_data):
        """統計情報計算のテスト"""
        stats = calculate_index_statistics(sample_index_data, window=3)

        # 型チェック
        assert isinstance(stats, pd.DataFrame)
        assert "close" in stats.columns
        assert "daily_change_pct" in stats.columns
        assert "ma_daily_change" in stats.columns
        assert "std_daily_change" in stats.columns

        # daily_change_pctの計算確認
        # Day 1: (1010 - 1000) / 1000 * 100 = 1.0%
        assert abs(stats["daily_change_pct"].iloc[1] - 1.0) < 0.01

        # Day 2: (1005 - 1010) / 1010 * 100 ≈ -0.495%
        assert abs(stats["daily_change_pct"].iloc[2] - (-0.495)) < 0.01


class TestEdgeCases:
    """エッジケースのテスト"""

    def test_single_day_data(self):
        """1日分のデータ（前日比計算不可）"""
        single_day_data = pd.DataFrame(
            {"Close": [1000.0]}, index=pd.date_range("2023-01-01", periods=1)
        )

        signal = index_daily_change_signal(
            single_day_data, max_daily_change_pct=1.0, direction="below"
        )

        # 初日はNaNのためFalse
        assert not signal.iloc[0]

    def test_all_nan_close_prices(self):
        """全てNaNのClose価格"""
        nan_data = pd.DataFrame(
            {"Close": [None, None, None]}, index=pd.date_range("2023-01-01", periods=3)
        )

        signal = index_daily_change_signal(
            nan_data, max_daily_change_pct=1.0, direction="below"
        )

        # 全てFalse（NaN値はFalseに置換）
        assert not signal.any()

    def test_zero_threshold(self):
        """閾値0%のテスト"""
        data = {
            "Close": [
                1000.0,
                1010.0,  # +1.0%
                1010.0,  # 0%
                1005.0,  # -0.5%
            ]
        }
        df = pd.DataFrame(data, index=pd.date_range("2023-01-01", periods=4))

        signal_below = index_daily_change_signal(
            df, max_daily_change_pct=0.0, direction="below"
        )

        # Day 1: +1.0% → False
        assert not signal_below.iloc[1]
        # Day 2: 0% → True（0以下）
        assert signal_below.iloc[2]
        # Day 3: -0.5% → True
        assert signal_below.iloc[3]

        signal_above = index_daily_change_signal(
            df, max_daily_change_pct=0.0, direction="above"
        )

        # Day 1: +1.0% → True（0超）
        assert signal_above.iloc[1]
        # Day 2: 0% → False（0以下）
        assert not signal_above.iloc[2]
        # Day 3: -0.5% → False
        assert not signal_above.iloc[3]
