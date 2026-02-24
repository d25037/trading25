"""
リトレースメントシグナルのユニットテスト

フィボナッチリトレースメントベースの下落率判定シグナルの検証
"""

import pandas as pd
import pytest

from src.domains.strategy.signals.breakout import retracement_signal


class TestRetracementSignal:
    """リトレースメントシグナルのテストクラス"""

    def test_basic_break_signal(self):
        """基本動作: direction="break"でリトレースメントレベル下抜け検出"""
        # 最高値100から38.2%下落 = 61.8の価格まで下落
        # lookback_period分の前データを追加（ローリング計算のため）
        high = pd.Series(
            [100, 100, 100, 100, 100, 100, 100],
            index=pd.date_range("2020-01-01", periods=7),
        )
        close = pd.Series(
            [100, 100, 100, 90, 80, 61, 60],
            index=pd.date_range("2020-01-01", periods=7),
        )

        result = retracement_signal(
            high=high,
            close=close,
            lookback_period=5,
            retracement_level=0.382,  # 38.2%下落
            direction="break",
            price_column="close",
        )

        # 61.8レベル（100 * (1 - 0.382)）を下回るのは最後の2日
        assert result.iloc[-2]  # 61は61.8以下
        assert result.iloc[-1]  # 60は61.8以下
        assert not result.iloc[3]  # 90は61.8より大きい

    def test_basic_recovery_signal(self):
        """基本動作: direction="recovery"でリトレースメントレベル上抜け検出"""
        # lookback_period分の前データを追加
        high = pd.Series(
            [100, 100, 100, 100, 100, 100, 100],
            index=pd.date_range("2020-01-01", periods=7),
        )
        close = pd.Series(
            [100, 100, 50, 55, 60, 65, 70], index=pd.date_range("2020-01-01", periods=7)
        )

        result = retracement_signal(
            high=high,
            close=close,
            lookback_period=5,
            retracement_level=0.382,  # 38.2%下落 = 61.8価格
            direction="recovery",
            price_column="close",
        )

        # 61.8レベル（100 * (1 - 0.382) = 61.8）を上回るのは最後の2日
        assert not result.iloc[-3]  # 60は61.8未満
        assert result.iloc[-2]  # 65は61.8より大きい
        assert result.iloc[-1]  # 70は61.8より大きい

    def test_fibonacci_level_618(self):
        """フィボナッチレベル: 61.8%押し目検出"""
        # lookback_period分の前データを追加
        high = pd.Series(
            [100, 100, 100, 100, 100, 100, 100],
            index=pd.date_range("2020-01-01", periods=7),
        )
        close = pd.Series(
            [100, 100, 80, 60, 50, 38, 30], index=pd.date_range("2020-01-01", periods=7)
        )

        result = retracement_signal(
            high=high,
            close=close,
            lookback_period=5,
            retracement_level=0.618,  # 61.8%下落 = 38.2価格
            direction="break",
            price_column="close",
        )

        # 38.2レベル（100 * (1 - 0.618) = 38.2）を下回るのは最後の2日
        assert result.iloc[-2]  # 38は38.2以下
        assert result.iloc[-1]  # 30は38.2以下
        assert not result.iloc[-3]  # 50は38.2より大きい

    def test_fibonacci_level_50(self):
        """フィボナッチレベル: 50%半値押し検出"""
        # lookback_period分の前データを追加
        high = pd.Series(
            [100, 100, 100, 100, 100, 100, 100],
            index=pd.date_range("2020-01-01", periods=7),
        )
        close = pd.Series(
            [100, 100, 80, 70, 60, 49, 45], index=pd.date_range("2020-01-01", periods=7)
        )

        result = retracement_signal(
            high=high,
            close=close,
            lookback_period=5,
            retracement_level=0.5,  # 50%下落 = 50価格
            direction="break",
            price_column="close",
        )

        # 50レベル（100 * (1 - 0.5) = 50）を下回るのは最後の2日
        assert result.iloc[-2]  # 49は50未満
        assert result.iloc[-1]  # 45は50未満
        assert not result.iloc[-3]  # 60は50より大きい

    def test_price_column_low(self):
        """price_column="low"で安値判定"""
        # lookback_period分の前データを追加
        high = pd.Series(
            [100, 100, 100, 100, 100, 100, 100],
            index=pd.date_range("2020-01-01", periods=7),
        )
        close = pd.Series(
            [100, 100, 90, 85, 80, 75, 70], index=pd.date_range("2020-01-01", periods=7)
        )
        low = pd.Series(
            [100, 100, 88, 83, 78, 60, 55], index=pd.date_range("2020-01-01", periods=7)
        )

        result = retracement_signal(
            high=high,
            close=close,
            low=low,
            lookback_period=5,
            retracement_level=0.382,  # 38.2%下落 = 61.8価格
            direction="break",
            price_column="low",
        )

        # 安値が61.8を下回るのは最後の2日
        assert result.iloc[-2]  # low=60は61.8未満
        assert result.iloc[-1]  # low=55は61.8未満
        assert not result.iloc[-3]  # low=78は61.8より大きい

    def test_rolling_high_update(self):
        """ローリング最高値の更新処理確認"""
        # 最高値が途中で更新される
        # lookback_period分の前データを追加
        high = pd.Series(
            [100, 100, 100, 120, 110, 100, 90],
            index=pd.date_range("2020-01-01", periods=7),
        )
        close = pd.Series(
            [100, 100, 100, 120, 110, 73, 70],
            index=pd.date_range("2020-01-01", periods=7),
        )

        result = retracement_signal(
            high=high,
            close=close,
            lookback_period=5,
            retracement_level=0.382,  # 38.2%下落
            direction="break",
            price_column="close",
        )

        # 最高値120に対して38.2%下落 = 74.16価格
        # 73, 70が74.16を下回る
        assert result.iloc[-2]  # 73 < 74.16
        assert result.iloc[-1]  # 70 < 74.16
        assert not result.iloc[4]  # 110 > 74.16

    def test_empty_series(self):
        """空のSeriesでエラーなし"""
        high = pd.Series([], dtype=float)
        close = pd.Series([], dtype=float)

        result = retracement_signal(
            high=high,
            close=close,
            lookback_period=20,
            retracement_level=0.382,
            direction="break",
            price_column="close",
        )

        assert len(result) == 0
        assert result.dtype == bool

    def test_nan_handling(self):
        """NaN値の処理確認（fillna(False)）"""
        high = pd.Series(
            [100, 100, None, 100, 100], index=pd.date_range("2020-01-01", periods=5)
        )
        close = pd.Series(
            [100, 90, 80, 60, 55], index=pd.date_range("2020-01-01", periods=5)
        )

        result = retracement_signal(
            high=high,
            close=close,
            lookback_period=5,
            retracement_level=0.382,
            direction="break",
            price_column="close",
        )

        # NaNはFalseとして扱われる
        assert result.isna().sum() == 0
        assert result.dtype == bool

    def test_invalid_direction(self):
        """不正なdirectionでValueError"""
        high = pd.Series([100, 100, 100], index=pd.date_range("2020-01-01", periods=3))
        close = pd.Series([100, 90, 80], index=pd.date_range("2020-01-01", periods=3))

        with pytest.raises(ValueError, match="不正なdirection"):
            retracement_signal(
                high=high,
                close=close,
                lookback_period=3,
                retracement_level=0.382,
                direction="invalid",
                price_column="close",
            )

    def test_invalid_price_column(self):
        """不正なprice_columnでValueError"""
        high = pd.Series([100, 100, 100], index=pd.date_range("2020-01-01", periods=3))
        close = pd.Series([100, 90, 80], index=pd.date_range("2020-01-01", periods=3))

        with pytest.raises(ValueError, match="不正なprice_column"):
            retracement_signal(
                high=high,
                close=close,
                lookback_period=3,
                retracement_level=0.382,
                direction="break",
                price_column="invalid",
            )

    def test_missing_low_data(self):
        """price_column="low"でlowデータ未指定時にValueError"""
        high = pd.Series([100, 100, 100], index=pd.date_range("2020-01-01", periods=3))
        close = pd.Series([100, 90, 80], index=pd.date_range("2020-01-01", periods=3))

        with pytest.raises(ValueError, match="lowデータが必須"):
            retracement_signal(
                high=high,
                close=close,
                low=None,
                lookback_period=3,
                retracement_level=0.382,
                direction="break",
                price_column="low",
            )

    def test_fibonacci_level_236(self):
        """フィボナッチレベル: 23.6%浅い押し目検出"""
        # lookback_period分の前データを追加
        high = pd.Series(
            [100, 100, 100, 100, 100, 100, 100],
            index=pd.date_range("2020-01-01", periods=7),
        )
        close = pd.Series(
            [100, 100, 90, 85, 80, 76, 70], index=pd.date_range("2020-01-01", periods=7)
        )

        result = retracement_signal(
            high=high,
            close=close,
            lookback_period=5,
            retracement_level=0.236,  # 23.6%下落 = 76.4価格
            direction="break",
            price_column="close",
        )

        # 76.4レベル（100 * (1 - 0.236) = 76.4）を下回るのは最後の2日
        assert result.iloc[-2]  # 76 < 76.4
        assert result.iloc[-1]  # 70 < 76.4
        assert not result.iloc[-3]  # 80 > 76.4

    def test_fibonacci_level_786(self):
        """フィボナッチレベル: 78.6%非常に深い押し目検出"""
        # lookback_period分の前データを追加
        high = pd.Series(
            [100, 100, 100, 100, 100, 100, 100],
            index=pd.date_range("2020-01-01", periods=7),
        )
        close = pd.Series(
            [100, 100, 80, 60, 40, 21, 15], index=pd.date_range("2020-01-01", periods=7)
        )

        result = retracement_signal(
            high=high,
            close=close,
            lookback_period=5,
            retracement_level=0.786,  # 78.6%下落 = 21.4価格
            direction="break",
            price_column="close",
        )

        # 21.4レベル（100 * (1 - 0.786) = 21.4）を下回るのは最後の2日
        assert result.iloc[-2]  # 21 < 21.4
        assert result.iloc[-1]  # 15 < 21.4
        assert not result.iloc[-3]  # 40 > 21.4
