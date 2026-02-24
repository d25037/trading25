"""
β値シグナルのユニットテスト（統一Signalsシステム対応）

テスト対象:
- calculate_beta
- rolling_beta_calculation
- beta_range_signal
- rolling_beta_multi_signal
- beta_stock_screen_signal
- dynamic_beta_signal
"""

import unittest
import pandas as pd
import numpy as np

from src.domains.strategy.signals.beta import (
    calculate_beta,
    rolling_beta_calculation,
    beta_range_signal,
    rolling_beta_multi_signal,
    beta_stock_screen_signal,
    dynamic_beta_signal,
)


class TestBetaSignals(unittest.TestCase):
    """β値シグナルのテストクラス（統一Signalsシステム）"""

    def setUp(self):
        """テストケース共通の設定"""
        # テスト用の価格データ（100日間）
        dates = pd.date_range("2024-01-01", periods=100, freq="D")

        # 市場価格データ（ベンチマーク）
        np.random.seed(42)
        market_returns = np.random.normal(0.001, 0.02, 100)
        self.market_price = pd.Series(
            100 * np.cumprod(1 + market_returns), index=dates, name="market"
        )

        # β=1.2の銘柄価格データ（市場より高い感応度）
        high_beta_returns = 1.2 * market_returns + np.random.normal(0, 0.01, 100)
        self.high_beta_price = pd.Series(
            100 * np.cumprod(1 + high_beta_returns), index=dates, name="high_beta_stock"
        )

        # β=0.8の銘柄価格データ（市場より低い感応度）
        low_beta_returns = 0.8 * market_returns + np.random.normal(0, 0.01, 100)
        self.low_beta_price = pd.Series(
            100 * np.cumprod(1 + low_beta_returns), index=dates, name="low_beta_stock"
        )

        # 複数銘柄価格データ
        self.multi_stock_prices = pd.DataFrame(
            {"high_beta": self.high_beta_price, "low_beta": self.low_beta_price}
        )

    def test_calculate_beta_high_beta_stock(self):
        """高β銘柄のβ値計算テスト"""
        stock_returns = self.high_beta_price.pct_change().dropna()
        market_returns = self.market_price.pct_change().dropna()

        beta = calculate_beta(stock_returns, market_returns)

        # β値が1.0以上であることを確認（高β銘柄）
        self.assertGreater(beta, 1.0)
        self.assertLess(beta, 2.0)  # 理論値1.2の範囲内
        self.assertFalse(np.isnan(beta))

    def test_calculate_beta_low_beta_stock(self):
        """低β銘柄のβ値計算テスト"""
        stock_returns = self.low_beta_price.pct_change().dropna()
        market_returns = self.market_price.pct_change().dropna()

        beta = calculate_beta(stock_returns, market_returns)

        # β値が1.0未満であることを確認（低β銘柄）
        self.assertLess(beta, 1.0)
        self.assertGreater(beta, 0.5)  # 理論値0.8の範囲内
        self.assertFalse(np.isnan(beta))

    def test_calculate_beta_insufficient_data(self):
        """データ不足時のβ値計算テスト"""
        # 1データポイントのみ
        stock_returns = pd.Series([0.01], index=[pd.Timestamp("2024-01-01")])
        market_returns = pd.Series([0.005], index=[pd.Timestamp("2024-01-01")])

        beta = calculate_beta(stock_returns, market_returns)

        # データ不足でNaNが返されることを確認
        self.assertTrue(np.isnan(beta))

    def test_calculate_beta_zero_market_variance(self):
        """市場分散がゼロの場合のβ値計算テスト"""
        # 市場リターンが全てゼロ（分散ゼロ）
        stock_returns = pd.Series(
            [0.01, 0.02, -0.01], index=pd.date_range("2024-01-01", periods=3)
        )
        market_returns = pd.Series(
            [0.0, 0.0, 0.0], index=pd.date_range("2024-01-01", periods=3)
        )

        beta = calculate_beta(stock_returns, market_returns)

        # 分散ゼロでNaNが返されることを確認
        self.assertTrue(np.isnan(beta))

    def test_rolling_beta_calculation(self):
        """ローリングβ値計算テスト"""
        rolling_beta = rolling_beta_calculation(
            self.high_beta_price, self.market_price, window=30
        )

        # 結果の基本検証
        self.assertIsInstance(rolling_beta, pd.Series)
        self.assertEqual(len(rolling_beta), len(self.high_beta_price))

        # 最初のwindow-1日分はNaN
        self.assertTrue(rolling_beta.iloc[:29].isna().all())

        # window日目以降はβ値が計算されている
        valid_betas = rolling_beta.dropna()
        self.assertGreater(len(valid_betas), 50)  # 十分な数のβ値

        # β値の範囲確認（高β銘柄）
        self.assertTrue((valid_betas > 0.8).all())
        self.assertTrue((valid_betas < 2.0).all())

    def test_beta_range_signal_high_beta_stock(self):
        """高β銘柄の範囲シグナルテスト"""
        # β値0.5-1.5の範囲でフィルタリング
        filter_result = beta_range_signal(
            self.high_beta_price,
            self.market_price,
            beta_min=0.5,
            beta_max=1.5,
            lookback_period=30,
        )

        # 結果の基本検証
        self.assertIsInstance(filter_result, pd.Series)
        self.assertEqual(len(filter_result), len(self.high_beta_price))

        # boolean値の確認
        self.assertTrue(filter_result.dtype == bool)

        # 一部期間でフィルター条件を満たすことを確認
        self.assertTrue(filter_result.any())

    def test_beta_range_signal_out_of_range(self):
        """範囲外β値のシグナルテスト"""
        # 非常に狭い範囲（0.9-1.1）でフィルタリング
        filter_result = beta_range_signal(
            self.high_beta_price,  # β≈1.2の銘柄
            self.market_price,
            beta_min=0.9,
            beta_max=1.1,  # 高β銘柄は通過しない範囲
            lookback_period=50,
        )

        # フィルター通過率が低いことを確認
        pass_rate = filter_result.mean()
        self.assertLess(pass_rate, 0.5)  # 50%未満の通過率

    def test_rolling_beta_multi_signal(self):
        """複数銘柄ローリングβ値シグナルテスト"""
        multi_filter_result = rolling_beta_multi_signal(
            self.multi_stock_prices,
            self.market_price,
            beta_min=0.7,
            beta_max=1.3,
            lookback_period=30,
        )

        # 結果の基本検証
        self.assertIsInstance(multi_filter_result, pd.DataFrame)
        self.assertEqual(multi_filter_result.shape, self.multi_stock_prices.shape)
        self.assertTrue(
            (multi_filter_result.columns == self.multi_stock_prices.columns).all()
        )

        # boolean値の確認
        for dtype in multi_filter_result.dtypes:
            self.assertTrue(dtype is bool or str(dtype) == "bool")

        # 各銘柄でフィルター結果が得られることを確認
        for col in multi_filter_result.columns:
            self.assertTrue(multi_filter_result[col].any())

    def test_beta_stock_screen_signal(self):
        """β値株式スクリーニングテスト"""
        # テスト用のマルチ銘柄データ作成
        multi_stock_data = {
            "stock_A": {
                "D": pd.DataFrame(
                    {
                        "Close": self.high_beta_price,
                        "Volume": pd.Series(
                            np.random.randint(100, 1000, 100),
                            index=self.high_beta_price.index,
                        ),
                    }
                )
            },
            "stock_B": {
                "D": pd.DataFrame(
                    {
                        "Close": self.low_beta_price,
                        "Volume": pd.Series(
                            np.random.randint(100, 1000, 100),
                            index=self.low_beta_price.index,
                        ),
                    }
                )
            },
        }

        market_data = pd.DataFrame(
            {
                "Close": self.market_price,
                "Volume": pd.Series(
                    np.random.randint(1000, 10000, 100), index=self.market_price.index
                ),
            }
        )

        screen_result = beta_stock_screen_signal(
            multi_stock_data,
            market_data,
            beta_min=0.5,
            beta_max=1.5,
            lookback_period=30,
        )

        # 結果の基本検証
        self.assertIsInstance(screen_result, dict)
        self.assertEqual(len(screen_result), 2)
        self.assertIn("stock_A", screen_result)
        self.assertIn("stock_B", screen_result)

        # boolean値の確認
        for stock_code, result in screen_result.items():
            self.assertIsInstance(result, bool)

    def test_dynamic_beta_signal(self):
        """動的β値シグナルテスト"""
        filter_result = dynamic_beta_signal(
            self.high_beta_price,
            self.market_price,
            target_beta=1.0,
            tolerance=0.3,  # β値0.7-1.3の範囲
            lookback_period=30,
        )

        # 結果の基本検証
        self.assertIsInstance(filter_result, pd.Series)
        self.assertEqual(len(filter_result), len(self.high_beta_price))
        self.assertTrue(filter_result.dtype == bool)

        # 一部期間でフィルター条件を満たすことを確認
        self.assertTrue(filter_result.any())

    def test_beta_calculation_with_missing_data(self):
        """欠損データありでのβ値計算テスト"""
        # 欠損データを含む価格シリーズを作成
        price_with_na = self.high_beta_price.copy()
        price_with_na.iloc[20:25] = np.nan  # 5日分の欠損データ

        rolling_beta = rolling_beta_calculation(
            price_with_na, self.market_price, window=30
        )

        # 欠損データがあっても計算が完了することを確認
        self.assertIsInstance(rolling_beta, pd.Series)

        # 欠損データ期間以外でβ値が計算されていることを確認
        valid_betas = rolling_beta.dropna()
        self.assertGreater(len(valid_betas), 40)

    def test_edge_case_identical_series(self):
        """同一シリーズでのβ値計算テスト（β=1のケース）"""
        # 同一の価格シリーズを使用
        beta = calculate_beta(
            self.market_price.pct_change().dropna(),
            self.market_price.pct_change().dropna(),
        )

        # β値が1.0に近い値であることを確認
        self.assertAlmostEqual(beta, 1.0, places=2)


class TestBetaSignalValidation(unittest.TestCase):
    """β値シグナルのバリデーションテスト（統一Signalsシステム）"""

    def test_invalid_beta_range(self):
        """無効なβ値範囲でのエラーハンドリングテスト"""
        dates = pd.date_range("2024-01-01", periods=10, freq="D")
        price_data = pd.Series(range(100, 110), index=dates)
        market_data = pd.Series(range(100, 110), index=dates)

        # beta_min > beta_maxの場合
        filter_result = beta_range_signal(
            price_data,
            market_data,
            beta_min=1.5,
            beta_max=0.5,  # 無効な範囲
            lookback_period=5,
        )

        # 全てFalseが返されることを確認
        self.assertTrue((~filter_result).all())

    def test_empty_series(self):
        """空のシリーズでの処理テスト"""
        empty_series = pd.Series([], dtype=float)

        beta = calculate_beta(empty_series, empty_series)

        # 空シリーズでNaNが返されることを確認
        self.assertTrue(np.isnan(beta))


if __name__ == "__main__":
    unittest.main()
