"""
財務指標シグナルユニットテスト

fundamental.pyの財務指標シグナル関数をテスト
"""

import pytest
import pandas as pd
import numpy as np

from src.strategies.signals.fundamental import (
    is_undervalued_by_per,
    is_undervalued_by_pbr,
    is_growing_eps,
    is_growing_profit,
    is_growing_sales,
    is_growing_dividend_per_share,
    is_high_roe,
    is_high_roa,
    is_high_dividend_yield,
    is_high_operating_margin,
    operating_cash_flow_threshold,
    simple_fcf_threshold,
    is_undervalued_growth_by_peg,
    is_expected_growth_eps,
    cfo_yield_threshold,
    is_growing_cfo_yield,
    simple_fcf_yield_threshold,
    is_growing_simple_fcf_yield,
    market_cap_threshold,
)


class TestIsUndervaluedByPer:
    """is_undervalued_by_per()のテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.close = pd.Series(np.linspace(100, 200, 100), index=self.dates)
        self.eps = pd.Series(np.ones(100) * 10, index=self.dates)  # EPS=10

    def test_per_basic(self):
        """PERシグナル基本テスト"""
        # PER = 100/10 = 10 → 15以下なので割安
        signal = is_undervalued_by_per(
            self.close.iloc[0:50], self.eps.iloc[0:50], threshold=15.0
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == 50
        # 最初の方は割安判定
        assert signal.iloc[0:30].sum() > 0

    def test_per_threshold_effect(self):
        """PER閾値の効果テスト"""
        signal_low = is_undervalued_by_per(self.close, self.eps, threshold=10.0)
        signal_high = is_undervalued_by_per(self.close, self.eps, threshold=30.0)

        assert isinstance(signal_low, pd.Series)
        assert isinstance(signal_high, pd.Series)
        # 高い閾値の方がTrue数が多い
        assert signal_high.sum() >= signal_low.sum()

    def test_per_zero_eps(self):
        """EPS=0の場合の処理"""
        eps_with_zero = self.eps.copy()
        eps_with_zero.iloc[0:10] = 0

        signal = is_undervalued_by_per(self.close, eps_with_zero, threshold=15.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # EPS=0の部分はFalse
        assert not signal.iloc[0:10].any()

    def test_per_negative_eps(self):
        """負のEPSの場合の処理"""
        eps_negative = self.eps.copy()
        eps_negative.iloc[0:10] = -5

        signal = is_undervalued_by_per(self.close, eps_negative, threshold=15.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 負のEPSの部分はFalse
        assert not signal.iloc[0:10].any()

    def test_per_nan_handling(self):
        """NaN処理テスト"""
        eps_with_nan = self.eps.copy()
        eps_with_nan.iloc[0:10] = np.nan

        signal = is_undervalued_by_per(self.close, eps_with_nan, threshold=15.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # NaNの部分はFalse
        assert not signal.iloc[0:10].any()


class TestIsUndervaluedByPbr:
    """is_undervalued_by_pbr()のテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.close = pd.Series(np.linspace(50, 150, 100), index=self.dates)
        self.bps = pd.Series(np.ones(100) * 100, index=self.dates)  # BPS=100

    def test_pbr_basic(self):
        """PBRシグナル基本テスト"""
        # PBR = 50/100 = 0.5 → 1.0以下なので割安
        signal = is_undervalued_by_pbr(
            self.close.iloc[0:50], self.bps.iloc[0:50], threshold=1.0
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == 50
        # 最初の方は割安判定
        assert signal.iloc[0:30].sum() > 0

    def test_pbr_threshold_effect(self):
        """PBR閾値の効果テスト"""
        signal_low = is_undervalued_by_pbr(self.close, self.bps, threshold=0.8)
        signal_high = is_undervalued_by_pbr(self.close, self.bps, threshold=2.0)

        assert isinstance(signal_low, pd.Series)
        assert isinstance(signal_high, pd.Series)
        # 高い閾値の方がTrue数が多い
        assert signal_high.sum() >= signal_low.sum()


class TestIsGrowingEps:
    """is_growing_eps()のテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        # EPS成長パターン
        self.eps = pd.Series(np.linspace(10, 20, 100), index=self.dates)

    def test_eps_growth_basic(self):
        """EPS成長シグナル基本テスト"""
        signal = is_growing_eps(self.eps, growth_threshold=0.05, periods=10)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.eps)
        # 成長しているのでTrueが発生（閾値を緩和）
        assert signal.iloc[20:].sum() > 0

    def test_eps_growth_threshold_effect(self):
        """成長率閾値の効果テスト"""
        signal_low = is_growing_eps(self.eps, growth_threshold=0.05, periods=10)
        signal_high = is_growing_eps(self.eps, growth_threshold=0.5, periods=10)

        assert isinstance(signal_low, pd.Series)
        assert isinstance(signal_high, pd.Series)
        # 低い閾値の方がTrue数が多い
        assert signal_low.sum() >= signal_high.sum()

    def test_eps_negative_growth(self):
        """負の成長（減益）の場合"""
        eps_declining = pd.Series(np.linspace(20, 10, 100), index=self.dates)

        signal = is_growing_eps(eps_declining, growth_threshold=0.1, periods=10)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 減益なのでTrue数は少ない
        assert signal.sum() == 0


class TestIsHighRoe:
    """is_high_roe()のテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        # ROE=15%
        self.roe = pd.Series(np.ones(100) * 15, index=self.dates)

    def test_roe_basic(self):
        """ROEシグナル基本テスト"""
        signal = is_high_roe(self.roe, threshold=10.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.roe)
        # ROE=15% > 10%なので全てTrue
        assert signal.sum() == len(self.roe)

    def test_roe_threshold_effect(self):
        """ROE閾値の効果テスト"""
        signal_low = is_high_roe(self.roe, threshold=5.0)
        signal_high = is_high_roe(self.roe, threshold=20.0)

        assert isinstance(signal_low, pd.Series)
        assert isinstance(signal_high, pd.Series)
        # 低い閾値の方がTrue数が多い
        assert signal_low.sum() >= signal_high.sum()

    def test_roe_negative(self):
        """負のROEの場合"""
        roe_negative = pd.Series(np.ones(100) * -5, index=self.dates)

        signal = is_high_roe(roe_negative, threshold=10.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 負のROEはFalse
        assert signal.sum() == 0




class TestIsHighRoa:
    """is_high_roa()のテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        # ROA=8%
        self.roa = pd.Series(np.ones(100) * 8, index=self.dates)

    def test_roa_basic(self):
        """ROAシグナル基本テスト"""
        signal = is_high_roa(self.roa, threshold=5.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.roa)
        # ROA=8% > 5%なので全てTrue
        assert signal.sum() == len(self.roa)

    def test_roa_threshold_effect(self):
        """ROA閾値の効果テスト"""
        signal_low = is_high_roa(self.roa, threshold=3.0)
        signal_high = is_high_roa(self.roa, threshold=10.0)

        assert isinstance(signal_low, pd.Series)
        assert isinstance(signal_high, pd.Series)
        # 低い閾値の方がTrue数が多い
        assert signal_low.sum() >= signal_high.sum()

    def test_roa_negative(self):
        """負のROAの場合"""
        roa_negative = pd.Series(np.ones(100) * -1, index=self.dates)

        signal = is_high_roa(roa_negative, threshold=5.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 負のROAはFalse
        assert signal.sum() == 0

class TestIsUndervaluedGrowthByPeg:
    """is_undervalued_growth_by_peg()のテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.close = pd.Series(np.ones(100) * 100, index=self.dates)
        self.eps = pd.Series(np.ones(100) * 10, index=self.dates)
        # 20%成長予想
        self.next_year_forecast_eps = pd.Series(np.ones(100) * 12, index=self.dates)

    def test_peg_ratio_basic(self):
        """PEG Ratioシグナル基本テスト"""
        # PER = 100/10 = 10, 成長率 = (12-10)/10 = 20%, PEG = 10/0.2 = 50
        signal = is_undervalued_growth_by_peg(
            self.close, self.eps, self.next_year_forecast_eps, threshold=100.0
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.close)
        # PEG=50 < 100なので全てTrue
        assert signal.sum() > 0

    def test_peg_ratio_threshold_effect(self):
        """PEG Ratio閾値の効果テスト"""
        signal_low = is_undervalued_growth_by_peg(
            self.close, self.eps, self.next_year_forecast_eps, threshold=1.0
        )
        signal_high = is_undervalued_growth_by_peg(
            self.close, self.eps, self.next_year_forecast_eps, threshold=100.0
        )

        assert isinstance(signal_low, pd.Series)
        assert isinstance(signal_high, pd.Series)
        # 高い閾値の方がTrue数が多い
        assert signal_high.sum() >= signal_low.sum()


class TestIsExpectedGrowthEps:
    """is_expected_growth_eps()のテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.eps = pd.Series(np.ones(100) * 10, index=self.dates)
        # 15%成長予想
        self.next_year_forecast_eps = pd.Series(np.ones(100) * 11.5, index=self.dates)

    def test_forward_growth_basic(self):
        """Forward EPS成長シグナル基本テスト"""
        # 成長率 = (11.5-10)/10 = 15%
        signal = is_expected_growth_eps(
            self.eps, self.next_year_forecast_eps, growth_threshold=0.1
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.eps)
        # 15% > 10%なので全てTrue
        assert signal.sum() == len(self.eps)

    def test_forward_growth_threshold_effect(self):
        """Forward成長率閾値の効果テスト"""
        signal_low = is_expected_growth_eps(
            self.eps, self.next_year_forecast_eps, growth_threshold=0.05
        )
        signal_high = is_expected_growth_eps(
            self.eps, self.next_year_forecast_eps, growth_threshold=0.5
        )

        assert isinstance(signal_low, pd.Series)
        assert isinstance(signal_high, pd.Series)
        # 低い閾値の方がTrue数が多い
        assert signal_low.sum() >= signal_high.sum()


# =====================================================================
# 新規シグナルテスト（2026-01追加）
# =====================================================================


class TestIsGrowingProfit:
    """is_growing_profit()のテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        # Profit成長パターン
        self.profit = pd.Series(np.linspace(1000, 2000, 100), index=self.dates)

    def test_profit_growth_basic(self):
        """Profit成長シグナル基本テスト"""
        signal = is_growing_profit(self.profit, growth_threshold=0.05, periods=10)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.profit)
        # 成長しているのでTrueが発生
        assert signal.iloc[20:].sum() > 0

    def test_profit_negative_growth(self):
        """負の成長（減益）の場合"""
        profit_declining = pd.Series(np.linspace(2000, 1000, 100), index=self.dates)

        signal = is_growing_profit(profit_declining, growth_threshold=0.1, periods=10)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 減益なのでTrue数は0
        assert signal.sum() == 0


class TestIsGrowingSales:
    """is_growing_sales()のテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        # Sales成長パターン
        self.sales = pd.Series(np.linspace(10000, 20000, 100), index=self.dates)

    def test_sales_growth_basic(self):
        """Sales成長シグナル基本テスト"""
        signal = is_growing_sales(self.sales, growth_threshold=0.05, periods=10)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.sales)
        # 成長しているのでTrueが発生
        assert signal.iloc[20:].sum() > 0

    def test_sales_threshold_effect(self):
        """成長率閾値の効果テスト"""
        signal_low = is_growing_sales(self.sales, growth_threshold=0.05, periods=10)
        signal_high = is_growing_sales(self.sales, growth_threshold=0.5, periods=10)

        assert isinstance(signal_low, pd.Series)
        assert isinstance(signal_high, pd.Series)
        # 低い閾値の方がTrue数が多い
        assert signal_low.sum() >= signal_high.sum()


class TestIsHighOperatingMargin:
    """is_high_operating_margin()のテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        # 営業利益率=15%
        self.operating_margin = pd.Series(np.ones(100) * 15, index=self.dates)

    def test_operating_margin_basic(self):
        """営業利益率シグナル基本テスト"""
        signal = is_high_operating_margin(self.operating_margin, threshold=10.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.operating_margin)
        # 15% > 10%なので全てTrue
        assert signal.sum() == len(self.operating_margin)

    def test_operating_margin_threshold_effect(self):
        """営業利益率閾値の効果テスト"""
        signal_low = is_high_operating_margin(self.operating_margin, threshold=5.0)
        signal_high = is_high_operating_margin(self.operating_margin, threshold=20.0)

        assert isinstance(signal_low, pd.Series)
        assert isinstance(signal_high, pd.Series)
        # 低い閾値の方がTrue数が多い
        assert signal_low.sum() >= signal_high.sum()

    def test_operating_margin_negative(self):
        """負の営業利益率の場合"""
        margin_negative = pd.Series(np.ones(100) * -5, index=self.dates)

        signal = is_high_operating_margin(margin_negative, threshold=10.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 負の営業利益率はFalse
        assert signal.sum() == 0


class TestOperatingCashFlowThreshold:
    """operating_cash_flow_threshold()のテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        # 正の営業CF
        self.operating_cash_flow = pd.Series(np.ones(100) * 1000, index=self.dates)

    def test_cash_flow_basic(self):
        """営業CFシグナル基本テスト"""
        signal = operating_cash_flow_threshold(self.operating_cash_flow, threshold=0.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.operating_cash_flow)
        # 正のCFなので全てTrue
        assert signal.sum() == len(self.operating_cash_flow)

    def test_cash_flow_negative(self):
        """負の営業CFの場合"""
        cf_negative = pd.Series(np.ones(100) * -500, index=self.dates)

        signal = operating_cash_flow_threshold(cf_negative, threshold=0.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 負のCFはFalse
        assert signal.sum() == 0

    def test_cash_flow_threshold(self):
        """営業CF閾値テスト"""
        signal = operating_cash_flow_threshold(self.operating_cash_flow, threshold=500.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 1000 > 500なので全てTrue
        assert signal.sum() == len(self.operating_cash_flow)


class TestSimpleFCFThreshold:
    """simple_fcf_threshold()のテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        # 正の営業CF
        self.operating_cash_flow = pd.Series(np.ones(100) * 1000, index=self.dates)
        # 負の投資CF（設備投資等）
        self.investing_cash_flow = pd.Series(np.ones(100) * -500, index=self.dates)

    def test_simple_fcf_basic(self):
        """簡易FCFシグナル基本テスト"""
        # FCF = 1000 + (-500) = 500 > 0
        signal = simple_fcf_threshold(
            self.operating_cash_flow, self.investing_cash_flow, threshold=0.0
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.operating_cash_flow)
        # 正のFCFなので全てTrue
        assert signal.sum() == len(self.operating_cash_flow)

    def test_simple_fcf_threshold_effect(self):
        """簡易FCF閾値の効果テスト"""
        signal_low = simple_fcf_threshold(
            self.operating_cash_flow, self.investing_cash_flow, threshold=0.0
        )
        signal_high = simple_fcf_threshold(
            self.operating_cash_flow, self.investing_cash_flow, threshold=600.0
        )

        assert isinstance(signal_low, pd.Series)
        assert isinstance(signal_high, pd.Series)
        # FCF=500 < 600なので高い閾値では全てFalse
        assert signal_low.sum() > signal_high.sum()
        assert signal_high.sum() == 0

    def test_simple_fcf_negative(self):
        """負のFCFの場合"""
        # 投資CFが大きい場合
        investing_cf_large = pd.Series(np.ones(100) * -1500, index=self.dates)
        # FCF = 1000 + (-1500) = -500 < 0
        signal = simple_fcf_threshold(
            self.operating_cash_flow, investing_cf_large, threshold=0.0
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 負のFCFはFalse
        assert signal.sum() == 0

    def test_simple_fcf_condition_below(self):
        """below条件のテスト"""
        # FCF = 1000 + (-500) = 500
        signal = simple_fcf_threshold(
            self.operating_cash_flow,
            self.investing_cash_flow,
            threshold=600.0,
            condition="below",
        )

        assert isinstance(signal, pd.Series)
        # FCF=500 < 600 → True
        assert signal.all()

    def test_simple_fcf_nan_handling(self):
        """NaN処理テスト"""
        operating_cf_with_nan = self.operating_cash_flow.copy()
        operating_cf_with_nan.iloc[0:10] = np.nan

        signal = simple_fcf_threshold(
            operating_cf_with_nan, self.investing_cash_flow, threshold=0.0
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # NaNの部分はFalse
        assert not signal.iloc[0:10].any()

    def test_simple_fcf_mixed_values(self):
        """正負混在のFCFテスト"""
        # 正負が交互になるパターン
        operating_cf = pd.Series(
            [1000, 500, 800, 200, 1500, 100, 300, 400, 2000, 50] * 10,
            index=self.dates,
        )
        investing_cf = pd.Series(
            [-500, -600, -200, -300, -1000, -200, -500, -100, -800, -100] * 10,
            index=self.dates,
        )
        # FCF = [500, -100, 600, -100, 500, -100, -200, 300, 1200, -50]
        signal = simple_fcf_threshold(operating_cf, investing_cf, threshold=0.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 正のFCFのみTrue
        expected_fcf = operating_cf + investing_cf
        expected_true_count = (expected_fcf >= 0).sum()
        assert signal.sum() == expected_true_count


class TestIsHighDividendYield:
    """is_high_dividend_yield()のテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.close = pd.Series(np.ones(100) * 1000, index=self.dates)
        # 配当30円 → 利回り3%
        self.dividend_fy = pd.Series(np.ones(100) * 30, index=self.dates)

    def test_dividend_yield_basic(self):
        """配当利回りシグナル基本テスト"""
        # 利回り = 30/1000 * 100 = 3%
        signal = is_high_dividend_yield(self.dividend_fy, self.close, threshold=2.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.close)
        # 3% > 2%なので全てTrue
        assert signal.sum() == len(self.close)

    def test_dividend_yield_threshold_effect(self):
        """配当利回り閾値の効果テスト"""
        signal_low = is_high_dividend_yield(self.dividend_fy, self.close, threshold=1.0)
        signal_high = is_high_dividend_yield(
            self.dividend_fy, self.close, threshold=5.0
        )

        assert isinstance(signal_low, pd.Series)
        assert isinstance(signal_high, pd.Series)
        # 低い閾値の方がTrue数が多い
        assert signal_low.sum() >= signal_high.sum()

    def test_dividend_yield_zero_dividend(self):
        """無配の場合"""
        dividend_zero = pd.Series(np.zeros(100), index=self.dates)

        signal = is_high_dividend_yield(dividend_zero, self.close, threshold=2.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 無配はFalse
        assert signal.sum() == 0

    def test_dividend_yield_nan_handling(self):
        """NaN処理テスト"""
        dividend_with_nan = self.dividend_fy.copy()
        dividend_with_nan.iloc[0:10] = np.nan

        signal = is_high_dividend_yield(dividend_with_nan, self.close, threshold=2.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # NaNの部分はFalse
        assert not signal.iloc[0:10].any()

    def test_dividend_yield_zero_close(self):
        """株価0の場合（ゼロ除算対策）"""
        close_with_zero = self.close.copy()
        close_with_zero.iloc[0:10] = 0

        signal = is_high_dividend_yield(self.dividend_fy, close_with_zero, threshold=2.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # Close=0の部分はFalse
        assert not signal.iloc[0:10].any()


class TestDividendPerShareGrowth:
    """is_growing_dividend_per_share()のテスト"""

    def setup_method(self):
        self.dates = pd.date_range("2023-01-01", periods=120)
        # 開示ごとに 10 -> 12 -> 18 へ成長
        self.dividend_fy = pd.Series([10.0] * 40 + [12.0] * 40 + [18.0] * 40, index=self.dates)

    def test_growth_basic(self):
        signal = is_growing_dividend_per_share(
            self.dividend_fy, growth_threshold=0.15, periods=1, condition="above"
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 比較対象がない最初のブロックはFalse
        assert not signal.iloc[:40].any()
        # 12/10=+20%, 18/12=+50% のため後半はTrue
        assert signal.iloc[45:].all()

    def test_growth_condition_below(self):
        signal = is_growing_dividend_per_share(
            self.dividend_fy, growth_threshold=0.3, periods=1, condition="below"
        )

        # 2回目開示(+20%)はTrue、3回目開示(+50%)はFalse
        assert signal.iloc[45]
        assert not signal.iloc[90]

    def test_growth_insufficient_periods(self):
        signal = is_growing_dividend_per_share(
            self.dividend_fy, growth_threshold=0.1, periods=5, condition="above"
        )
        assert not signal.any()

    def test_growth_nan_handling(self):
        dividend_with_nan = self.dividend_fy.copy()
        dividend_with_nan.iloc[10:20] = np.nan

        signal = is_growing_dividend_per_share(dividend_with_nan, growth_threshold=0.1)

        assert isinstance(signal, pd.Series)
        assert not signal.iloc[10:20].any()


# =====================================================================
# 境界値・エッジケーステスト
# =====================================================================


class TestEdgeCases:
    """エッジケーステスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)

    def test_all_nan_series(self):
        """全てNaNのSeriesを処理"""
        all_nan = pd.Series(np.nan, index=self.dates)

        # 各シグナルがクラッシュしないことを確認
        signal_per = is_undervalued_by_per(all_nan, all_nan, threshold=15.0)
        assert isinstance(signal_per, pd.Series)
        assert signal_per.sum() == 0

        signal_pbr = is_undervalued_by_pbr(all_nan, all_nan, threshold=1.0)
        assert isinstance(signal_pbr, pd.Series)
        assert signal_pbr.sum() == 0

        signal_margin = is_high_operating_margin(all_nan, threshold=10.0)
        assert isinstance(signal_margin, pd.Series)
        assert signal_margin.sum() == 0

        signal_cf = operating_cash_flow_threshold(all_nan, threshold=0.0)
        assert isinstance(signal_cf, pd.Series)
        assert signal_cf.sum() == 0

    def test_empty_series(self):
        """空のSeriesを処理"""
        empty = pd.Series([], dtype=float)

        # 各シグナルがクラッシュしないことを確認
        signal_margin = is_high_operating_margin(empty, threshold=10.0)
        assert isinstance(signal_margin, pd.Series)
        assert len(signal_margin) == 0

        signal_cf = operating_cash_flow_threshold(empty, threshold=0.0)
        assert isinstance(signal_cf, pd.Series)
        assert len(signal_cf) == 0

    def test_inf_values(self):
        """Inf値の処理"""
        data_with_inf = pd.Series(np.ones(100) * 15, index=self.dates)
        data_with_inf.iloc[0:5] = np.inf
        data_with_inf.iloc[5:10] = -np.inf

        signal = is_high_operating_margin(data_with_inf, threshold=10.0)
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_mixed_positive_negative_growth(self):
        """正負が混在する成長パターン"""
        # 変動するProfit
        profit_mixed = pd.Series(
            [100, 120, 90, 150, 80, 200, 50, 180, 70, 160] * 10, index=self.dates
        )

        signal = is_growing_profit(profit_mixed, growth_threshold=0.1, periods=5)
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_boundary_threshold_values(self):
        """境界値テスト（閾値が0）"""
        roe = pd.Series(np.ones(100) * 0.001, index=self.dates)

        # threshold=0でも動作すること
        signal = is_high_roe(roe, threshold=0.0)
        assert isinstance(signal, pd.Series)
        assert signal.sum() > 0

    def test_periods_edge_cases_in_growth(self):
        """成長率計算のperiodsエッジケース"""
        profit = pd.Series(np.linspace(100, 200, 100), index=self.dates)

        # periods=1（最小）
        signal_min = is_growing_profit(profit, growth_threshold=0.0, periods=1)
        assert isinstance(signal_min, pd.Series)

        # periods=99（データ長-1）
        signal_max = is_growing_profit(profit, growth_threshold=0.0, periods=99)
        assert isinstance(signal_max, pd.Series)

    def test_single_value_series(self):
        """単一値のSeries"""
        single = pd.Series([100.0])

        signal = is_high_operating_margin(single, threshold=50.0)
        assert isinstance(signal, pd.Series)
        assert len(signal) == 1
        assert signal.iloc[0] is True or signal.iloc[0] is np.True_


class TestPBREdgeCases:
    """PBRシグナルのエッジケーステスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.close = pd.Series(np.ones(100) * 100, index=self.dates)
        self.bps = pd.Series(np.ones(100) * 100, index=self.dates)

    def test_pbr_zero_bps(self):
        """BPS=0の場合（ゼロ除算対策）"""
        bps_with_zero = self.bps.copy()
        bps_with_zero.iloc[0:10] = 0

        signal = is_undervalued_by_pbr(self.close, bps_with_zero, threshold=1.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # BPS=0の部分はFalse
        assert not signal.iloc[0:10].any()

    def test_pbr_negative_bps(self):
        """負のBPS（債務超過）の場合"""
        bps_negative = self.bps.copy()
        bps_negative.iloc[0:10] = -50

        signal = is_undervalued_by_pbr(self.close, bps_negative, threshold=1.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 負のBPSの部分はFalse
        assert not signal.iloc[0:10].any()

    def test_pbr_nan_handling(self):
        """NaN処理テスト"""
        bps_with_nan = self.bps.copy()
        bps_with_nan.iloc[0:10] = np.nan

        signal = is_undervalued_by_pbr(self.close, bps_with_nan, threshold=1.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # NaNの部分はFalse
        assert not signal.iloc[0:10].any()


class TestOperatingCashFlowEdgeCases:
    """営業キャッシュフローシグナルのエッジケーステスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)

    def test_mixed_positive_negative_cf(self):
        """正負混在のキャッシュフロー"""
        cf_mixed = pd.Series(
            [1000, -500, 800, -200, 1500, -1000, 300, -100, 2000, -50] * 10,
            index=self.dates,
        )

        signal = operating_cash_flow_threshold(cf_mixed, threshold=0.0)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 正のCFのみTrue
        expected_true_count = sum(1 for v in cf_mixed if v > 0)
        assert signal.sum() == expected_true_count

    def test_cf_with_threshold(self):
        """閾値付きキャッシュフローテスト"""
        cf = pd.Series([100, 200, 500, 1000, 2000] * 20, index=self.dates)

        signal_low = operating_cash_flow_threshold(cf, threshold=100.0)
        signal_high = operating_cash_flow_threshold(cf, threshold=1500.0)

        # 高い閾値の方がTrue数が少ない
        assert signal_low.sum() > signal_high.sum()


# =====================================================================
# condition パラメータテスト（2026-01追加）
# =====================================================================


class TestConditionParameterPER:
    """PERのconditionパラメータテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.close = pd.Series(np.ones(100) * 100, index=self.dates)
        self.eps = pd.Series(np.ones(100) * 10, index=self.dates)  # PER = 10

    def test_condition_below_default(self):
        """below条件（デフォルト）"""
        signal = is_undervalued_by_per(self.close, self.eps, threshold=15.0)
        # PER=10 < 15 → True
        assert signal.all()

    def test_condition_below_explicit(self):
        """below条件（明示的指定）"""
        signal = is_undervalued_by_per(
            self.close, self.eps, threshold=15.0, condition="below"
        )
        # PER=10 < 15 → True
        assert signal.all()

    def test_condition_above(self):
        """above条件（割高判定）"""
        signal = is_undervalued_by_per(
            self.close, self.eps, threshold=8.0, condition="above"
        )
        # PER=10 > 8 → True
        assert signal.all()

    def test_condition_above_false(self):
        """above条件（条件不一致）"""
        signal = is_undervalued_by_per(
            self.close, self.eps, threshold=15.0, condition="above"
        )
        # PER=10 > 15 → False
        assert not signal.any()

    def test_backward_compatibility(self):
        """condition未指定でも動作（後方互換性）"""
        signal_old = is_undervalued_by_per(self.close, self.eps, threshold=15.0)
        signal_new = is_undervalued_by_per(
            self.close, self.eps, threshold=15.0, condition="below"
        )
        pd.testing.assert_series_equal(signal_old, signal_new)


class TestConditionParameterPBR:
    """PBRのconditionパラメータテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.close = pd.Series(np.ones(100) * 80, index=self.dates)
        self.bps = pd.Series(np.ones(100) * 100, index=self.dates)  # PBR = 0.8

    def test_condition_below_default(self):
        """below条件（デフォルト）"""
        signal = is_undervalued_by_pbr(self.close, self.bps, threshold=1.0)
        # PBR=0.8 < 1.0 → True
        assert signal.all()

    def test_condition_above(self):
        """above条件"""
        signal = is_undervalued_by_pbr(
            self.close, self.bps, threshold=0.5, condition="above"
        )
        # PBR=0.8 > 0.5 → True
        assert signal.all()


class TestConditionParameterROE:
    """ROEのconditionパラメータテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.roe = pd.Series(np.ones(100) * 15, index=self.dates)  # ROE = 15%

    def test_condition_above_default(self):
        """above条件（デフォルト）"""
        signal = is_high_roe(self.roe, threshold=10.0)
        # ROE=15% >= 10% → True
        assert signal.all()

    def test_condition_below(self):
        """below条件（低ROE判定）"""
        signal = is_high_roe(self.roe, threshold=20.0, condition="below")
        # ROE=15% < 20% → True
        assert signal.all()

    def test_condition_below_false(self):
        """below条件（条件不一致）"""
        signal = is_high_roe(self.roe, threshold=10.0, condition="below")
        # ROE=15% < 10% → False
        assert not signal.any()


class TestConditionParameterOperatingMargin:
    """営業利益率のconditionパラメータテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.operating_margin = pd.Series(np.ones(100) * 12, index=self.dates)

    def test_condition_above_default(self):
        """above条件（デフォルト）"""
        signal = is_high_operating_margin(self.operating_margin, threshold=10.0)
        # 12% >= 10% → True
        assert signal.all()

    def test_condition_below(self):
        """below条件"""
        signal = is_high_operating_margin(
            self.operating_margin, threshold=15.0, condition="below"
        )
        # 12% < 15% → True
        assert signal.all()


class TestConditionParameterOperatingCashFlow:
    """営業CFのconditionパラメータテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.operating_cash_flow = pd.Series(np.ones(100) * 1000, index=self.dates)

    def test_condition_above_default(self):
        """above条件（デフォルト）"""
        signal = operating_cash_flow_threshold(self.operating_cash_flow, threshold=0.0)
        # 1000 > 0 → True
        assert signal.all()

    def test_condition_below(self):
        """below条件"""
        signal = operating_cash_flow_threshold(
            self.operating_cash_flow, threshold=2000.0, condition="below"
        )
        # 1000 < 2000 → True
        assert signal.all()


class TestConditionParameterDividendYield:
    """配当利回りのconditionパラメータテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.close = pd.Series(np.ones(100) * 1000, index=self.dates)
        # 配当30円 → 利回り3%
        self.dividend_fy = pd.Series(np.ones(100) * 30, index=self.dates)

    def test_condition_above_default(self):
        """above条件（デフォルト）"""
        signal = is_high_dividend_yield(self.dividend_fy, self.close, threshold=2.0)
        # 3% >= 2% → True
        assert signal.all()

    def test_condition_below(self):
        """below条件"""
        signal = is_high_dividend_yield(
            self.dividend_fy, self.close, threshold=5.0, condition="below"
        )
        # 3% < 5% → True
        assert signal.all()


class TestConditionParameterPEGRatio:
    """PEG Ratioのconditionパラメータテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.close = pd.Series(np.ones(100) * 100, index=self.dates)
        self.eps = pd.Series(np.ones(100) * 10, index=self.dates)
        # 20%成長予想 → PEG = (100/10) / 0.2 = 50
        self.next_year_forecast_eps = pd.Series(np.ones(100) * 12, index=self.dates)

    def test_condition_below_default(self):
        """below条件（デフォルト）"""
        signal = is_undervalued_growth_by_peg(
            self.close, self.eps, self.next_year_forecast_eps, threshold=100.0
        )
        # PEG=50 < 100 → True
        assert signal.all()

    def test_condition_above(self):
        """above条件"""
        signal = is_undervalued_growth_by_peg(
            self.close,
            self.eps,
            self.next_year_forecast_eps,
            threshold=30.0,
            condition="above",
        )
        # PEG=50 > 30 → True
        assert signal.all()


class TestConditionParameterForwardEPS:
    """Forward EPS成長率のconditionパラメータテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.eps = pd.Series(np.ones(100) * 10, index=self.dates)
        # 15%成長予想
        self.next_year_forecast_eps = pd.Series(np.ones(100) * 11.5, index=self.dates)

    def test_condition_above_default(self):
        """above条件（デフォルト）"""
        signal = is_expected_growth_eps(
            self.eps, self.next_year_forecast_eps, growth_threshold=0.1
        )
        # 15% > 10% → True
        assert signal.all()

    def test_condition_below(self):
        """below条件"""
        signal = is_expected_growth_eps(
            self.eps,
            self.next_year_forecast_eps,
            growth_threshold=0.2,
            condition="below",
        )
        # 15% < 20% → True
        assert signal.all()


class TestConditionParameterProfitGrowth:
    """Profit成長率のconditionパラメータテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        # 100%成長パターン（period=10で比較して10%以上の成長率を確保）
        self.profit = pd.Series(np.linspace(1000, 2000, 100), index=self.dates)

    def test_condition_above_default(self):
        """above条件（デフォルト）"""
        signal = is_growing_profit(self.profit, growth_threshold=0.05, periods=10)
        # 後半は成長しているのでTrueが発生
        assert signal.iloc[20:].sum() > 0

    def test_condition_below(self):
        """below条件"""
        signal = is_growing_profit(
            self.profit, growth_threshold=0.5, periods=10, condition="below"
        )
        # 成長率 < 50%なのでTrue
        assert signal.iloc[20:].sum() > 0


class TestConditionParameterSalesGrowth:
    """Sales成長率のconditionパラメータテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        # 100%成長パターン（period=10で比較して10%以上の成長率を確保）
        self.sales = pd.Series(np.linspace(10000, 20000, 100), index=self.dates)

    def test_condition_above_default(self):
        """above条件（デフォルト）"""
        signal = is_growing_sales(self.sales, growth_threshold=0.05, periods=10)
        assert signal.iloc[20:].sum() > 0

    def test_condition_below(self):
        """below条件"""
        signal = is_growing_sales(
            self.sales, growth_threshold=0.5, periods=10, condition="below"
        )
        assert signal.iloc[20:].sum() > 0


# =====================================================================
# exclude_negative パラメータテスト（2026-01追加）
# =====================================================================


class TestExcludeNegativeParameter:
    """PER/PBRのexclude_negativeパラメータテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.close = pd.Series(np.ones(100) * 100, index=self.dates)
        # EPS: 正の値と負の値を混在
        self.eps = pd.Series(np.ones(100) * 10, index=self.dates)
        self.eps.iloc[0:20] = -5  # 最初の20日間は負のEPS（PERが負）
        # BPS: 正の値と負の値を混在
        self.bps = pd.Series(np.ones(100) * 100, index=self.dates)
        self.bps.iloc[0:20] = -50  # 最初の20日間は負のBPS（PBRが負）

    def test_per_exclude_negative_true_default(self):
        """PER: exclude_negative=True（デフォルト）で負のPERを除外"""
        signal = is_undervalued_by_per(
            self.close, self.eps, threshold=15.0, exclude_negative=True
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 負のEPS（PER負）の部分はFalse
        assert not signal.iloc[0:20].any()
        # 正のEPS部分はPER=10 < 15なのでTrue
        assert signal.iloc[20:].all()

    def test_per_exclude_negative_false(self):
        """PER: exclude_negative=Falseで負のPERも含める"""
        # 負のPERも閾値未満なら対象になる
        signal = is_undervalued_by_per(
            self.close, self.eps, threshold=15.0, exclude_negative=False
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 負のEPSの部分もFalse（EPSが0以下なのでeps.where(eps > 0, np.nan)でNaNになる）
        # ただしこれはEPS計算の制約であり、exclude_negativeはPER値の負を制御
        # 正のEPS部分はPER=10 < 15なのでTrue
        assert signal.iloc[20:].all()

    def test_per_exclude_negative_false_with_valid_negative_per(self):
        """PER: exclude_negative=Falseで計算上負になるPERも対象"""
        # 株価が負の場合（実際にはありえないが、テスト用）
        close_with_negative = self.close.copy()
        # 正のEPSで負のclose → 負のPER
        close_with_negative.iloc[30:40] = -100

        signal_exclude = is_undervalued_by_per(
            close_with_negative,
            pd.Series(np.ones(100) * 10, index=self.dates),  # 全て正のEPS
            threshold=0.0,
            condition="below",
            exclude_negative=True,
        )
        signal_include = is_undervalued_by_per(
            close_with_negative,
            pd.Series(np.ones(100) * 10, index=self.dates),  # 全て正のEPS
            threshold=0.0,
            condition="below",
            exclude_negative=False,
        )

        # exclude=Trueの場合、負のPERはFalse
        assert not signal_exclude.iloc[30:40].any()
        # exclude=Falseの場合、負のPER < 0 → True
        assert signal_include.iloc[30:40].all()

    def test_pbr_exclude_negative_true_default(self):
        """PBR: exclude_negative=True（デフォルト）で負のPBRを除外"""
        signal = is_undervalued_by_pbr(
            self.close, self.bps, threshold=1.5, exclude_negative=True
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 負のBPS（PBR負）の部分はFalse
        assert not signal.iloc[0:20].any()
        # 正のBPS部分はPBR=1.0 < 1.5なのでTrue
        assert signal.iloc[20:].all()

    def test_pbr_exclude_negative_false(self):
        """PBR: exclude_negative=Falseで負のPBRも含める"""
        # 正のclose、負のBPS → 負のPBR
        signal_exclude = is_undervalued_by_pbr(
            self.close,
            self.bps,
            threshold=0.0,
            condition="below",
            exclude_negative=True,
        )
        signal_include = is_undervalued_by_pbr(
            self.close,
            self.bps,
            threshold=0.0,
            condition="below",
            exclude_negative=False,
        )

        # exclude=Trueの場合、負のPBRはFalse
        assert not signal_exclude.iloc[0:20].any()
        # exclude=Falseでも、BPS <= 0 はbps.where(bps > 0, np.nan)でNaNになるためFalse
        # これはPBRの計算ロジック上の制約
        assert not signal_include.iloc[0:20].any()

    def test_backward_compatibility_per(self):
        """PER: exclude_negative未指定でも動作（後方互換性）"""
        signal_default = is_undervalued_by_per(self.close, self.eps, threshold=15.0)
        signal_explicit = is_undervalued_by_per(
            self.close, self.eps, threshold=15.0, exclude_negative=True
        )
        pd.testing.assert_series_equal(signal_default, signal_explicit)

    def test_backward_compatibility_pbr(self):
        """PBR: exclude_negative未指定でも動作（後方互換性）"""
        signal_default = is_undervalued_by_pbr(self.close, self.bps, threshold=1.5)
        signal_explicit = is_undervalued_by_pbr(
            self.close, self.bps, threshold=1.5, exclude_negative=True
        )
        pd.testing.assert_series_equal(signal_default, signal_explicit)


# =====================================================================
# consecutive_periods パラメータテスト（2026-01追加）
# =====================================================================


class TestConsecutivePeriodsOperatingCashFlow:
    """営業CFのconsecutive_periodsパラメータテスト"""

    def setup_method(self):
        """テストデータ作成

        決算発表を模したデータ：
        - 日付: 100日間
        - 決算発表: 10日ごとに発表（10回分）
        - 各発表間はffillで補完
        """
        self.dates = pd.date_range("2023-01-01", periods=100)

        # 発表値: 正→正→負→正→正→正→負→正→正→正
        release_values = [1000, 800, -200, 500, 600, 900, -100, 1200, 700, 1100]

        # 日次データ作成（ffillで補完）
        cf_data = []
        for i, val in enumerate(release_values):
            cf_data.extend([val] * 10)
        self.operating_cash_flow = pd.Series(cf_data, index=self.dates)

    def test_consecutive_periods_1_default(self):
        """consecutive_periods=1（デフォルト）は通常動作"""
        signal = operating_cash_flow_threshold(
            self.operating_cash_flow, threshold=0.0, consecutive_periods=1
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 正のCFのときはTrue、負のときはFalse
        # 正: 1000, 800, 500, 600, 900, 1200, 700, 1100 (8回)
        # 負: -200, -100 (2回)
        assert signal.iloc[0:10].all()  # 1000 > 0
        assert signal.iloc[10:20].all()  # 800 > 0
        assert not signal.iloc[20:30].any()  # -200 < 0
        assert signal.iloc[30:40].all()  # 500 > 0

    def test_consecutive_periods_2(self):
        """consecutive_periods=2で直近2回連続条件チェック"""
        signal = operating_cash_flow_threshold(
            self.operating_cash_flow, threshold=0.0, consecutive_periods=2
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

        # 発表履歴: 正→正→負→正→正→正→負→正→正→正
        # 連続2回正:
        #   - 1回目(正)+2回目(正) → 2回目以降True (日10-19)
        #   - 2回目(正)+3回目(負) → False (日20-29)
        #   - 3回目(負)+4回目(正) → False (日30-39)
        #   - 4回目(正)+5回目(正) → True (日40-49)
        #   - 5回目(正)+6回目(正) → True (日50-59)
        #   - 6回目(正)+7回目(負) → False (日60-69)
        #   - 7回目(負)+8回目(正) → False (日70-79)
        #   - 8回目(正)+9回目(正) → True (日80-89)
        #   - 9回目(正)+10回目(正) → True (日90-99)

        # 最初の10日間は1回目の発表のみでconsecutive_periods=2を満たせない
        assert not signal.iloc[0:10].any()
        # 2回目発表後（正→正）
        assert signal.iloc[10:20].all()
        # 3回目発表後（正→負）
        assert not signal.iloc[20:30].any()
        # 4回目発表後（負→正）
        assert not signal.iloc[30:40].any()
        # 5回目発表後（正→正）
        assert signal.iloc[40:50].all()

    def test_consecutive_periods_3(self):
        """consecutive_periods=3で直近3回連続条件チェック"""
        signal = operating_cash_flow_threshold(
            self.operating_cash_flow, threshold=0.0, consecutive_periods=3
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

        # 発表履歴: 正→正→負→正→正→正→負→正→正→正
        # 連続3回正:
        #   - 1+2+3(正正負) → False
        #   - 2+3+4(正負正) → False
        #   - 3+4+5(負正正) → False
        #   - 4+5+6(正正正) → True (日50-59)
        #   - 5+6+7(正正負) → False (日60-69)
        #   - 6+7+8(正負正) → False (日70-79)
        #   - 7+8+9(負正正) → False (日80-89)
        #   - 8+9+10(正正正) → True (日90-99)

        # 5回目発表後は連続3回正ではない
        assert not signal.iloc[40:50].any()
        # 6回目発表後（正正正）
        assert signal.iloc[50:60].all()
        # 7回目発表後（正正負）
        assert not signal.iloc[60:70].any()
        # 10回目発表後（正正正）
        assert signal.iloc[90:100].all()

    def test_consecutive_periods_backward_compatibility(self):
        """consecutive_periods未指定でも動作（後方互換性）"""
        signal_default = operating_cash_flow_threshold(
            self.operating_cash_flow, threshold=0.0
        )
        signal_explicit = operating_cash_flow_threshold(
            self.operating_cash_flow, threshold=0.0, consecutive_periods=1
        )
        pd.testing.assert_series_equal(signal_default, signal_explicit)

    def test_consecutive_periods_with_threshold(self):
        """閾値付きconsecutive_periodsテスト"""
        signal = operating_cash_flow_threshold(
            self.operating_cash_flow, threshold=500.0, consecutive_periods=2
        )

        assert isinstance(signal, pd.Series)
        # 500以上連続2回のパターンをチェック
        # 1000 >= 500 → True, 800 >= 500 → True, -200 < 500 → False, ...
        # 発表履歴(閾値500以上): True→True→False→True→True→True→False→True→True→True
        # 連続2回True:
        #   - 1+2 → True (日10-19)
        #   - 4+5 → True (日40-49)
        #   - 5+6 → True (日50-59)
        #   - 8+9 → True (日80-89)
        #   - 9+10 → True (日90-99)
        assert signal.iloc[10:20].all()  # 1000, 800 両方 >= 500
        assert not signal.iloc[20:40].any()  # -200, 500 (500は境界)


class TestConsecutivePeriodsSimpleFCF:
    """簡易FCFのconsecutive_periodsパラメータテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)

        # 営業CF発表値
        cfo_values = [1000, 800, 500, 600, 900, 700, 1200, 800, 1000, 900]
        # 投資CF発表値（通常負）
        cfi_values = [-500, -600, -700, -400, -300, -800, -500, -400, -600, -500]
        # FCF = [500, 200, -200, 200, 600, -100, 700, 400, 400, 400]

        # 日次データ作成（ffillで補完）
        cfo_data = []
        cfi_data = []
        for cfo, cfi in zip(cfo_values, cfi_values):
            cfo_data.extend([cfo] * 10)
            cfi_data.extend([cfi] * 10)

        self.operating_cash_flow = pd.Series(cfo_data, index=self.dates)
        self.investing_cash_flow = pd.Series(cfi_data, index=self.dates)

    def test_consecutive_periods_1_default(self):
        """consecutive_periods=1（デフォルト）は通常動作"""
        signal = simple_fcf_threshold(
            self.operating_cash_flow,
            self.investing_cash_flow,
            threshold=0.0,
            consecutive_periods=1,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # FCF = [500, 200, -200, 200, 600, -100, 700, 400, 400, 400]
        assert signal.iloc[0:10].all()  # 500 > 0
        assert signal.iloc[10:20].all()  # 200 > 0
        assert not signal.iloc[20:30].any()  # -200 < 0
        assert signal.iloc[30:40].all()  # 200 > 0

    def test_consecutive_periods_2(self):
        """consecutive_periods=2で直近2回連続条件チェック"""
        signal = simple_fcf_threshold(
            self.operating_cash_flow,
            self.investing_cash_flow,
            threshold=0.0,
            consecutive_periods=2,
        )

        assert isinstance(signal, pd.Series)
        # FCF = [500, 200, -200, 200, 600, -100, 700, 400, 400, 400]
        # 正負: 正→正→負→正→正→負→正→正→正→正
        # 連続2回正:
        #   - 1+2(正正) → True (日10-19)
        #   - 2+3(正負) → False (日20-29)
        #   - 4+5(正正) → True (日40-49)
        #   - 7+8(正正) → True (日70-79)
        assert not signal.iloc[0:10].any()  # 1回目のみ
        assert signal.iloc[10:20].all()  # 正→正
        assert not signal.iloc[20:30].any()  # 正→負
        assert not signal.iloc[30:40].any()  # 負→正
        assert signal.iloc[40:50].all()  # 正→正

    def test_consecutive_periods_backward_compatibility(self):
        """consecutive_periods未指定でも動作（後方互換性）"""
        signal_default = simple_fcf_threshold(
            self.operating_cash_flow,
            self.investing_cash_flow,
            threshold=0.0,
        )
        signal_explicit = simple_fcf_threshold(
            self.operating_cash_flow,
            self.investing_cash_flow,
            threshold=0.0,
            consecutive_periods=1,
        )
        pd.testing.assert_series_equal(signal_default, signal_explicit)


class TestConsecutivePeriodsEdgeCases:
    """consecutive_periodsのエッジケーステスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)

    def test_consecutive_periods_insufficient_data(self):
        """発表回数が足りない場合"""
        # 2回分の発表しかないデータ
        cf_data = [1000] * 50 + [800] * 50
        operating_cash_flow = pd.Series(cf_data, index=self.dates)

        signal = operating_cash_flow_threshold(
            operating_cash_flow, threshold=0.0, consecutive_periods=5
        )

        assert isinstance(signal, pd.Series)
        # 発表回数2回 < consecutive_periods=5 なので全てFalse
        assert not signal.any()

    def test_consecutive_periods_all_same_value(self):
        """全て同じ値の場合（発表回数=1とみなされる）"""
        operating_cash_flow = pd.Series(np.ones(100) * 1000, index=self.dates)

        signal = operating_cash_flow_threshold(
            operating_cash_flow, threshold=0.0, consecutive_periods=2
        )

        assert isinstance(signal, pd.Series)
        # 値が変化しないので発表回数=1とみなされる
        assert not signal.any()

    def test_consecutive_periods_with_nan(self):
        """NaNを含むデータ"""
        cf_data = [1000] * 30 + [np.nan] * 10 + [800] * 30 + [900] * 30
        operating_cash_flow = pd.Series(cf_data, index=self.dates)

        signal = operating_cash_flow_threshold(
            operating_cash_flow, threshold=0.0, consecutive_periods=2
        )

        assert isinstance(signal, pd.Series)
        # NaN部分はFalse
        assert not signal.iloc[30:40].any()


# =====================================================================
# CFO利回り・簡易FCF利回りシグナルテスト（2026-01追加）
# =====================================================================


class TestCfoYieldThreshold:
    """cfo_yield_threshold()のテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        # 終値=1000円、発行済み株式=100万株、自己株式=10万株
        # 時価総額 = 1000 × (100万 - 10万) = 9億円
        # CFO = 4500万円 → CFO利回り = 4500万 / 9億 × 100 = 5%
        self.close = pd.Series(np.ones(100) * 1000, index=self.dates)
        self.operating_cash_flow = pd.Series(np.ones(100) * 45_000_000, index=self.dates)
        self.shares_outstanding = pd.Series(np.ones(100) * 1_000_000, index=self.dates)
        self.treasury_shares = pd.Series(np.ones(100) * 100_000, index=self.dates)

    def test_cfo_yield_basic(self):
        """CFO利回りシグナル基本テスト"""
        # CFO利回り = 5%
        signal = cfo_yield_threshold(
            self.close,
            self.operating_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=5.0,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.close)
        # 5% >= 5% なのでTrue
        assert signal.all()

    def test_cfo_yield_threshold_effect(self):
        """CFO利回り閾値の効果テスト"""
        signal_low = cfo_yield_threshold(
            self.close,
            self.operating_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=3.0,
        )
        signal_high = cfo_yield_threshold(
            self.close,
            self.operating_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=10.0,
        )

        # 低い閾値の方がTrue数が多い
        assert signal_low.sum() >= signal_high.sum()
        # 5% >= 3% → True
        assert signal_low.all()
        # 5% >= 10% → False
        assert not signal_high.any()

    def test_cfo_yield_condition_below(self):
        """below条件のテスト"""
        signal = cfo_yield_threshold(
            self.close,
            self.operating_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=10.0,
            condition="below",
        )

        # 5% < 10% → True
        assert signal.all()

    def test_cfo_yield_use_floating_shares_false(self):
        """use_floating_shares=Falseのテスト"""
        # 発行済み株式全体を使用
        # 時価総額 = 1000 × 100万 = 10億円
        # CFO利回り = 4500万 / 10億 × 100 = 4.5%
        signal_floating = cfo_yield_threshold(
            self.close,
            self.operating_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=4.6,
            use_floating_shares=True,
        )
        signal_all = cfo_yield_threshold(
            self.close,
            self.operating_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=4.6,
            use_floating_shares=False,
        )

        # 流通株式ベース: 5% >= 4.6% → True
        assert signal_floating.all()
        # 発行済み株式ベース: 4.5% >= 4.6% → False
        assert not signal_all.any()

    def test_cfo_yield_treasury_shares_null(self):
        """自己株式がNullの場合"""
        treasury_shares_nan = pd.Series([np.nan] * 100, index=self.dates)

        # 自己株式がNaNの場合、0として扱われる
        # 時価総額 = 1000 × 100万 = 10億円
        # CFO利回り = 4500万 / 10億 × 100 = 4.5%
        signal = cfo_yield_threshold(
            self.close,
            self.operating_cash_flow,
            self.shares_outstanding,
            treasury_shares_nan,
            threshold=4.5,
        )

        assert isinstance(signal, pd.Series)
        # 4.5% >= 4.5% → True
        assert signal.all()

    def test_cfo_yield_zero_shares(self):
        """株式数が0の場合（ゼロ除算対策）"""
        shares_zero = pd.Series(np.zeros(100), index=self.dates)

        signal = cfo_yield_threshold(
            self.close,
            self.operating_cash_flow,
            shares_zero,
            self.treasury_shares,
            threshold=5.0,
        )

        assert isinstance(signal, pd.Series)
        # 株式数0 → 時価総額NaN → False
        assert not signal.any()

    def test_cfo_yield_negative_cfo(self):
        """負のCFOの場合"""
        operating_cash_flow_negative = pd.Series(np.ones(100) * -45_000_000, index=self.dates)

        signal = cfo_yield_threshold(
            self.close,
            operating_cash_flow_negative,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=0.0,
            condition="below",
        )

        assert isinstance(signal, pd.Series)
        # -5% < 0% → True
        assert signal.all()

    def test_cfo_yield_nan_handling(self):
        """NaN処理テスト"""
        operating_cash_flow_nan = self.operating_cash_flow.copy()
        operating_cash_flow_nan.iloc[0:10] = np.nan

        signal = cfo_yield_threshold(
            self.close,
            operating_cash_flow_nan,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=5.0,
        )

        assert isinstance(signal, pd.Series)
        # NaNの部分はFalse
        assert not signal.iloc[0:10].any()
        # 正常な部分はTrue
        assert signal.iloc[10:].all()


class TestSimpleFcfYieldThreshold:
    """simple_fcf_yield_threshold()のテスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        # 終値=1000円、発行済み株式=100万株、自己株式=10万株
        # 時価総額 = 1000 × (100万 - 10万) = 9億円
        # CFO = 6000万円、CFI = -2400万円 → FCF = 3600万円
        # FCF利回り = 3600万 / 9億 × 100 = 4%
        self.close = pd.Series(np.ones(100) * 1000, index=self.dates)
        self.operating_cash_flow = pd.Series(np.ones(100) * 60_000_000, index=self.dates)
        self.investing_cash_flow = pd.Series(np.ones(100) * -24_000_000, index=self.dates)
        self.shares_outstanding = pd.Series(np.ones(100) * 1_000_000, index=self.dates)
        self.treasury_shares = pd.Series(np.ones(100) * 100_000, index=self.dates)

    def test_simple_fcf_yield_basic(self):
        """簡易FCF利回りシグナル基本テスト"""
        # FCF利回り = 4%
        signal = simple_fcf_yield_threshold(
            self.close,
            self.operating_cash_flow,
            self.investing_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=4.0,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.close)
        # 4% >= 4% なのでTrue
        assert signal.all()

    def test_simple_fcf_yield_threshold_effect(self):
        """簡易FCF利回り閾値の効果テスト"""
        signal_low = simple_fcf_yield_threshold(
            self.close,
            self.operating_cash_flow,
            self.investing_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=2.0,
        )
        signal_high = simple_fcf_yield_threshold(
            self.close,
            self.operating_cash_flow,
            self.investing_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=8.0,
        )

        # 低い閾値の方がTrue数が多い
        assert signal_low.sum() >= signal_high.sum()
        # 4% >= 2% → True
        assert signal_low.all()
        # 4% >= 8% → False
        assert not signal_high.any()

    def test_simple_fcf_yield_condition_below(self):
        """below条件のテスト"""
        signal = simple_fcf_yield_threshold(
            self.close,
            self.operating_cash_flow,
            self.investing_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=8.0,
            condition="below",
        )

        # 4% < 8% → True
        assert signal.all()

    def test_simple_fcf_yield_use_floating_shares_false(self):
        """use_floating_shares=Falseのテスト"""
        # 発行済み株式全体を使用
        # 時価総額 = 1000 × 100万 = 10億円
        # FCF利回り = 3600万 / 10億 × 100 = 3.6%
        signal_floating = simple_fcf_yield_threshold(
            self.close,
            self.operating_cash_flow,
            self.investing_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=3.7,
            use_floating_shares=True,
        )
        signal_all = simple_fcf_yield_threshold(
            self.close,
            self.operating_cash_flow,
            self.investing_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=3.7,
            use_floating_shares=False,
        )

        # 流通株式ベース: 4% >= 3.7% → True
        assert signal_floating.all()
        # 発行済み株式ベース: 3.6% >= 3.7% → False
        assert not signal_all.any()

    def test_simple_fcf_yield_negative_fcf(self):
        """負のFCFの場合"""
        # CFO < |CFI| の場合、FCFは負
        investing_cash_flow_large = pd.Series(np.ones(100) * -100_000_000, index=self.dates)
        # FCF = 6000万 + (-1億) = -4000万
        # FCF利回り = -4000万 / 9億 × 100 ≈ -4.44%

        signal = simple_fcf_yield_threshold(
            self.close,
            self.operating_cash_flow,
            investing_cash_flow_large,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=0.0,
            condition="below",
        )

        assert isinstance(signal, pd.Series)
        # -4.44% < 0% → True
        assert signal.all()

    def test_simple_fcf_yield_zero_shares(self):
        """株式数が0の場合（ゼロ除算対策）"""
        shares_zero = pd.Series(np.zeros(100), index=self.dates)

        signal = simple_fcf_yield_threshold(
            self.close,
            self.operating_cash_flow,
            self.investing_cash_flow,
            shares_zero,
            self.treasury_shares,
            threshold=4.0,
        )

        assert isinstance(signal, pd.Series)
        # 株式数0 → 時価総額NaN → False
        assert not signal.any()

    def test_simple_fcf_yield_nan_handling(self):
        """NaN処理テスト"""
        operating_cash_flow_nan = self.operating_cash_flow.copy()
        operating_cash_flow_nan.iloc[0:10] = np.nan

        signal = simple_fcf_yield_threshold(
            self.close,
            operating_cash_flow_nan,
            self.investing_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=4.0,
        )

        assert isinstance(signal, pd.Series)
        # NaNの部分はFalse
        assert not signal.iloc[0:10].any()
        # 正常な部分はTrue
        assert signal.iloc[10:].all()

    def test_simple_fcf_yield_treasury_shares_null(self):
        """自己株式がNullの場合"""
        treasury_shares_nan = pd.Series([np.nan] * 100, index=self.dates)

        # 自己株式がNaNの場合、0として扱われる
        # 時価総額 = 1000 × 100万 = 10億円
        # FCF利回り = 3600万 / 10億 × 100 = 3.6%
        signal = simple_fcf_yield_threshold(
            self.close,
            self.operating_cash_flow,
            self.investing_cash_flow,
            self.shares_outstanding,
            treasury_shares_nan,
            threshold=3.5,  # 浮動小数点精度を考慮して少し低い閾値
        )

        assert isinstance(signal, pd.Series)
        # 3.6% >= 3.5% → True
        assert signal.all()


class TestCfoYieldGrowth:
    """is_growing_cfo_yield()のテスト"""

    def setup_method(self):
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.close = pd.Series(np.ones(100) * 1000.0, index=self.dates)
        # 開示間の価格ノイズ（成長率判定に影響しないことを確認）
        self.close.iloc[60:80] = 6000.0
        self.operating_cash_flow = pd.Series([45_000_000.0] * 50 + [67_500_000.0] * 50, index=self.dates)
        self.shares_outstanding = pd.Series(np.ones(100) * 1_000_000.0, index=self.dates)
        self.treasury_shares = pd.Series(np.zeros(100), index=self.dates)

    def test_growth_release_based(self):
        signal = is_growing_cfo_yield(
            self.close,
            self.operating_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            growth_threshold=0.1,
            periods=1,
            condition="above",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert not signal.iloc[:50].any()
        assert signal.iloc[50:].all()
        # False -> True の1回だけ遷移
        transitions = signal.astype(int).diff().fillna(0).ne(0).sum()
        assert transitions == 1

    def test_growth_condition_below_with_floating_share_effect(self):
        treasury = pd.Series([200_000.0] * 50 + [0.0] * 50, index=self.dates)
        operating = pd.Series(np.ones(100) * 50_000_000.0, index=self.dates)

        signal_floating = is_growing_cfo_yield(
            self.close,
            operating,
            self.shares_outstanding,
            treasury,
            growth_threshold=0.0,
            periods=1,
            condition="below",
            use_floating_shares=True,
        )
        signal_total = is_growing_cfo_yield(
            self.close,
            operating,
            self.shares_outstanding,
            treasury,
            growth_threshold=0.0,
            periods=1,
            condition="below",
            use_floating_shares=False,
        )

        assert signal_floating.iloc[50:].all()
        assert not signal_total.any()

    def test_growth_insufficient_periods(self):
        signal = is_growing_cfo_yield(
            self.close,
            self.operating_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            growth_threshold=0.1,
            periods=5,
        )
        assert not signal.any()

    def test_growth_nan_handling(self):
        operating_with_nan = self.operating_cash_flow.copy()
        operating_with_nan.iloc[0:10] = np.nan

        signal = is_growing_cfo_yield(
            self.close,
            operating_with_nan,
            self.shares_outstanding,
            self.treasury_shares,
            growth_threshold=0.1,
            periods=1,
        )
        assert not signal.iloc[0:10].any()

    def test_growth_treasury_all_nan_keeps_release_based_behavior(self):
        close_with_noise = self.close.copy()
        close_with_noise.iloc[20:30] = 4500.0
        close_with_noise.iloc[70:80] = 7000.0
        treasury_all_nan = pd.Series(np.nan, index=self.dates)

        signal = is_growing_cfo_yield(
            close_with_noise,
            self.operating_cash_flow,
            self.shares_outstanding,
            treasury_all_nan,
            growth_threshold=0.1,
            periods=1,
            condition="above",
            use_floating_shares=True,
        )

        assert not signal.iloc[:50].any()
        assert signal.iloc[50:].all()
        transitions = signal.astype(int).diff().fillna(0).ne(0).sum()
        assert transitions == 1

    def test_growth_ignores_treasury_change_when_not_using_floating_shares(self):
        treasury_changes = pd.Series([300_000.0] * 50 + [0.0] * 50, index=self.dates)
        operating_constant = pd.Series(np.ones(100) * 50_000_000.0, index=self.dates)

        signal = is_growing_cfo_yield(
            self.close,
            operating_constant,
            self.shares_outstanding,
            treasury_changes,
            growth_threshold=0.0,
            periods=1,
            condition="above",
            use_floating_shares=False,
        )

        assert not signal.any()


class TestSimpleFcfYieldGrowth:
    """is_growing_simple_fcf_yield()のテスト"""

    def setup_method(self):
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.close = pd.Series(np.ones(100) * 1000.0, index=self.dates)
        self.close.iloc[60:80] = 5000.0
        self.operating_cash_flow = pd.Series([60_000_000.0] * 50 + [90_000_000.0] * 50, index=self.dates)
        self.investing_cash_flow = pd.Series([-24_000_000.0] * 50 + [-30_000_000.0] * 50, index=self.dates)
        self.shares_outstanding = pd.Series(np.ones(100) * 1_000_000.0, index=self.dates)
        self.treasury_shares = pd.Series(np.zeros(100), index=self.dates)

    def test_growth_release_based(self):
        signal = is_growing_simple_fcf_yield(
            self.close,
            self.operating_cash_flow,
            self.investing_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            growth_threshold=0.2,
            periods=1,
            condition="above",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert not signal.iloc[:50].any()
        assert signal.iloc[50:].all()

    def test_growth_condition_below(self):
        signal = is_growing_simple_fcf_yield(
            self.close,
            self.operating_cash_flow,
            self.investing_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            growth_threshold=1.0,
            periods=1,
            condition="below",
        )

        assert signal.iloc[50:].all()

    def test_growth_insufficient_periods(self):
        signal = is_growing_simple_fcf_yield(
            self.close,
            self.operating_cash_flow,
            self.investing_cash_flow,
            self.shares_outstanding,
            self.treasury_shares,
            growth_threshold=0.2,
            periods=5,
        )
        assert not signal.any()

    def test_growth_nan_handling(self):
        investing_with_nan = self.investing_cash_flow.copy()
        investing_with_nan.iloc[0:10] = np.nan

        signal = is_growing_simple_fcf_yield(
            self.close,
            self.operating_cash_flow,
            investing_with_nan,
            self.shares_outstanding,
            self.treasury_shares,
            growth_threshold=0.2,
            periods=1,
        )

        assert not signal.iloc[0:10].any()


class TestYieldSignalsEdgeCases:
    """利回りシグナルのエッジケーステスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)

    def test_all_nan_series(self):
        """全てNaNのSeriesを処理"""
        all_nan = pd.Series(np.nan, index=self.dates)
        shares = pd.Series(np.ones(100) * 1_000_000, index=self.dates)

        signal_cfo = cfo_yield_threshold(all_nan, all_nan, shares, all_nan, threshold=5.0)
        assert isinstance(signal_cfo, pd.Series)
        assert not signal_cfo.any()

        signal_fcf = simple_fcf_yield_threshold(
            all_nan, all_nan, all_nan, shares, all_nan, threshold=5.0
        )
        assert isinstance(signal_fcf, pd.Series)
        assert not signal_fcf.any()

    def test_empty_series(self):
        """空のSeriesを処理"""
        empty = pd.Series([], dtype=float)

        signal_cfo = cfo_yield_threshold(empty, empty, empty, empty, threshold=5.0)
        assert isinstance(signal_cfo, pd.Series)
        assert len(signal_cfo) == 0

        signal_fcf = simple_fcf_yield_threshold(
            empty, empty, empty, empty, empty, threshold=5.0
        )
        assert isinstance(signal_fcf, pd.Series)
        assert len(signal_fcf) == 0

    def test_inf_values(self):
        """Inf値の処理"""
        close = pd.Series(np.ones(100) * 1000, index=self.dates)
        close.iloc[0:5] = np.inf
        cfo = pd.Series(np.ones(100) * 1_000_000, index=self.dates)
        shares = pd.Series(np.ones(100) * 1_000_000, index=self.dates)
        treasury = pd.Series(np.zeros(100), index=self.dates)

        signal = cfo_yield_threshold(close, cfo, shares, treasury, threshold=0.0)
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_negative_shares_outstanding(self):
        """発行済み株式数が負の場合（異常データ）"""
        close = pd.Series(np.ones(100) * 1000, index=self.dates)
        cfo = pd.Series(np.ones(100) * 1_000_000, index=self.dates)
        shares = pd.Series(np.ones(100) * -1_000_000, index=self.dates)  # 負の株式数
        treasury = pd.Series(np.zeros(100), index=self.dates)

        signal = cfo_yield_threshold(close, cfo, shares, treasury, threshold=5.0)
        assert isinstance(signal, pd.Series)
        # 負の株式数は無効 → False
        assert not signal.any()

    def test_treasury_shares_greater_than_outstanding(self):
        """自己株式が発行済み株式より多い場合（異常データ）"""
        close = pd.Series(np.ones(100) * 1000, index=self.dates)
        cfo = pd.Series(np.ones(100) * 1_000_000, index=self.dates)
        shares = pd.Series(np.ones(100) * 1_000_000, index=self.dates)
        treasury = pd.Series(np.ones(100) * 2_000_000, index=self.dates)  # 自己株式 > 発行済み

        signal = cfo_yield_threshold(close, cfo, shares, treasury, threshold=5.0)
        assert isinstance(signal, pd.Series)
        # 流通株式が負 → 無効 → False
        assert not signal.any()


class TestFundamentalSignalParamsConfig:
    """FundamentalSignalParamsのconfigバリデーションテスト"""

    def test_forward_eps_growth_field_exists(self):
        """forward_eps_growthフィールドが正しくパースされること"""
        from src.models.signals.fundamental import FundamentalSignalParams

        params = FundamentalSignalParams(
            forward_eps_growth={"enabled": True, "threshold": 0.15, "condition": "above"}
        )
        assert params.forward_eps_growth.enabled is True
        assert params.forward_eps_growth.threshold == 0.15
        assert params.forward_eps_growth.condition == "above"

    def test_eps_growth_field_exists(self):
        """eps_growth（実績ベース）フィールドが正しくパースされること"""
        from src.models.signals.fundamental import FundamentalSignalParams

        params = FundamentalSignalParams(
            eps_growth={"enabled": True, "threshold": 0.2, "periods": 2, "condition": "above"}
        )
        assert params.eps_growth.enabled is True
        assert params.eps_growth.threshold == 0.2
        assert params.eps_growth.periods == 2
        assert params.eps_growth.condition == "above"

    def test_eps_growth_default_periods(self):
        """eps_growthのperiodsデフォルト値が1であること"""
        from src.models.signals.fundamental import FundamentalSignalParams

        params = FundamentalSignalParams()
        assert params.eps_growth.periods == 1

    def test_profit_growth_default_periods(self):
        """profit_growthのperiodsデフォルト値が1であること"""
        from src.models.signals.fundamental import FundamentalSignalParams

        params = FundamentalSignalParams()
        assert params.profit_growth.periods == 1

    def test_sales_growth_default_periods(self):
        """sales_growthのperiodsデフォルト値が1であること"""
        from src.models.signals.fundamental import FundamentalSignalParams

        params = FundamentalSignalParams()
        assert params.sales_growth.periods == 1

    def test_both_eps_growth_fields_coexist(self):
        """forward_eps_growthとeps_growthが共存できること"""
        from src.models.signals.fundamental import FundamentalSignalParams

        params = FundamentalSignalParams(
            forward_eps_growth={"enabled": True, "threshold": 0.1},
            eps_growth={"enabled": True, "threshold": 0.2, "periods": 1},
        )
        assert params.forward_eps_growth.enabled is True
        assert params.eps_growth.enabled is True
        assert params.forward_eps_growth.threshold == 0.1
        assert params.eps_growth.threshold == 0.2

    def test_dividend_per_share_growth_field_exists(self):
        """dividend_per_share_growthフィールドが正しくパースされること"""
        from src.models.signals.fundamental import FundamentalSignalParams

        params = FundamentalSignalParams(
            dividend_per_share_growth={
                "enabled": True,
                "threshold": 0.15,
                "periods": 2,
                "condition": "above",
            }
        )
        assert params.dividend_per_share_growth.enabled is True
        assert params.dividend_per_share_growth.threshold == 0.15
        assert params.dividend_per_share_growth.periods == 2
        assert params.dividend_per_share_growth.condition == "above"

    def test_yield_growth_fields_default(self):
        """yield成長率フィールドのデフォルト値が正しいこと"""
        from src.models.signals.fundamental import FundamentalSignalParams

        params = FundamentalSignalParams()
        assert params.cfo_yield_growth.periods == 1
        assert params.simple_fcf_yield_growth.periods == 1
        assert params.cfo_yield_growth.use_floating_shares is True
        assert params.simple_fcf_yield_growth.use_floating_shares is True


# =====================================================================
# 時価総額シグナルテスト（2026-02追加）
# =====================================================================


class TestMarketCapThreshold:
    """market_cap_threshold()のテスト"""

    def setup_method(self):
        """テストデータ作成

        終値=1000円、発行済み株式=100万株、自己株式=10万株
        流通株式ベース時価総額 = 1000 × (100万 - 10万) = 9億円 = 9.0億円
        発行済み全体ベース時価総額 = 1000 × 100万 = 10億円 = 10.0億円
        """
        self.dates = pd.date_range("2024-01-01", periods=100)
        self.close = pd.Series(np.ones(100) * 1000.0, index=self.dates)
        self.shares_outstanding = pd.Series(np.ones(100, dtype=int) * 1_000_000, index=self.dates)
        self.treasury_shares = pd.Series(np.ones(100, dtype=int) * 100_000, index=self.dates)

    def test_basic_above(self):
        """基本テスト: 時価総額9億円、threshold=9.0、above → 全True"""
        signal = market_cap_threshold(
            self.close,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=9.0,
            condition="above",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == 100
        # 9億円 >= 9.0億円 → True
        assert signal.all()

    def test_threshold_effect(self):
        """閾値の効果: 低閾値はTrue、高閾値はFalse"""
        signal_low = market_cap_threshold(
            self.close,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=5.0,
            condition="above",
        )
        signal_high = market_cap_threshold(
            self.close,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=20.0,
            condition="above",
        )
        # 9億円 >= 5億円 → True
        assert signal_low.all()
        # 9億円 >= 20億円 → False
        assert not signal_high.any()

    def test_condition_below(self):
        """below条件: 9億円 < 10億円 → True"""
        signal = market_cap_threshold(
            self.close,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=10.0,
            condition="below",
        )
        # 9億円 < 10億円 → True
        assert signal.all()

    def test_boundary(self):
        """境界値: 9億円 >= 9.0億円 → True"""
        signal = market_cap_threshold(
            self.close,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=9.0,
            condition="above",
        )
        assert signal.all()

    def test_floating_shares_false(self):
        """use_floating_shares=False: 発行済み全体ベース"""
        # floating: 9億円、total: 10億円
        signal_floating = market_cap_threshold(
            self.close,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=9.5,
            condition="above",
            use_floating_shares=True,
        )
        signal_total = market_cap_threshold(
            self.close,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=9.5,
            condition="above",
            use_floating_shares=False,
        )
        # 流通: 9億 < 9.5億 → False
        assert not signal_floating.any()
        # 全体: 10億 >= 9.5億 → True
        assert signal_total.all()

    def test_treasury_shares_nan(self):
        """自己株式NaN → 0扱い、時価総額=10億円"""
        treasury_nan = pd.Series([np.nan] * 100, index=self.dates)
        signal = market_cap_threshold(
            self.close,
            self.shares_outstanding,
            treasury_nan,
            threshold=10.0,
            condition="above",
        )
        # NaN→0: 1000 × 1_000_000 = 10億円 >= 10億円 → True
        assert signal.all()

    def test_zero_shares(self):
        """株式数0 → 全False"""
        shares_zero = pd.Series(np.zeros(100, dtype=int), index=self.dates)
        signal = market_cap_threshold(
            self.close,
            shares_zero,
            self.treasury_shares,
            threshold=1.0,
            condition="above",
        )
        assert not signal.any()

    def test_nan_handling(self):
        """Close=NaN → False"""
        close_nan = self.close.copy()
        close_nan.iloc[0:10] = np.nan
        signal = market_cap_threshold(
            close_nan,
            self.shares_outstanding,
            self.treasury_shares,
            threshold=5.0,
            condition="above",
        )
        # NaN部分はFalse
        assert not signal.iloc[0:10].any()
        # 正常部分はTrue
        assert signal.iloc[10:].all()

    def test_large_cap(self):
        """大数値: close=5000, shares=2億 → 1兆円=10000億"""
        dates = pd.date_range("2024-01-01", periods=10)
        close = pd.Series(np.ones(10) * 5000.0, index=dates)
        shares = pd.Series(np.ones(10, dtype=int) * 200_000_000, index=dates)
        treasury = pd.Series(np.zeros(10, dtype=int), index=dates)
        signal = market_cap_threshold(
            close, shares, treasury, threshold=10000.0, condition="above"
        )
        # 5000 × 2億 = 1兆円 = 10000億円 >= 10000億円 → True
        assert signal.all()

    def test_varying_close(self):
        """日次変動でthreshold境界を越えるパターン"""
        dates = pd.date_range("2024-01-01", periods=10)
        # close: 800, 900, 1000, 1100, 1200...
        close = pd.Series([800.0, 900.0, 1000.0, 1100.0, 1200.0] * 2, index=dates)
        shares = pd.Series(np.ones(10, dtype=int) * 1_000_000, index=dates)
        treasury = pd.Series(np.zeros(10, dtype=int), index=dates)
        # 時価総額(億): 8, 9, 10, 11, 12, 8, 9, 10, 11, 12
        signal = market_cap_threshold(
            close, shares, treasury, threshold=10.0, condition="above"
        )
        expected = [False, False, True, True, True, False, False, True, True, True]
        assert list(signal) == expected


if __name__ == "__main__":
    pytest.main([__file__])
