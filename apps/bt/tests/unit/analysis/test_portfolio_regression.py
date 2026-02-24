"""
ポートフォリオ回帰分析のテスト

主成分とベンチマークの相関・回帰分析機能を検証します。
"""

import pytest
import pandas as pd
import numpy as np

from src.domains.analytics.portfolio_regression import (
    RegressionResult,
    align_pc_and_benchmark_dates,
    perform_pc_regression,
    calculate_benchmark_returns,
    analyze_pcs_vs_benchmark,
)


@pytest.fixture
def sample_pc_series() -> pd.Series:
    """テスト用主成分時系列"""
    dates = pd.date_range("2024-01-01", periods=100, freq="D")
    # PC1 = 0.8 * TOPIX_return + noise（β=0.8を期待）
    np.random.seed(42)
    values = np.random.randn(100)
    return pd.Series(values, index=dates, dtype=float)


@pytest.fixture
def sample_benchmark_returns() -> pd.Series:
    """テスト用ベンチマークリターン"""
    dates = pd.date_range("2024-01-01", periods=100, freq="D")
    np.random.seed(42)
    values = np.random.randn(100) * 0.01  # 1% 標準偏差
    return pd.Series(values, index=dates, dtype=float)


@pytest.fixture
def sample_benchmark_df() -> pd.DataFrame:
    """テスト用ベンチマーク価格DataFrame"""
    dates = pd.date_range("2024-01-01", periods=100, freq="D")
    np.random.seed(42)
    close = 100 * np.exp(np.cumsum(np.random.randn(100) * 0.01))
    return pd.DataFrame(
        {"Open": close, "High": close * 1.01, "Low": close * 0.99, "Close": close},
        index=dates,
    )


class TestRegressionResult:
    """RegressionResult データクラステスト"""

    def test_is_significant_true(self):
        result = RegressionResult(
            pc_name="PC1",
            correlation=0.8,
            alpha=0.0,
            beta=0.5,
            r_squared=0.64,
            p_value=0.001,
            std_error=0.05,
            n_observations=100,
        )
        assert result.is_significant(alpha_level=0.05)

    def test_is_significant_false(self):
        result = RegressionResult(
            pc_name="PC1",
            correlation=0.1,
            alpha=0.0,
            beta=0.05,
            r_squared=0.01,
            p_value=0.5,
            std_error=0.1,
            n_observations=100,
        )
        assert not result.is_significant(alpha_level=0.05)

    def test_to_dict(self):
        result = RegressionResult(
            pc_name="PC1",
            correlation=0.8,
            alpha=0.0,
            beta=0.5,
            r_squared=0.64,
            p_value=0.001,
            std_error=0.05,
            n_observations=100,
        )
        d = result.to_dict()
        assert d["pc_name"] == "PC1"
        assert d["beta"] == 0.5


class TestAlignPcAndBenchmarkDates:
    """日付アライメント機能テスト"""

    def test_perfect_alignment(self, sample_pc_series, sample_benchmark_returns):
        """完全一致する日付範囲"""
        pc_aligned, bench_aligned = align_pc_and_benchmark_dates(
            sample_pc_series, sample_benchmark_returns
        )
        assert len(pc_aligned) == 100
        assert len(bench_aligned) == 100
        assert pc_aligned.index.equals(bench_aligned.index)

    def test_partial_overlap(self):
        """部分的に重複する日付範囲"""
        dates1 = pd.date_range("2024-01-01", periods=100, freq="D")
        dates2 = pd.date_range("2024-02-01", periods=100, freq="D")

        pc = pd.Series(np.random.randn(100), index=dates1, dtype=float)
        bench = pd.Series(np.random.randn(100), index=dates2, dtype=float)

        pc_aligned, bench_aligned = align_pc_and_benchmark_dates(pc, bench)

        # 重複期間のみが残る
        assert len(pc_aligned) > 30  # 最低観測数を満たす
        assert pc_aligned.index.equals(bench_aligned.index)

    def test_no_overlap_raises_error(self):
        """重複なしでエラー発生"""
        dates1 = pd.date_range("2024-01-01", periods=30, freq="D")
        dates2 = pd.date_range("2024-06-01", periods=30, freq="D")

        pc = pd.Series(np.random.randn(30), index=dates1, dtype=float)
        bench = pd.Series(np.random.randn(30), index=dates2, dtype=float)

        with pytest.raises(ValueError, match="No overlapping dates"):
            align_pc_and_benchmark_dates(pc, bench)

    def test_insufficient_data_raises_error(self):
        """データ不足でエラー発生"""
        dates = pd.date_range("2024-01-01", periods=20, freq="D")

        pc = pd.Series(np.random.randn(20), index=dates, dtype=float)
        bench = pd.Series(np.random.randn(20), index=dates, dtype=float)

        with pytest.raises(ValueError, match="Insufficient data"):
            align_pc_and_benchmark_dates(pc, bench)

    def test_nan_removal(self):
        """NaN除去機能"""
        dates = pd.date_range("2024-01-01", periods=100, freq="D")

        pc_values = np.random.randn(100)
        pc_values[10:20] = np.nan  # 10個のNaN

        pc = pd.Series(pc_values, index=dates, dtype=float)
        bench = pd.Series(np.random.randn(100), index=dates, dtype=float)

        pc_aligned, bench_aligned = align_pc_and_benchmark_dates(pc, bench)

        assert len(pc_aligned) == 90  # 100 - 10 NaN
        assert not pc_aligned.isna().any()
        assert not bench_aligned.isna().any()


class TestPerformPcRegression:
    """回帰分析実行テスト"""

    def test_basic_regression(self, sample_pc_series, sample_benchmark_returns):
        """基本的な回帰分析"""
        result = perform_pc_regression(
            sample_pc_series, sample_benchmark_returns, pc_name="PC1"
        )

        assert result.pc_name == "PC1"
        assert -1.0 <= result.correlation <= 1.0
        assert result.r_squared >= 0.0
        assert result.r_squared <= 1.0
        assert result.p_value >= 0.0
        assert result.std_error > 0.0
        assert result.n_observations == 100

    def test_perfect_correlation(self):
        """完全相関（β=1, R²=1）"""
        dates = pd.date_range("2024-01-01", periods=100, freq="D")
        np.random.seed(42)
        bench = pd.Series(np.random.randn(100), index=dates, dtype=float)
        pc = bench.copy()  # 完全一致

        result = perform_pc_regression(pc, bench, pc_name="PC1")

        assert abs(result.correlation - 1.0) < 1e-10
        assert abs(result.beta - 1.0) < 1e-2
        assert result.r_squared > 0.99
        assert result.p_value < 0.001

    def test_no_correlation(self):
        """無相関（β≈0, R²≈0）"""
        dates = pd.date_range("2024-01-01", periods=100, freq="D")
        np.random.seed(42)
        bench = pd.Series(np.random.randn(100), index=dates, dtype=float)
        np.random.seed(99)
        pc = pd.Series(np.random.randn(100), index=dates, dtype=float)

        result = perform_pc_regression(pc, bench, pc_name="PC1")

        assert abs(result.correlation) < 0.2  # ほぼ無相関
        assert result.r_squared < 0.1
        # p値は高い（統計的に有意でない）


class TestCalculateBenchmarkReturns:
    """ベンチマークリターン計算テスト"""

    def test_basic_returns_calculation(self, sample_benchmark_df):
        """基本的なリターン計算"""
        returns = calculate_benchmark_returns(sample_benchmark_df, price_column="Close")

        assert len(returns) == 99  # pct_change()で最初の1行が欠損
        assert not returns.isna().any()
        assert isinstance(returns.index, pd.DatetimeIndex)

    def test_empty_dataframe_raises_error(self):
        """空DataFrameでエラー発生"""
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="Benchmark DataFrame is empty"):
            calculate_benchmark_returns(empty_df)

    def test_invalid_column_raises_error(self, sample_benchmark_df):
        """存在しないカラムでエラー発生"""
        with pytest.raises(ValueError, match="Column 'InvalidCol' not found"):
            calculate_benchmark_returns(sample_benchmark_df, price_column="InvalidCol")

    def test_open_price_returns(self, sample_benchmark_df):
        """Open価格でのリターン計算"""
        returns = calculate_benchmark_returns(sample_benchmark_df, price_column="Open")

        assert len(returns) == 99
        assert not returns.isna().any()


class TestAnalyzePcsVsBenchmark:
    """一括回帰分析テスト"""

    @pytest.fixture
    def sample_pcs_df(self) -> pd.DataFrame:
        """テスト用主成分DataFrame"""
        dates = pd.date_range("2024-01-01", periods=100, freq="D")
        np.random.seed(42)
        return pd.DataFrame(
            {
                "PC1": np.random.randn(100),
                "PC2": np.random.randn(100),
                "PC3": np.random.randn(100),
            },
            index=dates,
        )

    def test_analyze_all_components(self, sample_pcs_df, sample_benchmark_returns):
        """全主成分の分析"""
        results = analyze_pcs_vs_benchmark(sample_pcs_df, sample_benchmark_returns)

        assert len(results) == 3
        assert "PC1" in results
        assert "PC2" in results
        assert "PC3" in results

        for result in results.values():
            assert isinstance(result, RegressionResult)

    def test_max_components_limit(self, sample_pcs_df, sample_benchmark_returns):
        """最大主成分数制限"""
        results = analyze_pcs_vs_benchmark(
            sample_pcs_df, sample_benchmark_returns, max_components=2
        )

        assert len(results) == 2
        assert "PC1" in results
        assert "PC2" in results
        assert "PC3" not in results

    def test_skip_invalid_component(self, sample_benchmark_returns):
        """無効な主成分をスキップ"""
        dates_mismatch = pd.date_range("2025-01-01", periods=20, freq="D")
        pcs_df = pd.DataFrame(
            {"PC1": np.random.randn(20)},
            index=dates_mismatch,
        )

        # 日付が一致しないため、スキップされる（エラーにならない）
        results = analyze_pcs_vs_benchmark(pcs_df, sample_benchmark_returns)

        assert len(results) == 0  # 全てスキップ


class TestIntegration:
    """統合テスト"""

    def test_full_workflow(self):
        """完全ワークフロー: 価格データ → リターン → 回帰分析"""
        # ベンチマーク価格データ生成
        dates = pd.date_range("2024-01-01", periods=100, freq="D")
        np.random.seed(42)
        close = 100 * np.exp(np.cumsum(np.random.randn(100) * 0.01))
        benchmark_df = pd.DataFrame(
            {"Open": close, "High": close, "Low": close, "Close": close},
            index=dates,
        )

        # リターン計算
        benchmark_returns = calculate_benchmark_returns(benchmark_df)

        # 主成分データ生成（β=0.8を模擬）
        pcs_df = pd.DataFrame(
            {"PC1": benchmark_returns.values * 0.8 + np.random.randn(99) * 0.001},
            index=benchmark_returns.index,
        )

        # 回帰分析
        results = analyze_pcs_vs_benchmark(pcs_df, benchmark_returns)

        assert len(results) == 1
        result = results["PC1"]

        # β≈0.8を期待（ノイズがあるので近似）
        assert 0.7 < result.beta < 0.9
        assert result.r_squared > 0.5
        assert result.is_significant()
