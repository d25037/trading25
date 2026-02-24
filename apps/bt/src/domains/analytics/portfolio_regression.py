"""
ポートフォリオ回帰分析

主成分（PCs）と市場指数（TOPIX等）の相関・回帰分析を実行します。
β係数・R²値・統計的有意性を計算し、各PCの市場感応度を定量化します。
"""

from __future__ import annotations

from typing import Dict, Tuple, Optional, Any, cast
from dataclasses import dataclass
import pandas as pd
from scipy import stats
from loguru import logger


@dataclass
class RegressionResult:
    """線形回帰分析結果（型安全）"""

    pc_name: str  # 主成分名（例: "PC1"）
    correlation: float  # Pearson相関係数 [-1, 1]
    alpha: float  # 回帰切片（定数項）
    beta: float  # 回帰係数（傾き・TOPIX感応度）
    r_squared: float  # 決定係数 [0, 1]
    p_value: float  # β係数のp値（統計的有意性）
    std_error: float  # β係数の標準誤差
    n_observations: int  # 観測データ数

    def is_significant(self, alpha_level: float = 0.05) -> bool:
        """
        統計的に有意かどうか判定

        Args:
            alpha_level: 有意水準（デフォルト: 0.05）

        Returns:
            bool: p値が有意水準未満の場合True
        """
        return self.p_value < alpha_level

    def to_dict(self) -> Dict[str, Any]:
        """
        辞書形式に変換（表示用）

        Returns:
            Dict[str, Any]: 回帰結果の辞書
        """
        return {
            "pc_name": self.pc_name,
            "correlation": self.correlation,
            "alpha": self.alpha,
            "beta": self.beta,
            "r_squared": self.r_squared,
            "p_value": self.p_value,
            "std_error": self.std_error,
            "n_observations": self.n_observations,
        }


def calculate_benchmark_returns(
    benchmark_df: pd.DataFrame,
    price_column: str = "Close",
) -> pd.Series[float]:
    """
    ベンチマーク価格データから日次リターンを計算

    Args:
        benchmark_df: ベンチマーク価格DataFrame（VectorBT標準形式: OHLC）
        price_column: 使用する価格カラム（デフォルト: "Close"）

    Returns:
        pd.Series[float]: 日次リターン時系列（DatetimeIndex付き）

    Raises:
        ValueError: 価格データが空・カラムが存在しない場合

    Note:
        - pct_change()で対数リターンに近い単純リターンを計算
        - 最初の行（NaN）は自動削除される
    """
    if benchmark_df.empty:
        raise ValueError("Benchmark DataFrame is empty")

    if price_column not in benchmark_df.columns:
        raise ValueError(
            f"Column '{price_column}' not found in benchmark data. "
            f"Available: {list(benchmark_df.columns)}"
        )

    # 日次リターン計算
    returns = benchmark_df[price_column].pct_change()
    returns = returns.dropna()

    logger.debug(
        f"Calculated {len(returns)} daily returns from {price_column} "
        f"({returns.index[0]} to {returns.index[-1]})"
    )

    return returns


def align_pc_and_benchmark_dates(
    pc_series: pd.Series[float],
    benchmark_returns: pd.Series[float],
) -> Tuple[pd.Series[float], pd.Series[float]]:
    """
    PCシリーズとベンチマークリターンの日付範囲を揃える

    Args:
        pc_series: 主成分時系列（DatetimeIndexを持つSeries）
        benchmark_returns: ベンチマークリターン時系列（DatetimeIndexを持つSeries）

    Returns:
        Tuple[pd.Series[float], pd.Series[float]]: 揃えたPC・ベンチマークリターン

    Raises:
        ValueError: 共通期間が存在しない場合・データ数が不足する場合

    Note:
        - 共通期間のみを抽出（inner join）
        - NaN値を除去
        - 最低30観測値を要求（統計的信頼性のため）
    """
    # 共通期間を抽出
    common_index = pc_series.index.intersection(benchmark_returns.index)

    if len(common_index) == 0:
        raise ValueError(
            f"No overlapping dates between PC data ({pc_series.index[0]} to {pc_series.index[-1]}) "
            f"and benchmark data ({benchmark_returns.index[0]} to {benchmark_returns.index[-1]})"
        )

    # 共通期間でアライン
    pc_aligned = pc_series.reindex(common_index)
    benchmark_aligned = benchmark_returns.reindex(common_index)

    # NaN除去
    valid_mask = pc_aligned.notna() & benchmark_aligned.notna()
    pc_clean = pc_aligned[valid_mask]
    benchmark_clean = benchmark_aligned[valid_mask]

    # 最低観測数チェック
    MIN_OBSERVATIONS = 30
    if len(pc_clean) < MIN_OBSERVATIONS:
        raise ValueError(
            f"Insufficient data for regression: {len(pc_clean)} observations "
            f"(minimum {MIN_OBSERVATIONS} required)"
        )

    logger.debug(
        f"Aligned {len(pc_clean)} observations "
        f"from {pc_clean.index[0]} to {pc_clean.index[-1]}"
    )

    return pc_clean, benchmark_clean


def perform_pc_regression(
    pc_series: pd.Series[float],
    benchmark_returns: pd.Series[float],
    pc_name: str = "PC",
) -> RegressionResult:
    """
    単一主成分とベンチマークの線形回帰分析

    Args:
        pc_series: 主成分時系列
        benchmark_returns: ベンチマークリターン時系列
        pc_name: 主成分名（表示用）

    Returns:
        RegressionResult: 回帰分析結果

    Raises:
        ValueError: データ不足・日付不整合の場合

    Formula:
        PC = α + β * TOPIX_return + ε
        - β: TOPIX感応度（1標準偏差のTOPIX変動に対するPCの変動）
        - R²: TOPIXで説明できるPCの分散比率
        - p-value: β係数の統計的有意性（H0: β=0）
    """
    # 日付アライメント
    pc_aligned, benchmark_aligned = align_pc_and_benchmark_dates(
        pc_series, benchmark_returns
    )

    # Pearson相関係数
    pearson_result = stats.pearsonr(pc_aligned.values, benchmark_aligned.values)
    correlation = cast(float, pearson_result[0])

    # 線形回帰（scipy.stats.linregress）
    # Returns: LinregressResult with slope, intercept, rvalue, pvalue, stderr
    linreg_result = stats.linregress(
        benchmark_aligned.values,  # X: 独立変数（TOPIX）
        pc_aligned.values,  # Y: 従属変数（PC）
    )

    r_squared = linreg_result.rvalue**2

    logger.info(
        f"{pc_name}: corr={correlation:.4f}, β={linreg_result.slope:.4f}, "
        f"R²={r_squared:.4f}, p={linreg_result.pvalue:.4e}"
    )

    return RegressionResult(
        pc_name=pc_name,
        correlation=float(correlation),
        alpha=float(linreg_result.intercept),
        beta=float(linreg_result.slope),
        r_squared=float(r_squared),
        p_value=float(linreg_result.pvalue),
        std_error=float(linreg_result.stderr),
        n_observations=len(pc_aligned),
    )


def analyze_pcs_vs_benchmark(
    principal_components_df: pd.DataFrame,
    benchmark_returns: pd.Series[float],
    max_components: Optional[int] = None,
) -> Dict[str, RegressionResult]:
    """
    全主成分とベンチマークの回帰分析を一括実行

    Args:
        principal_components_df: 主成分時系列DataFrame（列: PC1, PC2, ...）
        benchmark_returns: ベンチマークリターン時系列
        max_components: 分析する最大主成分数（Noneの場合は全成分）

    Returns:
        Dict[str, RegressionResult]: {PC名: 回帰結果}

    Example:
        >>> results = analyze_pcs_vs_benchmark(pca_result['principal_components'], topix_returns)
        >>> print(results['PC1'].beta)  # 第1主成分のTOPIX感応度
    """
    if max_components is None:
        max_components = len(principal_components_df.columns)

    results = {}

    for pc_name in list(principal_components_df.columns[:max_components]):
        try:
            pc_series = principal_components_df[pc_name]
            result = perform_pc_regression(
                pc_series, benchmark_returns, pc_name=pc_name
            )
            results[pc_name] = result

        except ValueError as e:
            logger.warning(f"Skipping {pc_name}: {e}")
            continue

    logger.info(f"Completed regression analysis for {len(results)} components")
    return results
