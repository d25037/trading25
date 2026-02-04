"""
ポートフォリオリスク分析

相関係数・VaR・分散寄与度等のリスク指標を計算します。
"""

from __future__ import annotations

from typing import Any, Dict, Optional
import pandas as pd
import numpy as np
from loguru import logger


def calculate_correlation_matrix(returns_df: pd.DataFrame) -> pd.DataFrame:
    """
    銘柄間の相関係数行列を計算

    Args:
        returns_df: 日次リターン行列（行: 日付, 列: 銘柄コード）

    Returns:
        pd.DataFrame: 相関係数行列
    """
    correlation = returns_df.corr()
    logger.info(f"Calculated correlation matrix: {correlation.shape}")
    return correlation


def calculate_portfolio_volatility(
    returns_df: pd.DataFrame, weights: Optional[pd.Series[float]] = None
) -> float:
    """
    ポートフォリオ全体のボラティリティ（標準偏差）を計算

    Args:
        returns_df: 日次リターン行列
        weights: 各銘柄のウェイト（指定しない場合は等ウェイト）

    Returns:
        float: 年率換算ボラティリティ
    """
    if weights is None:
        # 等ウェイト
        weights = pd.Series(
            [1.0 / len(returns_df.columns)] * len(returns_df.columns),
            index=returns_df.columns,
        )

    # 共分散行列
    cov_matrix = returns_df.cov()

    # ポートフォリオ分散 = w^T * Cov * w
    portfolio_variance_scalar = weights.dot(cov_matrix).dot(weights)
    portfolio_variance = float(portfolio_variance_scalar.real) if hasattr(portfolio_variance_scalar, 'real') else float(portfolio_variance_scalar)  # type: ignore[union-attr]

    # 年率換算（252営業日）
    annual_volatility: float = np.sqrt(portfolio_variance * 252)

    logger.info(f"Portfolio volatility (annual): {annual_volatility:.4f}")
    return float(annual_volatility)


def calculate_risk_contribution(
    returns_df: pd.DataFrame, weights: Optional[pd.Series[float]] = None
) -> pd.Series[float]:
    """
    各銘柄のリスク寄与度（分散寄与度）を計算

    Args:
        returns_df: 日次リターン行列
        weights: 各銘柄のウェイト（指定しない場合は等ウェイト）

    Returns:
        pd.Series[float]: 各銘柄のリスク寄与度（合計=1.0）
    """
    if weights is None:
        weights = pd.Series(
            [1.0 / len(returns_df.columns)] * len(returns_df.columns),
            index=returns_df.columns,
        )

    # 共分散行列
    cov_matrix = returns_df.cov()

    # ポートフォリオ分散
    portfolio_variance_scalar = weights.dot(cov_matrix).dot(weights)
    portfolio_variance = float(portfolio_variance_scalar.real) if hasattr(portfolio_variance_scalar, 'real') else float(portfolio_variance_scalar)  # type: ignore[union-attr]

    # 各銘柄のリスク寄与度 = w_i * (Cov * w)_i / portfolio_variance
    marginal_risk = cov_matrix.dot(weights)
    risk_contribution: pd.Series[float] = weights * marginal_risk / portfolio_variance

    logger.info(
        f"Calculated risk contribution for {len(risk_contribution)} stocks"
    )
    return risk_contribution


def calculate_var(
    returns_df: pd.DataFrame,
    weights: Optional[pd.Series[float]] = None,
    confidence_level: float = 0.95,
) -> float:
    """
    ポートフォリオのVaR（Value at Risk）を計算（Historical VaR）

    Args:
        returns_df: 日次リターン行列
        weights: 各銘柄のウェイト（指定しない場合は等ウェイト）
        confidence_level: 信頼区間（デフォルト95%）

    Returns:
        float: VaR（負の値 = 損失）
    """
    if weights is None:
        weights = pd.Series(
            [1.0 / len(returns_df.columns)] * len(returns_df.columns),
            index=returns_df.columns,
        )

    # ポートフォリオリターン時系列
    portfolio_returns = returns_df.dot(weights)

    # Historical VaR（パーセンタイル法）
    var = np.percentile(portfolio_returns.dropna(), (1 - confidence_level) * 100)

    logger.info(f"VaR ({confidence_level*100}%): {var:.4%}")
    return float(var)


def calculate_cvar(
    returns_df: pd.DataFrame,
    weights: Optional[pd.Series[float]] = None,
    confidence_level: float = 0.95,
) -> float:
    """
    ポートフォリオのCVaR（Conditional VaR / Expected Shortfall）を計算

    Args:
        returns_df: 日次リターン行列
        weights: 各銘柄のウェイト（指定しない場合は等ウェイト）
        confidence_level: 信頼区間（デフォルト95%）

    Returns:
        float: CVaR（負の値 = 損失）
    """
    if weights is None:
        weights = pd.Series(
            [1.0 / len(returns_df.columns)] * len(returns_df.columns),
            index=returns_df.columns,
        )

    # ポートフォリオリターン時系列
    portfolio_returns = returns_df.dot(weights)

    # VaR閾値
    var = calculate_var(returns_df, weights, confidence_level)

    # CVaR = VaRを下回るリターンの平均
    cvar = portfolio_returns[portfolio_returns <= var].mean()

    logger.info(f"CVaR ({confidence_level*100}%): {cvar:.4%}")
    return float(cvar)


def calculate_sharpe_ratio(
    returns_df: pd.DataFrame,
    weights: Optional[pd.Series[float]] = None,
    risk_free_rate: float = 0.0,
) -> float:
    """
    ポートフォリオのシャープレシオを計算

    Args:
        returns_df: 日次リターン行列
        weights: 各銘柄のウェイト（指定しない場合は等ウェイト）
        risk_free_rate: 無リスク金利（年率）

    Returns:
        float: シャープレシオ
    """
    if weights is None:
        weights = pd.Series(
            [1.0 / len(returns_df.columns)] * len(returns_df.columns),
            index=returns_df.columns,
        )

    # ポートフォリオリターン時系列
    portfolio_returns = returns_df.dot(weights)

    # 年率換算平均リターン
    annual_return = portfolio_returns.mean() * 252

    # 年率換算ボラティリティ
    annual_volatility = portfolio_returns.std() * np.sqrt(252)

    # シャープレシオ
    if annual_volatility == 0:
        sharpe = 0.0
    else:
        sharpe = (annual_return - risk_free_rate) / annual_volatility

    logger.info(f"Sharpe Ratio: {sharpe:.4f}")
    return float(sharpe)


def calculate_diversification_metrics(
    returns_df: pd.DataFrame,
) -> Dict[str, float]:
    """
    分散効果を示す指標を計算

    Args:
        returns_df: 日次リターン行列

    Returns:
        Dict[str, float]: 分散効果指標
            - avg_correlation: 平均相関係数
            - max_correlation: 最大相関係数
            - min_correlation: 最小相関係数
            - diversification_ratio: 分散比率（>1で分散効果あり）
    """
    # 相関係数行列
    corr_matrix = returns_df.corr()

    # 対角成分を除外（自己相関=1.0を除く）
    mask = np.ones(corr_matrix.shape, dtype=bool)
    np.fill_diagonal(mask, False)
    off_diagonal = corr_matrix.where(mask)

    avg_corr = off_diagonal.mean().mean()
    max_corr = off_diagonal.max().max()
    min_corr = off_diagonal.min().min()

    # 分散比率 = (等ウェイトポートフォリオの加重平均ボラティリティ) / (ポートフォリオボラティリティ)
    individual_vols = returns_df.std()
    avg_individual_vol = individual_vols.mean()
    portfolio_vol = calculate_portfolio_volatility(returns_df)

    if portfolio_vol > 0:
        diversification_ratio = (avg_individual_vol * np.sqrt(252)) / portfolio_vol
    else:
        diversification_ratio = 1.0

    logger.info(
        f"Diversification metrics: avg_corr={avg_corr:.3f}, div_ratio={diversification_ratio:.3f}"
    )

    return {
        "avg_correlation": float(avg_corr),
        "max_correlation": float(max_corr),
        "min_correlation": float(min_corr),
        "diversification_ratio": float(diversification_ratio),
    }


def analyze_portfolio_risk(
    returns_df: pd.DataFrame,
    weights: Optional[pd.Series[float]] = None,
    confidence_level: float = 0.95,
    risk_free_rate: float = 0.0,
) -> Dict[str, Any]:
    """
    ポートフォリオの包括的リスク分析

    Args:
        returns_df: 日次リターン行列
        weights: 各銘柄のウェイト（指定しない場合は等ウェイト）
        confidence_level: VaR/CVaRの信頼区間
        risk_free_rate: シャープレシオ計算用無リスク金利

    Returns:
        Dict[str, any]: 分析結果
    """
    logger.info("Starting comprehensive portfolio risk analysis")

    # ウェイト設定
    if weights is None:
        weights = pd.Series(
            [1.0 / len(returns_df.columns)] * len(returns_df.columns),
            index=returns_df.columns,
        )

    # 各指標を計算
    results = {
        "correlation_matrix": calculate_correlation_matrix(returns_df),
        "portfolio_volatility": calculate_portfolio_volatility(returns_df, weights),
        "risk_contribution": calculate_risk_contribution(returns_df, weights),
        "var": calculate_var(returns_df, weights, confidence_level),
        "cvar": calculate_cvar(returns_df, weights, confidence_level),
        "sharpe_ratio": calculate_sharpe_ratio(
            returns_df, weights, risk_free_rate
        ),
        "diversification_metrics": calculate_diversification_metrics(returns_df),
        "weights": weights,
        "num_stocks": len(returns_df.columns),
        "num_days": len(returns_df),
    }

    logger.info("Portfolio risk analysis completed")
    return results
