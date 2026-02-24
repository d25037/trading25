"""
ポートフォリオ主成分分析（PCA）

銘柄間の共変動構造を主成分分析で分解し、リスク構造を理解します。
"""

from __future__ import annotations

from typing import Any, Dict, Optional
import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from loguru import logger


def perform_pca_analysis(
    returns_df: pd.DataFrame, n_components: Optional[int] = None
) -> Dict[str, Any]:
    """
    主成分分析（PCA）を実行

    Args:
        returns_df: 日次リターン行列（行: 日付, 列: 銘柄コード）
        n_components: 抽出する主成分数（指定しない場合は全成分）

    Returns:
        Dict[str, any]: PCA分析結果
            - pca_model: PCAモデル
            - explained_variance_ratio: 各主成分の分散説明率
            - cumulative_variance_ratio: 累積分散説明率
            - components: 主成分負荷量（主成分×銘柄）
            - principal_components: 主成分スコア時系列（日付×主成分）
            - n_components: 主成分数
    """
    logger.info(
        f"Performing PCA analysis on {returns_df.shape[1]} stocks, {returns_df.shape[0]} days"
    )

    # 欠損値処理（NaNを0で埋める、または行削除）
    returns_clean = returns_df.fillna(0)

    # 標準化（平均0、分散1に正規化）
    scaler = StandardScaler()
    returns_scaled = scaler.fit_transform(returns_clean)

    # PCA実行
    if n_components is None:
        n_components = min(returns_clean.shape)

    pca = PCA(n_components=n_components)
    principal_components = pca.fit_transform(returns_scaled)

    # 主成分負荷量（各銘柄が各主成分にどれだけ寄与しているか）
    components_df = pd.DataFrame(
        pca.components_,
        columns=returns_df.columns,
        index=[f"PC{i+1}" for i in range(pca.n_components_)],
    )

    # 主成分スコア時系列
    principal_components_df = pd.DataFrame(
        principal_components,
        index=returns_df.index,
        columns=[f"PC{i+1}" for i in range(pca.n_components_)],
    )

    # 分散説明率
    explained_variance = pca.explained_variance_ratio_
    cumulative_variance = np.cumsum(explained_variance)

    logger.info(
        f"PCA completed: {pca.n_components_} components, "
        f"cumulative variance explained: {cumulative_variance[-1]:.2%}"
    )

    return {
        "pca_model": pca,
        "scaler": scaler,
        "explained_variance_ratio": pd.Series(
            explained_variance, index=[f"PC{i+1}" for i in range(len(explained_variance))]
        ),
        "cumulative_variance_ratio": pd.Series(
            cumulative_variance, index=[f"PC{i+1}" for i in range(len(cumulative_variance))]
        ),
        "components": components_df,
        "principal_components": principal_components_df,
        "n_components": pca.n_components_,
    }


def get_top_contributors(
    components_df: pd.DataFrame, pc_index: int = 0, top_n: int = 5
) -> pd.Series[float]:
    """
    指定した主成分への貢献度が高い銘柄を取得

    Args:
        components_df: 主成分負荷量DataFrame（主成分×銘柄）
        pc_index: 主成分インデックス（0-indexed）
        top_n: 取得する上位銘柄数

    Returns:
        pd.Series[float]: 貢献度の高い銘柄（絶対値でソート）
    """
    pc_name = f"PC{pc_index + 1}"
    if pc_name not in components_df.index:
        raise ValueError(f"Principal component {pc_name} not found")

    loadings = components_df.loc[pc_name]

    # 絶対値でソート（正負問わず影響が大きい銘柄）
    top_loadings = loadings.abs().sort_values(ascending=False).head(top_n)

    # 元の符号付き値を返す（loadingsはSeriesなのでインデックス指定で確実にSeriesを返す）
    selected = loadings[top_loadings.index]
    result: pd.Series[float] = pd.Series(selected.values, index=selected.index, dtype=float)
    return result


def calculate_pca_diversification_score(
    explained_variance_ratio: pd.Series[float], threshold: float = 0.8
) -> Dict[str, Any]:
    """
    PCAに基づく分散効果スコアを計算

    Args:
        explained_variance_ratio: 各主成分の分散説明率
        threshold: 累積分散説明率の閾値（デフォルト80%）

    Returns:
        Dict[str, any]: 分散効果スコア
            - n_components_for_threshold: 閾値達成に必要な主成分数
            - diversification_score: 分散スコア（低いほど分散が効いている）
    """
    values = explained_variance_ratio.to_numpy()
    cumulative = np.cumsum(values)

    # 閾値を超える最小の主成分数
    n_components_needed = np.argmax(cumulative >= threshold) + 1

    # 分散スコア: 第1主成分の寄与率（低いほど分散が効いている）
    first_pc_ratio = explained_variance_ratio.iloc[0]

    logger.info(
        f"Diversification score: {n_components_needed} components needed for {threshold:.0%} variance"
    )

    return {
        "n_components_for_threshold": n_components_needed,
        "diversification_score": float(first_pc_ratio),
        "first_pc_variance_ratio": float(first_pc_ratio),
    }


def analyze_stock_clustering(
    components_df: pd.DataFrame, n_top_pcs: int = 3
) -> pd.DataFrame:
    """
    主成分負荷量から銘柄のクラスタリング傾向を分析

    Args:
        components_df: 主成分負荷量DataFrame（主成分×銘柄）
        n_top_pcs: 使用する上位主成分数

    Returns:
        pd.DataFrame: 銘柄ごとの主成分負荷量（銘柄×主成分）
    """
    # 上位n_top_pcs個の主成分のみを使用
    top_components = components_df.iloc[:n_top_pcs, :]

    # 転置（銘柄×主成分）
    stock_loadings = top_components.T

    # 各銘柄で最も影響の大きい主成分を特定
    dominant_pc = stock_loadings.abs().idxmax(axis=1)

    stock_loadings["Dominant_PC"] = dominant_pc

    logger.info(
        f"Analyzed clustering for {len(stock_loadings)} stocks using top {n_top_pcs} PCs"
    )

    return stock_loadings


def perform_full_pca_analysis(
    returns_df: pd.DataFrame,
    n_components: Optional[int] = None,
    variance_threshold: float = 0.8,
) -> Dict[str, Any]:
    """
    包括的PCA分析

    Args:
        returns_df: 日次リターン行列
        n_components: 抽出する主成分数
        variance_threshold: 累積分散説明率の閾値

    Returns:
        Dict[str, any]: 包括的分析結果
    """
    logger.info("Starting comprehensive PCA analysis")

    # PCA実行
    pca_results = perform_pca_analysis(returns_df, n_components)

    # 分散効果スコア
    diversification_score = calculate_pca_diversification_score(
        pca_results["explained_variance_ratio"], variance_threshold
    )

    # 銘柄クラスタリング
    stock_clustering = analyze_stock_clustering(
        pca_results["components"], n_top_pcs=3
    )

    # 各主成分への上位貢献銘柄
    top_contributors_per_pc = {}
    for i in range(min(3, pca_results["n_components"])):
        top_contributors_per_pc[f"PC{i+1}"] = get_top_contributors(
            pca_results["components"], pc_index=i, top_n=5
        )

    # 結果を統合
    results = {
        **pca_results,
        "diversification_score": diversification_score,
        "stock_clustering": stock_clustering,
        "top_contributors_per_pc": top_contributors_per_pc,
    }

    logger.info("Comprehensive PCA analysis completed")
    return results
