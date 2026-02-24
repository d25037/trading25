"""portfolio_pca.py のテスト"""

import numpy as np
import pandas as pd
import pytest

from src.domains.analytics.portfolio_pca import (
    analyze_stock_clustering,
    calculate_pca_diversification_score,
    get_top_contributors,
    perform_full_pca_analysis,
    perform_pca_analysis,
)


def _returns_df(n=100, stocks=5, seed=42):
    np.random.seed(seed)
    idx = pd.date_range("2025-01-01", periods=n, freq="D")
    data = np.random.randn(n, stocks) * 0.02
    return pd.DataFrame(data, index=idx, columns=[f"S{i}" for i in range(stocks)])


class TestPerformPcaAnalysis:
    def test_result_keys(self):
        result = perform_pca_analysis(_returns_df())
        expected_keys = {
            "pca_model", "explained_variance_ratio", "cumulative_variance_ratio",
            "components", "principal_components", "n_components",
        }
        assert expected_keys <= set(result.keys())

    def test_explained_variance_sums_to_one(self):
        result = perform_pca_analysis(_returns_df())
        total = result["explained_variance_ratio"].sum()
        assert total == pytest.approx(1.0, abs=0.01)

    def test_cumulative_variance_monotonic(self):
        result = perform_pca_analysis(_returns_df())
        cumvar = result["cumulative_variance_ratio"].values
        assert all(cumvar[i] <= cumvar[i + 1] for i in range(len(cumvar) - 1))

    def test_custom_n_components(self):
        result = perform_pca_analysis(_returns_df(), n_components=2)
        assert result["n_components"] == 2

    def test_components_shape(self):
        result = perform_pca_analysis(_returns_df(), n_components=3)
        assert result["components"].shape == (3, 5)

    def test_principal_components_shape(self):
        result = perform_pca_analysis(_returns_df(), n_components=3)
        assert result["principal_components"].shape == (100, 3)

    def test_nan_handling(self):
        df = _returns_df()
        df.iloc[0, 0] = np.nan
        result = perform_pca_analysis(df)
        assert result["n_components"] > 0


class TestGetTopContributors:
    def test_returns_correct_count(self):
        pca_result = perform_pca_analysis(_returns_df())
        top = get_top_contributors(pca_result["components"], pc_index=0, top_n=3)
        assert len(top) == 3

    def test_invalid_pc_index(self):
        pca_result = perform_pca_analysis(_returns_df(), n_components=2)
        with pytest.raises(ValueError, match="not found"):
            get_top_contributors(pca_result["components"], pc_index=5)

    def test_sorted_by_absolute_value(self):
        pca_result = perform_pca_analysis(_returns_df())
        top = get_top_contributors(pca_result["components"], pc_index=0, top_n=5)
        abs_values = top.abs().values
        assert all(abs_values[i] >= abs_values[i + 1] for i in range(len(abs_values) - 1))


class TestPcaDiversificationScore:
    def test_result_keys(self):
        pca_result = perform_pca_analysis(_returns_df())
        score = calculate_pca_diversification_score(pca_result["explained_variance_ratio"])
        expected_keys = {"n_components_for_threshold", "diversification_score", "first_pc_variance_ratio"}
        assert expected_keys <= set(score.keys())

    def test_first_pc_ratio(self):
        pca_result = perform_pca_analysis(_returns_df())
        score = calculate_pca_diversification_score(pca_result["explained_variance_ratio"])
        assert 0 < score["first_pc_variance_ratio"] <= 1.0


class TestAnalyzeStockClustering:
    def test_returns_dataframe(self):
        pca_result = perform_pca_analysis(_returns_df())
        clustering = analyze_stock_clustering(pca_result["components"])
        assert isinstance(clustering, pd.DataFrame)
        assert "Dominant_PC" in clustering.columns
        assert len(clustering) == 5


class TestPerformFullPcaAnalysis:
    def test_comprehensive(self):
        result = perform_full_pca_analysis(_returns_df())
        expected_keys = {"diversification_score", "stock_clustering", "top_contributors_per_pc"}
        assert expected_keys <= set(result.keys())
        assert "PC1" in result["top_contributors_per_pc"]
