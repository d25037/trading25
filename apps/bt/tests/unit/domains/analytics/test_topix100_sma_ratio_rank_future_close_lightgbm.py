from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import src.domains.analytics.topix100_sma_ratio_rank_future_close_lightgbm as lightgbm_module
from src.domains.analytics.topix100_sma_ratio_rank_future_close import (
    HORIZON_ORDER,
    run_topix100_sma_ratio_rank_future_close_research,
)
from src.domains.analytics.topix100_sma_ratio_rank_future_close_lightgbm import (
    DEFAULT_WALKFORWARD_STEP,
    DEFAULT_WALKFORWARD_TEST_WINDOW,
    DEFAULT_WALKFORWARD_TRAIN_WINDOW,
    LIGHTGBM_LIBOMP_INSTALL_HINT,
    LIGHTGBM_RESEARCH_INSTALL_HINT,
    _run_fixed_split_diagnostic,
    format_topix100_sma_ratio_rank_future_close_lightgbm_notebook_error,
    run_topix100_sma_ratio_rank_future_close_lightgbm_research,
)
from src.domains.analytics.topix_sma_ratio_rank_future_close_selection import (
    _analyze_ranked_panel,
    _extract_global_row,
)
from src.domains.analytics.topix_sma_ratio_rank_future_close_support import (
    RANKING_FEATURE_ORDER,
)
from src.domains.backtest.core.walkforward import generate_walkforward_splits
from tests.unit.analytics_market_research_db import build_topix100_research_market_db


class FakeLGBMRanker:
    instances: list["FakeLGBMRanker"] = []

    def __init__(self, **kwargs):  # noqa: ANN003
        self.kwargs = kwargs
        self.fit_group: list[int] = []
        self.fit_row_count = 0
        self.fit_indices: list[int] = []
        self.predict_row_count = 0
        self.predict_indices: list[int] = []
        self.feature_importances_ = np.array([], dtype=float)
        type(self).instances.append(self)

    def fit(self, X, y, group):  # noqa: ANN001, ANN201
        _ = y
        frame = pd.DataFrame(X)
        self.fit_row_count = int(len(frame))
        self.fit_indices = [int(value) for value in frame.index.tolist()]
        self.fit_group = [int(value) for value in group]
        self.feature_importances_ = np.array([60, 50, 40, 30, 20, 10], dtype=float)
        return self

    def predict(self, X):  # noqa: ANN001, ANN201
        frame = pd.DataFrame(X)
        self.predict_row_count = int(len(frame))
        self.predict_indices = [int(value) for value in frame.index.tolist()]
        weights = np.array([6, 5, 4, 3, 2, 1], dtype=float)
        return frame.to_numpy(dtype=float) @ weights


@pytest.fixture(scope="module")
def analytics_db_path(tmp_path_factory: pytest.TempPathFactory) -> str:
    return build_topix100_research_market_db(
        tmp_path_factory.mktemp("topix100-sma-lightgbm") / "market-two-split.duckdb",
        start_date="2021-01-04",
        periods=320,
    )


@pytest.fixture(scope="module")
def long_analytics_db_path(tmp_path_factory: pytest.TempPathFactory) -> str:
    return build_topix100_research_market_db(
        tmp_path_factory.mktemp("topix100-sma-lightgbm") / "market-walkforward-default.duckdb",
        start_date="2018-01-04",
        periods=1200,
    )


@pytest.fixture(scope="module")
def base_result(analytics_db_path: str):
    return run_topix100_sma_ratio_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )


@pytest.fixture(scope="module")
def long_base_result(long_analytics_db_path: str):
    return run_topix100_sma_ratio_rank_future_close_research(
        long_analytics_db_path,
        min_constituents_per_day=10,
    )


@pytest.fixture(scope="module", autouse=True)
def fake_lightgbm_ranker_loader():
    original_loader = lightgbm_module._load_lightgbm_ranker_cls
    lightgbm_module._load_lightgbm_ranker_cls = lambda: FakeLGBMRanker
    yield
    lightgbm_module._load_lightgbm_ranker_cls = original_loader


@pytest.fixture(scope="module")
def default_walkforward_result(long_base_result):
    FakeLGBMRanker.instances = []
    result = run_topix100_sma_ratio_rank_future_close_lightgbm_research(
        long_base_result,
        include_diagnostic=False,
    )
    FakeLGBMRanker.instances = []
    return result


@pytest.fixture(scope="module")
def short_walkforward_result_bundle(base_result):
    FakeLGBMRanker.instances = []
    result = run_topix100_sma_ratio_rank_future_close_lightgbm_research(
        base_result,
        train_window=60,
        test_window=20,
        step=20,
        include_diagnostic=False,
    )
    instances = list(FakeLGBMRanker.instances)
    FakeLGBMRanker.instances = []
    return result, instances


@pytest.fixture(scope="module")
def fixed_split_diagnostic(base_result):
    FakeLGBMRanker.instances = []
    diagnostic = _run_fixed_split_diagnostic(
        base_result,
        ranker_cls=FakeLGBMRanker,
        feature_columns=tuple(RANKING_FEATURE_ORDER),
    )
    FakeLGBMRanker.instances = []
    return diagnostic


def _build_scored_df(event_panel_df: pd.DataFrame, horizon_key: str) -> pd.DataFrame:
    return (
        event_panel_df.dropna(subset=[f"{horizon_key}_return"])
        .sort_values(["date", "code"])
        .reset_index(drop=True)
    )


def test_walkforward_helper_uses_default_split_config(
    long_base_result,
    default_walkforward_result,
) -> None:
    lightgbm_result = default_walkforward_result

    expected_splits = generate_walkforward_splits(
        pd.DatetimeIndex(long_base_result.event_panel_df["date"].unique()),
        train_window=DEFAULT_WALKFORWARD_TRAIN_WINDOW,
        test_window=DEFAULT_WALKFORWARD_TEST_WINDOW,
        step=DEFAULT_WALKFORWARD_STEP,
    )
    config_row = lightgbm_result.walkforward.split_config_df.iloc[0]

    assert int(config_row["train_window"]) == DEFAULT_WALKFORWARD_TRAIN_WINDOW
    assert int(config_row["test_window"]) == DEFAULT_WALKFORWARD_TEST_WINDOW
    assert int(config_row["step"]) == DEFAULT_WALKFORWARD_STEP
    assert int(config_row["generated_split_count"]) == len(expected_splits)
    assert (
        lightgbm_result.walkforward.split_coverage_df["split_index"].nunique()
        == len(expected_splits)
    )


def test_walkforward_helper_trains_lightgbm_on_train_only_dates_and_test_only_scores(
    base_result,
    short_walkforward_result_bundle,
) -> None:
    lightgbm_result, ranker_instances = short_walkforward_result_bundle

    coverage_df = lightgbm_result.walkforward.split_coverage_df.sort_values(
        ["split_index", "selected_horizon_key"],
        kind="stable",
    ).reset_index(drop=True)
    expected_rows = coverage_df[
        (coverage_df["lightgbm_train_row_count"] > 0)
        & (coverage_df["lightgbm_test_row_count"] > 0)
    ].copy()
    walkforward_instances = ranker_instances[: len(expected_rows)]

    assert len(walkforward_instances) == len(expected_rows)
    assert len(ranker_instances) == len(expected_rows)

    for row, instance in zip(
        expected_rows.to_dict(orient="records"),
        walkforward_instances,
        strict=True,
    ):
        horizon_key = str(row["selected_horizon_key"])
        scored_df = _build_scored_df(base_result.event_panel_df, horizon_key)
        train_df = scored_df[
            (scored_df["date"] >= row["train_start"])
            & (scored_df["date"] <= row["train_end"])
        ].copy()
        test_df = scored_df[
            (scored_df["date"] >= row["test_start"])
            & (scored_df["date"] <= row["test_end"])
        ].copy()

        assert instance.kwargs["objective"] == "lambdarank"
        assert instance.kwargs["importance_type"] == "gain"
        assert instance.fit_group == (
            train_df.groupby("date", sort=False)["code"].size().astype(int).tolist()
        )
        assert instance.fit_row_count == len(train_df)
        assert instance.predict_row_count == len(test_df)

        fit_dates = set(scored_df.loc[instance.fit_indices, "date"].tolist())
        predict_dates = set(scored_df.loc[instance.predict_indices, "date"].tolist())
        assert fit_dates
        assert predict_dates
        assert all(row["train_start"] <= date <= row["train_end"] for date in fit_dates)
        assert all(row["test_start"] <= date <= row["test_end"] for date in predict_dates)
        assert fit_dates.isdisjoint(predict_dates)


def test_walkforward_baseline_selection_uses_train_metrics_and_oos_panel_has_no_duplicates(
    base_result,
    short_walkforward_result_bundle,
) -> None:
    lightgbm_result, _ = short_walkforward_result_bundle

    walkforward = lightgbm_result.walkforward
    for row in walkforward.baseline_selected_feature_df.to_dict(orient="records"):
        train_ranked_panel_df = base_result.ranked_panel_df[
            (base_result.ranked_panel_df["date"] >= row["train_start"])
            & (base_result.ranked_panel_df["date"] <= row["train_end"])
        ].copy()
        train_analysis = _analyze_ranked_panel(train_ranked_panel_df)
        global_row = _extract_global_row(
            train_analysis["global_significance_df"],
            ranking_feature=str(row["ranking_feature"]),
            horizon_key=str(row["horizon_key"]),
        )
        assert global_row is not None
        assert float(row["train_q1_minus_q10_mean"]) == pytest.approx(
            float(global_row["q1_minus_q10_mean"])
        )

    test_dates = set(walkforward.split_coverage_df["test_start"].tolist()) | set(
        walkforward.split_coverage_df["test_end"].tolist()
    )
    assert not walkforward.ranked_panel_df.empty
    assert not walkforward.ranked_panel_df.duplicated(
        subset=["date", "code", "selected_horizon_key"]
    ).any()
    assert set(walkforward.ranked_panel_df["date"]).issubset(
        set(
            base_result.event_panel_df[
                (base_result.event_panel_df["date"] >= min(test_dates))
                & (base_result.event_panel_df["date"] <= max(test_dates))
            ]["date"]
        )
    )


def test_walkforward_helper_returns_oos_comparison_and_feature_importance(
    short_walkforward_result_bundle,
    fixed_split_diagnostic,
) -> None:
    lightgbm_result, _ = short_walkforward_result_bundle

    comparison = lightgbm_result.walkforward.comparison_summary_df
    split_spread = lightgbm_result.walkforward.split_spread_df
    assert set(comparison["model_name"]) == {"baseline", "lightgbm"}
    assert set(split_spread["model_name"]) == {"baseline", "lightgbm"}
    assert set(comparison["selected_horizon_key"]) == set(HORIZON_ORDER)
    assert set(split_spread["selected_horizon_key"]) == set(HORIZON_ORDER)

    for horizon_key in HORIZON_ORDER:
        importance_df = lightgbm_result.walkforward.feature_importance_df[
            lightgbm_result.walkforward.feature_importance_df["selected_horizon_key"]
            == horizon_key
        ]
        assert not importance_df.empty
        assert importance_df["mean_importance_gain"].tolist() == sorted(
            importance_df["mean_importance_gain"].tolist(),
            reverse=True,
        )
        assert importance_df["importance_rank"].tolist() == [1, 2, 3, 4, 5, 6]

    assert not lightgbm_result.walkforward.exploratory_gate_df.empty
    assert lightgbm_result.walkforward.overall_gate_status in {
        "passed",
        "failed",
        "insufficient_coverage",
    }
    assert not fixed_split_diagnostic.comparison_summary_df.empty
    assert not fixed_split_diagnostic.feature_importance_df.empty


def test_notebook_error_formatter_surfaces_install_and_libomp_hints() -> None:
    missing_message = (
        format_topix100_sma_ratio_rank_future_close_lightgbm_notebook_error(
            ModuleNotFoundError("No module named 'lightgbm'")
        )
    )
    runtime_message = (
        format_topix100_sma_ratio_rank_future_close_lightgbm_notebook_error(
            OSError("dlopen(...): Library not loaded: libomp.dylib")
        )
    )

    assert LIGHTGBM_RESEARCH_INSTALL_HINT in missing_message
    assert LIGHTGBM_LIBOMP_INSTALL_HINT in runtime_message
