from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import src.domains.analytics.topix100_sma_ratio_rank_future_close_lightgbm as lightgbm_module
from src.domains.analytics.topix_rank_future_close_core import _assign_feature_deciles
from src.domains.analytics.topix100_sma_ratio_rank_future_close import (
    HORIZON_ORDER,
    Topix100SmaRatioRankFutureCloseResearchResult,
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
    _build_composite_ranked_panel,
    _extract_global_row,
)
from src.domains.analytics.topix_sma_ratio_rank_future_close_support import (
    RANKING_FEATURE_LABEL_MAP,
    RANKING_FEATURE_ORDER,
)
from src.domains.backtest.core.walkforward import generate_walkforward_splits


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


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame()


def _build_synthetic_event_panel(
    *,
    start_date: str,
    periods: int,
    code_count: int = 10,
) -> pd.DataFrame:
    dates = pd.bdate_range(start_date, periods=periods).strftime("%Y-%m-%d").tolist()
    midpoint = (code_count - 1) / 2.0
    records: list[dict[str, object]] = []

    for day_index, date_value in enumerate(dates):
        seasonal_fast = float(np.sin(day_index / 13.0))
        seasonal_slow = float(np.cos(day_index / 29.0))
        for code_index in range(code_count):
            centered_code = float(code_index - midpoint)
            code = f"{1000 + code_index}"
            jitter = float(
                0.0009 * np.sin((day_index * 0.37) + (code_index * 1.11))
            )
            close = 100.0 + (0.04 * day_index) + (0.7 * code_index)
            volume = 1_000_000.0 + (7_500.0 * code_index) + (75.0 * day_index)
            price_sma_5_20 = (
                (0.018 * centered_code)
                + (0.003 * seasonal_fast)
                - (0.0004 * (day_index % 5))
                + (0.35 * jitter)
            )
            price_sma_20_80 = (
                (0.024 * centered_code)
                + (0.0035 * seasonal_slow)
                + (0.0003 * (day_index % 7))
                + (0.25 * jitter)
            )
            price_sma_50_150 = (
                (0.017 * centered_code)
                - (0.0025 * seasonal_fast)
                + (0.0002 * (day_index % 9))
                - (0.20 * jitter)
            )
            volume_sma_5_20 = (0.014 * centered_code) + (0.002 * seasonal_slow) + (0.30 * jitter)
            volume_sma_20_80 = (0.011 * centered_code) - (0.0015 * seasonal_fast) - (0.22 * jitter)
            volume_sma_50_150 = (0.009 * centered_code) + (0.0012 * seasonal_slow) + (0.18 * jitter)

            t_plus_1_return = (
                (0.17 * price_sma_5_20)
                + (0.06 * price_sma_20_80)
                - (0.03 * volume_sma_50_150)
                + (0.0007 * seasonal_fast)
                + (0.40 * jitter)
            )
            t_plus_5_return = (
                (0.15 * price_sma_20_80)
                + (0.05 * price_sma_50_150)
                - (0.025 * volume_sma_20_80)
                + (0.0009 * seasonal_slow)
                - (0.30 * jitter)
            )
            t_plus_10_return = (
                (0.13 * price_sma_50_150)
                + (0.05 * price_sma_20_80)
                - (0.02 * volume_sma_5_20)
                + (0.0011 * seasonal_fast)
                + (0.25 * jitter)
            )

            record = {
                "date": date_value,
                "code": code,
                "company_name": f"Company {code}",
                "close": float(close),
                "volume": float(volume),
                "date_constituent_count": code_count,
                "price_sma_5_20": float(price_sma_5_20),
                "price_sma_20_80": float(price_sma_20_80),
                "price_sma_50_150": float(price_sma_50_150),
                "volume_sma_5_20": float(volume_sma_5_20),
                "volume_sma_20_80": float(volume_sma_20_80),
                "volume_sma_50_150": float(volume_sma_50_150),
            }
            for horizon_key, horizon_days, horizon_return in (
                ("t_plus_1", 1, t_plus_1_return),
                ("t_plus_5", 5, t_plus_5_return),
                ("t_plus_10", 10, t_plus_10_return),
            ):
                if day_index + horizon_days >= periods:
                    record[f"{horizon_key}_close"] = np.nan
                    record[f"{horizon_key}_return"] = np.nan
                else:
                    record[f"{horizon_key}_close"] = float(close * (1.0 + horizon_return))
                    record[f"{horizon_key}_return"] = float(horizon_return)
            records.append(record)

    return pd.DataFrame.from_records(records)


def _build_synthetic_ranked_panel(event_panel_df: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    base_columns = [
        "date",
        "code",
        "company_name",
        "close",
        "volume",
        "date_constituent_count",
        *[f"{key}_close" for key in HORIZON_ORDER],
        *[f"{key}_return" for key in HORIZON_ORDER],
    ]
    for feature_name in RANKING_FEATURE_ORDER:
        ranked_panel_df = event_panel_df[base_columns].copy()
        ranked_panel_df["ranking_feature"] = feature_name
        ranked_panel_df["ranking_feature_label"] = RANKING_FEATURE_LABEL_MAP[feature_name]
        ranked_panel_df["ranking_value"] = event_panel_df[feature_name].astype(float)
        frames.append(ranked_panel_df)
    return _assign_feature_deciles(
        pd.concat(frames, ignore_index=True),
        known_feature_order=RANKING_FEATURE_ORDER,
    )


def _build_selected_composite_df(event_panel_df: pd.DataFrame) -> pd.DataFrame:
    configs = [
        ("t_plus_1", "price_sma_5_20", "high", "volume_sma_5_20", "high", "rank_mean"),
        ("t_plus_5", "price_sma_20_80", "high", "volume_sma_20_80", "high", "rank_mean"),
        ("t_plus_10", "price_sma_50_150", "high", "volume_sma_50_150", "high", "rank_product"),
    ]
    records: list[dict[str, object]] = []
    for (
        horizon_key,
        price_feature,
        price_direction,
        volume_feature,
        volume_direction,
        score_method,
    ) in configs:
        composite_ranked_panel_df = _build_composite_ranked_panel(
            event_panel_df,
            price_feature=price_feature,
            price_direction=price_direction,
            volume_feature=volume_feature,
            volume_direction=volume_direction,
            score_method=score_method,
        )
        records.append(
            {
                "selected_horizon_key": horizon_key,
                "ranking_feature": str(composite_ranked_panel_df["ranking_feature"].iloc[0]),
                "ranking_feature_label": str(
                    composite_ranked_panel_df["ranking_feature_label"].iloc[0]
                ),
                "price_feature": price_feature,
                "price_feature_label": RANKING_FEATURE_LABEL_MAP[price_feature],
                "price_direction": price_direction,
                "volume_feature": volume_feature,
                "volume_feature_label": RANKING_FEATURE_LABEL_MAP[volume_feature],
                "volume_direction": volume_direction,
                "score_method": score_method,
            }
        )
    return pd.DataFrame.from_records(records)


def _build_synthetic_base_result(
    *,
    start_date: str,
    periods: int,
) -> Topix100SmaRatioRankFutureCloseResearchResult:
    event_panel_df = _build_synthetic_event_panel(start_date=start_date, periods=periods)
    ranked_panel_df = _build_synthetic_ranked_panel(event_panel_df)
    selected_composite_df = _build_selected_composite_df(event_panel_df)
    available_start = str(event_panel_df["date"].min())
    available_end = str(event_panel_df["date"].max())
    analysis_end = str(
        event_panel_df.loc[event_panel_df["t_plus_10_return"].notna(), "date"].max()
    )
    return Topix100SmaRatioRankFutureCloseResearchResult(
        db_path="synthetic://topix100-sma-lightgbm",
        source_mode="snapshot",
        source_detail="synthetic",
        universe_key="topix100",
        universe_label="TOPIX100",
        available_start_date=available_start,
        available_end_date=available_end,
        default_start_date=available_start,
        analysis_start_date=available_start,
        analysis_end_date=analysis_end,
        lookback_years=3,
        min_constituents_per_day=10,
        universe_constituent_count=10,
        topix100_constituent_count=10,
        stock_day_count=int(len(event_panel_df)),
        ranked_event_count=int(event_panel_df["t_plus_1_return"].notna().sum()),
        valid_date_count=int(event_panel_df["date"].nunique()),
        discovery_end_date="2021-12-31",
        validation_start_date="2022-01-01",
        event_panel_df=event_panel_df,
        ranked_panel_df=ranked_panel_df,
        ranking_feature_summary_df=_empty_df(),
        decile_future_summary_df=_empty_df(),
        daily_group_means_df=_empty_df(),
        global_significance_df=_empty_df(),
        pairwise_significance_df=_empty_df(),
        extreme_vs_middle_summary_df=_empty_df(),
        extreme_vs_middle_daily_means_df=_empty_df(),
        extreme_vs_middle_significance_df=_empty_df(),
        nested_volume_split_panel_df=_empty_df(),
        nested_volume_split_summary_df=_empty_df(),
        nested_volume_split_daily_means_df=_empty_df(),
        nested_volume_split_global_significance_df=_empty_df(),
        nested_volume_split_pairwise_significance_df=_empty_df(),
        nested_volume_split_interaction_df=_empty_df(),
        q1_q10_volume_split_panel_df=_empty_df(),
        q1_q10_volume_split_summary_df=_empty_df(),
        q1_q10_volume_split_daily_means_df=_empty_df(),
        q1_q10_volume_split_global_significance_df=_empty_df(),
        q1_q10_volume_split_pairwise_significance_df=_empty_df(),
        q1_q10_volume_split_interaction_df=_empty_df(),
        q10_middle_volume_split_panel_df=_empty_df(),
        q10_middle_volume_split_summary_df=_empty_df(),
        q10_middle_volume_split_daily_means_df=_empty_df(),
        q10_middle_volume_split_pairwise_significance_df=_empty_df(),
        q10_low_hypothesis_df=_empty_df(),
        feature_selection_df=_empty_df(),
        selected_feature_df=_empty_df(),
        composite_candidate_df=_empty_df(),
        selected_composite_df=selected_composite_df,
        selected_composite_ranking_summary_df=_empty_df(),
        selected_composite_future_summary_df=_empty_df(),
        selected_composite_daily_group_means_df=_empty_df(),
        selected_composite_global_significance_df=_empty_df(),
        selected_composite_pairwise_significance_df=_empty_df(),
    )


@pytest.fixture(scope="module")
def base_result():
    return _build_synthetic_base_result(start_date="2021-01-04", periods=160)


@pytest.fixture(scope="module")
def long_base_result():
    return _build_synthetic_base_result(start_date="2018-01-04", periods=884)


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


@pytest.fixture(scope="module")
def train_analysis_by_split(base_result, short_walkforward_result_bundle):
    lightgbm_result, _ = short_walkforward_result_bundle
    split_df = lightgbm_result.walkforward.split_coverage_df[
        ["split_index", "train_start", "train_end"]
    ].drop_duplicates()
    analyses: dict[int, dict[str, pd.DataFrame]] = {}
    for row in split_df.to_dict(orient="records"):
        train_ranked_panel_df = base_result.ranked_panel_df[
            (base_result.ranked_panel_df["date"] >= row["train_start"])
            & (base_result.ranked_panel_df["date"] <= row["train_end"])
        ].copy()
        analyses[int(row["split_index"])] = _analyze_ranked_panel(train_ranked_panel_df)
    return analyses


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

    coverage_df = lightgbm_result.walkforward.split_coverage_df.copy()
    coverage_df["_selected_horizon_key_order"] = pd.Categorical(
        coverage_df["selected_horizon_key"],
        categories=list(HORIZON_ORDER),
        ordered=True,
    )
    coverage_df = coverage_df.sort_values(
        ["split_index", "_selected_horizon_key_order"],
        kind="stable",
    ).reset_index(drop=True)
    expected_rows = coverage_df[
        (coverage_df["lightgbm_train_row_count"] > 0)
        & (coverage_df["lightgbm_test_row_count"] > 0)
    ].copy()
    expected_rows = expected_rows.drop(columns="_selected_horizon_key_order")
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
    train_analysis_by_split,
) -> None:
    lightgbm_result, _ = short_walkforward_result_bundle

    walkforward = lightgbm_result.walkforward
    for row in walkforward.baseline_selected_feature_df.to_dict(orient="records"):
        train_analysis = train_analysis_by_split[int(row["split_index"])]
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
