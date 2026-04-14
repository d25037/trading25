"""Tests for walk-forward TOPIX100 next-session intraday LightGBM research."""

from __future__ import annotations

import pandas as pd

from src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm_walkforward import (
    _run_walkforward_from_panel,
)


class _FakeRegressor:
    def __init__(self, **kwargs) -> None:
        self.params = kwargs
        self.feature_importances_: list[float] = []

    def fit(self, X, y, categorical_feature=None):  # noqa: ANN001
        self.feature_importances_ = [float(index + 1) for index in range(X.shape[1])]
        return self

    def predict(self, X):  # noqa: ANN001
        working = X.copy()
        score = pd.Series(0.0, index=working.index, dtype=float)
        for column in working.columns:
            if str(getattr(working[column].dtype, "name", "")) == "category":
                score = score.add(working[column].cat.codes.astype(float), fill_value=0.0)
            else:
                score = score.add(pd.to_numeric(working[column], errors="coerce").fillna(0.0))
        return score.to_numpy(dtype=float)


def _build_fake_feature_panel() -> pd.DataFrame:
    dates = [f"2025-01-{day:02d}" for day in range(1, 17)]
    rows: list[dict[str, object]] = []
    for date_index, date in enumerate(dates):
        for code_index in range(10):
            decile_num = code_index + 1
            rows.append(
                {
                    "date": date,
                    "code": f"{1000 + code_index}",
                    "company_name": f"Stock {1000 + code_index}",
                    "sample_split": "full",
                    "state_event_id": f"{1000 + code_index}:{date_index + 1}",
                    "segment_id": date_index + 1,
                    "decile_num": decile_num,
                    "decile": f"Q{decile_num}",
                    "price_vs_sma_50_gap": 0.25 - 0.025 * code_index + 0.005 * date_index,
                    "volume_sma_5_20": (1.15 if code_index % 2 == 0 else 0.75) + 0.02 * date_index,
                    "recent_return_1d": 0.01 * (5 - code_index),
                    "recent_return_3d": 0.015 * (5 - code_index),
                    "recent_return_5d": 0.02 * (5 - code_index),
                    "intraday_return": 0.003 * (5 - code_index),
                    "range_pct": 0.02 + 0.001 * code_index,
                    "segment_return": -0.04 if code_index % 4 < 2 else 0.03,
                    "segment_abs_return": 0.04 if code_index % 4 < 2 else 0.03,
                    "segment_day_count": 2 + code_index % 3,
                    "next_session_intraday_return": (
                        0.01
                        + 0.12 * (0.25 - 0.025 * code_index)
                        + (0.02 if code_index % 4 < 2 else -0.015)
                        + (0.012 if code_index < 5 else -0.008)
                    ),
                }
            )
    return pd.DataFrame.from_records(rows)


def test_run_walkforward_from_panel_produces_oos_tables(monkeypatch) -> None:
    feature_panel_df = _build_fake_feature_panel()

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm_walkforward._load_lightgbm_regressor_cls",
        lambda: _FakeRegressor,
    )

    result = _run_walkforward_from_panel(
        db_path="/tmp/market.duckdb",
        source_mode="snapshot",
        source_detail="synthetic",
        available_start_date="2025-01-01",
        available_end_date="2025-01-16",
        price_feature="price_vs_sma_50_gap",
        volume_feature="volume_sma_5_20",
        short_window_streaks=3,
        long_window_streaks=53,
        validation_ratio=0.3,
        top_k_values=(1, 3),
        train_window=6,
        test_window=4,
        step=4,
        purge_signal_dates=1,
        feature_panel_df=feature_panel_df,
        categorical_feature_columns=("decile",),
        continuous_feature_columns=(
            "price_vs_sma_50_gap",
            "volume_sma_5_20",
            "recent_return_1d",
            "recent_return_3d",
            "recent_return_5d",
            "intraday_return",
            "range_pct",
            "segment_return",
            "segment_abs_return",
            "segment_day_count",
        ),
    )

    assert result.split_count >= 1
    assert result.purge_signal_dates == 1
    assert not result.walkforward_model_summary_df.empty
    assert not result.walkforward_model_comparison_df.empty
    assert not result.walkforward_split_comparison_df.empty
    assert not result.portfolio_stats_df.empty
    assert not result.daily_return_distribution_df.empty
    assert not result.walkforward_feature_importance_df.empty
    pair_stats = result.portfolio_stats_df[
        (result.portfolio_stats_df["model_name"] == "lightgbm")
        & (result.portfolio_stats_df["top_k"] == 3)
        & (result.portfolio_stats_df["series_name"] == "pair_50_50")
    ]
    assert not pair_stats.empty
