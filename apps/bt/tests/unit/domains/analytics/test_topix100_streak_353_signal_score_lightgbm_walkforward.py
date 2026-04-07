"""Tests for the walk-forward TOPIX100 streak 3/53 LightGBM score research."""

from __future__ import annotations

import pandas as pd

from src.domains.analytics.topix100_streak_353_signal_score_lightgbm_walkforward import (
    _run_walkforward_from_panels,
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


def _build_fake_panels() -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = [f"2025-01-{day:02d}" for day in range(1, 17)]
    feature_rows: list[dict[str, object]] = []
    horizon_rows: list[dict[str, object]] = []

    for date_index, date in enumerate(dates):
        for code_index in range(10):
            code = f"{1000 + code_index}"
            company_name = f"Stock {code}"
            price_gap = 0.24 - 0.03 * code_index + 0.004 * date_index
            volume_ratio = 1.10 if code_index % 2 == 0 else 0.82
            short_mode = "bearish" if code_index % 4 < 2 else "bullish"
            long_mode = "bearish" if code_index < 5 else "bullish"
            future_return_5d = (
                0.012
                + 0.15 * price_gap
                + (0.02 if short_mode == "bearish" else -0.02)
                + (0.015 if long_mode == "bearish" else -0.01)
            )
            future_return_1d = (
                -0.004
                - 0.04 * price_gap
                + (0.016 if short_mode == "bullish" else -0.01)
                + (0.01 if long_mode == "bullish" else -0.005)
            )
            decile_num = code_index + 1
            decile = f"Q{decile_num}"
            volume_bucket = "volume_high" if code_index % 2 == 0 else "volume_low"

            feature_rows.append(
                {
                    "date": date,
                    "code": code,
                    "company_name": company_name,
                    "sample_split": "full",
                    "state_event_id": f"{code}:{date_index+1}",
                    "segment_id": date_index + 1,
                    "decile_num": decile_num,
                    "decile": decile,
                    "volume_bucket": volume_bucket,
                    "short_mode": short_mode,
                    "long_mode": long_mode,
                    "state_key": f"long_{long_mode}__short_{short_mode}",
                    "state_label": "state",
                    "price_vs_sma_50_gap": price_gap,
                    "volume_sma_5_20": volume_ratio,
                    "recent_return_1d": 0.01 * (5 - code_index),
                    "recent_return_3d": 0.015 * (5 - code_index),
                    "recent_return_5d": 0.02 * (5 - code_index),
                    "intraday_return": 0.003 * (5 - code_index),
                    "range_pct": 0.02 + 0.001 * code_index,
                    "segment_return": -0.04 if short_mode == "bearish" else 0.03,
                    "segment_abs_return": 0.04 if short_mode == "bearish" else 0.03,
                    "segment_day_count": 2 + code_index % 3,
                    "future_return_1d": future_return_1d,
                    "future_return_5d": future_return_5d,
                    "short_edge_1d": -future_return_1d,
                }
            )
            for horizon_days, future_return in ((1, future_return_1d), (5, future_return_5d)):
                horizon_rows.append(
                    {
                        "date": date,
                        "code": code,
                        "company_name": company_name,
                        "sample_split": "full",
                        "state_key": f"long_{long_mode}__short_{short_mode}",
                        "state_label": "state",
                        "short_mode": short_mode,
                        "long_mode": long_mode,
                        "horizon_days": horizon_days,
                        "decile": decile,
                        "volume_bucket": volume_bucket,
                        "future_return": future_return,
                    }
                )

    return pd.DataFrame.from_records(feature_rows), pd.DataFrame.from_records(horizon_rows)


def test_run_walkforward_from_panels_produces_oos_tables(monkeypatch) -> None:
    feature_panel_df, state_decile_horizon_panel_df = _build_fake_panels()

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm_walkforward._load_lightgbm_regressor_cls",
        lambda: _FakeRegressor,
    )

    result = _run_walkforward_from_panels(
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
        long_target_horizon_days=5,
        short_target_horizon_days=1,
        top_k_values=(2, 4),
        train_window=6,
        test_window=4,
        step=4,
        state_decile_horizon_panel_df=state_decile_horizon_panel_df,
        feature_panel_df=feature_panel_df,
        categorical_feature_columns=("decile", "volume_bucket", "short_mode", "long_mode"),
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
    assert not result.walkforward_model_summary_df.empty
    assert not result.walkforward_model_comparison_df.empty
    assert not result.walkforward_split_comparison_df.empty
    assert not result.walkforward_feature_importance_df.empty
