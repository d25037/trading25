from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.domains.analytics.topix100_streak_353_next_session_open_to_close_10d_lightgbm import (
    build_feature_panel_from_state_event_df as build_feature_panel_from_state_event_df_10d,
)
from src.domains.analytics.topix100_streak_353_next_session_open_to_close_5d_excess_vs_topix_lightgbm_walkforward import (
    RAW_RETURN_COLUMN,
    TARGET_COLUMN,
    _BaselineLookupRow,
    _BaselineScorecard,
    _build_baseline_validation_prediction_df,
    _build_lightgbm_validation_prediction_df,
)
from src.domains.analytics.topix100_streak_353_next_session_open_to_close_5d_lightgbm import (
    _build_feature_panel_from_state_event_df as build_feature_panel_from_state_event_df_5d,
)
from src.domains.analytics.topix100_streak_353_next_session_open_to_open_5d_lightgbm import (
    _build_feature_panel_from_state_event_df as build_feature_panel_from_state_event_df_open_to_open_5d,
)


PRICE_FEATURE = "close_to_sma_50"
VOLUME_FEATURE = "volume_to_sma_20"


def _build_event_panel(day_count: int = 12) -> pd.DataFrame:
    dates = pd.date_range("2020-01-01", periods=day_count, freq="B")
    close_values = np.arange(100.0, 100.0 + day_count)
    return pd.DataFrame(
        {
            "date": dates.strftime("%Y-%m-%d"),
            "code": ["1332"] * day_count,
            "company_name": ["Test Co"] * day_count,
            "open": close_values - 0.5,
            "high": close_values + 1.0,
            "low": close_values - 1.0,
            "close": close_values,
            PRICE_FEATURE: np.linspace(0.1, 1.2, day_count),
            VOLUME_FEATURE: np.linspace(1.1, 2.2, day_count),
            "date_constituent_count": [100] * day_count,
        }
    )


def _build_state_panel(day_count: int = 12) -> pd.DataFrame:
    dates = pd.date_range("2020-01-01", periods=day_count, freq="B")
    rows: list[dict[str, object]] = []
    for index, date in enumerate(dates):
        rows.append(
            {
                "state_event_id": index + 1,
                "code": "1332",
                "company_name": "Test Co",
                "sample_split": "discovery" if index < day_count - 2 else "validation",
                "segment_id": index + 10,
                "date": date.strftime("%Y-%m-%d"),
                "segment_return": 0.01 * (index + 1),
                "segment_day_count": 3,
                "base_streak_mode": "bullish",
                "short_mode": "bullish",
                "long_mode": "bullish",
                "state_key": "long_bullish__short_bullish",
                "state_label": "Long Bullish / Short Bullish",
            }
        )
    return pd.DataFrame(rows)


def _build_sparse_open_to_open_event_panel() -> pd.DataFrame:
    calendar = pd.date_range("2020-01-01", periods=10, freq="B").strftime("%Y-%m-%d")
    rows: list[dict[str, object]] = []
    for index, date in enumerate(calendar):
        rows.append(
            {
                "date": date,
                "code": "9999",
                "company_name": "Calendar Co",
                "open": 200.0 + index,
                "high": 201.0 + index,
                "low": 199.0 + index,
                "close": 200.5 + index,
                PRICE_FEATURE: 0.1 + index,
                VOLUME_FEATURE: 1.0 + index,
                "date_constituent_count": 100,
            }
        )
        if date != "2020-01-09":
            rows.append(
                {
                    "date": date,
                    "code": "1332",
                    "company_name": "Test Co",
                    "open": 100.0 + index,
                    "high": 101.0 + index,
                    "low": 99.0 + index,
                    "close": 100.5 + index,
                    PRICE_FEATURE: 0.2 + index,
                    VOLUME_FEATURE: 1.5 + index,
                    "date_constituent_count": 100,
                }
            )
    return pd.DataFrame(rows)


def _build_sparse_open_to_open_state_panel() -> pd.DataFrame:
    dates = [
        date
        for date in pd.date_range("2020-01-01", periods=10, freq="B").strftime("%Y-%m-%d")
        if date != "2020-01-09"
    ]
    rows: list[dict[str, object]] = []
    for index, date in enumerate(dates):
        rows.append(
            {
                "state_event_id": index + 1,
                "code": "1332",
                "company_name": "Test Co",
                "sample_split": "discovery",
                "segment_id": index + 10,
                "date": date,
                "segment_return": 0.01 * (index + 1),
                "segment_day_count": 3,
                "base_streak_mode": "bullish",
                "short_mode": "bullish",
                "long_mode": "bullish",
                "state_key": "long_bullish__short_bullish",
                "state_label": "Long Bullish / Short Bullish",
            }
        )
    return pd.DataFrame(rows)


def _build_excess_feature_panel() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "sample_split": ["discovery", "discovery", "validation", "validation"],
            "date": ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-06"],
            "code": ["1332", "1333", "1332", "1333"],
            "company_name": ["A", "B", "A", "B"],
            "decile_num": [1, 2, 1, 2],
            "decile": ["Q1", "Q2", "Q1", "Q2"],
            PRICE_FEATURE: [0.5, 0.2, 0.6, 0.1],
            VOLUME_FEATURE: [1.5, 1.2, 1.6, 1.1],
            "recent_return_1d": [0.01, -0.01, 0.02, -0.02],
            "recent_return_3d": [0.03, -0.03, 0.04, -0.04],
            "recent_return_5d": [0.05, -0.05, 0.06, -0.06],
            "intraday_return": [0.01, -0.01, 0.02, -0.02],
            "range_pct": [0.02, 0.03, 0.02, 0.03],
            RAW_RETURN_COLUMN: [0.10, -0.04, 0.08, -0.02],
            TARGET_COLUMN: [0.06, -0.08, 0.05, -0.03],
            "swing_entry_date": [
                "2020-01-02",
                "2020-01-03",
                "2020-01-06",
                "2020-01-07",
            ],
            "swing_exit_date": [
                "2020-01-08",
                "2020-01-09",
                "2020-01-13",
                "2020-01-14",
            ],
        }
    )


def test_5d_feature_panel_builds_next_session_open_to_close_target() -> None:
    feature_panel_df = build_feature_panel_from_state_event_df_5d(
        event_panel_df=_build_event_panel(),
        state_event_df=_build_state_panel(),
        price_feature=PRICE_FEATURE,
        volume_feature=VOLUME_FEATURE,
    )

    first_row = feature_panel_df.iloc[0]
    assert first_row["swing_entry_date"] == "2020-01-02"
    assert first_row["swing_exit_date"] == "2020-01-08"
    expected = 105.0 / 100.5 - 1.0
    assert first_row["next_session_open_to_close_5d_return"] == pytest.approx(expected)


def test_10d_feature_panel_builds_next_session_open_to_close_target() -> None:
    feature_panel_df = build_feature_panel_from_state_event_df_10d(
        event_panel_df=_build_event_panel(),
        state_event_df=_build_state_panel(),
        price_feature=PRICE_FEATURE,
        volume_feature=VOLUME_FEATURE,
    )

    first_row = feature_panel_df.iloc[0]
    assert first_row["swing_entry_date"] == "2020-01-02"
    assert first_row["swing_exit_date"] == "2020-01-15"
    expected = 110.0 / 100.5 - 1.0
    assert first_row["next_session_open_to_close_10d_return"] == pytest.approx(expected)


def test_5d_feature_panel_builds_next_session_open_to_open_target() -> None:
    feature_panel_df = build_feature_panel_from_state_event_df_open_to_open_5d(
        event_panel_df=_build_event_panel(),
        state_event_df=_build_state_panel(),
        price_feature=PRICE_FEATURE,
        volume_feature=VOLUME_FEATURE,
    )

    first_row = feature_panel_df.iloc[0]
    assert first_row["swing_entry_date"] == "2020-01-02"
    assert first_row["swing_exit_date"] == "2020-01-09"
    expected = 105.5 / 100.5 - 1.0
    assert first_row["next_session_open_to_open_5d_return"] == pytest.approx(expected)


def test_5d_open_to_open_target_does_not_jump_across_sparse_history() -> None:
    feature_panel_df = build_feature_panel_from_state_event_df_open_to_open_5d(
        event_panel_df=_build_sparse_open_to_open_event_panel(),
        state_event_df=_build_sparse_open_to_open_state_panel(),
        price_feature=PRICE_FEATURE,
        volume_feature=VOLUME_FEATURE,
    )

    assert "2020-01-01" not in feature_panel_df["date"].tolist()


def test_excess_baseline_prediction_keeps_raw_return_for_evaluation() -> None:
    feature_panel_df = _build_excess_feature_panel()
    scorecard = _BaselineScorecard(
        universe_return=0.01,
        rows_by_subset={
            "universe": {
                "universe": _BaselineLookupRow(
                    subset_key="universe",
                    selector_value_key="universe",
                    avg_target_return=0.01,
                    date_count=20,
                    avg_stock_count=2.0,
                )
            }
        },
    )

    prediction_df = _build_baseline_validation_prediction_df(
        feature_panel_df,
        baseline_scorecard=scorecard,
        target_column=TARGET_COLUMN,
        evaluation_column=RAW_RETURN_COLUMN,
    )

    assert prediction_df["realized_return"].tolist() == [0.08, -0.02]
    assert prediction_df["target_excess_return"].tolist() == [0.05, -0.03]


class _FakeRegressor:
    def __init__(self, **_: object) -> None:
        self.feature_importances_: list[float] = []

    def fit(
        self,
        train_matrix: pd.DataFrame,
        target: pd.Series,
        *,
        categorical_feature: list[str],
    ) -> None:
        del target, categorical_feature
        self.feature_importances_ = [1.0] * train_matrix.shape[1]

    def predict(self, validation_matrix: pd.DataFrame) -> np.ndarray:
        return np.linspace(0.2, 0.1, len(validation_matrix))


def test_excess_lightgbm_prediction_keeps_raw_return_for_evaluation() -> None:
    feature_panel_df = _build_excess_feature_panel()

    prediction_df, feature_importance_df = _build_lightgbm_validation_prediction_df(
        feature_panel_df,
        regressor_cls=_FakeRegressor,
        categorical_feature_columns=("decile",),
        continuous_feature_columns=(
            PRICE_FEATURE,
            VOLUME_FEATURE,
            "recent_return_1d",
            "recent_return_3d",
            "recent_return_5d",
            "intraday_return",
            "range_pct",
        ),
        target_column=TARGET_COLUMN,
        evaluation_column=RAW_RETURN_COLUMN,
    )

    assert prediction_df["realized_return"].tolist() == [0.08, -0.02]
    assert prediction_df["target_excess_return"].tolist() == [0.05, -0.03]
    assert len(feature_importance_df) == 8
