"""Tests for TOPIX100 intraday refit-cadence ablation research."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pandas as pd

from src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm import (
    _build_validation_topk_tables,
)
from src.domains.analytics.topix100_streak_353_next_session_intraday_refit_cadence_ablation import (
    _build_refit_schedule,
    run_topix100_streak_353_next_session_intraday_refit_cadence_ablation_research,
)


def _build_fake_cadence_prediction_df() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    dates = ["2025-01-06", "2025-01-07", "2025-01-08"]
    return_map = [0.03, 0.022, 0.015, -0.004, -0.017, -0.028]
    reference_scores = [
        [0.92, 0.81, 0.68, -0.12, -0.58, -0.77],
        [0.91, 0.79, 0.64, -0.1, -0.55, -0.73],
        [0.89, 0.76, 0.6, -0.08, -0.5, -0.7],
    ]
    daily_scores = [
        [0.92, 0.81, 0.68, -0.12, -0.58, -0.77],
        [0.9, 0.78, 0.03, 0.06, -0.54, -0.72],
        [0.88, 0.16, 0.61, -0.09, -0.18, -0.69],
    ]
    baseline_scores = [
        [0.18, 0.11, 0.08, -0.03, -0.1, -0.14],
        [0.17, 0.1, 0.05, -0.02, -0.09, -0.12],
        [0.15, 0.09, 0.04, -0.01, -0.08, -0.11],
    ]

    for cadence_days, score_grid in ((126, reference_scores), (1, daily_scores)):
        for date_index, date_value in enumerate(dates, start=1):
            for model_name, score_values in (
                ("baseline", baseline_scores[date_index - 1]),
                ("lightgbm", score_grid[date_index - 1]),
            ):
                for code_index, (score, realized_return) in enumerate(
                    zip(score_values, return_map, strict=True),
                    start=1,
                ):
                    rows.append(
                        {
                            "cadence_days": cadence_days,
                            "refit_index": date_index if cadence_days == 1 else 1,
                            "train_start": "2024-01-01",
                            "train_end": "2024-12-31",
                            "test_start": dates[0],
                            "test_end": dates[-1],
                            "is_partial_tail": False,
                            "model_name": model_name,
                            "date": date_value,
                            "code": f"{1000 + code_index}",
                            "company_name": f"Stock {1000 + code_index}",
                            "decile_num": code_index,
                            "decile": f"Q{code_index}",
                            "volume_bucket": "volume_high" if code_index % 2 else "volume_low",
                            "short_mode": "bearish" if code_index <= 3 else "bullish",
                            "long_mode": "bullish" if code_index <= 3 else "bearish",
                            "state_key": "state",
                            "state_label": "state",
                            "score": score,
                            "realized_return": realized_return,
                        }
                    )
    return pd.DataFrame.from_records(rows)


def _build_fake_artifacts():
    prediction_df = _build_fake_cadence_prediction_df()
    pick_frames: list[pd.DataFrame] = []
    daily_frames: list[pd.DataFrame] = []
    for cadence_days, scoped_prediction_df in prediction_df.groupby(
        "cadence_days",
        observed=True,
        sort=False,
    ):
        cadence_days_value = int(cast(Any, cadence_days))
        pick_df, daily_df = _build_validation_topk_tables(
            scoped_prediction_df,
            top_k_values=(3,),
        )
        pick_frames.append(
            pick_df.assign(
                cadence_days=cadence_days_value,
                refit_index=1,
                train_start="2024-01-01",
                train_end="2024-12-31",
                test_start="2025-01-06",
                test_end="2025-01-08",
                is_partial_tail=False,
            )
        )
        daily_frames.append(
            daily_df.assign(
                cadence_days=cadence_days_value,
                refit_index=1,
                train_start="2024-01-01",
                train_end="2024-12-31",
                test_start="2025-01-06",
                test_end="2025-01-08",
                is_partial_tail=False,
            )
        )
    return SimpleNamespace(
        cadence_config_df=pd.DataFrame(
            [
                {"cadence_days": 1, "refit_count": 3, "covered_day_count": 3},
                {"cadence_days": 126, "refit_count": 1, "covered_day_count": 3},
            ]
        ),
        cadence_schedule_df=pd.DataFrame(
            [
                {
                    "cadence_days": 1,
                    "refit_index": 1,
                    "train_start": "2024-01-01",
                    "train_end": "2024-12-31",
                    "test_start": "2025-01-06",
                    "test_end": "2025-01-06",
                    "train_row_count": 18,
                    "test_row_count": 12,
                    "train_date_count": 3,
                    "test_date_count": 1,
                    "is_partial_tail": False,
                },
                {
                    "cadence_days": 126,
                    "refit_index": 1,
                    "train_start": "2024-01-01",
                    "train_end": "2024-12-31",
                    "test_start": "2025-01-06",
                    "test_end": "2025-01-08",
                    "train_row_count": 18,
                    "test_row_count": 36,
                    "train_date_count": 3,
                    "test_date_count": 3,
                    "is_partial_tail": True,
                },
            ]
        ),
        cadence_prediction_df=prediction_df,
        cadence_topk_pick_df=pd.concat(pick_frames, ignore_index=True),
        cadence_topk_daily_df=pd.concat(daily_frames, ignore_index=True),
        cadence_feature_importance_split_df=pd.DataFrame(
            [
                {
                    "cadence_days": 1,
                    "refit_index": 1,
                    "train_start": "2024-01-01",
                    "train_end": "2024-12-31",
                    "test_start": "2025-01-06",
                    "test_end": "2025-01-06",
                    "is_partial_tail": False,
                    "model_name": "lightgbm",
                    "feature_name": "decile",
                    "importance_gain": 120.0,
                    "importance_share": 0.7,
                    "importance_rank": 1,
                },
                {
                    "cadence_days": 126,
                    "refit_index": 1,
                    "train_start": "2024-01-01",
                    "train_end": "2024-12-31",
                    "test_start": "2025-01-06",
                    "test_end": "2025-01-08",
                    "is_partial_tail": True,
                    "model_name": "lightgbm",
                    "feature_name": "decile",
                    "importance_gain": 150.0,
                    "importance_share": 0.75,
                    "importance_rank": 1,
                },
            ]
        ),
    )


def test_build_refit_schedule_supports_purge_and_partial_tail() -> None:
    schedule = _build_refit_schedule(
        unique_dates=pd.date_range("2025-01-01", periods=10, freq="B"),
        train_window=3,
        purge_signal_dates=1,
        cadence_days=4,
    )

    assert len(schedule) == 2
    assert schedule[0].train_start == "2025-01-01"
    assert schedule[0].train_end == "2025-01-03"
    assert schedule[0].test_start == "2025-01-07"
    assert schedule[0].test_end == "2025-01-10"
    assert schedule[0].is_partial_tail is False
    assert schedule[1].train_start == "2025-01-07"
    assert schedule[1].train_end == "2025-01-09"
    assert schedule[1].test_start == "2025-01-13"
    assert schedule[1].test_end == "2025-01-14"
    assert schedule[1].is_partial_tail is True


def test_run_refit_cadence_ablation_builds_reference_comparisons(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_refit_cadence_ablation.run_topix100_price_vs_sma_rank_future_close_research",
        lambda *args, **kwargs: SimpleNamespace(
            source_mode="snapshot",
            source_detail="synthetic",
            event_panel_df=pd.DataFrame(),
        ),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_refit_cadence_ablation.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_refit_cadence_ablation._build_feature_panel_df",
        lambda **kwargs: pd.DataFrame({"date": ["2025-01-06", "2025-01-07", "2025-01-08"]}),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_refit_cadence_ablation._build_refit_cadence_prediction_artifacts",
        lambda **kwargs: _build_fake_artifacts(),
    )

    result = run_topix100_streak_353_next_session_intraday_refit_cadence_ablation_research(
        "/tmp/market.duckdb",
        top_k_values=(3,),
        train_window=756,
        refit_cadence_days=(1, 126),
        reference_cadence_days=126,
    )

    assert result.refit_cadence_days == (1, 126)
    assert set(result.cadence_model_summary_df["cadence_days"]) == {1, 126}
    lightgbm_daily_vs_reference = result.cadence_vs_reference_df[
        (result.cadence_vs_reference_df["cadence_days"] == 1)
        & (result.cadence_vs_reference_df["model_name"] == "lightgbm")
        & (result.cadence_vs_reference_df["top_k"] == 3)
        & (result.cadence_vs_reference_df["series_name"] == "pair_50_50")
    ]
    assert not lightgbm_daily_vs_reference.empty
    assert float(lightgbm_daily_vs_reference.iloc[0]["avg_daily_return_delta_vs_reference"]) < 0.0
    lightgbm_alignment = result.cadence_score_alignment_df[
        (result.cadence_score_alignment_df["cadence_days"] == 1)
        & (result.cadence_score_alignment_df["model_name"] == "lightgbm")
    ]
    assert not lightgbm_alignment.empty
    assert float(lightgbm_alignment.iloc[0]["avg_score_rank_corr"]) < 1.0
    lightgbm_overlap = result.cadence_book_overlap_df[
        (result.cadence_book_overlap_df["cadence_days"] == 1)
        & (result.cadence_book_overlap_df["model_name"] == "lightgbm")
        & (result.cadence_book_overlap_df["top_k"] == 3)
    ]
    assert not lightgbm_overlap.empty
    assert float(lightgbm_overlap.iloc[0]["avg_signed_book_overlap_rate"]) < 1.0
    assert not result.cadence_turnover_df.empty
    assert not result.cadence_feature_importance_df.empty
