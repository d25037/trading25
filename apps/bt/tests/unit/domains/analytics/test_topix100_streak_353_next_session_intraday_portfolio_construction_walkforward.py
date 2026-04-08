"""Tests for intraday portfolio-construction walk-forward research."""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm import (
    _build_validation_topk_tables,
)
from src.domains.analytics.topix100_streak_353_next_session_intraday_portfolio_construction_walkforward import (
    _build_absolute_score_pick_df,
    _build_variant_daily_df,
    run_topix100_streak_353_next_session_intraday_portfolio_construction_walkforward_research,
)


def _build_fake_walkforward_prediction_df() -> pd.DataFrame:
    dates = ["2025-01-01", "2025-01-02"]
    rows: list[dict[str, object]] = []
    score_map = {
        "baseline": [0.6, 0.5, 0.4, -0.3, -0.2, -0.1],
        "lightgbm": [0.9, 0.8, 0.7, -0.6, -0.5, -0.4],
    }
    return_map = {
        "baseline": [0.018, 0.016, 0.014, -0.008, -0.009, -0.007],
        "lightgbm": [0.03, 0.025, 0.02, -0.01, -0.015, -0.005],
    }
    for split_index, date in enumerate(dates, start=1):
        for model_name, scores in score_map.items():
            returns = return_map[model_name]
            for code_index, (score, realized_return) in enumerate(
                zip(scores, returns, strict=True),
                start=1,
            ):
                rows.append(
                    {
                        "model_name": model_name,
                        "split_index": 1,
                        "train_start": "2024-01-01",
                        "train_end": "2024-12-31",
                        "test_start": "2025-01-01",
                        "test_end": "2025-01-02",
                        "date": date,
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
                        "realized_return": realized_return + split_index * 0.0001,
                    }
                )
    return pd.DataFrame.from_records(rows)


def _build_reference_pick_df(prediction_df: pd.DataFrame) -> pd.DataFrame:
    pick_df, _daily_df = _build_validation_topk_tables(prediction_df, top_k_values=(3,))
    pick_df = pick_df.assign(
        split_index=1,
        train_start="2024-01-01",
        train_end="2024-12-31",
        test_start="2025-01-01",
        test_end="2025-01-02",
    )
    return pick_df


def test_build_absolute_score_pick_df_assigns_direction_from_score_sign() -> None:
    prediction_df = _build_fake_walkforward_prediction_df()

    pick_df = _build_absolute_score_pick_df(prediction_df, selection_count=5)
    daily_df = _build_variant_daily_df(pick_df, prediction_df)

    lightgbm_daily = daily_df[
        (daily_df["variant_key"] == "abs_score_signed")
        & (daily_df["model_name"] == "lightgbm")
        & (daily_df["date"] == "2025-01-01")
    ]

    assert not lightgbm_daily.empty
    row = lightgbm_daily.iloc[0]
    assert row["selected_stock_count"] == 5
    assert row["long_count"] == 3
    assert row["short_count"] == 2
    assert row["flat_count"] == 0
    assert row["net_exposure"] == 0.2


def test_run_portfolio_construction_walkforward_compares_variants(monkeypatch) -> None:
    prediction_df = _build_fake_walkforward_prediction_df()
    reference_pick_df = _build_reference_pick_df(prediction_df)

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_portfolio_construction_walkforward.run_topix100_price_vs_sma_rank_future_close_research",
        lambda *args, **kwargs: SimpleNamespace(
            source_mode="snapshot",
            source_detail="synthetic",
            event_panel_df=pd.DataFrame(),
        ),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_portfolio_construction_walkforward.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_portfolio_construction_walkforward._build_feature_panel_df",
        lambda **kwargs: pd.DataFrame({"date": ["2025-01-01", "2025-01-02"]}),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_portfolio_construction_walkforward._build_walkforward_prediction_artifacts",
        lambda **kwargs: SimpleNamespace(
            split_count=1,
            split_config_df=pd.DataFrame(
                [
                    {
                        "split_index": 1,
                        "train_start": "2024-01-01",
                        "train_end": "2024-12-31",
                        "test_start": "2025-01-01",
                        "test_end": "2025-01-02",
                    }
                ]
            ),
            walkforward_prediction_df=prediction_df,
            walkforward_topk_pick_df=reference_pick_df,
        ),
    )

    result = run_topix100_streak_353_next_session_intraday_portfolio_construction_walkforward_research(
        "/tmp/market.duckdb",
        reference_top_k=3,
        absolute_selection_count=5,
        train_window=6,
        test_window=2,
        step=2,
    )

    assert result.variant_keys == ("top_bottom_reference", "abs_score_signed")
    assert set(result.variant_model_summary_df["variant_key"]) == set(result.variant_keys)
    lightgbm_abs = result.variant_vs_reference_df[
        (result.variant_vs_reference_df["variant_key"] == "abs_score_signed")
        & (result.variant_vs_reference_df["model_name"] == "lightgbm")
    ]
    assert not lightgbm_abs.empty
    assert float(lightgbm_abs.iloc[0]["selected_edge_delta_vs_reference"]) > 0.0
    assert not result.portfolio_stats_df.empty
