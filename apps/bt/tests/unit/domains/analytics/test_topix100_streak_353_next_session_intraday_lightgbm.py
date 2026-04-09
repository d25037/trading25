"""Tests for TOPIX100 next-session intraday LightGBM research."""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm import (
    _build_feature_panel_df,
    _slice_feature_panel_to_recent_dates,
    _build_validation_model_comparison_df,
    _resolve_runtime_walkforward_split,
    load_topix100_streak_353_next_session_intraday_lightgbm_research_bundle,
    run_topix100_streak_353_next_session_intraday_lightgbm_research,
    write_topix100_streak_353_next_session_intraday_lightgbm_research_bundle,
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


def _build_fake_inputs() -> tuple[SimpleNamespace, SimpleNamespace]:
    dates = [f"2026-01-{day:02d}" for day in range(1, 8)]
    event_rows: list[dict[str, object]] = []
    state_rows: list[dict[str, object]] = []

    for date_index, date in enumerate(dates):
        for code_index in range(12):
            code = f"{1000 + code_index}"
            company_name = f"Stock {code}"
            open_price = 100.0 + code_index + date_index
            close_price = open_price * (1.0 + 0.004 * (4 - code_index))
            high_price = max(open_price, close_price) * 1.01
            low_price = min(open_price, close_price) * 0.99

            short_mode = "bearish" if code_index % 4 < 2 else "bullish"
            long_mode = "bearish" if code_index < 6 else "bullish"
            state_key = f"long_{long_mode}__short_{short_mode}"
            state_label = (
                f"Long {'Bearish' if long_mode == 'bearish' else 'Bullish'} / "
                f"Short {'Bearish' if short_mode == 'bearish' else 'Bullish'}"
            )
            sample_split = "discovery" if date_index < 4 else "validation"
            segment_return = (-0.06 if short_mode == "bearish" else 0.05) + 0.002 * date_index
            segment_day_count = 2 + (code_index % 3)

            event_rows.append(
                {
                    "code": code,
                    "company_name": company_name,
                    "date": date,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": 1000 + code_index * 20 + date_index * 5,
                    "price_vs_sma_50_gap": 0.28 - 0.025 * code_index + 0.01 * date_index,
                    "volume_sma_5_20": (1.20 if code_index % 2 == 0 else 0.70)
                    + 0.04 * date_index,
                    "date_constituent_count": 12,
                }
            )
            state_rows.append(
                {
                    "state_event_id": f"{code}:{date_index + 1}",
                    "code": code,
                    "company_name": company_name,
                    "sample_split": sample_split,
                    "segment_id": date_index + 1,
                    "segment_end_date": date,
                    "segment_return": segment_return,
                    "segment_day_count": segment_day_count,
                    "base_streak_mode": short_mode,
                    "short_mode": short_mode,
                    "long_mode": long_mode,
                    "state_key": state_key,
                    "state_label": state_label,
                }
            )

    price_result = SimpleNamespace(
        event_panel_df=pd.DataFrame.from_records(event_rows),
        source_mode="snapshot",
        source_detail="synthetic",
    )
    state_result = SimpleNamespace(
        state_event_df=pd.DataFrame.from_records(state_rows),
    )
    return price_result, state_result


def test_build_feature_panel_df_adds_next_session_intraday_target() -> None:
    price_result, state_result = _build_fake_inputs()

    panel_df = _build_feature_panel_df(
        event_panel_df=price_result.event_panel_df,
        state_result=state_result,
        price_feature="price_vs_sma_50_gap",
        volume_feature="volume_sma_5_20",
    )

    assert not panel_df.empty
    assert "next_session_intraday_return" in panel_df.columns
    assert {"discovery", "validation"} <= set(panel_df["sample_split"])


def test_build_validation_model_comparison_df_computes_lifts() -> None:
    summary_df = pd.DataFrame.from_records(
        [
            {
                "model_name": "baseline",
                "top_k": 3,
                "avg_long_return": 0.003,
                "avg_short_edge": 0.002,
                "avg_long_short_spread": 0.005,
                "spread_hit_rate_positive": 0.55,
            },
            {
                "model_name": "lightgbm",
                "top_k": 3,
                "avg_long_return": 0.009,
                "avg_short_edge": 0.007,
                "avg_long_short_spread": 0.016,
                "spread_hit_rate_positive": 0.70,
            },
        ]
    )

    comparison_df = _build_validation_model_comparison_df(summary_df)

    assert len(comparison_df) == 1
    row = comparison_df.iloc[0]
    assert float(row["long_return_lift_vs_baseline"]) == pytest.approx(0.006)
    assert float(row["short_edge_lift_vs_baseline"]) == pytest.approx(0.005)
    assert float(row["spread_lift_vs_baseline"]) == pytest.approx(0.011)


def test_resolve_runtime_walkforward_split_matches_complete_block() -> None:
    dates = pd.bdate_range("2021-01-04", periods=900).strftime("%Y-%m-%d")
    feature_panel_df = pd.DataFrame({"date": dates})
    snapshot_df = pd.DataFrame({"date": [dates[800]], "code": ["1001"]})

    split = _resolve_runtime_walkforward_split(
        feature_panel_df=feature_panel_df,
        target_date=str(dates[800]),
        snapshot_df=snapshot_df,
        train_window_days=756,
        test_window_days=126,
        step_days=126,
        purge_signal_dates=0,
        allow_partial_test_window=True,
    )

    assert split is not None
    assert split.train_start == str(dates[0])
    assert split.train_end == str(dates[755])
    assert split.test_start == str(dates[756])
    assert split.test_end == str(dates[881])
    assert split.is_partial_tail is False


def test_resolve_runtime_walkforward_split_supports_partial_tail() -> None:
    dates = pd.bdate_range("2021-01-04", periods=950).strftime("%Y-%m-%d")
    feature_panel_df = pd.DataFrame({"date": dates})
    snapshot_df = pd.DataFrame({"date": [dates[920]], "code": ["1001"]})

    split = _resolve_runtime_walkforward_split(
        feature_panel_df=feature_panel_df,
        target_date=str(dates[920]),
        snapshot_df=snapshot_df,
        train_window_days=756,
        test_window_days=126,
        step_days=126,
        purge_signal_dates=0,
        allow_partial_test_window=True,
    )

    assert split is not None
    assert split.train_start == str(dates[126])
    assert split.train_end == str(dates[881])
    assert split.test_start == str(dates[882])
    assert split.test_end == str(dates[949])
    assert split.is_partial_tail is True


def test_slice_feature_panel_to_recent_dates_uses_trailing_signal_window() -> None:
    dates = pd.bdate_range("2026-01-05", periods=10).strftime("%Y-%m-%d")
    feature_panel_df = pd.DataFrame(
        {
            "date": [date for date in dates for _ in range(2)],
            "code": ["1001", "1002"] * 10,
        }
    )

    sliced_df, start_date, end_date = _slice_feature_panel_to_recent_dates(
        feature_panel_df,
        max_date_count=4,
    )

    assert start_date == str(dates[6])
    assert end_date == str(dates[9])
    assert sliced_df["date"].nunique() == 4
    assert set(sliced_df["date"]) == {str(date) for date in dates[6:10]}


def test_run_research_builds_signed_intraday_scorecard(monkeypatch, tmp_path) -> None:
    price_result, state_result = _build_fake_inputs()

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm.run_topix100_price_vs_sma_rank_future_close_research",
        lambda *args, **kwargs: price_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: state_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._load_lightgbm_regressor_cls",
        lambda: _FakeRegressor,
    )

    result = run_topix100_streak_353_next_session_intraday_lightgbm_research(
        str(tmp_path / "market.duckdb"),
        top_k_values=(1, 3, 5),
    )

    assert result.top_k_values == (1, 3, 5)
    assert set(result.validation_model_summary_df["model_name"]) == {"baseline", "lightgbm"}
    assert set(result.validation_topk_pick_df["selection_side"]) == {"long", "short"}
    assert "next_session_intraday_return" in result.feature_panel_df.columns

    bundle = write_topix100_streak_353_next_session_intraday_lightgbm_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260407_090000_testabcd",
    )
    loaded = load_topix100_streak_353_next_session_intraday_lightgbm_research_bundle(
        bundle.bundle_dir
    )

    assert loaded.top_k_values == (1, 3, 5)
    assert not loaded.validation_model_comparison_df.empty
