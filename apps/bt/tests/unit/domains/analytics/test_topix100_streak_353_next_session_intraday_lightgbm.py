"""Tests for TOPIX100 next-session intraday LightGBM research."""

from __future__ import annotations

import pandas as pd
import pytest

from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
    Topix100PriceVsSmaRankFutureCloseResearchResult,
)
from src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm import (
    _score_topix100_streak_353_next_session_intraday_lightgbm_snapshot_cached,
    _build_feature_panel_df,
    _slice_feature_panel_to_recent_dates,
    _build_validation_model_comparison_df,
    _resolve_snapshot_score_source_run_id,
    _resolve_runtime_walkforward_split,
    load_topix100_streak_353_next_session_intraday_lightgbm_research_bundle,
    run_topix100_streak_353_next_session_intraday_lightgbm_research,
    score_topix100_streak_353_next_session_intraday_lightgbm_snapshot,
    write_topix100_streak_353_next_session_intraday_lightgbm_research_bundle,
)
from src.domains.analytics.topix100_streak_353_transfer import (
    Topix100Streak353TransferResearchResult,
)


@pytest.fixture(autouse=True)
def _clear_snapshot_score_cache() -> None:
    _score_topix100_streak_353_next_session_intraday_lightgbm_snapshot_cached.cache_clear()
    yield
    _score_topix100_streak_353_next_session_intraday_lightgbm_snapshot_cached.cache_clear()


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


def _build_fake_inputs() -> tuple[
    Topix100PriceVsSmaRankFutureCloseResearchResult,
    Topix100Streak353TransferResearchResult,
]:
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

    price_result = Topix100PriceVsSmaRankFutureCloseResearchResult(
        db_path="/tmp/market.duckdb",
        event_panel_df=pd.DataFrame.from_records(event_rows),
        source_mode="snapshot",
        source_detail="synthetic",
        available_start_date=dates[0],
        available_end_date=dates[-1],
        default_start_date=dates[0],
        analysis_start_date=dates[0],
        analysis_end_date=dates[-1],
        lookback_years=10,
        min_constituents_per_day=1,
        price_sma_windows=(50,),
        price_feature_order=("price_vs_sma_50_gap",),
        volume_sma_windows=((5, 20),),
        volume_feature_order=("volume_sma_5_20",),
        topix100_constituent_count=12,
        stock_day_count=len(event_rows),
        valid_date_count=len(dates),
        ranked_panel_df=pd.DataFrame(),
        ranking_feature_summary_df=pd.DataFrame(),
        decile_future_summary_df=pd.DataFrame(),
        daily_group_means_df=pd.DataFrame(),
        global_significance_df=pd.DataFrame(),
        pairwise_significance_df=pd.DataFrame(),
        price_bucket_daily_means_df=pd.DataFrame(),
        price_bucket_summary_df=pd.DataFrame(),
        price_bucket_pairwise_significance_df=pd.DataFrame(),
        group_hypothesis_df=pd.DataFrame(),
        price_volume_split_panel_df=pd.DataFrame(),
        price_volume_split_daily_means_df=pd.DataFrame(),
        price_volume_split_summary_df=pd.DataFrame(),
        price_volume_split_pairwise_significance_df=pd.DataFrame(),
        split_hypothesis_df=pd.DataFrame(),
    )
    state_result = Topix100Streak353TransferResearchResult(
        db_path="/tmp/market.duckdb",
        source_mode="snapshot",
        source_detail="synthetic",
        available_start_date=dates[0],
        available_end_date=dates[-1],
        analysis_start_date=dates[0],
        analysis_end_date=dates[-1],
        short_window_streaks=3,
        long_window_streaks=53,
        future_horizons=(1,),
        validation_ratio=0.25,
        min_stock_events_per_state=1,
        min_constituents_per_date_state=1,
        topix100_constituent_count=12,
        covered_constituent_count=12,
        valid_event_count=len(state_rows),
        valid_date_count=len(dates),
        state_event_df=pd.DataFrame.from_records(state_rows),
        state_horizon_event_df=pd.DataFrame(),
        state_event_summary_df=pd.DataFrame(),
        state_date_panel_df=pd.DataFrame(),
        state_date_summary_df=pd.DataFrame(),
        stock_state_mean_df=pd.DataFrame(),
        state_stock_consistency_df=pd.DataFrame(),
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


def test_snapshot_scoring_truncates_history_at_target_date(monkeypatch) -> None:
    target_date = "2026-01-05"
    price_feature = "price_vs_sma_50_gap"
    volume_feature = "volume_sma_5_20"
    full_history_df = pd.DataFrame.from_records(
        [
            {
                "date": "2026-01-04",
                "code": "1001",
                "company_name": "Stock 1001",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            },
            {
                "date": target_date,
                "code": "1001",
                "company_name": "Stock 1001",
                "open": 101.0,
                "high": 102.0,
                "low": 100.0,
                "close": 101.5,
                "volume": 1005,
            },
            {
                "date": "2026-01-06",
                "code": "1001",
                "company_name": "Stock 1001",
                "open": 102.0,
                "high": 103.0,
                "low": 101.0,
                "close": 102.5,
                "volume": 1010,
            },
        ]
    )
    captured: dict[str, object] = {}

    class _FakeContext:
        def __enter__(self):
            return type("FakeConnectionContext", (), {"connection": object()})()

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._resolve_snapshot_score_source_run_id",
        lambda categorical_feature_columns: None,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._open_analysis_connection",
        lambda db_path: _FakeContext(),
    )

    def _fake_query_topix100_stock_history(connection, end_date):  # noqa: ANN001
        captured["query_end_date"] = end_date
        if end_date == target_date:
            return full_history_df[full_history_df["date"] <= target_date].copy()
        return full_history_df.copy()

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._query_topix100_stock_history",
        _fake_query_topix100_stock_history,
    )

    def _fake_enrich_event_panel(
        history_df,  # noqa: ANN001
        *,
        analysis_start_date,
        analysis_end_date,
        min_constituents_per_day,
        price_sma_windows,
        volume_sma_windows,
    ):
        captured["analysis_end_date"] = analysis_end_date
        captured["event_panel_max_date"] = str(history_df["date"].max())
        return pd.DataFrame.from_records(
            [
                {
                    "date": target_date,
                    "code": "1001",
                    "company_name": "Stock 1001",
                }
            ]
        )

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._enrich_event_panel",
        _fake_enrich_event_panel,
    )

    def _fake_build_daily_state_panel_df(history_df, **kwargs):  # noqa: ANN001
        captured["state_history_max_date"] = str(history_df["date"].max())
        has_future_rows = str(history_df["date"].max()) > target_date
        return pd.DataFrame.from_records([{"date": target_date if has_future_rows else "2026-01-04"}])

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm.build_topix100_streak_daily_state_panel_df",
        _fake_build_daily_state_panel_df,
    )

    def _fake_build_feature_panel_from_state_event_df(
        *,
        event_panel_df,
        state_event_df,
        price_feature: str,
        volume_feature: str,
    ) -> pd.DataFrame:
        has_target_row = target_date in set(state_event_df["date"].astype(str))
        rows = [
            {
                "date": "2026-01-04",
                "code": "1001",
                "company_name": "Stock 1001",
                "decile_num": 1,
                "decile": "Q1",
                "volume_bucket": "volume_low",
                "current_streak_day_count": 4,
                "current_streak_segment_return": -0.03,
                "current_streak_segment_abs_return": 0.03,
                "short_mode": "bearish",
                "long_mode": "bearish",
                "state_key": "train_state",
                "state_label": "Train State",
                price_feature: 0.1,
                volume_feature: 1.0,
                "recent_return_1d": -0.01,
                "recent_return_3d": -0.02,
                "recent_return_5d": -0.03,
                "intraday_return": -0.01,
                "range_pct": 0.02,
                "next_session_intraday_return": 0.01,
            }
        ]
        if has_target_row:
            rows.append(
                {
                    "date": target_date,
                    "code": "1001",
                    "company_name": "Stock 1001",
                    "decile_num": 10,
                    "decile": "Q10",
                    "volume_bucket": "volume_high",
                    "current_streak_day_count": 9,
                    "current_streak_segment_return": 0.25,
                    "current_streak_segment_abs_return": 0.25,
                    "short_mode": "bullish",
                    "long_mode": "bullish",
                    "state_key": "future_contaminated_state",
                    "state_label": "Future Contaminated State",
                    price_feature: -9.0,
                    volume_feature: 2.0,
                    "recent_return_1d": 0.03,
                    "recent_return_3d": 0.05,
                    "recent_return_5d": 0.07,
                    "intraday_return": 0.02,
                    "range_pct": 0.04,
                    "next_session_intraday_return": 0.99,
                }
            )
        return pd.DataFrame.from_records(rows)

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._build_feature_panel_from_state_event_df",
        _fake_build_feature_panel_from_state_event_df,
    )

    def _fake_build_scoring_snapshot_df(**kwargs):  # noqa: ANN001
        captured["used_snapshot_fallback"] = True
        return pd.DataFrame.from_records(
            [
                {
                    "date": target_date,
                    "code": "1001",
                    "company_name": "Stock 1001",
                    "decile_num": 2,
                    "decile": "Q2",
                    "volume_bucket": "volume_low",
                    "current_streak_day_count": 5,
                    "current_streak_segment_return": -0.05,
                    "current_streak_segment_abs_return": 0.05,
                    "short_mode": "bearish",
                    "long_mode": "bearish",
                    "state_key": "point_in_time_snapshot",
                    "state_label": "Point in Time Snapshot",
                    price_feature: 7.5,
                    volume_feature: 0.8,
                    "recent_return_1d": -0.02,
                    "recent_return_3d": -0.03,
                    "recent_return_5d": -0.04,
                    "intraday_return": -0.01,
                    "range_pct": 0.02,
                }
            ]
        )

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._build_scoring_snapshot_df",
        _fake_build_scoring_snapshot_df,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._build_category_lookup",
        lambda feature_panel_df: {},
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._load_lightgbm_regressor_cls",
        lambda: _FakeRegressor,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._predict_lightgbm_snapshot_scores",
        lambda **kwargs: {
            str(kwargs["snapshot_df"].iloc[0]["code"]): float(
                kwargs["snapshot_df"].iloc[0][price_feature]
            )
        },
    )

    snapshot = score_topix100_streak_353_next_session_intraday_lightgbm_snapshot(
        "/tmp/market.duckdb",
        target_date=target_date,
        price_feature=price_feature,
        volume_feature=volume_feature,
    )

    assert captured["query_end_date"] == target_date
    assert captured["analysis_end_date"] == target_date
    assert captured["event_panel_max_date"] == target_date
    assert captured["state_history_max_date"] == target_date
    assert captured["used_snapshot_fallback"] is True
    assert snapshot.rows_by_code["1001"].state_key == "point_in_time_snapshot"
    assert snapshot.rows_by_code["1001"].intraday_score == pytest.approx(7.5)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"price_feature": "unsupported"}, "Unsupported price_feature"),
        ({"volume_feature": "unsupported"}, "Unsupported volume_feature"),
        ({"short_window_streaks": 53, "long_window_streaks": 53}, "short_window_streaks"),
        ({"train_lookback_days": 0}, "train_lookback_days"),
        ({"test_window_days": 0}, "test_window_days"),
        ({"step_days": 0}, "step_days"),
        ({"purge_signal_dates": -1}, "purge_signal_dates"),
    ],
)
def test_snapshot_scoring_validates_runtime_parameters(
    kwargs: dict[str, object],
    message: str,
) -> None:
    base_kwargs = {
        "db_path": "/tmp/market.duckdb",
        "target_date": "2026-01-05",
        "price_feature": "price_vs_sma_50_gap",
        "volume_feature": "volume_sma_5_20",
        "short_window_streaks": 3,
        "long_window_streaks": 53,
        "train_lookback_days": 756,
        "test_window_days": 126,
        "step_days": 126,
        "purge_signal_dates": 0,
    }
    base_kwargs.update(kwargs)

    with pytest.raises(ValueError, match=message):
        score_topix100_streak_353_next_session_intraday_lightgbm_snapshot(**base_kwargs)


def test_resolve_snapshot_score_source_run_id_handles_runtime_and_non_runtime_variants(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm.find_latest_research_bundle_path",
        lambda experiment_id: "/tmp/bundle" if experiment_id else None,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm.load_research_bundle_info",
        lambda bundle_path: type("BundleInfo", (), {"run_id": "run-123"})(),
    )

    assert _resolve_snapshot_score_source_run_id(("decile",)) == "run-123"
    assert _resolve_snapshot_score_source_run_id(("decile", "volume_bucket")) is None


def test_snapshot_scoring_returns_empty_when_history_or_event_panel_is_empty(
    monkeypatch,
) -> None:
    class _FakeContext:
        def __enter__(self):
            return type("FakeConnectionContext", (), {"connection": object()})()

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._resolve_snapshot_score_source_run_id",
        lambda categorical_feature_columns: None,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._open_analysis_connection",
        lambda db_path: _FakeContext(),
    )

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._query_topix100_stock_history",
        lambda connection, end_date: pd.DataFrame(),
    )
    empty_history = score_topix100_streak_353_next_session_intraday_lightgbm_snapshot(
        "/tmp/market.duckdb",
        target_date="2026-01-05",
    )
    assert empty_history.rows_by_code == {}
    assert empty_history.split_train_start is None

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._query_topix100_stock_history",
        lambda connection, end_date: pd.DataFrame.from_records(
            [
                {
                    "date": "2026-01-05",
                    "code": "1001",
                    "company_name": "Stock 1001",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._enrich_event_panel",
        lambda **kwargs: pd.DataFrame(),
    )
    empty_panel = score_topix100_streak_353_next_session_intraday_lightgbm_snapshot(
        "/tmp/market.duckdb",
        target_date="2026-01-05",
    )
    assert empty_panel.rows_by_code == {}
    assert empty_panel.split_train_end is None


def test_snapshot_scoring_returns_empty_when_state_pipeline_or_training_slice_fails(
    monkeypatch,
) -> None:
    target_date = "2026-01-05"

    class _FakeContext:
        def __enter__(self):
            return type("FakeConnectionContext", (), {"connection": object()})()

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    history_df = pd.DataFrame.from_records(
        [
            {
                "date": "2026-01-04",
                "code": "1001",
                "company_name": "Stock 1001",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            },
            {
                "date": target_date,
                "code": "1001",
                "company_name": "Stock 1001",
                "open": 101.0,
                "high": 102.0,
                "low": 100.0,
                "close": 101.5,
                "volume": 1005,
            },
        ]
    )

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._resolve_snapshot_score_source_run_id",
        lambda categorical_feature_columns: None,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._open_analysis_connection",
        lambda db_path: _FakeContext(),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._query_topix100_stock_history",
        lambda connection, end_date: history_df.copy(),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._enrich_event_panel",
        lambda *args, **kwargs: pd.DataFrame.from_records(
            [
                {
                    "date": "2026-01-04",
                    "code": "1001",
                    "company_name": "Stock 1001",
                }
            ]
        ),
    )

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm.build_topix100_streak_daily_state_panel_df",
        lambda history_df, **kwargs: (_ for _ in ()).throw(ValueError("bad-state")),
    )
    state_failure = score_topix100_streak_353_next_session_intraday_lightgbm_snapshot(
        "/tmp/market.duckdb",
        target_date=target_date,
    )
    assert state_failure.rows_by_code == {}

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm.build_topix100_streak_daily_state_panel_df",
        lambda history_df, **kwargs: pd.DataFrame.from_records([{"date": "2026-01-04"}]),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._build_feature_panel_from_state_event_df",
        lambda **kwargs: pd.DataFrame.from_records(
            [
                {
                    "date": target_date,
                    "code": "1001",
                    "company_name": "Stock 1001",
                    "decile_num": 1,
                    "decile": "Q1",
                    "volume_bucket": "volume_low",
                    "current_streak_day_count": 4,
                    "current_streak_segment_return": -0.03,
                    "current_streak_segment_abs_return": 0.03,
                    "short_mode": "bearish",
                    "long_mode": "bearish",
                    "state_key": "state",
                    "state_label": "State",
                    "price_vs_sma_50_gap": 0.1,
                    "volume_sma_5_20": 1.0,
                    "recent_return_1d": -0.01,
                    "recent_return_3d": -0.02,
                    "recent_return_5d": -0.03,
                    "intraday_return": -0.01,
                    "range_pct": 0.02,
                    "next_session_intraday_return": 0.01,
                }
            ]
        ),
    )
    no_training = score_topix100_streak_353_next_session_intraday_lightgbm_snapshot(
        "/tmp/market.duckdb",
        target_date=target_date,
    )
    assert no_training.rows_by_code == {}


def test_run_research_builds_signed_intraday_scorecard(monkeypatch, tmp_path) -> None:
    price_result, state_result = _build_fake_inputs()

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm._build_research_feature_panel_df",
        lambda **kwargs: (
            price_result,
            _build_feature_panel_df(
                event_panel_df=price_result.event_panel_df,
                state_result=state_result,
                price_feature="price_vs_sma_50_gap",
                volume_feature="volume_sma_5_20",
            ),
        ),
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
