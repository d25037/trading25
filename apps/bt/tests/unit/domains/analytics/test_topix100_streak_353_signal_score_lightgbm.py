"""Tests for TOPIX100 streak 3/53 stage-2 LightGBM score research."""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from src.domains.analytics.topix100_streak_353_signal_score_lightgbm import (
    Topix100Streak353SignalScoreLightgbmResearchError,
    _load_lightgbm_regressor_cls,
    _build_baseline_lookup_df,
    _build_feature_panel_df,
    _build_scoring_snapshot_df,
    _build_validation_model_comparison_df,
    _score_topix100_streak_353_signal_lightgbm_snapshot_cached,
    format_topix100_streak_353_signal_score_lightgbm_notebook_error,
    get_topix100_streak_353_signal_score_lightgbm_bundle_path_for_run_id,
    get_topix100_streak_353_signal_score_lightgbm_latest_bundle_path,
    load_topix100_streak_353_signal_score_lightgbm_research_bundle,
    score_topix100_streak_353_signal_lightgbm_snapshot,
    write_topix100_streak_353_signal_score_lightgbm_research_bundle,
    run_topix100_streak_353_signal_score_lightgbm_research,
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


@pytest.fixture(autouse=True)
def _clear_runtime_snapshot_cache() -> None:
    _score_topix100_streak_353_signal_lightgbm_snapshot_cached.cache_clear()
    yield
    _score_topix100_streak_353_signal_lightgbm_snapshot_cached.cache_clear()


def _build_fake_inputs() -> tuple[SimpleNamespace, SimpleNamespace]:
    dates = [f"2026-01-{day:02d}" for day in range(1, 7)]
    event_rows: list[dict[str, object]] = []
    state_rows: list[dict[str, object]] = []
    horizon_rows: list[dict[str, object]] = []

    for date_index, date in enumerate(dates):
        for code_index in range(12):
            code = f"{1000 + code_index}"
            company_name = f"Stock {code}"
            price_gap = 0.30 - 0.02 * code_index + 0.005 * date_index
            volume_ratio = 1.20 if code_index % 2 == 0 else 0.75
            volume_ratio += 0.03 * date_index
            open_price = 100.0 + code_index + date_index
            close_price = open_price * (1.0 + 0.002 * (5 - code_index))
            high_price = max(open_price, close_price) * 1.01
            low_price = min(open_price, close_price) * 0.99

            short_mode = "bearish" if code_index % 4 < 2 else "bullish"
            long_mode = "bearish" if code_index < 6 else "bullish"
            state_key = f"long_{long_mode}__short_{short_mode}"
            state_label = (
                f"Long {'Bearish' if long_mode == 'bearish' else 'Bullish'} / "
                f"Short {'Bearish' if short_mode == 'bearish' else 'Bullish'}"
            )
            segment_return = (-0.05 if short_mode == "bearish" else 0.04) + 0.002 * date_index
            segment_day_count = 2 + (code_index % 3)
            future_return_5d = (
                0.015
                + 0.18 * price_gap
                + (0.03 if short_mode == "bearish" else -0.02)
                + (0.015 if long_mode == "bearish" else -0.01)
            )
            future_return_1d = (
                -0.005
                - 0.04 * price_gap
                + (0.02 if short_mode == "bullish" else -0.01)
                + (0.01 if long_mode == "bullish" else -0.005)
            )
            sample_split = "discovery" if date_index < 3 else "validation"

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
                    "price_vs_sma_50_gap": price_gap,
                    "volume_sma_5_20": volume_ratio,
                    "t_plus_1_return": future_return_1d,
                    "t_plus_5_return": future_return_5d,
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
                    "future_return_1d": future_return_1d,
                    "future_return_5d": future_return_5d,
                }
            )
            for horizon_days, future_return in ((1, future_return_1d), (5, future_return_5d)):
                horizon_rows.append(
                    {
                        "date": date,
                        "code": code,
                        "company_name": company_name,
                        "sample_split": sample_split,
                        "state_key": state_key,
                        "state_label": state_label,
                        "short_mode": short_mode,
                        "long_mode": long_mode,
                        "horizon_days": horizon_days,
                        "future_return": future_return,
                    }
                )

    price_result = SimpleNamespace(
        event_panel_df=pd.DataFrame.from_records(event_rows),
        source_mode="snapshot",
        source_detail="synthetic",
        available_start_date=dates[0],
        available_end_date=dates[-1],
    )
    state_result = SimpleNamespace(
        state_event_df=pd.DataFrame.from_records(state_rows),
        state_horizon_event_df=pd.DataFrame.from_records(horizon_rows),
    )
    return price_result, state_result


def test_build_feature_panel_df_adds_state_and_continuous_features() -> None:
    price_result, state_result = _build_fake_inputs()

    panel_df = _build_feature_panel_df(
        event_panel_df=price_result.event_panel_df,
        state_result=state_result,
        price_feature="price_vs_sma_50_gap",
        volume_feature="volume_sma_5_20",
        long_target_horizon_days=5,
        short_target_horizon_days=1,
    )

    assert not panel_df.empty
    assert {"recent_return_1d", "segment_abs_return", "short_edge_1d"} <= set(
        panel_df.columns
    )
    assert set(panel_df["sample_split"]) == {"discovery", "validation"}


def test_build_scoring_snapshot_df_joins_live_state_snapshot(monkeypatch) -> None:
    price_result, _state_result = _build_fake_inputs()
    history_df = price_result.event_panel_df[
        ["date", "code", "company_name", "close"]
    ].copy()
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm.build_topix100_streak_state_snapshot_df",
        lambda *args, **kwargs: pd.DataFrame.from_records(
            [
                {
                    "date": "2026-01-06",
                    "code": "1000",
                    "company_name": "Stock 1000",
                    "current_streak_day_count": 4,
                    "current_streak_segment_return": -0.08,
                    "current_streak_segment_abs_return": 0.08,
                    "short_mode": "bearish",
                    "long_mode": "bearish",
                    "state_key": "long_bearish__short_bearish",
                    "state_label": "Long Bearish / Short Bearish",
                },
                {
                    "date": "2026-01-06",
                    "code": "1001",
                    "company_name": "Stock 1001",
                    "current_streak_day_count": 3,
                    "current_streak_segment_return": 0.05,
                    "current_streak_segment_abs_return": 0.05,
                    "short_mode": "bullish",
                    "long_mode": "bullish",
                    "state_key": "long_bullish__short_bullish",
                    "state_label": "Long Bullish / Short Bullish",
                },
            ]
        ),
    )

    snapshot_df = _build_scoring_snapshot_df(
        event_panel_df=price_result.event_panel_df,
        history_df=history_df,
        target_date="2026-01-06",
        price_feature="price_vs_sma_50_gap",
        volume_feature="volume_sma_5_20",
        short_window_streaks=3,
        long_window_streaks=53,
    )

    assert set(snapshot_df["code"]) == {"1000", "1001"}
    assert set(snapshot_df["short_mode"]) == {"bearish", "bullish"}
    assert "recent_return_5d" in snapshot_df.columns
    assert "segment_return" in snapshot_df.columns


def test_build_baseline_lookup_df_uses_discovery_only() -> None:
    price_result, state_result = _build_fake_inputs()
    panel_df = _build_feature_panel_df(
        event_panel_df=price_result.event_panel_df,
        state_result=state_result,
        price_feature="price_vs_sma_50_gap",
        volume_feature="volume_sma_5_20",
        long_target_horizon_days=5,
        short_target_horizon_days=1,
    )
    horizon_panel_df = pd.concat(
        [
            panel_df.assign(
                horizon_days=1,
                future_return=panel_df["future_return_1d"],
                volume_bucket_label=panel_df["volume_bucket"],
                price_feature="price_vs_sma_50_gap",
                price_feature_label="Price/SMA50",
                volume_feature="volume_sma_5_20",
                volume_feature_label="Volume 5/20",
            )[
                [
                    "date",
                    "code",
                    "company_name",
                    "sample_split",
                    "state_key",
                    "state_label",
                    "short_mode",
                    "long_mode",
                    "horizon_days",
                    "decile_num",
                    "decile",
                    "volume_bucket",
                    "volume_bucket_label",
                    "future_return",
                    "price_feature",
                    "price_feature_label",
                    "volume_feature",
                    "volume_feature_label",
                ]
            ],
            panel_df.assign(
                horizon_days=5,
                future_return=panel_df["future_return_5d"],
                volume_bucket_label=panel_df["volume_bucket"],
                price_feature="price_vs_sma_50_gap",
                price_feature_label="Price/SMA50",
                volume_feature="volume_sma_5_20",
                volume_feature_label="Volume 5/20",
            )[
                [
                    "date",
                    "code",
                    "company_name",
                    "sample_split",
                    "state_key",
                    "state_label",
                    "short_mode",
                    "long_mode",
                    "horizon_days",
                    "decile_num",
                    "decile",
                    "volume_bucket",
                    "volume_bucket_label",
                    "future_return",
                    "price_feature",
                    "price_feature_label",
                    "volume_feature",
                    "volume_feature_label",
                ]
            ],
        ],
        ignore_index=True,
    )

    baseline_lookup_df = _build_baseline_lookup_df(
        horizon_panel_df,
        future_horizons=(1, 5),
    )

    assert not baseline_lookup_df.empty
    assert set(baseline_lookup_df["sample_split"]) == {"discovery"}
    assert "universe" in set(baseline_lookup_df["subset_key"])


def test_run_research_builds_validation_comparison(monkeypatch) -> None:
    module_price_result, module_state_result = _build_fake_inputs()

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm.run_topix100_price_vs_sma_rank_future_close_research",
        lambda *args, **kwargs: module_price_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: module_state_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm._load_lightgbm_regressor_cls",
        lambda: _FakeRegressor,
    )

    result = run_topix100_streak_353_signal_score_lightgbm_research(
        "/tmp/market.duckdb",
        top_k_values=(1, 2),
    )

    assert not result.validation_model_summary_df.empty
    assert set(result.validation_model_summary_df["model_name"]) == {
        "baseline",
        "lightgbm",
    }
    assert set(result.validation_model_summary_df["side"]) == {"long", "short"}
    assert not result.validation_model_comparison_df.empty
    assert not result.feature_importance_df.empty


def test_bundle_roundtrip_and_bundle_path_helpers(tmp_path, monkeypatch) -> None:
    module_price_result, module_state_result = _build_fake_inputs()

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm.run_topix100_price_vs_sma_rank_future_close_research",
        lambda *args, **kwargs: module_price_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: module_state_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm._load_lightgbm_regressor_cls",
        lambda: _FakeRegressor,
    )

    result = run_topix100_streak_353_signal_score_lightgbm_research(
        "/tmp/market.duckdb",
        top_k_values=(5, 10),
    )
    bundle = write_topix100_streak_353_signal_score_lightgbm_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260406_220000_testabcd",
    )
    loaded = load_topix100_streak_353_signal_score_lightgbm_research_bundle(bundle.bundle_dir)

    assert bundle.bundle_dir.exists()
    assert bundle.summary_path.exists()
    assert loaded.validation_model_summary_df.equals(result.validation_model_summary_df)
    assert (
        get_topix100_streak_353_signal_score_lightgbm_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    assert (
        get_topix100_streak_353_signal_score_lightgbm_bundle_path_for_run_id(
            "20260406_220000_testabcd",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )


def test_score_snapshot_trains_on_history_and_scores_current(monkeypatch) -> None:
    module_price_result, module_state_result = _build_fake_inputs()
    history_df = module_price_result.event_panel_df.copy()

    class _FakeContext:
        def __enter__(self):
            return SimpleNamespace(connection=object())

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm.get_topix100_streak_353_signal_score_lightgbm_latest_bundle_path",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm._open_analysis_connection",
        lambda db_path: _FakeContext(),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm._query_topix100_stock_history",
        lambda connection, end_date: history_df,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm._enrich_event_panel",
        lambda *args, **kwargs: module_price_result.event_panel_df,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm._build_state_event_df",
        lambda *args, **kwargs: module_state_result.state_event_df,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm._build_scoring_snapshot_df",
        lambda *args, **kwargs: pd.DataFrame.from_records(
            [
                {
                    "date": "2026-01-06",
                    "code": "1000",
                    "company_name": "Stock 1000",
                    "decile_num": 1,
                    "decile": "Q1",
                    "volume_bucket": "volume_low",
                    "segment_day_count": 4,
                    "segment_return": -0.08,
                    "segment_abs_return": 0.08,
                    "price_vs_sma_50_gap": 0.30,
                    "volume_sma_5_20": 1.20,
                    "recent_return_1d": -0.01,
                    "recent_return_3d": -0.03,
                    "recent_return_5d": -0.05,
                    "intraday_return": -0.01,
                    "range_pct": 0.02,
                    "short_mode": "bearish",
                    "long_mode": "bearish",
                    "state_key": "long_bearish__short_bearish",
                    "state_label": "Long Bearish / Short Bearish",
                },
                {
                    "date": "2026-01-06",
                    "code": "1001",
                    "company_name": "Stock 1001",
                    "decile_num": 10,
                    "decile": "Q10",
                    "volume_bucket": "volume_high",
                    "segment_day_count": 3,
                    "segment_return": 0.05,
                    "segment_abs_return": 0.05,
                    "price_vs_sma_50_gap": -0.10,
                    "volume_sma_5_20": 0.80,
                    "recent_return_1d": 0.01,
                    "recent_return_3d": 0.02,
                    "recent_return_5d": 0.03,
                    "intraday_return": 0.01,
                    "range_pct": 0.01,
                    "short_mode": "bullish",
                    "long_mode": "bullish",
                    "state_key": "long_bullish__short_bullish",
                    "state_label": "Long Bullish / Short Bullish",
                },
            ]
        ),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm._load_lightgbm_regressor_cls",
        lambda: _FakeRegressor,
    )

    snapshot = score_topix100_streak_353_signal_lightgbm_snapshot(
        "/tmp/market-empty.duckdb",
        target_date="2026-01-05",
    )

    assert snapshot.score_source_run_id is None
    assert set(snapshot.rows_by_code) == {"1000", "1001"}
    assert snapshot.rows_by_code["1000"].long_score_5d is not None
    assert snapshot.rows_by_code["1001"].short_score_1d is not None


def test_score_snapshot_returns_empty_when_state_pipeline_fails(monkeypatch) -> None:
    class _FakeContext:
        def __enter__(self):
            return SimpleNamespace(connection=object())

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm.get_topix100_streak_353_signal_score_lightgbm_latest_bundle_path",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm._open_analysis_connection",
        lambda db_path: _FakeContext(),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm._query_topix100_stock_history",
        lambda connection, end_date: pd.DataFrame.from_records([{"code": "1000"}]),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm._enrich_event_panel",
        lambda *args, **kwargs: pd.DataFrame.from_records([{"code": "1000"}]),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm._build_state_event_df",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("bad state")),
    )

    snapshot = score_topix100_streak_353_signal_lightgbm_snapshot(
        "/tmp/market.duckdb",
        target_date="2026-01-06",
    )

    assert snapshot.rows_by_code == {}


def test_lightgbm_error_helpers_cover_missing_runtime_paths(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_signal_score_lightgbm.import_module",
        lambda name: (_ for _ in ()).throw(ModuleNotFoundError("missing")),
    )
    with pytest.raises(Topix100Streak353SignalScoreLightgbmResearchError):
        _load_lightgbm_regressor_cls()

    assert "Install it with" in format_topix100_streak_353_signal_score_lightgbm_notebook_error(
        ModuleNotFoundError("missing")
    )
    assert "libomp" in format_topix100_streak_353_signal_score_lightgbm_notebook_error(
        OSError("libomp missing")
    )
    assert (
        format_topix100_streak_353_signal_score_lightgbm_notebook_error(
            Topix100Streak353SignalScoreLightgbmResearchError("boom")
        )
        == "boom"
    )


def test_build_validation_model_comparison_df_returns_lift_table() -> None:
    comparison_df = _build_validation_model_comparison_df(
        pd.DataFrame.from_records(
            [
                {
                    "side": "long",
                    "model_name": "baseline",
                    "top_k": 10,
                    "avg_selected_edge": 0.01,
                    "avg_edge_spread_vs_universe": 0.005,
                    "hit_rate_positive_edge": 0.5,
                },
                {
                    "side": "long",
                    "model_name": "lightgbm",
                    "top_k": 10,
                    "avg_selected_edge": 0.015,
                    "avg_edge_spread_vs_universe": 0.008,
                    "hit_rate_positive_edge": 0.6,
                },
            ]
        )
    )

    assert len(comparison_df) == 1
    row = comparison_df.iloc[0]
    assert float(row["edge_lift_vs_baseline"]) == pytest.approx(0.005)
    assert float(row["spread_lift_vs_baseline"]) == pytest.approx(0.003)
