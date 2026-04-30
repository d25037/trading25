"""Tests for forward EPS technical horizon decomposition helpers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

import src.domains.analytics.forward_eps_technical_horizon_decomposition as module
from src.domains.analytics.forward_eps_technical_horizon_decomposition import (
    ForwardEpsTechnicalHorizonDecompositionResult,
    _build_horizon_bucket_summary_df,
    _build_horizon_candidate_summary_df,
    _build_horizon_contrast_summary_df,
    _build_horizon_tail_profile_df,
    _build_threshold_summary_df,
    _prepare_horizon_frame,
    get_forward_eps_technical_horizon_decomposition_bundle_path_for_run_id,
    get_forward_eps_technical_horizon_decomposition_latest_bundle_path,
    load_forward_eps_technical_horizon_decomposition_bundle,
    run_forward_eps_technical_horizon_decomposition,
    write_forward_eps_technical_horizon_decomposition_bundle,
)


def _sample_enriched_trades() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "window_label": [
                "train_pre_holdout",
                "train_pre_holdout",
                "train_pre_holdout",
                "full",
                "full",
                "holdout_6m",
            ],
            "market_scope": ["prime", "prime", "standard", "prime", "standard", "prime"],
            "symbol": ["1001", "1002", "2001", "1001", "2001", "1002"],
            "entry_date": [
                "2024-05-01",
                "2024-05-02",
                "2024-05-07",
                "2024-05-08",
                "2024-05-09",
                "2024-05-10",
            ],
            "trade_return_pct": [12.0, -14.0, 6.0, -12.0, 18.0, 4.0],
        }
    )


def _fake_stock_data(
    dataset_name: str,
    stock_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    del dataset_name, start_date, end_date
    index = pd.bdate_range("2024-01-01", "2024-05-10")
    base = 100.0 if stock_code != "2001" else 80.0
    step = 1.0 if stock_code != "1002" else 2.0
    close = pd.Series([base + i * step for i in range(len(index))], index=index)
    return pd.DataFrame(
        {
            "Open": close,
            "High": close * 1.01,
            "Low": close * 0.99,
            "Close": close,
            "Volume": 100_000,
        },
        index=index,
    )


def test_prepare_horizon_frame_adds_entry_time_technical_features(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(module, "load_stock_data", _fake_stock_data)

    frame = _prepare_horizon_frame(
        _sample_enriched_trades(),
        dataset_name="primeExTopix500",
        risk_ratio_type="sharpe",
    )

    assert {"rsi_10", "runup_20d_pct", "risk_adjusted_return_60d"}.issubset(
        frame.columns
    )
    assert frame["technical_feature_date"].notna().all()
    assert frame["runup_10d_pct"].notna().all()


def test_horizon_tables_and_candidates_use_train_thresholds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(module, "load_stock_data", _fake_stock_data)
    frame = _prepare_horizon_frame(
        _sample_enriched_trades(),
        dataset_name="primeExTopix500",
        risk_ratio_type="sharpe",
    )

    thresholds = _build_threshold_summary_df(frame=frame, threshold_quantile=0.8)
    buckets = _build_horizon_bucket_summary_df(
        frame=frame,
        quantile_bucket_count=2,
        severe_loss_threshold_pct=-10.0,
    )
    contrasts = _build_horizon_contrast_summary_df(buckets)
    tail = _build_horizon_tail_profile_df(
        frame=frame,
        severe_loss_threshold_pct=-10.0,
    )
    candidates = _build_horizon_candidate_summary_df(
        frame=frame,
        threshold_summary_df=thresholds,
        severe_loss_threshold_pct=-10.0,
        threshold_quantile=0.8,
        size_haircut=0.5,
    )

    assert thresholds["market_scope"].isin(["all", "prime", "standard"]).all()
    assert not buckets.empty
    assert not contrasts.empty
    assert "median_runup_60d_pct" in tail.columns
    legacy_full = candidates[
        (candidates["window_label"] == "full")
        & (candidates["market_scope"] == "all")
        & (candidates["candidate_name"] == "legacy_20_60_runup_rar60_q80_overlap_ge2")
    ].iloc[0]
    assert legacy_full["calibration_market_scope"] == "all"
    assert legacy_full["haircut_size_multiplier"] == 0.5


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"holdout_months": 0}, "holdout_months"),
        ({"severe_loss_threshold_pct": 0.0}, "severe_loss_threshold_pct"),
        ({"quantile_bucket_count": 1}, "quantile_bucket_count"),
        ({"threshold_quantile": 1.0}, "threshold_quantile"),
        ({"size_haircut": 1.5}, "size_haircut"),
    ],
)
def test_run_validates_input_boundaries(kwargs: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        run_forward_eps_technical_horizon_decomposition(**kwargs)


def test_public_run_summary_and_bundle_roundtrip(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(module, "load_stock_data", _fake_stock_data)
    base_result = SimpleNamespace(
        strategy_name="production/forward_eps_driven",
        dataset_name="primeExTopix500",
        holdout_months=6,
        severe_loss_threshold_pct=-10.0,
        quantile_bucket_count=2,
        analysis_start_date="2024-01-01",
        analysis_end_date="2024-05-10",
        dataset_summary_df=pd.DataFrame({"dataset_name": ["primeExTopix500"]}),
        scenario_summary_df=pd.DataFrame(
            {
                "window_label": ["full", "holdout_6m"],
                "trade_count": [6, 1],
                "avg_trade_return_pct": [2.3333333333, 4.0],
            }
        ),
        market_scope_summary_df=pd.DataFrame(
            {
                "window_label": ["full"],
                "market_scope": ["all"],
                "trade_count": [6],
            }
        ),
        enriched_trade_df=_sample_enriched_trades(),
    )
    monkeypatch.setattr(
        module,
        "run_forward_eps_trade_archetype_decomposition",
        lambda **_: base_result,
    )

    result = run_forward_eps_technical_horizon_decomposition(
        holdout_months=6,
        quantile_bucket_count=2,
        size_haircut=0.5,
    )
    bundle = write_forward_eps_technical_horizon_decomposition_bundle(
        result,
        output_root=tmp_path,
        run_id="unit_roundtrip",
        notes="unit test",
    )
    loaded = load_forward_eps_technical_horizon_decomposition_bundle(bundle.bundle_dir)

    assert isinstance(loaded, ForwardEpsTechnicalHorizonDecompositionResult)
    assert result.horizon_candidate_summary_df.shape[0] > 0
    assert loaded.strategy_name == result.strategy_name
    assert loaded.threshold_summary_df.shape[0] == result.threshold_summary_df.shape[0]
    assert (
        get_forward_eps_technical_horizon_decomposition_bundle_path_for_run_id(
            "unit_roundtrip",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_forward_eps_technical_horizon_decomposition_latest_bundle_path(
        output_root=tmp_path
    ) == bundle.bundle_dir
