from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.free_float_liquidity_gap import (
    FreeFloatLiquidityGapResult,
    write_free_float_liquidity_gap_bundle,
)
from src.domains.analytics.free_float_liquidity_regime_decomposition import (
    FREE_FLOAT_LIQUIDITY_REGIME_DECOMPOSITION_EXPERIMENT_ID,
    build_summary_markdown,
    get_free_float_liquidity_regime_decomposition_bundle_path_for_run_id,
    get_free_float_liquidity_regime_decomposition_latest_bundle_path,
    load_free_float_liquidity_regime_decomposition_bundle,
    run_free_float_liquidity_regime_decomposition,
    write_free_float_liquidity_regime_decomposition_bundle,
)


def test_free_float_liquidity_regime_decomposition_splits_high_residual_direction(
    tmp_path: Path,
) -> None:
    db_path = _build_regime_db(tmp_path / "market.duckdb")
    input_bundle = _write_gap_bundle(tmp_path, db_path)

    result = run_free_float_liquidity_regime_decomposition(
        input_bundle,
        output_root=tmp_path,
        recent_return_windows=(20, 60),
        high_residual_z=1.0,
        low_residual_z=-1.0,
        recovery_change_threshold=0.25,
    )

    regimes = set(result.enriched_observation_df["liquidity_regime"].astype(str))
    assert "rerating_participation" in regimes
    assert "distribution_stress" in regimes
    assert "stale_liquidity" in regimes
    assert "liquidity_recovery" in regimes
    assert not result.regime_forward_return_df.empty
    assert {"prime", "standard", "all"}.issubset(
        set(result.market_regime_diagnostics_df["market_scope"].astype(str))
    )
    assert not result.latest_prime_regime_df.empty

    rerating = result.enriched_observation_df[
        result.enriched_observation_df["code"].astype(str) == "1111"
    ].iloc[0]
    distribution = result.enriched_observation_df[
        result.enriched_observation_df["code"].astype(str) == "2222"
    ].iloc[0]
    assert rerating["liquidity_regime"] == "rerating_participation"
    assert distribution["liquidity_regime"] == "distribution_stress"


def test_free_float_liquidity_regime_decomposition_writes_and_loads_bundle(
    tmp_path: Path,
) -> None:
    db_path = _build_regime_db(tmp_path / "market.duckdb")
    input_bundle = _write_gap_bundle(tmp_path, db_path)
    result = run_free_float_liquidity_regime_decomposition(
        input_bundle, output_root=tmp_path
    )

    summary = build_summary_markdown(result)
    assert "Regime Forward Returns" in summary
    assert "Latest Prime Regimes" in summary

    bundle = write_free_float_liquidity_regime_decomposition_bundle(
        result,
        output_root=tmp_path,
        run_id="regime-test",
    )
    loaded = load_free_float_liquidity_regime_decomposition_bundle(bundle.bundle_dir)

    assert (
        bundle.experiment_id == FREE_FLOAT_LIQUIDITY_REGIME_DECOMPOSITION_EXPERIMENT_ID
    )
    assert (
        get_free_float_liquidity_regime_decomposition_bundle_path_for_run_id(
            "regime-test",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_free_float_liquidity_regime_decomposition_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    pd.testing.assert_frame_equal(
        loaded.regime_forward_return_df.reset_index(drop=True),
        result.regime_forward_return_df.reset_index(drop=True),
        check_dtype=False,
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {"recent_return_windows": (0,)},
            "recent_return_windows must contain positive integers",
        ),
        ({"high_residual_z": 0.0}, "high_residual_z must be positive"),
        ({"low_residual_z": 0.0}, "low_residual_z must be negative"),
        (
            {"recovery_change_threshold": 0.0},
            "recovery_change_threshold must be positive",
        ),
    ],
)
def test_free_float_liquidity_regime_decomposition_rejects_invalid_params(
    tmp_path: Path,
    kwargs: dict[str, Any],
    message: str,
) -> None:
    db_path = _build_regime_db(tmp_path / "market.duckdb")
    input_bundle = _write_gap_bundle(tmp_path, db_path)

    with pytest.raises(ValueError, match=message):
        run_free_float_liquidity_regime_decomposition(
            input_bundle, output_root=tmp_path, **kwargs
        )


def _build_regime_db(db_path: Path) -> Path:
    dates = pd.bdate_range("2024-01-02", periods=90).strftime("%Y-%m-%d").tolist()
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE stock_data (
            code TEXT,
            date TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE
        )
        """
    )
    rows: list[tuple[str, str, float, float, float, float, float]] = []
    for idx, date in enumerate(dates):
        up_close = 100.0 + idx * 2.0
        down_close = 300.0 - idx * 2.0
        stale_close = 100.0
        recovery_close = 80.0 + idx * 0.2
        rows.extend(
            [
                ("1111", date, up_close, up_close, up_close, up_close, 1000.0),
                ("2222", date, down_close, down_close, down_close, down_close, 1000.0),
                (
                    "3333",
                    date,
                    stale_close,
                    stale_close,
                    stale_close,
                    stale_close,
                    1000.0,
                ),
                (
                    "4444",
                    date,
                    recovery_close,
                    recovery_close,
                    recovery_close,
                    recovery_close,
                    1000.0,
                ),
            ]
        )
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    conn.close()
    return db_path


def _write_gap_bundle(tmp_path: Path, db_path: Path) -> Path:
    observation_date = "2024-04-15"
    observation_df = pd.DataFrame(
        [
            _observation("1111", observation_date, "prime", 60, 1.5, 0.4, 10.0),
            _observation("2222", observation_date, "prime", 60, 1.6, 0.5, -10.0),
            _observation("3333", observation_date, "standard", 60, -1.3, -0.1, 1.0),
            _observation("4444", observation_date, "standard", 60, -1.2, 0.5, 2.0),
        ]
    )
    result = FreeFloatLiquidityGapResult(
        db_path=str(db_path),
        source_mode="live",
        source_detail="fixture",
        market_source="fixture",
        analysis_start_date=observation_date,
        analysis_end_date=observation_date,
        adv_windows=(60,),
        horizons=(20,),
        change_window=20,
        observation_stride_sessions=20,
        bucket_count=5,
        min_regression_observations=3,
        feature_policy="fixture",
        observation_df=observation_df,
        market_regression_df=pd.DataFrame(columns=["market_scope"]),
        residual_bucket_forward_return_df=pd.DataFrame(columns=["market_scope"]),
        residual_change_bucket_forward_return_df=pd.DataFrame(columns=["market_scope"]),
        market_sample_diagnostics_df=pd.DataFrame(columns=["market_scope"]),
    )
    bundle = write_free_float_liquidity_gap_bundle(
        result,
        output_root=tmp_path,
        run_id="gap-fixture",
    )
    return bundle.bundle_dir


def _observation(
    code: str,
    date: str,
    market_scope: str,
    adv_window: int,
    residual_z: float,
    residual_change: float,
    forward_return: float,
) -> dict[str, Any]:
    return {
        "code": code,
        "date": date,
        "company_name": f"Stock {code}",
        "market_scope": market_scope,
        "market_code": {"prime": "0111", "standard": "0112"}[market_scope],
        "market_name": market_scope,
        "sector_33_name": "Services",
        "adv_window": adv_window,
        "change_window": 20,
        "close": 100.0,
        "volume": 1000.0,
        "adv_jpy": 1_000_000.0,
        "adv_mil_jpy": 1.0,
        "prior_adv_jpy": 900_000.0,
        "adv_log_change": 0.1,
        "free_float_market_cap_jpy": 1_000_000_000.0,
        "free_float_market_cap_bil_jpy": 1.0,
        "prior_free_float_market_cap_jpy": 1_000_000_000.0,
        "shares_outstanding": 10_000_000.0,
        "treasury_shares": 0.0,
        "free_float_ratio_pct": 100.0,
        "log_adv": 1.0,
        "log_free_float_market_cap": 1.0,
        "liquidity_residual": residual_z,
        "liquidity_residual_z": residual_z,
        "liquidity_implied_free_float_market_cap_bil_jpy": 1.0,
        "liquidity_implied_ffcap_gap_pct": 100.0 * residual_z,
        "prior_liquidity_residual": residual_z - residual_change,
        "liquidity_residual_change": residual_change,
        "forward_return_20d_pct": forward_return,
        "forward_topix_return_20d_pct": 0.0,
        "forward_excess_return_20d_pct": forward_return,
    }
