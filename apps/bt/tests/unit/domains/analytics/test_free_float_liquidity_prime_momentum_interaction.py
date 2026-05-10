from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from src.domains.analytics.free_float_liquidity_prime_momentum_interaction import (
    FREE_FLOAT_LIQUIDITY_PRIME_MOMENTUM_INTERACTION_EXPERIMENT_ID,
    build_summary_markdown,
    get_free_float_liquidity_prime_momentum_interaction_bundle_path_for_run_id,
    get_free_float_liquidity_prime_momentum_interaction_latest_bundle_path,
    load_free_float_liquidity_prime_momentum_interaction_bundle,
    run_free_float_liquidity_prime_momentum_interaction,
    write_free_float_liquidity_prime_momentum_interaction_bundle,
)
from src.domains.analytics.free_float_liquidity_regime_decomposition import (
    FreeFloatLiquidityRegimeDecompositionResult,
    write_free_float_liquidity_regime_decomposition_bundle,
)


def test_prime_momentum_interaction_builds_regression_and_buckets(
    tmp_path: Path,
) -> None:
    input_bundle = _write_regime_bundle(tmp_path)

    result = run_free_float_liquidity_prime_momentum_interaction(
        input_bundle,
        output_root=tmp_path,
        horizons=(20, 60),
        min_observations=10,
    )

    assert not result.prime_panel_df.empty
    assert not result.factor_regression_df.empty
    assert not result.interaction_bucket_df.empty
    assert not result.momentum_residual_summary_df.empty
    assert "momentum_liquidity_interaction" in set(
        result.factor_regression_df["model_name"].astype(str)
    )
    assert "positive_momentum_high_minus_neutral" in set(
        result.momentum_residual_summary_df["comparison_name"].astype(str)
    )


def test_prime_momentum_interaction_writes_and_loads_bundle(tmp_path: Path) -> None:
    input_bundle = _write_regime_bundle(tmp_path)
    result = run_free_float_liquidity_prime_momentum_interaction(
        input_bundle,
        output_root=tmp_path,
        min_observations=10,
    )

    summary = build_summary_markdown(result)
    assert "Factor Regressions" in summary
    assert "Interaction Buckets" in summary

    bundle = write_free_float_liquidity_prime_momentum_interaction_bundle(
        result,
        output_root=tmp_path,
        run_id="prime-momentum-test",
    )
    loaded = load_free_float_liquidity_prime_momentum_interaction_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == FREE_FLOAT_LIQUIDITY_PRIME_MOMENTUM_INTERACTION_EXPERIMENT_ID
    )
    assert (
        get_free_float_liquidity_prime_momentum_interaction_bundle_path_for_run_id(
            "prime-momentum-test",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_free_float_liquidity_prime_momentum_interaction_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    pd.testing.assert_frame_equal(
        loaded.factor_regression_df.reset_index(drop=True),
        result.factor_regression_df.reset_index(drop=True),
        check_dtype=False,
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"horizons": (0,)}, "horizons must contain positive integers"),
        ({"min_observations": 9}, "min_observations must be at least 10"),
    ],
)
def test_prime_momentum_interaction_rejects_invalid_params(
    tmp_path: Path,
    kwargs: dict[str, Any],
    message: str,
) -> None:
    input_bundle = _write_regime_bundle(tmp_path)

    with pytest.raises(ValueError, match=message):
        run_free_float_liquidity_prime_momentum_interaction(
            input_bundle,
            output_root=tmp_path,
            **kwargs,
        )


def _write_regime_bundle(tmp_path: Path) -> Path:
    result = FreeFloatLiquidityRegimeDecompositionResult(
        db_path=str(tmp_path / "market.duckdb"),
        source_mode="live",
        source_detail="fixture",
        input_bundle_path=str(tmp_path / "gap"),
        input_run_id="gap-fixture",
        input_git_commit=None,
        analysis_start_date="2024-01-31",
        analysis_end_date="2024-05-31",
        recent_return_windows=(20, 60),
        high_residual_z=1.0,
        low_residual_z=-1.0,
        recovery_change_threshold=0.25,
        feature_policy="fixture",
        enriched_observation_df=_fixture_enriched_observations(),
        regime_forward_return_df=pd.DataFrame(columns=["market_scope"]),
        market_regime_diagnostics_df=pd.DataFrame(columns=["market_scope"]),
        latest_prime_regime_df=pd.DataFrame(columns=["date"]),
    )
    bundle = write_free_float_liquidity_regime_decomposition_bundle(
        result,
        output_root=tmp_path,
        run_id="regime-fixture",
    )
    return bundle.bundle_dir


def _fixture_enriched_observations() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    dates = pd.bdate_range("2024-01-31", periods=8).strftime("%Y-%m-%d").tolist()
    specs = [
        ("1111", "positive_20d_60d", 1.6, 10.0, 25.0, 8.0),
        ("2222", "positive_20d_60d", 0.0, 9.0, 20.0, 3.0),
        ("3333", "mixed_or_negative", 1.5, -4.0, -8.0, -2.0),
        ("4444", "positive_20d_60d", -1.3, 3.0, 5.0, 0.5),
    ]
    for date_index, date in enumerate(dates):
        for code, _state, residual_z, ret20, ret60, base_forward in specs:
            forward = base_forward + date_index * 0.1
            rows.append(
                {
                    "date": date,
                    "code": code,
                    "company_name": f"Stock {code}",
                    "sector_33_name": "Services",
                    "market_scope": "prime",
                    "adv_window": 60,
                    "liquidity_regime": (
                        "rerating_participation"
                        if residual_z >= 1.0 and ret20 > 0 and ret60 > 0
                        else "distribution_stress"
                        if residual_z >= 1.0
                        else "stale_liquidity"
                    ),
                    "recent_return_20d_pct": ret20 + date_index * 0.2,
                    "recent_return_60d_pct": ret60 + date_index * 0.3,
                    "liquidity_residual_z": residual_z,
                    "liquidity_residual_change": 0.2,
                    "adv_mil_jpy": 100.0,
                    "free_float_market_cap_bil_jpy": 50.0,
                    "forward_excess_return_20d_pct": forward / 2.0,
                    "forward_excess_return_60d_pct": forward,
                }
            )
    return pd.DataFrame(rows)
