from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.domains.analytics.annual_prime_value_technical_risk_decomposition import (
    ANNUAL_PRIME_VALUE_TECHNICAL_RISK_DECOMPOSITION_EXPERIMENT_ID,
    get_annual_prime_value_technical_risk_decomposition_bundle_path_for_run_id,
    get_annual_prime_value_technical_risk_decomposition_latest_bundle_path,
    load_annual_prime_value_technical_risk_decomposition_bundle,
    run_annual_prime_value_technical_risk_decomposition,
    write_annual_prime_value_technical_risk_decomposition_bundle,
)
from tests.unit.domains.analytics.test_annual_value_composite_technical_filter import (
    _write_value_bundle,
)


def test_run_annual_prime_value_technical_risk_decomposition_builds_tables(
    tmp_path: Path,
) -> None:
    value_bundle = _write_value_bundle(tmp_path)

    result = run_annual_prime_value_technical_risk_decomposition(
        value_bundle,
        output_root=tmp_path,
        selection_fractions=(0.80,),
        market_scope="prime",
        bucket_count=3,
    )

    assert result.input_run_id == "value-selection"
    assert result.market_scope == "prime"
    assert result.selected_event_count > 0
    assert result.risk_feature_count > 0
    assert {"beta_252d", "idiosyncratic_volatility_pct_252d", "beta_adjusted_event_return_pct"}.issubset(
        result.enriched_event_df.columns
    )
    assert not result.risk_bucket_summary_df.empty
    assert not result.risk_spread_df.empty
    assert not result.portfolio_daily_df.empty
    assert not result.portfolio_summary_df.empty
    assert {"volatility_20d_pct", "beta_252d"}.issubset(set(result.risk_spread_df["feature_key"].astype(str)))
    assert {"baseline", "low_bucket", "high_bucket"}.issubset(
        set(result.portfolio_summary_df["portfolio_variant"].astype(str))
    )


def test_write_and_load_annual_prime_value_technical_risk_decomposition_bundle(
    tmp_path: Path,
) -> None:
    value_bundle = _write_value_bundle(tmp_path)
    result = run_annual_prime_value_technical_risk_decomposition(
        value_bundle,
        output_root=tmp_path,
        selection_fractions=(0.80,),
        market_scope="prime",
        bucket_count=3,
    )

    bundle = write_annual_prime_value_technical_risk_decomposition_bundle(
        result,
        output_root=tmp_path,
        run_id="prime-risk",
    )
    loaded = load_annual_prime_value_technical_risk_decomposition_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == ANNUAL_PRIME_VALUE_TECHNICAL_RISK_DECOMPOSITION_EXPERIMENT_ID
    assert (
        get_annual_prime_value_technical_risk_decomposition_bundle_path_for_run_id(
            "prime-risk",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_annual_prime_value_technical_risk_decomposition_latest_bundle_path(output_root=tmp_path) == bundle.bundle_dir
    pd.testing.assert_frame_equal(
        loaded.risk_spread_df.reset_index(drop=True),
        result.risk_spread_df.reset_index(drop=True),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        loaded.portfolio_summary_df.reset_index(drop=True),
        result.portfolio_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
