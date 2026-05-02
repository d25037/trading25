from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.domains.analytics.annual_prime_value_volume_volatility_participation import (
    ANNUAL_PRIME_VALUE_VOLUME_VOLATILITY_PARTICIPATION_EXPERIMENT_ID,
    get_annual_prime_value_volume_volatility_participation_bundle_path_for_run_id,
    get_annual_prime_value_volume_volatility_participation_latest_bundle_path,
    load_annual_prime_value_volume_volatility_participation_bundle,
    run_annual_prime_value_volume_volatility_participation,
    write_annual_prime_value_volume_volatility_participation_bundle,
)
from tests.unit.domains.analytics.test_annual_value_composite_technical_filter import (
    _write_value_bundle,
)


def test_run_annual_prime_value_volume_volatility_participation_builds_tables(
    tmp_path: Path,
) -> None:
    value_bundle = _write_value_bundle(tmp_path)

    result = run_annual_prime_value_volume_volatility_participation(
        value_bundle,
        output_root=tmp_path,
        selection_fractions=(0.80,),
        market_scope="prime",
        bucket_count=3,
        participation_bucket_count=2,
    )

    assert result.input_run_id == "value-selection"
    assert result.market_scope == "prime"
    assert result.selected_event_count > 0
    assert {"volume_ratio_20_60", "trading_value_ratio_20_60", "volatility_60d_pct"}.issubset(
        result.enriched_event_df.columns
    )
    assert not result.volatility_participation_summary_df.empty
    assert not result.participation_split_df.empty
    assert {"high_vol_low_participation", "high_vol_high_participation"}.issubset(
        set(result.participation_split_df["variant"].astype(str))
    )
    assert not result.portfolio_daily_df.empty
    assert not result.portfolio_summary_df.empty


def test_write_and_load_annual_prime_value_volume_volatility_participation_bundle(
    tmp_path: Path,
) -> None:
    value_bundle = _write_value_bundle(tmp_path)
    result = run_annual_prime_value_volume_volatility_participation(
        value_bundle,
        output_root=tmp_path,
        selection_fractions=(0.80,),
        market_scope="prime",
        bucket_count=3,
        participation_bucket_count=2,
    )

    bundle = write_annual_prime_value_volume_volatility_participation_bundle(
        result,
        output_root=tmp_path,
        run_id="prime-participation",
    )
    loaded = load_annual_prime_value_volume_volatility_participation_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == ANNUAL_PRIME_VALUE_VOLUME_VOLATILITY_PARTICIPATION_EXPERIMENT_ID
    assert (
        get_annual_prime_value_volume_volatility_participation_bundle_path_for_run_id(
            "prime-participation",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_annual_prime_value_volume_volatility_participation_latest_bundle_path(
        output_root=tmp_path,
    ) == bundle.bundle_dir
    pd.testing.assert_frame_equal(
        loaded.participation_split_df.reset_index(drop=True),
        result.participation_split_df.reset_index(drop=True),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        loaded.portfolio_summary_df.reset_index(drop=True),
        result.portfolio_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
