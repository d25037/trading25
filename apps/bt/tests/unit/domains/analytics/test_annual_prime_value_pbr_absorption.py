from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.domains.analytics.annual_prime_value_pbr_absorption import (
    ANNUAL_PRIME_VALUE_PBR_ABSORPTION_EXPERIMENT_ID,
    get_annual_prime_value_pbr_absorption_bundle_path_for_run_id,
    get_annual_prime_value_pbr_absorption_latest_bundle_path,
    load_annual_prime_value_pbr_absorption_bundle,
    run_annual_prime_value_pbr_absorption,
    write_annual_prime_value_pbr_absorption_bundle,
)
from tests.unit.domains.analytics.test_annual_value_composite_technical_filter import (
    _write_value_bundle,
)


def test_run_annual_prime_value_pbr_absorption_builds_sensitivity_tables(
    tmp_path: Path,
) -> None:
    value_bundle = _write_value_bundle(tmp_path)

    result = run_annual_prime_value_pbr_absorption(
        value_bundle,
        output_root=tmp_path,
        selection_fraction=0.80,
        pbr_weights=(0.0, 0.05, 0.20),
        baseline_pbr_weight=0.05,
    )

    assert result.input_run_id == "value-selection"
    assert result.market_scope == "prime"
    assert result.selection_fraction == 0.80
    assert result.selected_event_count > 0
    assert set(result.weighted_selected_event_df["score_method"].astype(str)) == {
        "pbr_weight_00",
        "pbr_weight_05",
        "pbr_weight_20",
    }
    assert not result.weight_sensitivity_summary_df.empty
    assert {"cagr_pct", "median_pbr", "median_forward_per"}.issubset(
        result.weight_sensitivity_summary_df.columns
    )
    assert not result.selection_overlap_df.empty
    assert "added_by_variant" in set(result.pbr_swap_profile_df["direction"].astype(str))
    assert not result.portfolio_summary_df.empty


def test_write_and_load_annual_prime_value_pbr_absorption_bundle(
    tmp_path: Path,
) -> None:
    value_bundle = _write_value_bundle(tmp_path)
    result = run_annual_prime_value_pbr_absorption(
        value_bundle,
        output_root=tmp_path,
        selection_fraction=0.80,
        pbr_weights=(0.0, 0.05),
        baseline_pbr_weight=0.05,
    )

    bundle = write_annual_prime_value_pbr_absorption_bundle(
        result,
        output_root=tmp_path,
        run_id="prime-pbr-absorption",
    )
    loaded = load_annual_prime_value_pbr_absorption_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == ANNUAL_PRIME_VALUE_PBR_ABSORPTION_EXPERIMENT_ID
    assert (
        get_annual_prime_value_pbr_absorption_bundle_path_for_run_id(
            "prime-pbr-absorption",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_annual_prime_value_pbr_absorption_latest_bundle_path(
        output_root=tmp_path,
    ) == bundle.bundle_dir
    pd.testing.assert_frame_equal(
        loaded.weight_sensitivity_summary_df.reset_index(drop=True),
        result.weight_sensitivity_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        loaded.selection_overlap_df.reset_index(drop=True),
        result.selection_overlap_df.reset_index(drop=True),
        check_dtype=False,
    )
