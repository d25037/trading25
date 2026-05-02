from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.domains.analytics.annual_value_technical_feature_importance import (
    ANNUAL_VALUE_TECHNICAL_FEATURE_IMPORTANCE_EXPERIMENT_ID,
    get_annual_value_technical_feature_importance_bundle_path_for_run_id,
    get_annual_value_technical_feature_importance_latest_bundle_path,
    load_annual_value_technical_feature_importance_bundle,
    run_annual_value_technical_feature_importance,
    write_annual_value_technical_feature_importance_bundle,
)
from tests.unit.domains.analytics.test_annual_value_composite_technical_filter import (
    _write_value_bundle,
)


def test_run_annual_value_technical_feature_importance_builds_tables(
    tmp_path: Path,
) -> None:
    value_bundle = _write_value_bundle(tmp_path)

    result = run_annual_value_technical_feature_importance(
        value_bundle,
        output_root=tmp_path,
        bucket_count=3,
        focus_market_scope="standard",
        focus_selection_fraction=0.80,
        focus_score_methods=("fixed_55_25_20", "equal_weight", "walkforward_regression_weight"),
    )

    assert result.input_run_id == "value-selection"
    assert result.focus_score_methods == ("equal_weight", "walkforward_regression_weight")
    assert result.selected_event_count > 0
    assert result.technical_feature_count > 0
    assert {
        "price_to_sma250",
        "rsi_14",
        "volume_ratio_20_60",
        "topix_price_to_sma250",
        "range_position_252d",
    }.issubset(result.enriched_event_df.columns)
    assert not result.feature_bucket_summary_df.empty
    assert not result.feature_importance_df.empty
    assert set(result.feature_importance_df["feature_family"]).issuperset(
        {"trend", "momentum", "volume", "market_regime"}
    )
    assert "fixed_55_25_20" not in set(result.feature_importance_df["score_method"].astype(str))


def test_write_and_load_annual_value_technical_feature_importance_bundle(
    tmp_path: Path,
) -> None:
    value_bundle = _write_value_bundle(tmp_path)
    result = run_annual_value_technical_feature_importance(
        value_bundle,
        output_root=tmp_path,
        bucket_count=3,
        focus_market_scope="standard",
        focus_selection_fraction=0.80,
    )

    bundle = write_annual_value_technical_feature_importance_bundle(
        result,
        output_root=tmp_path,
        run_id="technical-feature-importance",
    )
    loaded = load_annual_value_technical_feature_importance_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == ANNUAL_VALUE_TECHNICAL_FEATURE_IMPORTANCE_EXPERIMENT_ID
    assert (
        get_annual_value_technical_feature_importance_bundle_path_for_run_id(
            "technical-feature-importance",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_annual_value_technical_feature_importance_latest_bundle_path(output_root=tmp_path) == bundle.bundle_dir
    pd.testing.assert_frame_equal(
        loaded.feature_importance_df.reset_index(drop=True),
        result.feature_importance_df.reset_index(drop=True),
        check_dtype=False,
    )
