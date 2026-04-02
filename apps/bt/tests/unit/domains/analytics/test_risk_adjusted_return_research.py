from __future__ import annotations

import pandas.testing as pdt

from src.domains.analytics.risk_adjusted_return_research import (
    RISK_ADJUSTED_RETURN_RESEARCH_EXPERIMENT_ID,
    get_risk_adjusted_return_bundle_path_for_run_id,
    get_risk_adjusted_return_latest_bundle_path,
    load_risk_adjusted_return_research_bundle,
    run_risk_adjusted_return_research,
    write_risk_adjusted_return_research_bundle,
)


def test_risk_adjusted_return_bundle_roundtrip(tmp_path) -> None:
    result = run_risk_adjusted_return_research(
        lookback_period=40,
        ratio_type="sharpe",
        seed=7,
        n_days=300,
    )

    bundle = write_risk_adjusted_return_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260401_123000_testabcd",
    )
    reloaded = load_risk_adjusted_return_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == RISK_ADJUSTED_RETURN_RESEARCH_EXPERIMENT_ID
    assert bundle.summary_path.exists()
    assert (
        get_risk_adjusted_return_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_risk_adjusted_return_latest_bundle_path(output_root=tmp_path) == bundle.bundle_dir
    assert reloaded.seed == 7
    assert reloaded.lookback_period == 40
    assert reloaded.ratio_type == "sharpe"
    pdt.assert_frame_equal(reloaded.series_df, result.series_df, check_dtype=False)
