from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.domains.analytics.annual_large_universe_factor_family import (
    ANNUAL_LARGE_UNIVERSE_FACTOR_FAMILY_EXPERIMENT_ID,
    get_annual_large_universe_factor_family_bundle_path_for_run_id,
    get_annual_large_universe_factor_family_latest_bundle_path,
    load_annual_large_universe_factor_family_bundle,
    run_annual_large_universe_factor_family,
    write_annual_large_universe_factor_family_bundle,
)
from tests.unit.domains.analytics.test_annual_large_universe_value_profile import (
    _build_market_db,
    _sample_event_ledger,
    _write_input_bundle,
)


def _factor_family_event_ledger() -> pd.DataFrame:
    frame = _sample_event_ledger()
    rank = frame.groupby(["year", "scale_category"]).cumcount().astype(float)
    strength = 7.0 - rank
    frame["per"] = frame["forward_per"].astype(float) + 1.5
    frame["cfo_yield_pct"] = 1.0 + strength * 0.35
    frame["fcf_yield_pct"] = 0.5 + strength * 0.25
    frame["dividend_yield_pct"] = 0.2 + strength * 0.12
    frame["forecast_dividend_yield_pct"] = 0.3 + strength * 0.13
    frame["roe_pct"] = 4.0 + strength * 0.6
    frame["roa_pct"] = 2.0 + strength * 0.3
    frame["operating_margin_pct"] = 3.0 + strength * 0.8
    frame["net_margin_pct"] = 2.0 + strength * 0.5
    frame["cfo_margin_pct"] = 4.0 + strength * 0.7
    frame["fcf_margin_pct"] = 1.0 + strength * 0.4
    frame["equity_ratio_pct"] = 25.0 + strength * 2.0
    frame["cfo_to_net_profit_ratio"] = 0.7 + strength * 0.08
    frame["payout_ratio_pct"] = 20.0 + strength * 1.5
    frame["forecast_payout_ratio_pct"] = 22.0 + strength * 1.4
    frame["forward_eps_to_actual_eps"] = 0.9 + strength * 0.04
    return frame


def test_run_annual_large_universe_factor_family_builds_family_tables(tmp_path: Path) -> None:
    event_ledger_df = _factor_family_event_ledger()
    db_path = _build_market_db(tmp_path / "market.duckdb", event_ledger_df)
    input_bundle = _write_input_bundle(tmp_path, db_path, event_ledger_df)

    result = run_annual_large_universe_factor_family(
        input_bundle,
        output_root=tmp_path,
        selection_fractions=(0.25,),
        min_observations=5,
    )

    assert result.input_run_id == "input-panel"
    assert set(result.factor_scored_panel_df["large_universe"].astype(str)) == {
        "topix100",
        "topix500",
    }
    assert "high_operating_margin_score" in result.factor_scored_panel_df.columns
    assert not result.factor_bucket_summary_df.empty
    assert not result.factor_regression_df.empty
    assert {"single_high_cfo_yield_score", "forward_per_plus_high_cfo_yield_score"}.issubset(
        set(result.profile_summary_df["score_method"].astype(str))
    )
    assert not result.portfolio_summary_df.empty


def test_write_and_load_annual_large_universe_factor_family_bundle(tmp_path: Path) -> None:
    event_ledger_df = _factor_family_event_ledger()
    db_path = _build_market_db(tmp_path / "market.duckdb", event_ledger_df)
    input_bundle = _write_input_bundle(tmp_path, db_path, event_ledger_df)
    result = run_annual_large_universe_factor_family(
        input_bundle,
        output_root=tmp_path,
        selection_fractions=(0.25,),
        min_observations=5,
    )

    bundle = write_annual_large_universe_factor_family_bundle(
        result,
        output_root=tmp_path,
        run_id="large-universe-factor-family",
    )
    loaded = load_annual_large_universe_factor_family_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == ANNUAL_LARGE_UNIVERSE_FACTOR_FAMILY_EXPERIMENT_ID
    assert (
        get_annual_large_universe_factor_family_bundle_path_for_run_id(
            "large-universe-factor-family",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_annual_large_universe_factor_family_latest_bundle_path(
        output_root=tmp_path,
    ) == bundle.bundle_dir
    pd.testing.assert_frame_equal(
        loaded.profile_summary_df.reset_index(drop=True),
        result.profile_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
