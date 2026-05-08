from __future__ import annotations

from pathlib import Path

from src.domains.analytics.annual_value_breakout_periodic_rebalance import (
    ANNUAL_VALUE_BREAKOUT_PERIODIC_REBALANCE_EXPERIMENT_ID,
    get_annual_value_breakout_periodic_rebalance_bundle_path_for_run_id,
    get_annual_value_breakout_periodic_rebalance_latest_bundle_path,
    load_annual_value_breakout_periodic_rebalance_bundle,
    run_annual_value_breakout_periodic_rebalance,
    write_annual_value_breakout_periodic_rebalance_bundle,
)
from tests.unit.domains.analytics.test_annual_value_periodic_rebalance import (
    _build_market_db,
)


def test_run_annual_value_breakout_periodic_rebalance_builds_baseline_and_breakout(
    tmp_path: Path,
) -> None:
    db_path = _build_market_db(tmp_path / "market.duckdb")

    result = run_annual_value_breakout_periodic_rebalance(
        db_path,
        markets=("standard",),
        rebalance_months=(6,),
        selection_counts=(2,),
        breakout_windows=(2,),
        breakout_lookback_sessions=(0, 5),
        start_year=2024,
        end_year=2024,
        min_train_observations=5,
    )

    assert result.breakout_windows == (2,)
    assert result.breakout_lookback_sessions == (0, 5)
    assert not result.breakout_feature_df.empty
    assert "signal_date" in result.breakout_feature_df.columns
    dated_features = result.breakout_feature_df.dropna(subset=["signal_date", "entry_date"])
    assert (
        dated_features["signal_date"].astype(str) < dated_features["entry_date"].astype(str)
    ).all()
    assert not result.selected_event_df.empty
    assert {"value_only", "breakout_signal", "breakout_recent", "breakout_additive"}.issubset(
        set(result.selected_event_df["breakout_policy"].astype(str))
    )
    assert not result.portfolio_daily_df.empty
    assert not result.portfolio_summary_df.empty

    focus = result.portfolio_summary_df[
        (result.portfolio_summary_df["market_scope"].astype(str) == "standard")
        & (result.portfolio_summary_df["score_method"].astype(str) == "equal_weight")
        & (result.portfolio_summary_df["liquidity_scenario"].astype(str) == "none")
        & (result.portfolio_summary_df["breakout_policy"].astype(str) == "value_only")
        & (result.portfolio_summary_df["rebalance_months"].astype(int) == 6)
        & (result.portfolio_summary_df["selection_count"].astype(int) == 2)
    ]
    assert len(focus) == 1
    assert int(focus.iloc[0]["realized_event_count"]) == 4


def test_write_and_load_annual_value_breakout_periodic_rebalance_bundle(tmp_path: Path) -> None:
    db_path = _build_market_db(tmp_path / "market.duckdb")
    result = run_annual_value_breakout_periodic_rebalance(
        db_path,
        markets=("standard",),
        rebalance_months=(6,),
        selection_counts=(2,),
        breakout_windows=(2,),
        breakout_lookback_sessions=(0, 5),
        start_year=2024,
        end_year=2024,
        min_train_observations=5,
    )

    bundle = write_annual_value_breakout_periodic_rebalance_bundle(
        result,
        output_root=tmp_path,
        run_id="value-breakout-test",
    )
    loaded = load_annual_value_breakout_periodic_rebalance_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == ANNUAL_VALUE_BREAKOUT_PERIODIC_REBALANCE_EXPERIMENT_ID
    assert (
        get_annual_value_breakout_periodic_rebalance_bundle_path_for_run_id(
            "value-breakout-test",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_annual_value_breakout_periodic_rebalance_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    assert loaded.portfolio_summary_df.shape == result.portfolio_summary_df.shape


def test_factor_weight_grid_and_portfolio_config_cap(tmp_path: Path) -> None:
    db_path = _build_market_db(tmp_path / "market.duckdb")

    result = run_annual_value_breakout_periodic_rebalance(
        db_path,
        markets=("standard",),
        rebalance_months=(6,),
        selection_counts=(2,),
        score_methods=("factor_w_pbr000_size050_fper050",),
        liquidity_scenarios=("none",),
        breakout_policies=("value_only", "breakout_additive"),
        breakout_windows=(2,),
        breakout_lookback_sessions=(0, 5),
        factor_weight_step=0.5,
        max_portfolio_configs=1,
        skip_portfolio_curves=True,
        start_year=2024,
        end_year=2024,
        min_train_observations=5,
    )

    assert "factor_w_pbr000_size050_fper050_score" in result.scored_panel_df.columns
    method_rows = result.score_method_params_df[
        result.score_method_params_df["score_method"].astype(str)
        == "factor_w_pbr000_size050_fper050"
    ]
    assert len(method_rows) == 1
    assert float(method_rows.iloc[0]["size_weight"]) == 0.5
    assert result.max_portfolio_configs == 1
    assert result.skip_portfolio_curves is True
    assert not result.selection_summary_df.empty
    assert result.portfolio_summary_df.empty
