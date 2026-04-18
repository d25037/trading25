"""Tests for production strategy robustness audit helpers."""

from __future__ import annotations

import contextlib
import json
from datetime import date

import pandas as pd
import src.domains.analytics.production_strategy_robustness_audit as audit_mod

from src.domains.analytics.production_strategy_robustness_audit import (
    ProductionStrategyRobustnessAuditResult,
    _compute_cagr_pct_from_portfolio_payload,
    _compute_equal_weight_benchmark,
    _compute_equal_weight_benchmark_rows,
    _build_windowed_simulation_cache_key,
    _build_analysis_windows,
    _build_comparison_df,
    _build_parameters_for_scenario,
    _build_sizing_lift_df,
    _day_before_iso,
    _load_dataset_summary,
    _get_or_prepare_windowed_simulation_state,
    _coerce_float,
    _coerce_int,
    _coerce_iso_value,
    _fmt_pct,
    _run_single_scenario,
    _stats_records_to_dict,
    _subtract_months_iso,
    get_production_strategy_robustness_audit_bundle_path_for_run_id,
    get_production_strategy_robustness_audit_latest_bundle_path,
    load_production_strategy_robustness_audit_bundle,
    run_production_strategy_robustness_audit,
    write_production_strategy_robustness_audit_bundle,
)
from src.domains.analytics.window_warmup import (
    estimate_strategy_indicator_warmup_calendar_days,
    resolve_window_load_start_date,
)


def test_subtract_months_iso_keeps_calendar_day_when_possible() -> None:
    assert _subtract_months_iso("2026-03-24", 6) == "2025-09-24"


def test_subtract_months_iso_clamps_to_month_end() -> None:
    assert _subtract_months_iso("2024-03-31", 1) == "2024-02-29"


def test_day_before_iso_returns_previous_calendar_day() -> None:
    assert _day_before_iso("2025-09-24") == "2025-09-23"


def test_stats_records_to_dict_extracts_metric_values() -> None:
    metrics = _stats_records_to_dict(
        [
            {"metric": "Total Return [%]", "value": 12.3},
            {"metric": "Benchmark Return [%]", "value": 4.5},
        ]
    )

    assert metrics == {
        "Total Return [%]": 12.3,
        "Benchmark Return [%]": 4.5,
    }


def test_build_comparison_df_adds_equal_weight_and_buy_and_hold_baselines() -> None:
    scenario_metrics_df = pd.DataFrame(
        [
            {
                "status": "ok",
                "strategy_name": "reference/buy_and_hold",
                "strategy_basename": "buy_and_hold",
                "is_reference": True,
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "holdout_6m",
                "portfolio_kind": "kelly",
                "total_return_pct": 10.0,
                "benchmark_return_pct": 8.0,
                "max_drawdown_pct": 5.0,
                "total_trades": 1,
                "sharpe_ratio": 0.8,
            },
            {
                "status": "ok",
                "strategy_name": "production/forward_eps_driven",
                "strategy_basename": "forward_eps_driven",
                "is_reference": False,
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "holdout_6m",
                "portfolio_kind": "kelly",
                "total_return_pct": 15.0,
                "benchmark_return_pct": 8.0,
                "max_drawdown_pct": 7.0,
                "total_trades": 20,
                "sharpe_ratio": 1.1,
            },
        ]
    )
    equal_weight_benchmark_df = pd.DataFrame(
        [
            {
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "holdout_6m",
                "equal_weight_constituent_count": 400,
                "equal_weight_total_return_pct": 12.0,
                "equal_weight_median_stock_total_return_pct": 9.0,
                "equal_weight_winner_ratio_pct": 58.0,
                "equal_weight_earliest_first_date": "2025-09-24",
                "equal_weight_latest_last_date": "2026-03-24",
            }
        ]
    )

    comparison_df = _build_comparison_df(
        scenario_metrics_df,
        equal_weight_benchmark_df,
    )
    row = comparison_df[
        comparison_df["strategy_name"] == "production/forward_eps_driven"
    ].iloc[0]

    assert row["equal_weight_total_return_pct"] == 12.0
    assert row["excess_return_vs_equal_weight_pct"] == 3.0
    assert bool(row["beat_equal_weight"]) is True
    assert row["buy_and_hold_total_return_pct"] == 10.0
    assert row["excess_return_vs_buy_and_hold_pct"] == 5.0
    assert bool(row["beat_buy_and_hold"]) is True


def test_build_comparison_df_preserves_schema_when_all_rows_failed() -> None:
    scenario_metrics_df = pd.DataFrame(
        [
            {
                "status": "failed",
                "strategy_name": "production/forward_eps_driven",
                "dataset_name": "primeExTopix500_20260325",
                "window_label": "full",
            }
        ]
    )

    comparison_df = _build_comparison_df(scenario_metrics_df, pd.DataFrame())

    assert comparison_df.empty
    assert list(comparison_df.columns) == list(scenario_metrics_df.columns)


def test_build_sizing_lift_df_compares_initial_and_kelly_rows() -> None:
    scenario_metrics_df = pd.DataFrame(
        [
            {
                "status": "ok",
                "strategy_name": "production/forward_eps_driven",
                "dataset_name": "primeExTopix500_20260325",
                "window_label": "full",
                "portfolio_kind": "initial",
                "is_reference": False,
                "total_return_pct": 5.0,
                "cagr_pct": 4.0,
                "benchmark_return_pct": 20.0,
                "max_gross_exposure_pct": 6.0,
                "max_drawdown_pct": 1.0,
                "sharpe_ratio": 0.4,
            },
            {
                "status": "ok",
                "strategy_name": "production/forward_eps_driven",
                "dataset_name": "primeExTopix500_20260325",
                "window_label": "full",
                "portfolio_kind": "kelly",
                "is_reference": False,
                "total_return_pct": 105.0,
                "cagr_pct": 80.0,
                "benchmark_return_pct": 20.0,
                "max_gross_exposure_pct": 98.0,
                "max_drawdown_pct": 25.0,
                "sharpe_ratio": 1.2,
            },
        ]
    )

    sizing_lift_df = _build_sizing_lift_df(scenario_metrics_df)
    row = sizing_lift_df.iloc[0]

    assert row["return_lift_pct"] == 100.0
    assert row["cagr_lift_pct"] == 76.0
    assert row["gross_exposure_lift_pct"] == 92.0
    assert row["drawdown_lift_pct"] == 24.0


def test_build_sizing_lift_df_preserves_schema_when_all_rows_failed() -> None:
    scenario_metrics_df = pd.DataFrame(
        [
            {
                "status": "failed",
                "strategy_name": "production/forward_eps_driven",
                "dataset_name": "primeExTopix500_20260325",
                "window_label": "full",
                "portfolio_kind": "kelly",
                "is_reference": False,
            }
        ]
    )

    sizing_lift_df = _build_sizing_lift_df(scenario_metrics_df)

    assert sizing_lift_df.empty
    assert list(sizing_lift_df.columns) == list(scenario_metrics_df.columns)


def test_run_single_scenario_passes_dataset_stock_codes_to_windowed_simulation(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        audit_mod,
        "_build_parameters_for_scenario",
        lambda **_: {"shared_config": {}},
    )

    def _fake_get_or_prepare_windowed_simulation_state(**kwargs):
        captured["stock_codes"] = kwargs["stock_codes"]
        return object()

    payload = {
        "value_series": {
            "index": ["2025-09-24", "2025-09-25", "2025-09-26"],
            "values": [1.0, 1.1, 1.2],
        },
        "final_stats": [
            {"metric": "Total Return [%]", "value": 12.0},
            {"metric": "Benchmark Return [%]", "value": 8.0},
            {"metric": "Max Gross Exposure [%]", "value": 20.0},
            {"metric": "Max Drawdown [%]", "value": 4.0},
            {"metric": "Win Rate [%]", "value": 55.0},
            {"metric": "Total Trades", "value": 9},
        ],
        "risk_metrics": {"sharpe_ratio": 1.1},
    }
    monkeypatch.setattr(
        audit_mod,
        "_get_or_prepare_windowed_simulation_state",
        _fake_get_or_prepare_windowed_simulation_state,
    )
    monkeypatch.setattr(
        audit_mod,
        "_build_windowed_simulation_result",
        lambda **_: object(),
    )
    monkeypatch.setattr(
        audit_mod,
        "build_backtest_report_payload",
        lambda _: {
            "initial_portfolio": payload,
            "kelly_portfolio": payload,
        },
    )

    rows = _run_single_scenario(
        runner=object(),  # type: ignore[arg-type]
        strategy_name="production/forward_eps_driven",
        dataset_info={
            "dataset_name": "primeExTopix500_20260325",
            "dataset_preset": "primeExTopix500",
        },
        stock_codes=["1301", "1332"],
        simulation_state_cache={},
        window_label="holdout_6m",
        window_start_date="2025-09-24",
        window_end_date="2026-03-24",
    )

    assert captured["stock_codes"] == ["1301", "1332"]
    assert [row["status"] for row in rows] == ["ok", "ok"]
    assert [row["total_trades"] for row in rows] == [9, 9]
    assert rows[0]["cagr_pct"] is not None


def test_compute_cagr_pct_from_portfolio_payload_uses_equity_curve() -> None:
    cagr_pct = _compute_cagr_pct_from_portfolio_payload(
        {
            "value_series": {
                "index": ["2025-09-24", "2025-09-25", "2025-09-26"],
                "values": [100.0, 105.0, 110.0],
            }
        }
    )

    assert cagr_pct is not None
    assert cagr_pct > 0.0


def test_build_windowed_simulation_cache_key_ignores_kelly_only_fields() -> None:
    common = {
        "entry_filter_params": {"bollinger_position": {"window": 50}},
        "exit_trigger_params": {"period_extrema_break": {"period": 60}},
    }

    key_a = _build_windowed_simulation_cache_key(
        parameters={
            **common,
            "shared_config": {
                "dataset": "primeExTopix500_20260325",
                "kelly_fraction": 0.5,
                "min_allocation": 0.01,
                "max_allocation": 0.5,
                "fees": 0.001,
            },
        },
        dataset_name="primeExTopix500_20260325",
        stock_codes=["1301", "1332"],
        window_start_date="2025-09-24",
        window_end_date="2026-03-24",
    )
    key_b = _build_windowed_simulation_cache_key(
        parameters={
            **common,
            "shared_config": {
                "dataset": "primeExTopix500_20260325",
                "kelly_fraction": 1.7,
                "min_allocation": 0.02,
                "max_allocation": 0.7,
                "fees": 0.001,
            },
        },
        dataset_name="primeExTopix500_20260325",
        stock_codes=["1301", "1332"],
        window_start_date="2025-09-24",
        window_end_date="2026-03-24",
    )

    assert key_a == key_b


def test_get_or_prepare_windowed_simulation_state_reuses_cache(monkeypatch) -> None:
    captured = {"calls": 0}

    def _fake_prepare(**kwargs):
        captured["calls"] += 1
        return kwargs["parameters"]["shared_config"]["kelly_fraction"]

    monkeypatch.setattr(
        audit_mod,
        "_prepare_windowed_simulation_state",
        _fake_prepare,
    )
    cache: dict[str, object] = {}
    parameters_a = {
        "entry_filter_params": {"bollinger_position": {"window": 50}},
        "exit_trigger_params": {"period_extrema_break": {"period": 60}},
        "shared_config": {"kelly_fraction": 0.5, "fees": 0.001},
    }
    parameters_b = {
        "entry_filter_params": {"bollinger_position": {"window": 50}},
        "exit_trigger_params": {"period_extrema_break": {"period": 60}},
        "shared_config": {"kelly_fraction": 1.7, "fees": 0.001},
    }

    state_a = _get_or_prepare_windowed_simulation_state(
        parameters=parameters_a,
        dataset_info={"dataset_name": "primeExTopix500_20260325"},
        stock_codes=["1301", "1332"],
        simulation_state_cache=cache,  # type: ignore[arg-type]
        window_start_date="2025-09-24",
        window_end_date="2026-03-24",
    )
    state_b = _get_or_prepare_windowed_simulation_state(
        parameters=parameters_b,
        dataset_info={"dataset_name": "primeExTopix500_20260325"},
        stock_codes=["1301", "1332"],
        simulation_state_cache=cache,  # type: ignore[arg-type]
        window_start_date="2025-09-24",
        window_end_date="2026-03-24",
    )

    assert captured["calls"] == 1
    assert state_a == state_b


def test_indicator_warmup_estimation_finds_nested_periods() -> None:
    warmup_days = estimate_strategy_indicator_warmup_calendar_days(
        {
            "entry_filter_params": {
                "bollinger_position": {"window": 50},
            },
            "exit_trigger_params": {
                "period_extrema_break": {"period": 60},
            },
        }
    )

    assert warmup_days == 95


def test_resolve_window_load_start_date_uses_window_start_without_warmup() -> None:
    assert resolve_window_load_start_date(
        dataset_start_date="2025-01-01",
        window_start_date="2025-09-24",
        warmup_calendar_days=0,
    ) == "2025-09-24"


def _make_robustness_result() -> ProductionStrategyRobustnessAuditResult:
    scenario_metrics_df = pd.DataFrame(
        [
            {
                "status": "ok",
                "strategy_name": "reference/buy_and_hold",
                "strategy_basename": "buy_and_hold",
                "is_reference": True,
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "holdout_6m",
                "window_start_date": "2025-09-24",
                "window_end_date": "2026-03-24",
                "portfolio_kind": "kelly",
                "kelly_fraction": 1.0,
                "total_return_pct": 10.0,
                "cagr_pct": 9.0,
                "benchmark_return_pct": 8.0,
                "max_gross_exposure_pct": 100.0,
                "max_drawdown_pct": 6.0,
                "win_rate_pct": 55.0,
                "total_trades": 1,
                "sharpe_ratio": 0.8,
                "sortino_ratio": None,
                "calmar_ratio": None,
                "annualized_volatility": None,
                "omega_ratio": None,
            },
            {
                "status": "ok",
                "strategy_name": "production/forward_eps_driven",
                "strategy_basename": "forward_eps_driven",
                "is_reference": False,
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "holdout_6m",
                "window_start_date": "2025-09-24",
                "window_end_date": "2026-03-24",
                "portfolio_kind": "initial",
                "kelly_fraction": 1.7,
                "total_return_pct": 5.0,
                "cagr_pct": 4.0,
                "benchmark_return_pct": 8.0,
                "max_gross_exposure_pct": 5.0,
                "max_drawdown_pct": 1.5,
                "win_rate_pct": 55.0,
                "total_trades": 4,
                "sharpe_ratio": 0.4,
                "sortino_ratio": None,
                "calmar_ratio": None,
                "annualized_volatility": None,
                "omega_ratio": None,
            },
            {
                "status": "ok",
                "strategy_name": "production/forward_eps_driven",
                "strategy_basename": "forward_eps_driven",
                "is_reference": False,
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "holdout_6m",
                "window_start_date": "2025-09-24",
                "window_end_date": "2026-03-24",
                "portfolio_kind": "kelly",
                "kelly_fraction": 1.7,
                "total_return_pct": 24.0,
                "cagr_pct": 22.0,
                "benchmark_return_pct": 8.0,
                "max_gross_exposure_pct": 95.0,
                "max_drawdown_pct": 14.0,
                "win_rate_pct": 58.0,
                "total_trades": 9,
                "sharpe_ratio": 1.1,
                "sortino_ratio": None,
                "calmar_ratio": None,
                "annualized_volatility": None,
                "omega_ratio": None,
            },
            {
                "status": "ok",
                "strategy_name": "production/range_break_v15",
                "strategy_basename": "range_break_v15",
                "is_reference": False,
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "holdout_6m",
                "window_start_date": "2025-09-24",
                "window_end_date": "2026-03-24",
                "portfolio_kind": "initial",
                "kelly_fraction": 1.2,
                "total_return_pct": 6.0,
                "cagr_pct": 5.0,
                "benchmark_return_pct": 8.0,
                "max_gross_exposure_pct": 8.0,
                "max_drawdown_pct": 2.0,
                "win_rate_pct": 51.0,
                "total_trades": 6,
                "sharpe_ratio": 0.5,
                "sortino_ratio": None,
                "calmar_ratio": None,
                "annualized_volatility": None,
                "omega_ratio": None,
            },
            {
                "status": "ok",
                "strategy_name": "production/range_break_v15",
                "strategy_basename": "range_break_v15",
                "is_reference": False,
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "holdout_6m",
                "window_start_date": "2025-09-24",
                "window_end_date": "2026-03-24",
                "portfolio_kind": "kelly",
                "kelly_fraction": 1.2,
                "total_return_pct": 18.0,
                "cagr_pct": 16.0,
                "benchmark_return_pct": 8.0,
                "max_gross_exposure_pct": 88.0,
                "max_drawdown_pct": 11.0,
                "win_rate_pct": 54.0,
                "total_trades": 10,
                "sharpe_ratio": 0.9,
                "sortino_ratio": None,
                "calmar_ratio": None,
                "annualized_volatility": None,
                "omega_ratio": None,
            },
        ]
    )
    equal_weight_benchmark_df = pd.DataFrame(
        [
            {
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "holdout_6m",
                "equal_weight_constituent_count": 400,
                "equal_weight_total_return_pct": 12.0,
                "equal_weight_median_stock_total_return_pct": 9.0,
                "equal_weight_winner_ratio_pct": 58.0,
                "equal_weight_earliest_first_date": "2025-09-24",
                "equal_weight_latest_last_date": "2026-03-24",
            }
        ]
    )
    comparison_df = _build_comparison_df(
        scenario_metrics_df,
        equal_weight_benchmark_df,
    )
    sizing_lift_df = _build_sizing_lift_df(scenario_metrics_df)
    return ProductionStrategyRobustnessAuditResult(
        db_path="multi://backtest-simulation",
        strategy_names=("production/forward_eps_driven", "production/range_break_v15"),
        dataset_names=("primeExTopix500_20260325",),
        holdout_months=6,
        holdout_start_date="2025-09-24",
        holdout_end_date="2026-03-24",
        analysis_start_date="2025-09-24",
        analysis_end_date="2026-03-24",
        dataset_summary_df=pd.DataFrame(
            [
                {
                    "dataset_name": "primeExTopix500_20260325",
                    "dataset_preset": "primeExTopix500",
                    "stocks": 400,
                    "stock_data_rows": 1000,
                    "dataset_start_date": "2025-09-24",
                    "dataset_end_date": "2026-03-24",
                    "train_end_date": "2025-09-23",
                    "holdout_start_date": "2025-09-24",
                    "holdout_end_date": "2026-03-24",
                }
            ]
        ),
        equal_weight_benchmark_df=equal_weight_benchmark_df,
        scenario_metrics_df=scenario_metrics_df,
        comparison_df=comparison_df,
        sizing_lift_df=sizing_lift_df,
    )


def test_run_production_strategy_robustness_audit_aggregates_metrics(
    monkeypatch,
) -> None:
    dataset_info = {
        "dataset_name": "primeExTopix500_20260325",
        "dataset_preset": "primeExTopix500",
        "dataset_start_date": "2025-01-01",
        "dataset_end_date": "2026-03-24",
        "train_end_date": "2025-09-23",
        "holdout_start_date": "2025-09-24",
        "holdout_end_date": "2026-03-24",
    }

    def _fake_run_single_scenario(**kwargs):
        strategy_name = kwargs["strategy_name"]
        dataset_info_local = kwargs["dataset_info"]
        window_label = kwargs["window_label"]
        base_return = 10.0 if strategy_name.endswith("forward_eps_driven") else 8.0
        if strategy_name == audit_mod.DEFAULT_REFERENCE_STRATEGY:
            base_return = 7.0
        return [
            {
                "status": "ok",
                "error": None,
                "strategy_name": strategy_name,
                "strategy_basename": strategy_name.split("/")[-1],
                "is_reference": strategy_name == audit_mod.DEFAULT_REFERENCE_STRATEGY,
                "dataset_name": dataset_info_local["dataset_name"],
                "dataset_preset": dataset_info_local["dataset_preset"],
                "window_label": window_label,
                "window_start_date": kwargs["window_start_date"],
                "window_end_date": kwargs["window_end_date"],
                "portfolio_kind": "initial",
                "kelly_fraction": 1.0,
                "total_return_pct": base_return / 2,
                "cagr_pct": base_return / 3,
                "benchmark_return_pct": 6.0,
                "max_gross_exposure_pct": 10.0,
                "max_drawdown_pct": 2.0,
                "win_rate_pct": 50.0,
                "total_trades": 4,
                "sharpe_ratio": 0.5,
                "sortino_ratio": None,
                "calmar_ratio": None,
                "annualized_volatility": None,
                "omega_ratio": None,
            },
            {
                "status": "ok",
                "error": None,
                "strategy_name": strategy_name,
                "strategy_basename": strategy_name.split("/")[-1],
                "is_reference": strategy_name == audit_mod.DEFAULT_REFERENCE_STRATEGY,
                "dataset_name": dataset_info_local["dataset_name"],
                "dataset_preset": dataset_info_local["dataset_preset"],
                "window_label": window_label,
                "window_start_date": kwargs["window_start_date"],
                "window_end_date": kwargs["window_end_date"],
                "portfolio_kind": "kelly",
                "kelly_fraction": 1.0,
                "total_return_pct": base_return,
                "cagr_pct": base_return - 1.0,
                "benchmark_return_pct": 6.0,
                "max_gross_exposure_pct": 80.0,
                "max_drawdown_pct": 10.0,
                "win_rate_pct": 55.0,
                "total_trades": 7,
                "sharpe_ratio": 1.0,
                "sortino_ratio": None,
                "calmar_ratio": None,
                "annualized_volatility": None,
                "omega_ratio": None,
            },
        ]

    monkeypatch.setattr(
        audit_mod,
        "_load_dataset_summary",
        lambda dataset_name, holdout_months: {**dataset_info, "dataset_name": dataset_name},
    )
    monkeypatch.setattr(
        audit_mod,
        "_compute_equal_weight_benchmark_rows",
        lambda **kwargs: [
            {
                "dataset_name": kwargs["dataset_info"]["dataset_name"],
                "dataset_preset": kwargs["dataset_info"]["dataset_preset"],
                "window_label": window["window_label"],
                "equal_weight_constituent_count": 400,
                "equal_weight_total_return_pct": 5.0,
                "equal_weight_median_stock_total_return_pct": 4.0,
                "equal_weight_winner_ratio_pct": 55.0,
                "equal_weight_earliest_first_date": window["window_start_date"],
                "equal_weight_latest_last_date": window["window_end_date"],
            }
            for window in kwargs["windows"]
        ],
    )
    monkeypatch.setattr(audit_mod, "_run_single_scenario", _fake_run_single_scenario)
    monkeypatch.setattr(audit_mod, "BacktestRunner", lambda: object())
    monkeypatch.setattr(
        audit_mod,
        "data_access_mode_context",
        lambda _mode: contextlib.nullcontext(),
    )
    monkeypatch.setattr(
        audit_mod,
        "get_stock_list",
        lambda _dataset_name: ["1301", "1332"],
    )

    result = run_production_strategy_robustness_audit(
        strategy_names=("production/forward_eps_driven",),
        dataset_names=("primeExTopix500_20260325",),
        holdout_months=6,
    )

    assert list(result.strategy_names) == ["production/forward_eps_driven"]
    assert set(result.scenario_metrics_df["window_label"]) == {
        "train_pre_holdout",
        "holdout_6m",
        "full",
    }
    holdout_row = result.comparison_df[
        (result.comparison_df["strategy_name"] == "production/forward_eps_driven")
        & (result.comparison_df["window_label"] == "holdout_6m")
        & (result.comparison_df["portfolio_kind"] == "kelly")
    ].iloc[0]
    assert bool(holdout_row["beat_equal_weight"]) is True
    assert bool(holdout_row["beat_buy_and_hold"]) is True
    assert not result.sizing_lift_df.empty


def test_write_and_load_production_strategy_robustness_bundle_round_trip(
    tmp_path,
) -> None:
    result = _make_robustness_result()

    bundle = write_production_strategy_robustness_audit_bundle(
        result,
        output_root=tmp_path,
        run_id="robustness_test_run",
        notes="round-trip",
    )
    reloaded = load_production_strategy_robustness_audit_bundle(bundle.bundle_dir)

    assert get_production_strategy_robustness_audit_latest_bundle_path(
        output_root=tmp_path
    ) == bundle.bundle_dir
    assert get_production_strategy_robustness_audit_bundle_path_for_run_id(
        "robustness_test_run",
        output_root=tmp_path,
    ) == bundle.bundle_dir
    assert reloaded.strategy_names == result.strategy_names
    assert reloaded.dataset_names == result.dataset_names
    pd.testing.assert_frame_equal(
        reloaded.comparison_df,
        result.comparison_df,
        check_dtype=False,
    )
    assert "Largest Kelly lift" in bundle.summary_path.read_text(encoding="utf-8")
    published = json.loads(bundle.published_summary_path.read_text(encoding="utf-8"))
    assert published["bestHoldoutKelly"]["strategyName"] == "production/forward_eps_driven"


def test_load_dataset_summary_and_build_analysis_windows_from_manifest(
    tmp_path,
    monkeypatch,
) -> None:
    manifest_path = tmp_path / "datasets" / "sample_dataset" / "manifest.v2.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps(
            {
                "dateRange": {"min": "2025-01-01", "max": "2026-03-24"},
                "dataset": {"preset": "primeExTopix500"},
                "counts": {"stocks": 400, "stock_data": 12345},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(audit_mod, "get_data_dir", lambda: tmp_path)

    summary = _load_dataset_summary("sample_dataset", holdout_months=6)
    windows = _build_analysis_windows(dataset_info=summary, holdout_months=6)

    assert summary["holdout_start_date"] == "2025-09-24"
    assert summary["train_end_date"] == "2025-09-23"
    assert [window["window_label"] for window in windows] == [
        "train_pre_holdout",
        "holdout_6m",
        "full",
    ]


def test_load_dataset_summary_clamps_train_end_before_dataset_start(
    tmp_path,
    monkeypatch,
) -> None:
    manifest_path = tmp_path / "datasets" / "tiny_dataset" / "manifest.v2.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps(
            {
                "dateRange": {"min": "2026-03-01", "max": "2026-03-24"},
                "dataset": {"preset": "primeExTopix500"},
                "counts": {"stocks": 20, "stock_data": 200},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(audit_mod, "get_data_dir", lambda: tmp_path)

    summary = _load_dataset_summary("tiny_dataset", holdout_months=6)

    assert summary["train_end_date"] is None


def test_compute_equal_weight_benchmark_rows_uses_reader_and_closes_it(
    tmp_path,
    monkeypatch,
) -> None:
    closed: list[str] = []

    class _FakeReader:
        def __init__(self, snapshot_dir: str) -> None:
            self.snapshot_dir = snapshot_dir

        def close(self) -> None:
            closed.append(self.snapshot_dir)

    monkeypatch.setattr(audit_mod, "get_data_dir", lambda: tmp_path)
    monkeypatch.setattr(audit_mod, "DatasetSnapshotReader", _FakeReader)
    monkeypatch.setattr(
        audit_mod,
        "_compute_equal_weight_benchmark",
        lambda reader, **kwargs: {
            "equal_weight_constituent_count": 3,
            "equal_weight_total_return_pct": 9.0,
            "equal_weight_median_stock_total_return_pct": 8.0,
            "equal_weight_winner_ratio_pct": 66.0,
            "equal_weight_earliest_first_date": kwargs["window_start_date"],
            "equal_weight_latest_last_date": kwargs["window_end_date"],
        },
    )

    rows = _compute_equal_weight_benchmark_rows(
        dataset_info={
            "dataset_name": "sample_dataset",
            "dataset_preset": "primeExTopix500",
        },
        windows=(
            {
                "window_label": "holdout_6m",
                "window_start_date": "2025-09-24",
                "window_end_date": "2026-03-24",
            },
        ),
    )

    assert rows[0]["equal_weight_total_return_pct"] == 9.0
    assert closed == [str(tmp_path / "datasets" / "sample_dataset")]


def test_compute_equal_weight_benchmark_handles_missing_and_present_rows() -> None:
    class _NoneReader:
        def query_one(self, *_args, **_kwargs):
            return None

    class _RowReader:
        def query_one(self, *_args, **_kwargs):
            return {
                "constituent_count": 4,
                "equal_weight_total_return_pct": 12.5,
                "median_stock_total_return_pct": 11.0,
                "winner_ratio_pct": 75.0,
                "earliest_first_date": date(2025, 9, 24),
                "latest_last_date": date(2026, 3, 24),
            }

    none_row = _compute_equal_weight_benchmark(
        _NoneReader(),
        window_start_date="2025-09-24",
        window_end_date="2026-03-24",
    )
    value_row = _compute_equal_weight_benchmark(
        _RowReader(),
        window_start_date="2025-09-24",
        window_end_date="2026-03-24",
    )

    assert none_row["equal_weight_constituent_count"] == 0
    assert value_row["equal_weight_earliest_first_date"] == "2025-09-24"
    assert value_row["equal_weight_total_return_pct"] == 12.5


def test_build_parameters_for_scenario_uses_reference_yaml_for_buy_and_hold(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    class _FakeRunner:
        def _build_parameters(self, strategy_config, **kwargs):
            captured["strategy_config"] = strategy_config
            captured["kwargs"] = kwargs
            return {"ok": True}

    monkeypatch.setattr(audit_mod, "load_yaml_file", lambda _path: {"name": "ref"})

    result = _build_parameters_for_scenario(
        runner=_FakeRunner(),  # type: ignore[arg-type]
        strategy_name=audit_mod.DEFAULT_REFERENCE_STRATEGY,
        config_override={"shared_config": {"dataset": "sample"}},
    )

    assert result == {"ok": True}
    assert captured["strategy_config"] == {"name": "ref"}
    assert captured["kwargs"] == {
        "config_override": {"shared_config": {"dataset": "sample"}},
        "strategy_name": audit_mod.DEFAULT_REFERENCE_STRATEGY,
    }


def test_stats_and_coerce_helpers_handle_invalid_values() -> None:
    metrics = _stats_records_to_dict(
        [
            "ignored",
            {"value": 1},
            {"metric": None, "value": 2},
            {"metric": "Sharpe Ratio", "value": "1.5"},
        ]
    )

    assert metrics == {"Sharpe Ratio": "1.5"}
    assert _coerce_float("bad") is None
    assert _coerce_int("bad") is None
    assert _coerce_iso_value(date(2025, 9, 24)) == "2025-09-24"
    assert _fmt_pct("bad") == "N/A"
