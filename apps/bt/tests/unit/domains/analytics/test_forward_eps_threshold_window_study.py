"""Tests for forward EPS threshold window study helpers."""

from __future__ import annotations

import contextlib
import json
from types import SimpleNamespace

import math

import pandas as pd
import src.domains.analytics.forward_eps_threshold_window_study as study_mod

from src.domains.analytics.forward_eps_threshold_window_study import (
    ForwardEpsThresholdWindowStudyResult,
    _build_rolling_analysis_windows,
    _build_rolling_summary_df,
    _build_threshold_comparison_df,
    _bool_ratio_pct,
    _decorate_metric_row,
    _evaluate_window,
    _extract_forward_eps_growth_threshold,
    _extract_risk_adjusted_return_threshold,
    _extract_volume_ratio_above_long_period,
    _extract_volume_ratio_above_short_period,
    _extract_volume_ratio_above_threshold,
    _fmt_int,
    _fmt_num,
    _fmt_pct,
    _fmt_threshold,
    _format_variant,
    _prepare_full_history_simulation_state,
    _prepare_shared_data_cache,
    _series_stat,
    _to_native,
    get_forward_eps_threshold_window_study_bundle_path_for_run_id,
    get_forward_eps_threshold_window_study_latest_bundle_path,
    load_forward_eps_threshold_window_study_bundle,
    run_forward_eps_threshold_window_study,
    write_forward_eps_threshold_window_study_bundle,
)


def test_build_rolling_analysis_windows_includes_full_and_latest_window() -> None:
    windows = _build_rolling_analysis_windows(
        dataset_info={
            "dataset_start_date": "2020-03-24",
            "dataset_end_date": "2021-03-24",
        },
        rolling_months=6,
        rolling_step_months=3,
    )

    assert windows[0] == {
        "window_label": "full",
        "window_start_date": "2020-03-24",
        "window_end_date": "2021-03-24",
        "window_type": "full",
    }
    assert windows[-1] == {
        "window_label": "rolling_6m_2020-09-24_2021-03-24",
        "window_start_date": "2020-09-24",
        "window_end_date": "2021-03-24",
        "window_type": "rolling",
    }
    assert [window["window_end_date"] for window in windows[1:]] == [
        "2020-09-24",
        "2020-12-24",
        "2021-03-24",
    ]


def test_build_threshold_comparison_df_adds_original_and_equal_weight_deltas() -> None:
    window_metrics_df = pd.DataFrame(
        [
            {
                "status": "ok",
                "strategy_name": "experimental/robustness/forward_eps_driven",
                "strategy_basename": "forward_eps_driven",
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "rolling_6m_2025-09-24_2026-03-24",
                "window_type": "rolling",
                "portfolio_kind": "kelly",
                "forward_eps_growth_threshold": 0.2,
                "total_return_pct": 24.0,
                "max_drawdown_pct": 30.0,
                "total_trades": 20,
                "sharpe_ratio": 1.2,
            },
            {
                "status": "ok",
                "strategy_name": "experimental/robustness/forward_eps_driven_forward_eps_0_4",
                "strategy_basename": "forward_eps_driven_forward_eps_0_4",
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "rolling_6m_2025-09-24_2026-03-24",
                "window_type": "rolling",
                "portfolio_kind": "kelly",
                "forward_eps_growth_threshold": 0.4,
                "total_return_pct": 48.0,
                "max_drawdown_pct": 42.0,
                "total_trades": 17,
                "sharpe_ratio": 1.9,
            },
        ]
    )
    equal_weight_benchmark_df = pd.DataFrame(
        [
            {
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "rolling_6m_2025-09-24_2026-03-24",
                "window_type": "rolling",
                "equal_weight_total_return_pct": 8.0,
            }
        ]
    )

    comparison_df = _build_threshold_comparison_df(
        window_metrics_df=window_metrics_df,
        equal_weight_benchmark_df=equal_weight_benchmark_df,
        baseline_strategy_name="experimental/robustness/forward_eps_driven",
    )
    threshold_row = comparison_df[
        comparison_df["forward_eps_growth_threshold"] == 0.4
    ].iloc[0]

    assert threshold_row["excess_return_vs_equal_weight_pct"] == 40.0
    assert bool(threshold_row["beat_equal_weight"]) is True
    assert threshold_row["original_total_return_pct"] == 24.0
    assert threshold_row["excess_return_vs_original_pct"] == 24.0
    assert threshold_row["drawdown_delta_vs_original_pct"] == 12.0
    assert bool(threshold_row["beat_original_return"]) is True
    assert bool(threshold_row["improve_original_drawdown"]) is False


def test_build_threshold_comparison_df_preserves_schema_when_all_rows_failed() -> None:
    window_metrics_df = pd.DataFrame(
        [
            {
                "status": "failed",
                "strategy_name": "experimental/robustness/forward_eps_driven",
                "window_type": "full",
            }
        ]
    )

    comparison_df = _build_threshold_comparison_df(
        window_metrics_df=window_metrics_df,
        equal_weight_benchmark_df=pd.DataFrame(),
        baseline_strategy_name="experimental/robustness/forward_eps_driven",
    )

    assert comparison_df.empty
    assert list(comparison_df.columns) == list(window_metrics_df.columns)


def test_build_rolling_summary_df_aggregates_window_ratios() -> None:
    comparison_df = pd.DataFrame(
        [
            {
                "strategy_name": "experimental/robustness/forward_eps_driven",
                "strategy_basename": "forward_eps_driven",
                "portfolio_kind": "kelly",
                "forward_eps_growth_threshold": 0.2,
                "risk_adjusted_return_threshold": 1.0,
                "volume_ratio_above_threshold": 1.7,
                "volume_ratio_above_short_period": 50,
                "volume_ratio_above_long_period": 150,
                "window_type": "rolling",
                "total_return_pct": 10.0,
                "excess_return_vs_equal_weight_pct": 1.0,
                "beat_equal_weight": True,
                "excess_return_vs_original_pct": 0.0,
                "beat_original_return": False,
                "improve_original_drawdown": False,
                "joint_improvement_vs_original": False,
                "max_drawdown_pct": 20.0,
                "drawdown_delta_vs_original_pct": 0.0,
                "sharpe_ratio": 1.0,
                "total_trades": 10,
            },
            {
                "strategy_name": "experimental/robustness/forward_eps_driven",
                "strategy_basename": "forward_eps_driven",
                "portfolio_kind": "kelly",
                "forward_eps_growth_threshold": 0.2,
                "risk_adjusted_return_threshold": 1.0,
                "volume_ratio_above_threshold": 1.7,
                "volume_ratio_above_short_period": 50,
                "volume_ratio_above_long_period": 150,
                "window_type": "rolling",
                "total_return_pct": -5.0,
                "excess_return_vs_equal_weight_pct": -3.0,
                "beat_equal_weight": False,
                "excess_return_vs_original_pct": 0.0,
                "beat_original_return": False,
                "improve_original_drawdown": False,
                "joint_improvement_vs_original": False,
                "max_drawdown_pct": 25.0,
                "drawdown_delta_vs_original_pct": 0.0,
                "sharpe_ratio": 0.5,
                "total_trades": 8,
            },
            {
                "strategy_name": "experimental/robustness/forward_eps_driven_forward_eps_0_4",
                "strategy_basename": "forward_eps_driven_forward_eps_0_4",
                "portfolio_kind": "kelly",
                "forward_eps_growth_threshold": 0.4,
                "risk_adjusted_return_threshold": 1.2,
                "volume_ratio_above_threshold": 1.4,
                "volume_ratio_above_short_period": 20,
                "volume_ratio_above_long_period": 60,
                "window_type": "rolling",
                "total_return_pct": 15.0,
                "excess_return_vs_equal_weight_pct": 6.0,
                "beat_equal_weight": True,
                "excess_return_vs_original_pct": 5.0,
                "beat_original_return": True,
                "improve_original_drawdown": True,
                "joint_improvement_vs_original": True,
                "max_drawdown_pct": 18.0,
                "drawdown_delta_vs_original_pct": -2.0,
                "sharpe_ratio": 1.3,
                "total_trades": 9,
            },
            {
                "strategy_name": "experimental/robustness/forward_eps_driven_forward_eps_0_4",
                "strategy_basename": "forward_eps_driven_forward_eps_0_4",
                "portfolio_kind": "kelly",
                "forward_eps_growth_threshold": 0.4,
                "risk_adjusted_return_threshold": 1.2,
                "volume_ratio_above_threshold": 1.4,
                "volume_ratio_above_short_period": 20,
                "volume_ratio_above_long_period": 60,
                "window_type": "rolling",
                "total_return_pct": 5.0,
                "excess_return_vs_equal_weight_pct": 1.0,
                "beat_equal_weight": True,
                "excess_return_vs_original_pct": 2.0,
                "beat_original_return": True,
                "improve_original_drawdown": False,
                "joint_improvement_vs_original": False,
                "max_drawdown_pct": 27.0,
                "drawdown_delta_vs_original_pct": 3.0,
                "sharpe_ratio": 0.9,
                "total_trades": 7,
            },
        ]
    )

    summary_df = _build_rolling_summary_df(comparison_df)
    threshold_row = summary_df[
        summary_df["forward_eps_growth_threshold"] == 0.4
    ].iloc[0]

    assert threshold_row["rolling_window_count"] == 2
    assert math.isclose(threshold_row["avg_total_return_pct"], 10.0)
    assert math.isclose(threshold_row["beat_equal_weight_window_ratio_pct"], 100.0)
    assert math.isclose(threshold_row["beat_original_return_window_ratio_pct"], 100.0)
    assert math.isclose(
        threshold_row["improve_original_drawdown_window_ratio_pct"], 50.0
    )
    assert math.isclose(threshold_row["joint_improvement_window_ratio_pct"], 50.0)
    assert math.isclose(threshold_row["avg_max_drawdown_pct"], 22.5)
    assert math.isclose(threshold_row["worst_max_drawdown_pct"], 27.0)
    assert threshold_row["risk_adjusted_return_threshold"] == 1.2
    assert threshold_row["volume_ratio_above_threshold"] == 1.4
    assert threshold_row["volume_ratio_above_short_period"] == 20
    assert threshold_row["volume_ratio_above_long_period"] == 60


def test_build_rolling_summary_df_preserves_schema_without_rolling_rows() -> None:
    comparison_df = pd.DataFrame(
        [
            {
                "strategy_name": "experimental/robustness/forward_eps_driven",
                "window_type": "full",
            }
        ]
    )

    summary_df = _build_rolling_summary_df(comparison_df)

    assert summary_df.empty
    assert list(summary_df.columns) == list(comparison_df.columns)


def _make_threshold_result() -> ForwardEpsThresholdWindowStudyResult:
    window_metrics_df = pd.DataFrame(
        [
            {
                "status": "ok",
                "strategy_name": "experimental/robustness/forward_eps_driven",
                "strategy_basename": "forward_eps_driven",
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "full",
                "window_type": "full",
                "window_start_date": "2025-03-24",
                "window_end_date": "2026-03-24",
                "portfolio_kind": "kelly",
                "kelly_fraction": 1.7,
                "forward_eps_growth_threshold": 0.2,
                "risk_adjusted_return_threshold": 1.0,
                "volume_ratio_above_threshold": 1.7,
                "volume_ratio_above_short_period": 50,
                "volume_ratio_above_long_period": 150,
                "total_return_pct": 24.0,
                "max_drawdown_pct": 30.0,
                "total_trades": 20,
                "sharpe_ratio": 1.2,
            },
            {
                "status": "ok",
                "strategy_name": (
                    "experimental/robustness/forward_eps_driven_forward_eps_0_35"
                ),
                "strategy_basename": "forward_eps_driven_forward_eps_0_35",
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "full",
                "window_type": "full",
                "window_start_date": "2025-03-24",
                "window_end_date": "2026-03-24",
                "portfolio_kind": "kelly",
                "kelly_fraction": 1.7,
                "forward_eps_growth_threshold": 0.35,
                "risk_adjusted_return_threshold": 1.2,
                "volume_ratio_above_threshold": 1.7,
                "volume_ratio_above_short_period": 50,
                "volume_ratio_above_long_period": 150,
                "total_return_pct": 30.0,
                "max_drawdown_pct": 26.0,
                "total_trades": 18,
                "sharpe_ratio": 1.4,
            },
            {
                "status": "ok",
                "strategy_name": "experimental/robustness/forward_eps_driven",
                "strategy_basename": "forward_eps_driven",
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "rolling_6m_2025-09-24_2026-03-24",
                "window_type": "rolling",
                "window_start_date": "2025-09-24",
                "window_end_date": "2026-03-24",
                "portfolio_kind": "kelly",
                "kelly_fraction": 1.7,
                "forward_eps_growth_threshold": 0.2,
                "risk_adjusted_return_threshold": 1.0,
                "volume_ratio_above_threshold": 1.7,
                "volume_ratio_above_short_period": 50,
                "volume_ratio_above_long_period": 150,
                "total_return_pct": 12.0,
                "max_drawdown_pct": 21.0,
                "total_trades": 10,
                "sharpe_ratio": 0.9,
            },
            {
                "status": "ok",
                "strategy_name": (
                    "experimental/robustness/forward_eps_driven_forward_eps_0_35"
                ),
                "strategy_basename": "forward_eps_driven_forward_eps_0_35",
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "rolling_6m_2025-09-24_2026-03-24",
                "window_type": "rolling",
                "window_start_date": "2025-09-24",
                "window_end_date": "2026-03-24",
                "portfolio_kind": "kelly",
                "kelly_fraction": 1.7,
                "forward_eps_growth_threshold": 0.35,
                "risk_adjusted_return_threshold": 1.2,
                "volume_ratio_above_threshold": 1.7,
                "volume_ratio_above_short_period": 50,
                "volume_ratio_above_long_period": 150,
                "total_return_pct": 15.0,
                "max_drawdown_pct": 19.0,
                "total_trades": 9,
                "sharpe_ratio": 1.1,
            },
        ]
    )
    equal_weight_benchmark_df = pd.DataFrame(
        [
            {
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "full",
                "window_type": "full",
                "equal_weight_total_return_pct": 8.0,
            },
            {
                "dataset_name": "primeExTopix500_20260325",
                "dataset_preset": "primeExTopix500",
                "window_label": "rolling_6m_2025-09-24_2026-03-24",
                "window_type": "rolling",
                "equal_weight_total_return_pct": 5.0,
            },
        ]
    )
    comparison_df = _build_threshold_comparison_df(
        window_metrics_df=window_metrics_df,
        equal_weight_benchmark_df=equal_weight_benchmark_df,
        baseline_strategy_name="experimental/robustness/forward_eps_driven",
    )
    rolling_summary_df = _build_rolling_summary_df(comparison_df)
    return ForwardEpsThresholdWindowStudyResult(
        db_path="multi://forward-eps-threshold-window-study",
        strategy_names=(
            "experimental/robustness/forward_eps_driven",
            "experimental/robustness/forward_eps_driven_forward_eps_0_35",
        ),
        dataset_name="primeExTopix500_20260325",
        baseline_strategy_name="experimental/robustness/forward_eps_driven",
        rolling_months=6,
        rolling_step_months=1,
        analysis_start_date="2025-03-24",
        analysis_end_date="2026-03-24",
        dataset_summary_df=pd.DataFrame(
            [
                {
                    "dataset_name": "primeExTopix500_20260325",
                    "dataset_preset": "primeExTopix500",
                    "rolling_months": 6,
                    "rolling_step_months": 1,
                    "rolling_window_count": 1,
                    "dataset_start_date": "2025-03-24",
                    "dataset_end_date": "2026-03-24",
                }
            ]
        ),
        equal_weight_benchmark_df=equal_weight_benchmark_df,
        window_metrics_df=window_metrics_df,
        comparison_df=comparison_df,
        rolling_summary_df=rolling_summary_df,
    )


def test_run_forward_eps_threshold_window_study_aggregates_windows(
    monkeypatch,
) -> None:
    dataset_info = {
        "dataset_name": "primeExTopix500_20260325",
        "dataset_preset": "primeExTopix500",
        "dataset_start_date": "2025-03-24",
        "dataset_end_date": "2026-03-24",
    }

    def _fake_build_parameters_for_scenario(*, strategy_name, **_kwargs):
        threshold = 0.2 if strategy_name.endswith("forward_eps_driven") else 0.35
        return {
            "shared_config": {
                "kelly_fraction": 1.7,
                "min_allocation": 0.01,
                "max_allocation": 0.5,
            },
            "entry_filter_params": {
                "fundamental": {"forward_eps_growth": {"threshold": threshold}},
                "risk_adjusted_return": {"threshold": 1.2},
                "volume_ratio_above": {
                    "ratio_threshold": 1.7,
                    "short_period": 50,
                    "long_period": 150,
                },
            },
            "exit_trigger_params": {},
        }

    def _fake_evaluate_window(**kwargs):
        threshold = kwargs["threshold"] or 0.0
        window = kwargs["window"]
        strategy_name = kwargs["strategy_name"]
        strategy_basename = strategy_name.split("/")[-1]
        total_return_pct = 12.0 if threshold == 0.2 else 15.0
        if window["window_type"] == "full":
            total_return_pct += 10.0
        return [
            {
                "status": "ok",
                "strategy_name": strategy_name,
                "strategy_basename": strategy_basename,
                "dataset_name": kwargs["dataset_info"]["dataset_name"],
                "dataset_preset": kwargs["dataset_info"]["dataset_preset"],
                "window_label": window["window_label"],
                "window_type": window["window_type"],
                "window_start_date": window["window_start_date"],
                "window_end_date": window["window_end_date"],
                "portfolio_kind": "kelly",
                "kelly_fraction": 1.7,
                "forward_eps_growth_threshold": threshold,
                "risk_adjusted_return_threshold": (
                    kwargs["risk_adjusted_return_threshold"]
                ),
                "volume_ratio_above_threshold": kwargs["volume_ratio_above_threshold"],
                "volume_ratio_above_short_period": (
                    kwargs["volume_ratio_above_short_period"]
                ),
                "volume_ratio_above_long_period": kwargs["volume_ratio_above_long_period"],
                "total_return_pct": total_return_pct,
                "max_drawdown_pct": 20.0 - threshold,
                "total_trades": 8,
                "sharpe_ratio": 1.0 + threshold,
            }
        ]

    monkeypatch.setattr(
        study_mod,
        "_load_dataset_summary",
        lambda dataset_name, holdout_months: {**dataset_info, "dataset_name": dataset_name},
    )
    monkeypatch.setattr(study_mod, "BacktestRunner", lambda: object())
    monkeypatch.setattr(
        study_mod,
        "_build_parameters_for_scenario",
        _fake_build_parameters_for_scenario,
    )
    monkeypatch.setattr(
        study_mod,
        "_compute_equal_weight_benchmark_rows",
        lambda **kwargs: [
            {
                "dataset_name": kwargs["dataset_info"]["dataset_name"],
                "dataset_preset": kwargs["dataset_info"]["dataset_preset"],
                "window_label": window["window_label"],
                "equal_weight_total_return_pct": 5.0,
            }
            for window in kwargs["windows"]
        ],
    )
    monkeypatch.setattr(
        study_mod,
        "data_access_mode_context",
        lambda _mode: contextlib.nullcontext(),
    )
    monkeypatch.setattr(
        study_mod,
        "get_stock_list",
        lambda _dataset_name: ["1301", "1332"],
    )
    monkeypatch.setattr(study_mod, "_prepare_shared_data_cache", lambda **_kwargs: ({}, None))
    monkeypatch.setattr(
        study_mod,
        "_prepare_full_history_simulation_state",
        lambda **kwargs: SimpleNamespace(strategy_name=kwargs["parameters"]),
    )
    monkeypatch.setattr(study_mod, "_evaluate_window", _fake_evaluate_window)

    result = run_forward_eps_threshold_window_study(
        strategy_names=(
            "experimental/robustness/forward_eps_driven",
            "experimental/robustness/forward_eps_driven_forward_eps_0_35",
        ),
        dataset_name="primeExTopix500_20260325",
        baseline_strategy_name="experimental/robustness/forward_eps_driven",
        rolling_months=6,
        rolling_step_months=6,
    )

    assert set(result.window_metrics_df["window_type"]) == {"full", "rolling"}
    best_variant = result.rolling_summary_df.sort_values(
        "avg_total_return_pct",
        ascending=False,
    ).iloc[0]
    assert best_variant["forward_eps_growth_threshold"] == 0.35
    assert get_forward_eps_threshold_window_study_bundle_path_for_run_id(
        "abc",
        output_root="/tmp/research",
    ).name == "abc"


def test_write_and_load_forward_eps_threshold_window_study_bundle_round_trip(
    tmp_path,
) -> None:
    result = _make_threshold_result()

    bundle = write_forward_eps_threshold_window_study_bundle(
        result,
        output_root=tmp_path,
        run_id="threshold_test_run",
    )
    reloaded = load_forward_eps_threshold_window_study_bundle(bundle.bundle_dir)

    assert get_forward_eps_threshold_window_study_latest_bundle_path(
        output_root=tmp_path
    ) == bundle.bundle_dir
    assert get_forward_eps_threshold_window_study_bundle_path_for_run_id(
        "threshold_test_run",
        output_root=tmp_path,
    ) == bundle.bundle_dir
    assert reloaded.rolling_summary_df.equals(result.rolling_summary_df)
    assert "Highest joint return+DD improvement variant" in bundle.summary_path.read_text(
        encoding="utf-8"
    )
    published = json.loads(bundle.published_summary_path.read_text(encoding="utf-8"))
    assert published["bestRollingKellyAverageReturn"]["threshold"] == 0.35


def test_prepare_full_history_simulation_state_and_shared_cache(monkeypatch) -> None:
    index = pd.Index(["2025-09-24", "2025-09-25"], name="date")
    open_data = pd.DataFrame({"1301": [100.0, 101.0]}, index=index)
    close_data = pd.DataFrame({"1301": [102.0, 103.0]}, index=index)
    entries = pd.DataFrame({"1301": [True, False]}, index=index)
    exits = pd.DataFrame({"1301": [False, True]}, index=index)
    recorded: dict[str, object] = {}

    class _FakeStrategy:
        def __init__(self, *, shared_config, entry_filter_params, exit_trigger_params):
            recorded["shared_config"] = shared_config
            recorded["entry_filter_params"] = entry_filter_params
            recorded["exit_trigger_params"] = exit_trigger_params
            self.multi_data_dict = None
            self.benchmark_data = None

        def run_multi_backtest(self) -> None:
            recorded["ran"] = True

        def _get_grouped_portfolio_inputs_cache(self):
            return open_data, close_data, entries, exits

        def load_multi_data(self):
            return {"close": {"1301": close_data}}

        def _should_load_benchmark(self) -> bool:
            return True

        def load_benchmark_data(self):
            return pd.DataFrame({"close": [1.0, 2.0]}, index=index)

    monkeypatch.setattr(
        study_mod.SharedConfig,
        "model_validate",
        staticmethod(lambda payload, context=None: {"payload": payload, "context": context}),
    )
    monkeypatch.setattr(
        study_mod.SignalParams,
        "model_validate",
        staticmethod(lambda payload: {"validated": payload}),
    )
    monkeypatch.setattr(study_mod, "YamlConfigurableStrategy", _FakeStrategy)

    state = _prepare_full_history_simulation_state(
        parameters={"shared_config": {}, "entry_filter_params": {}, "exit_trigger_params": {}},
        dataset_info={
            "dataset_name": "primeExTopix500_20260325",
            "dataset_start_date": "2025-09-24",
            "dataset_end_date": "2025-09-25",
        },
        stock_codes=["1301"],
        shared_multi_data_dict={"close": {"1301": close_data}},
        shared_benchmark_data=pd.DataFrame({"close": [1.0, 2.0]}, index=index),
    )
    shared_cache = _prepare_shared_data_cache(
        parameters={"shared_config": {}, "entry_filter_params": {}, "exit_trigger_params": {}},
        dataset_info={
            "dataset_name": "primeExTopix500_20260325",
            "dataset_start_date": "2025-09-24",
            "dataset_end_date": "2025-09-25",
        },
        stock_codes=["1301"],
    )

    assert recorded["ran"] is True
    assert state.full_entries.equals(entries)
    assert shared_cache[0] == {"close": {"1301": close_data}}
    assert shared_cache[1] is not None


def test_evaluate_window_success_and_failure_paths(monkeypatch) -> None:
    index = pd.Index(["2025-09-24", "2025-09-25"], name="date")
    open_data = pd.DataFrame({"1301": [100.0, 101.0]}, index=index)
    close_data = pd.DataFrame({"1301": [102.0, 103.0]}, index=index)
    entries = pd.DataFrame({"1301": [True, False]}, index=index)
    exits = pd.DataFrame({"1301": [False, True]}, index=index)
    cache_calls: list[tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]] = []

    class _FakeStrategy:
        stock_codes = ["1301"]

        def _set_grouped_portfolio_inputs_cache(
            self,
            open_slice,
            close_slice,
            entries_slice,
            exits_slice,
        ) -> None:
            cache_calls.append((open_slice, close_slice, entries_slice, exits_slice))

        def run_multi_backtest_from_cached_signals(self, allocation):
            return {"allocation": allocation}

        def optimize_allocation_kelly(self, *_args, **_kwargs):
            return 0.8, {"ok": True}

    monkeypatch.setattr(
        study_mod,
        "build_backtest_report_payload",
        lambda _result: {
            "initial_portfolio": {
                "final_stats": [
                    {"metric": "Total Return [%]", "value": 10.0},
                    {"metric": "Total Trades", "value": 3},
                    {"metric": "Max Drawdown [%]", "value": 5.0},
                ],
                "risk_metrics": {"sharpe_ratio": 0.8},
            },
            "kelly_portfolio": {
                "final_stats": [
                    {"metric": "Total Return [%]", "value": 12.0},
                    {"metric": "Total Trades", "value": 4},
                    {"metric": "Max Drawdown [%]", "value": 6.0},
                ],
                "risk_metrics": {"sharpe_ratio": 1.0},
            },
        },
    )

    state = study_mod._FullHistorySimulationState(
        strategy=_FakeStrategy(),  # type: ignore[arg-type]
        full_open_data=open_data,
        full_close_data=close_data,
        full_entries=entries,
        full_exits=exits,
    )
    success_rows = _evaluate_window(
        state=state,
        strategy_name="experimental/robustness/forward_eps_driven",
        dataset_info={
            "dataset_name": "primeExTopix500_20260325",
            "dataset_preset": "primeExTopix500",
        },
        window={
            "window_label": "rolling_6m_2025-09-24_2026-03-24",
            "window_start_date": "2025-09-24",
            "window_end_date": "2025-09-25",
            "window_type": "rolling",
        },
        threshold=0.35,
        risk_adjusted_return_threshold=1.2,
        volume_ratio_above_threshold=1.7,
        volume_ratio_above_short_period=50,
        volume_ratio_above_long_period=150,
        kelly_fraction=1.7,
        min_allocation=0.01,
        max_allocation=0.5,
    )
    failed_rows = _evaluate_window(
        state=state,
        strategy_name="experimental/robustness/forward_eps_driven",
        dataset_info={
            "dataset_name": "primeExTopix500_20260325",
            "dataset_preset": "primeExTopix500",
        },
        window={
            "window_label": "rolling_6m_2024-01-01_2024-01-02",
            "window_start_date": "2024-01-01",
            "window_end_date": "2024-01-02",
            "window_type": "rolling",
        },
        threshold=0.35,
        risk_adjusted_return_threshold=1.2,
        volume_ratio_above_threshold=1.7,
        volume_ratio_above_short_period=50,
        volume_ratio_above_long_period=150,
        kelly_fraction=1.7,
        min_allocation=0.01,
        max_allocation=0.5,
    )

    assert len(cache_calls) == 1
    assert [row["status"] for row in success_rows] == ["ok", "ok"]
    assert [row["status"] for row in failed_rows] == ["failed", "failed"]


def test_extract_and_format_helpers_cover_invalid_values() -> None:
    params = {
        "entry_filter_params": {
            "fundamental": {"forward_eps_growth": {"threshold": "0.35"}},
            "risk_adjusted_return": {"threshold": "bad"},
            "volume_ratio_above": {
                "ratio_threshold": "1.7",
                "short_period": "50",
                "long_period": "bad",
            },
        }
    }
    decorated = _decorate_metric_row(
        {"strategy_name": "demo"},
        window_type="rolling",
        threshold=0.35,
        risk_adjusted_return_threshold=None,
        volume_ratio_above_threshold=1.7,
        volume_ratio_above_short_period=50,
        volume_ratio_above_long_period=None,
        is_baseline=False,
    )

    assert _extract_forward_eps_growth_threshold(params) == 0.35
    assert _extract_risk_adjusted_return_threshold(params) is None
    assert _extract_volume_ratio_above_threshold(params) == 1.7
    assert _extract_volume_ratio_above_short_period(params) == 50
    assert _extract_volume_ratio_above_long_period(params) is None
    assert decorated["window_type"] == "rolling"
    assert _series_stat(pd.Series([None, None]), "mean") is None
    assert _bool_ratio_pct(pd.Series([None, None])) is None
    assert _fmt_pct("bad") == "n/a"
    assert _fmt_num(None) == "n/a"
    assert _fmt_threshold(None) == "n/a"
    assert _fmt_int("bad") == "n/a"
    assert _to_native(pd.Series([1]).iloc[0]) == 1
    assert _format_variant(
        {
            "forward_eps_growth_threshold": 0.35,
            "risk_adjusted_return_threshold": 1.2,
            "volume_ratio_above_short_period": 50,
            "volume_ratio_above_long_period": 150,
            "volume_ratio_above_threshold": 1.7,
        }
    ) == "feps=0.35, risk=1.20, volume=50/150@1.70"
