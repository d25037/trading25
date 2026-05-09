from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.domains.analytics.forward_eps_component_decomposition import (
    FORWARD_EPS_COMPONENT_DECOMPOSITION_EXPERIMENT_ID,
    load_forward_eps_component_decomposition_bundle,
    run_forward_eps_component_decomposition,
    write_forward_eps_component_decomposition_bundle,
)
from src.domains.analytics.forward_eps_trade_archetype_decomposition import (
    FORWARD_EPS_TRADE_ARCHETYPE_DECOMPOSITION_EXPERIMENT_ID,
)
from src.domains.analytics.research_bundle import write_research_bundle


def _fixture_enriched_trade_df() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for index in range(12):
        strong = index >= 6
        rows.append(
            {
                "dataset_name": "fixture",
                "window_label": "full",
                "market_scope": "prime" if index % 2 == 0 else "standard",
                "symbol": f"{1000 + index}",
                "trade_return_pct": 18.0 if strong else -8.0,
                "pbr": 0.6 if strong else 1.8,
                "forward_per": 8.0 if strong else 22.0,
                "market_cap_bil_jpy": 15.0 if strong else 90.0,
                "forward_eps_growth_value": 0.7 if strong else 0.2,
                "forward_eps_growth_margin": 0.35 if strong else -0.05,
                "days_since_disclosed": 2 if strong else 40,
                "volume_ratio_value": 5.0 if strong else 1.2,
                "volume_ratio_margin": 3.5 if strong else -0.3,
                "risk_adjusted_return_value": 1.5 if strong else 0.1,
                "stock_return_20d_pct": 16.0 if strong else -2.0,
                "stock_return_60d_pct": 28.0 if strong else 3.0,
                "rsi10": 72.0 if strong else 45.0,
            }
        )
    return pd.DataFrame(rows)


def _write_input_bundle(tmp_path: Path) -> Path:
    empty_metric_df = pd.DataFrame({"placeholder": pd.Series(dtype="float64")})
    bundle = write_research_bundle(
        experiment_id=FORWARD_EPS_TRADE_ARCHETYPE_DECOMPOSITION_EXPERIMENT_ID,
        module="tests.fixture",
        function="build",
        params={},
        db_path="fixture://forward-eps",
        analysis_start_date="2024-01-01",
        analysis_end_date="2024-12-31",
        result_metadata={
            "db_path": "fixture://forward-eps",
            "strategy_name": "production/forward_eps_driven",
            "dataset_name": "fixture",
            "holdout_months": 6,
            "severe_loss_threshold_pct": -10.0,
            "quantile_bucket_count": 5,
            "analysis_start_date": "2024-01-01",
            "analysis_end_date": "2024-12-31",
        },
        result_tables={
            "dataset_summary_df": empty_metric_df,
            "scenario_summary_df": empty_metric_df,
            "trade_ledger_df": empty_metric_df,
            "enriched_trade_df": _fixture_enriched_trade_df(),
            "market_scope_summary_df": empty_metric_df,
            "feature_bucket_summary_df": empty_metric_df,
            "value_feature_bucket_summary_df": empty_metric_df,
            "overlay_candidate_summary_df": empty_metric_df,
            "value_overlay_candidate_summary_df": empty_metric_df,
        },
        summary_markdown="# fixture\n",
        output_root=tmp_path,
        run_id="input-forward-eps",
    )
    return bundle.bundle_dir


def test_forward_eps_component_decomposition_builds_component_tables(
    tmp_path: Path,
) -> None:
    input_bundle = _write_input_bundle(tmp_path)

    result = run_forward_eps_component_decomposition(
        input_bundle,
        output_root=tmp_path,
        quantile_bucket_count=3,
    )

    assert result.input_run_id == "input-forward-eps"
    assert result.trade_count == 12
    assert {"value", "expectation", "attention", "price_momentum"}.issubset(
        set(result.component_bucket_summary_df["component_name"])
    )
    assert "value_attention_expectation_q80" in set(
        result.component_overlap_summary_df["candidate_name"]
    )
    assert {"univariate", "multivariate"}.issubset(
        set(result.component_regression_summary_df["model_name"])
    )


def test_forward_eps_component_decomposition_bundle_roundtrip(tmp_path: Path) -> None:
    input_bundle = _write_input_bundle(tmp_path)
    result = run_forward_eps_component_decomposition(
        input_bundle,
        output_root=tmp_path,
        quantile_bucket_count=3,
    )

    bundle = write_forward_eps_component_decomposition_bundle(
        result,
        output_root=tmp_path,
        run_id="component-decomposition",
    )
    loaded = load_forward_eps_component_decomposition_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == FORWARD_EPS_COMPONENT_DECOMPOSITION_EXPERIMENT_ID
    pd.testing.assert_frame_equal(
        loaded.component_overlap_summary_df.reset_index(drop=True),
        result.component_overlap_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
