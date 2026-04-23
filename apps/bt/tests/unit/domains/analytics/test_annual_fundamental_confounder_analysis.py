from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (
    ANNUAL_FIRST_OPEN_LAST_CLOSE_FUNDAMENTAL_PANEL_EXPERIMENT_ID,
)
from src.domains.analytics.annual_fundamental_confounder_analysis import (
    ANNUAL_FUNDAMENTAL_CONFOUNDER_ANALYSIS_EXPERIMENT_ID,
    get_annual_fundamental_confounder_analysis_bundle_path_for_run_id,
    get_annual_fundamental_confounder_analysis_latest_bundle_path,
    load_annual_fundamental_confounder_analysis_bundle,
    run_annual_fundamental_confounder_analysis,
    write_annual_fundamental_confounder_analysis_bundle,
)
from src.domains.analytics.research_bundle import write_research_bundle


def _sample_event_ledger() -> pd.DataFrame:
    records: list[dict[str, object]] = []
    markets = ("prime", "standard", "growth")
    years = ("2021", "2022", "2023", "2024")
    for year_index, year in enumerate(years):
        for market_index, market in enumerate(markets):
            for rank in range(12):
                code = f"{year_index}{market_index}{rank:02d}"
                low_value_strength = 11 - rank
                return_pct = (
                    -8.0
                    + low_value_strength * 2.0
                    + (2.0 if market == "standard" else 0.0)
                    + year_index * 0.5
                )
                records.append(
                    {
                        "event_id": f"{code}:{year}",
                        "year": year,
                        "code": code,
                        "company_name": f"Stock {code}",
                        "market": market,
                        "market_code": {"prime": "0111", "standard": "0112", "growth": "0113"}[
                            market
                        ],
                        "sector_33_name": "Machinery" if rank % 2 == 0 else "Services",
                        "status": "realized",
                        "event_return_pct": return_pct,
                        "pbr": 0.4 + rank * 0.12,
                        "forward_per": 4.0 + rank * 0.8,
                        "per": 5.0 + rank * 0.9,
                        "market_cap_bil_jpy": 2.0 + rank * 2.5,
                        "avg_trading_value_60d_mil_jpy": 1.0 + rank * 1.7,
                        "forecast_dividend_yield_pct": 0.2 + low_value_strength * 0.15,
                        "dividend_yield_pct": 0.1 + low_value_strength * 0.12,
                        "cfo_yield_pct": 1.0 + low_value_strength * 0.3,
                        "forward_eps_to_actual_eps": 0.8 + rank * 0.04,
                    }
                )
    return pd.DataFrame(records)


def _write_input_bundle(tmp_path: Path) -> Path:
    source_file = tmp_path / "source.duckdb"
    source_file.write_text("fixture", encoding="utf-8")
    bundle = write_research_bundle(
        experiment_id=ANNUAL_FIRST_OPEN_LAST_CLOSE_FUNDAMENTAL_PANEL_EXPERIMENT_ID,
        module="tests.fixture",
        function="build",
        params={},
        db_path=str(source_file),
        analysis_start_date="2021-01-04",
        analysis_end_date="2024-12-30",
        result_metadata={"db_path": str(source_file)},
        result_tables={"event_ledger_df": _sample_event_ledger()},
        summary_markdown="# fixture\n",
        output_root=tmp_path,
        run_id="input-panel",
    )
    return bundle.bundle_dir


def test_run_annual_fundamental_confounder_analysis_builds_core_tables(tmp_path: Path) -> None:
    input_bundle = _write_input_bundle(tmp_path)

    result = run_annual_fundamental_confounder_analysis(
        input_bundle,
        output_root=tmp_path,
        min_observations=8,
    )

    assert result.input_run_id == "input-panel"
    assert "low_pbr_score" in result.prepared_panel_df.columns
    assert not result.feature_correlation_df.empty
    assert not result.vif_df.empty
    assert not result.conditional_spread_df.empty
    assert not result.panel_regression_df.empty
    assert not result.fama_macbeth_df.empty
    assert not result.leave_one_year_out_df.empty

    standard_low_pbr = result.incremental_selection_df[
        (result.incremental_selection_df["market_scope"].astype(str) == "standard")
        & (result.incremental_selection_df["rule_name"] == "low_pbr")
    ]
    assert len(standard_low_pbr) == 1
    assert int(standard_low_pbr.iloc[0]["event_count"]) > 0


def test_write_and_load_annual_fundamental_confounder_analysis_bundle(tmp_path: Path) -> None:
    input_bundle = _write_input_bundle(tmp_path)
    result = run_annual_fundamental_confounder_analysis(
        input_bundle,
        output_root=tmp_path,
        min_observations=8,
    )

    bundle = write_annual_fundamental_confounder_analysis_bundle(
        result,
        output_root=tmp_path,
        run_id="confounder-test",
    )
    loaded = load_annual_fundamental_confounder_analysis_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == ANNUAL_FUNDAMENTAL_CONFOUNDER_ANALYSIS_EXPERIMENT_ID
    assert (
        get_annual_fundamental_confounder_analysis_bundle_path_for_run_id(
            "confounder-test",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_annual_fundamental_confounder_analysis_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    pd.testing.assert_frame_equal(
        loaded.incremental_selection_df.reset_index(drop=True),
        result.incremental_selection_df.reset_index(drop=True),
        check_dtype=False,
    )


def test_run_annual_fundamental_confounder_analysis_can_require_positive_pbr_and_forward_per(
    tmp_path: Path,
) -> None:
    ledger = _sample_event_ledger()
    ledger.loc[ledger.index[0], "pbr"] = -0.3
    ledger.loc[ledger.index[1], "forward_per"] = -1.5
    source_file = tmp_path / "source-positive-ratio.duckdb"
    source_file.write_text("fixture", encoding="utf-8")
    bundle = write_research_bundle(
        experiment_id=ANNUAL_FIRST_OPEN_LAST_CLOSE_FUNDAMENTAL_PANEL_EXPERIMENT_ID,
        module="tests.fixture",
        function="build",
        params={},
        db_path=str(source_file),
        analysis_start_date="2021-01-04",
        analysis_end_date="2024-12-30",
        result_metadata={"db_path": str(source_file)},
        result_tables={"event_ledger_df": ledger},
        summary_markdown="# fixture\n",
        output_root=tmp_path,
        run_id="input-panel-positive-ratio",
    )

    result = run_annual_fundamental_confounder_analysis(
        bundle.bundle_dir,
        output_root=tmp_path,
        min_observations=8,
        required_positive_columns=("pbr", "forward_per"),
    )

    assert result.required_positive_columns == ("pbr", "forward_per")
    assert result.input_realized_event_count == len(ledger)
    assert result.analysis_event_count == len(ledger) - 2
    assert (pd.to_numeric(result.prepared_panel_df["pbr"], errors="coerce") > 0).all()
    assert (pd.to_numeric(result.prepared_panel_df["forward_per"], errors="coerce") > 0).all()
