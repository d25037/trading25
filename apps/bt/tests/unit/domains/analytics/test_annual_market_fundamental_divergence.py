from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (
    ANNUAL_FIRST_OPEN_LAST_CLOSE_FUNDAMENTAL_PANEL_EXPERIMENT_ID,
)
from src.domains.analytics.annual_market_fundamental_divergence import (
    ANNUAL_MARKET_FUNDAMENTAL_DIVERGENCE_EXPERIMENT_ID,
    get_annual_market_fundamental_divergence_bundle_path_for_run_id,
    get_annual_market_fundamental_divergence_latest_bundle_path,
    load_annual_market_fundamental_divergence_bundle,
    run_annual_market_fundamental_divergence,
    write_annual_market_fundamental_divergence_bundle,
)
from src.domains.analytics.research_bundle import write_research_bundle


def _sample_event_ledger() -> pd.DataFrame:
    records: list[dict[str, object]] = []
    markets = ("prime", "standard", "growth")
    for year_index, year in enumerate(("2021", "2022", "2023")):
        for market_index, market in enumerate(markets):
            for rank in range(8):
                code = f"{year_index}{market_index}{rank:02d}"
                growth = market == "growth"
                standard = market == "standard"
                base_return = 8.0 + year_index + (3.0 if standard else 0.0) - (7.0 if growth else 0.0)
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
                        "event_return_pct": base_return - rank * 0.2,
                        "eps": (-4.0 - rank) if growth and rank < 5 else 8.0 + rank,
                        "forward_eps": None if growth and rank < 3 else 10.0 + rank,
                        "pbr": (5.0 + rank * 0.3) if growth else (0.7 + rank * 0.1),
                        "forward_per": (-6.0 - rank) if growth and rank < 5 else (5.0 + rank * 0.5),
                        "per": (30.0 + rank) if growth else (7.0 + rank * 0.5),
                        "market_cap_bil_jpy": (20.0 + rank) if growth else (80.0 + rank * 5.0),
                        "avg_trading_value_60d_mil_jpy": (3.0 + rank) if growth else (30.0 + rank * 3.0),
                        "roe_pct": (2.0 + rank * 0.2) if growth else (8.0 + rank * 0.3),
                        "roa_pct": (1.0 + rank * 0.1) if growth else (4.0 + rank * 0.2),
                        "equity_ratio_pct": (28.0 + rank) if growth else (45.0 + rank),
                        "cfo_margin_pct": (-5.0 + rank * 0.2) if growth else (9.0 + rank * 0.4),
                        "fcf_margin_pct": (-8.0 + rank * 0.1) if growth else (6.0 + rank * 0.3),
                        "cfo_to_net_profit_ratio": (-0.5 + rank * 0.1) if growth else (1.2 + rank * 0.05),
                        "cfo_yield_pct": (-3.0 + rank * 0.1) if growth else (4.0 + rank * 0.2),
                        "fcf_yield_pct": (-4.0 + rank * 0.1) if growth else (3.0 + rank * 0.2),
                        "dividend_yield_pct": 0.0 if growth else (2.0 + rank * 0.05),
                        "forecast_dividend_yield_pct": 0.0 if growth else (2.2 + rank * 0.05),
                        "payout_ratio_pct": 0.0 if growth else 30.0,
                        "forecast_payout_ratio_pct": 0.0 if growth else 32.0,
                        "forward_eps_to_actual_eps": -1.5 if growth and rank < 5 else 1.2,
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
        analysis_end_date="2023-12-29",
        result_metadata={"db_path": str(source_file)},
        result_tables={"event_ledger_df": _sample_event_ledger()},
        summary_markdown="# fixture\n",
        output_root=tmp_path,
        run_id="input-panel",
    )
    return bundle.bundle_dir


def test_run_annual_market_fundamental_divergence_builds_core_tables(tmp_path: Path) -> None:
    input_bundle = _write_input_bundle(tmp_path)

    result = run_annual_market_fundamental_divergence(
        input_bundle,
        output_root=tmp_path,
        min_observations=6,
    )

    assert result.input_run_id == "input-panel"
    assert result.analysis_event_count == len(_sample_event_ledger())
    assert "forward_per_non_positive_flag" in result.prepared_panel_df.columns
    assert not result.market_feature_profile_df.empty
    assert not result.market_pair_divergence_df.empty
    assert not result.feature_divergence_rank_df.empty
    assert not result.market_return_decomposition_df.empty

    growth_forward_per = result.market_feature_profile_df[
        (result.market_feature_profile_df["market"].astype(str) == "growth")
        & (result.market_feature_profile_df["feature_name"] == "forward_per_non_positive_flag")
    ]
    assert len(growth_forward_per) == 1
    assert float(growth_forward_per.iloc[0]["mean_value"]) > 50.0

    growth_features = result.feature_divergence_rank_df[
        result.feature_divergence_rank_df["market"].astype(str) == "growth"
    ]
    assert "pbr" in set(growth_features["feature_name"].astype(str))
    assert "forward_per_non_positive_flag" in set(growth_features["feature_name"].astype(str))
    assert (
        growth_features[growth_features["feature_name"] == "pbr"]["divergence_score"].notna().all()
    )

    market_only = result.market_return_decomposition_df[
        result.market_return_decomposition_df["model_name"] == "market_only"
    ]
    assert "growth" in set(market_only["term"].astype(str))
    assert not (
        (result.market_return_decomposition_df["term"] == "cap_bil_jpy_year_z")
        & (result.market_return_decomposition_df["term_type"] == "market")
    ).any()


def test_write_and_load_annual_market_fundamental_divergence_bundle(tmp_path: Path) -> None:
    input_bundle = _write_input_bundle(tmp_path)
    result = run_annual_market_fundamental_divergence(
        input_bundle,
        output_root=tmp_path,
        min_observations=6,
    )

    bundle = write_annual_market_fundamental_divergence_bundle(
        result,
        output_root=tmp_path,
        run_id="market-divergence-test",
    )
    loaded = load_annual_market_fundamental_divergence_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == ANNUAL_MARKET_FUNDAMENTAL_DIVERGENCE_EXPERIMENT_ID
    assert (
        get_annual_market_fundamental_divergence_bundle_path_for_run_id(
            "market-divergence-test",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_annual_market_fundamental_divergence_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    pd.testing.assert_frame_equal(
        loaded.feature_divergence_rank_df.reset_index(drop=True),
        result.feature_divergence_rank_df.reset_index(drop=True),
        check_dtype=False,
    )
