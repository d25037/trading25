from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import pytest

from src.domains.analytics.atr_expansion_forward_response import (
    OVERHEAT_RETURN_20D_THRESHOLD_PCT,
    AtrExpansionForwardResponseResult,
    _liquidity_color_atr_state_condition,
    build_summary_markdown,
    run_atr_expansion_forward_response_research,
    write_atr_expansion_forward_response_bundle,
)

from daily_ranking_market_v4_fixture import (
    upgrade_daily_ranking_fixture_to_market_v4,
)


def test_atr_expansion_forward_response_emits_tables(tmp_path: Path) -> None:
    db_path = _build_atr_expansion_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert not result.coverage_diagnostics_df.empty
    assert not result.atr_expansion_response_df.empty
    assert not result.return_regime_interaction_df.empty
    assert not result.atr_pair_interaction_df.empty
    assert not result.liquidity_color_atr_interaction_df.empty
    assert {
        "atr_feature",
        "expansion_bucket",
        "entry_mode",
        "horizon",
        "median_forward_excess_return_pct",
        "severe_loss_rate_pct",
    }.issubset(result.atr_expansion_response_df.columns)
    assert {
        "return_regime",
        "atr_expansion_state",
        "median_atr20_to_atr60",
    }.issubset(result.return_regime_interaction_df.columns)
    assert {
        "liquidity_regime",
        "ui_color",
        "atr_expansion_state",
        "median_atr20_to_atr60",
    }.issubset(result.liquidity_color_atr_interaction_df.columns)
    assert "overheat_excluded" in set(
        result.liquidity_color_atr_interaction_df["atr_expansion_state"].astype(str)
    )
    crowded_colors = set(
        result.liquidity_color_atr_interaction_df.loc[
            result.liquidity_color_atr_interaction_df["liquidity_regime"]
            == "crowded_rerating",
            "ui_color",
        ].astype(str)
    )
    assert crowded_colors
    assert crowded_colors.issubset({"green", "blue", "yellow"})
    assert "yellow" in crowded_colors
    neutral_colors = set(
        result.liquidity_color_atr_interaction_df.loc[
            result.liquidity_color_atr_interaction_df["liquidity_regime"]
            == "neutral_rerating",
            "ui_color",
        ].astype(str)
    )
    assert neutral_colors
    assert neutral_colors.issubset({"green", "blue"})
    assert {
        "atr20_pct",
        "atr60_pct",
        "atr20_to_atr60",
        "atr20_change_20d_pct",
        "recent_return_20d_pct",
        "forward_close_excess_return_20d_pct",
    }.issubset(result.observation_sample_df.columns)


def test_atr_expansion_forward_response_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_atr_expansion_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "ATR Expansion Response" in summary
    assert "Return Regime Interaction" in summary
    assert "ATR Pair Interaction" in summary
    assert "Liquidity Color ATR Interaction" in summary

    bundle = write_atr_expansion_forward_response_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def test_atr_expansion_fails_closed_for_incomplete_entry_mode_outcomes(
    tmp_path: Path,
) -> None:
    db_path = _build_atr_expansion_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            "UPDATE topix_data SET open = NULL WHERE date = '2024-04-01'"
        )
    finally:
        conn.close()

    result = _run_test_research(db_path)
    frame = result.atr_expansion_response_df
    next_open = frame.loc[
        (frame["entry_mode"] == "next_open_to_close")
        & (frame["outcome_coverage_status"] == "incomplete")
    ]
    close = frame.loc[frame["entry_mode"] == "close_to_close"]

    assert result.outcome_coverage_policy == (
        "complete_selected_membership_required_per_entry_mode"
    )
    assert not next_open.empty
    assert (
        next_open["selected_observation_count"]
        == next_open["complete_outcome_count"]
        + next_open["incomplete_outcome_count"]
    ).all()
    assert (next_open["incomplete_outcome_count"] > 0).all()
    assert next_open[
        [
            "mean_forward_excess_return_pct",
            "median_forward_excess_return_pct",
            "win_rate_pct",
            "severe_loss_rate_pct",
        ]
    ].isna().all().all()
    assert not close.empty
    assert (close["outcome_coverage_status"] == "complete").all()
    assert (
        close["selected_observation_count"] == close["complete_outcome_count"]
    ).all()
    assert np.isfinite(close["median_forward_excess_return_pct"]).all()
    assert set(result.coverage_diagnostics_df["outcome_coverage_policy"]) == {
        result.outcome_coverage_policy
    }


def test_atr_expansion_overheat_exclusion_uses_ranking_20d_threshold() -> None:
    condition = _liquidity_color_atr_state_condition("atr20_acceleration_ex_overheat")

    assert f"recent_return_20d_pct < {OVERHEAT_RETURN_20D_THRESHOLD_PCT}" in condition
    assert "atr20_change_20d_pct >= 25.0" in condition
    assert "atr20_to_atr60 >= 1.25" in condition


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"atr_windows": (20,)}, "atr_windows must include 20 and 60"),
        ({"horizons": (0,)}, "horizons must be positive"),
        ({"min_observations": 0}, "min_observations must be positive"),
        (
            {"severe_loss_threshold_pct": 0.0},
            "severe_loss_threshold_pct must be negative",
        ),
    ],
)
def test_atr_expansion_forward_response_rejects_invalid_params(
    tmp_path: Path,
    kwargs: dict[str, Any],
    message: str,
) -> None:
    db_path = _build_atr_expansion_db(tmp_path / "market.duckdb")

    with pytest.raises(ValueError, match=message):
        run_atr_expansion_forward_response_research(db_path, **kwargs)


def test_atr_expansion_forward_response_requires_existing_db(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        run_atr_expansion_forward_response_research(tmp_path / "missing.duckdb")


def _run_test_research(db_path: Path) -> AtrExpansionForwardResponseResult:
    return run_atr_expansion_forward_response_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-05-31",
        horizons=(5, 20),
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_atr_expansion_db(db_path: Path) -> Path:
    dates = pd.bdate_range("2023-08-01", "2024-07-31").strftime("%Y-%m-%d").tolist()
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE stock_data (
            code TEXT,
            date TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE topix_data (
            date TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_master_daily (
            date TEXT,
            code TEXT,
            company_name TEXT,
            market_code TEXT,
            market_name TEXT,
            scale_category TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE daily_valuation (
            code TEXT,
            date TEXT,
            price_basis_date TEXT,
            per DOUBLE,
            forward_per DOUBLE,
            pbr DOUBLE,
            market_cap DOUBLE,
            free_float_market_cap DOUBLE,
            p_op DOUBLE,
            forward_p_op DOUBLE,
            basis_version TEXT
        )
        """
    )

    stock_rows: list[tuple[str, str, float, float, float, float, int]] = []
    master_rows: list[tuple[str, str, str, str, str, str | None]] = []
    valuation_rows: list[
        tuple[str, str, str, float, float, float, float, float, float, float, str]
    ] = []
    codes = [
        (
            f"{1000 + code_idx}",
            f"Name {code_idx}",
            "0111",
            80.0 + code_idx,
            0.0004 + (code_idx % 5) * 0.00008,
            0.008 + (code_idx % 7) * 0.002,
            (
                (1_000_000 + code_idx * 100_000) * 10
                if code_idx < 14 or 100 <= code_idx < 110
                else (1_000_000 + code_idx * 100_000) // 5
                if code_idx < 20
                else 1_000_000 + code_idx * 100_000
            ),
        )
        for code_idx in range(120)
    ]
    for idx, date in enumerate(dates):
        topix_close = 1900.0 + idx * 0.35
        conn.execute(
            "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)",
            [
                date,
                topix_close * 0.999,
                topix_close * 1.003,
                topix_close * 0.997,
                topix_close,
            ],
        )
        for code_idx, (
            code,
            name,
            market_code,
            base,
            drift,
            base_range,
            base_volume,
        ) in enumerate(codes):
            expansion = 1.0 + max(0, idx - 90) / 120.0
            close = base * (1.0 + drift * idx) * (1.0 + 0.012 * ((idx % 17) - 8) / 8)
            open_price = close * (1.0 - 0.002)
            intraday_range = close * base_range * expansion
            high = max(open_price, close) + intraday_range / 2.0
            low = min(open_price, close) - intraday_range / 2.0
            stock_rows.append(
                (code, date, open_price, high, low, close, base_volume + idx * 100)
            )
            master_rows.append(
                (date, code, name, market_code, "Prime", "TOPIX Small 1")
            )
            if code_idx < 8:
                per = 4.0 + code_idx * 0.05
                forward_per = per * 0.6
                pbr = 0.5 + code_idx * 0.01
            elif code_idx < 14:
                per = 45.0 + code_idx
                forward_per = 35.0 + code_idx
                pbr = 0.55 + code_idx * 0.01
            elif 30 <= code_idx < 42:
                per = 5.0 + (code_idx - 30) * 0.1
                forward_per = per * 0.7
                pbr = 1.6 + code_idx * 0.01
            else:
                per = 25.0 + code_idx * 0.2
                forward_per = 24.0 + code_idx * 0.2
                pbr = 1.8 + code_idx * 0.01
            valuation_rows.append(
                (
                    code,
                    date,
                    date,
                    per,
                    forward_per,
                    pbr,
                    close * 10_000_000,
                    close * 8_000_000,
                    close * 4_000_000,
                    close * 3_500_000,
                    "unit",
                )
            )

    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?)",
        master_rows,
    )
    conn.executemany(
        "INSERT INTO daily_valuation VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        valuation_rows,
    )
    upgrade_daily_ranking_fixture_to_market_v4(conn)
    conn.close()
    return db_path
