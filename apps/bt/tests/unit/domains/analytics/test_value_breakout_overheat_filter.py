from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.domains.analytics.research_bundle import write_research_bundle
from src.domains.analytics.value_breakout_overheat_filter import (
    VALUE_BREAKOUT_OVERHEAT_FILTER_EXPERIMENT_ID,
    load_value_breakout_overheat_filter_bundle,
    run_value_breakout_overheat_filter,
    write_value_breakout_overheat_filter_bundle,
)


def _write_market_db(db_path: Path) -> str:
    dates = pd.bdate_range("2023-09-01", "2024-04-30")
    rows: list[tuple[object, ...]] = []
    topix_rows: list[tuple[object, ...]] = []
    for index, date in enumerate(dates):
        date_str = date.strftime("%Y-%m-%d")
        topix_close = 1000.0 + index * 0.5
        topix_rows.append((date_str, topix_close, topix_close, topix_close, topix_close, 1_000_000))
        for code, base, drift, late_boost in (
            ("1000", 80.0, 0.08, 0.95),
            ("2000", 90.0, 0.03, 0.05),
        ):
            boost = max(index - 95, 0) * late_boost
            close = base + index * drift + boost
            rows.append(
                (
                    code,
                    date_str,
                    close * 0.99,
                    close * 1.01,
                    close * 0.98,
                    close,
                    100_000,
                    1.0,
                    None,
                )
            )
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE stock_data (
                code TEXT,
                date TEXT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                adjustment_factor DOUBLE,
                created_at TEXT
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
                close DOUBLE,
                volume BIGINT
            )
            """
        )
        conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)", topix_rows)
    finally:
        conn.close()
    return str(db_path)


def _selected_event_df() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for code, company, rank, return_pct in (
        ("1000", "Short Climax", 1, -12.0),
        ("2000", "Calm Value", 2, 8.0),
    ):
        rows.append(
            {
                "market_scope": "standard",
                "score_method": "prime_size_tilt",
                "score_method_label": "Prime size tilt",
                "liquidity_scenario": "adv10m",
                "liquidity_scenario_label": "ADV60 >= 10mn",
                "breakout_policy": "breakout_additive",
                "breakout_policy_label": "Breakout additive",
                "breakout_window": 120,
                "breakout_lookback_sessions": 20,
                "selection_count": 10,
                "eligible_count": 20,
                "selection_rank": rank,
                "composite_score": 100.0 - rank,
                "value_composite_score": 90.0 - rank,
                "event_id": f"{code}:2024-01",
                "year": 2024,
                "rebalance_period": "2024-01",
                "rebalance_months": 3,
                "code": code,
                "company_name": company,
                "market": "standard",
                "market_code": "0112",
                "sector_33_name": "Machinery",
                "entry_date": "2024-01-15",
                "signal_date": "2024-01-12",
                "exit_date": "2024-03-29",
                "entry_open": 100.0,
                "exit_close": 100.0 * (1.0 + return_pct / 100.0),
                "event_return_pct": return_pct,
                "event_return_winsor_pct": return_pct,
            }
        )
    return pd.DataFrame(rows)


def _write_input_bundle(tmp_path: Path, db_path: str) -> Path:
    bundle = write_research_bundle(
        experiment_id="market-behavior/annual-value-breakout-periodic-rebalance",
        module="tests.fixture",
        function="build",
        params={},
        db_path=db_path,
        analysis_start_date="2024-01-15",
        analysis_end_date="2024-03-29",
        result_metadata={"db_path": db_path},
        result_tables={"selected_event_df": _selected_event_df()},
        summary_markdown="# fixture\n",
        output_root=tmp_path,
        run_id="input-value-breakout",
    )
    return bundle.bundle_dir


def test_value_breakout_overheat_filter_builds_event_and_portfolio_tables(
    tmp_path: Path,
) -> None:
    db_path = _write_market_db(tmp_path / "market.duckdb")
    input_bundle = _write_input_bundle(tmp_path, db_path)

    result = run_value_breakout_overheat_filter(
        input_bundle,
        db_path=db_path,
        output_root=tmp_path,
        holdout_months=0,
    )

    assert result.input_run_id == "input-value-breakout"
    assert result.selected_event_count == 2
    assert result.technical_feature_count == 2
    assert not result.threshold_summary_df.empty
    assert "short_climax_10d_q80_overlap_ge2" in set(result.overheat_rule_event_df["rule_name"])
    assert {"base", "exclude_no_refill", "haircut_0_5"}.issubset(
        set(result.portfolio_summary_df["variant_name"])
    )
    short_climax = result.overheat_rule_event_df[
        result.overheat_rule_event_df["rule_name"].eq("short_climax_10d_q80_overlap_ge2")
    ]
    assert short_climax["is_overheat"].any()


def test_write_and_load_value_breakout_overheat_filter_bundle(tmp_path: Path) -> None:
    db_path = _write_market_db(tmp_path / "market.duckdb")
    input_bundle = _write_input_bundle(tmp_path, db_path)
    result = run_value_breakout_overheat_filter(
        input_bundle,
        db_path=db_path,
        output_root=tmp_path,
        holdout_months=0,
    )

    bundle = write_value_breakout_overheat_filter_bundle(
        result,
        output_root=tmp_path,
        run_id="overheat-filter",
    )
    loaded = load_value_breakout_overheat_filter_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == VALUE_BREAKOUT_OVERHEAT_FILTER_EXPERIMENT_ID
    pd.testing.assert_frame_equal(
        loaded.portfolio_summary_df.reset_index(drop=True),
        result.portfolio_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
