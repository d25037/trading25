from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.domains.analytics.index_market_strength_research import (
    INDEX_MARKET_STRENGTH_RESEARCH_EXPERIMENT_ID,
    get_index_market_strength_research_bundle_path_for_run_id,
    get_index_market_strength_research_latest_bundle_path,
    load_index_market_strength_research_bundle,
    run_index_market_strength_research,
    write_index_market_strength_research_bundle,
)


def _build_indices_market_db(db_path: Path) -> str:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE index_master (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                name_english TEXT,
                category TEXT NOT NULL,
                data_start_date TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE indices_data (
                code TEXT NOT NULL,
                date TEXT NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                sector_name TEXT,
                created_at TEXT,
                PRIMARY KEY (code, date)
            )
            """
        )
        conn.executemany(
            "INSERT INTO index_master VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                ("I001", "Alpha sector", None, "sector33", "2024-01-01", None, None),
                ("I002", "Beta sector", None, "sector33", "2024-01-01", None, None),
                ("I003", "Gamma sector", None, "sector33", "2024-01-01", None, None),
                ("TOPIX", "TOPIX", None, "topix", "2024-01-01", None, None),
            ],
        )
        dates = pd.bdate_range("2024-01-02", periods=80)
        rows: list[tuple[object, ...]] = []
        for i, date in enumerate(dates):
            values = {
                "I001": 100.0 + i * 1.0,
                "I002": 140.0 - i * 0.5,
                "I003": 95.0 + ((i % 20) - 10) * 0.8 + i * 0.2,
                "TOPIX": 1000.0 + i * 0.1,
            }
            for code, close in values.items():
                rows.append(
                    (
                        code,
                        date.strftime("%Y-%m-%d"),
                        close,
                        close * 1.01,
                        close * 0.99,
                        close,
                        f"{code} sector",
                        None,
                    )
                )
        conn.executemany("INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()
    return str(db_path)


def test_run_index_market_strength_research_builds_feature_and_breadth_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_indices_market_db(tmp_path / "market.duckdb")

    result = run_index_market_strength_research(
        db_path,
        lookback_windows=(5, 20),
        horizon_sessions=5,
    )

    assert result.index_count == 3
    assert result.horizon_sessions == 5
    assert set(result.index_price_feature_df["code"]) == {"I001", "I002", "I003"}
    assert "forward_5d_return_pct" in result.index_price_feature_df.columns
    assert "return_20d_pct" in result.index_price_feature_df.columns
    assert "rebound_from_low_20d_pct" in result.index_price_feature_df.columns
    assert "price_position_20d" in result.index_price_feature_df.columns
    assert set(result.index_state_summary_df["feature_family"]).issuperset(
        {"return", "rebound_from_low", "price_position"}
    )
    assert set(result.breadth_state_df["lookback"]) == {5, 20}
    assert {"strong_breadth_ratio", "weak_breadth_ratio"}.issubset(
        result.breadth_state_df.columns
    )
    assert not result.breadth_state_summary_df.empty
    assert not result.feature_rank_df.empty


def test_write_and_load_index_market_strength_research_bundle(tmp_path: Path) -> None:
    db_path = _build_indices_market_db(tmp_path / "market.duckdb")
    result = run_index_market_strength_research(
        db_path,
        lookback_windows=(5,),
        horizon_sessions=5,
    )

    bundle = write_index_market_strength_research_bundle(
        result,
        output_root=tmp_path,
        run_id="index-market-strength",
    )
    loaded = load_index_market_strength_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == INDEX_MARKET_STRENGTH_RESEARCH_EXPERIMENT_ID
    assert (
        get_index_market_strength_research_bundle_path_for_run_id(
            "index-market-strength",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_index_market_strength_research_latest_bundle_path(output_root=tmp_path) == bundle.bundle_dir
    pd.testing.assert_frame_equal(
        loaded.index_state_summary_df.reset_index(drop=True),
        result.index_state_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
