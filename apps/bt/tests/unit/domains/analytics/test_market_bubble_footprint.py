from __future__ import annotations

from pathlib import Path

import duckdb

from src.domains.analytics.market_bubble_footprint import (
    BubbleFootprintResult,
    ReratingBubbleRegimeResult,
    build_bubble_footprint_summary_markdown,
    build_rerating_bubble_regime_summary_markdown,
    run_market_bubble_footprint_research,
    run_rerating_bubble_regime_forward_response_research,
    write_bubble_footprint_bundle,
    write_rerating_bubble_regime_bundle,
)


def test_market_bubble_footprint_classifies_monthly_market_regimes(
    tmp_path: Path,
) -> None:
    db_path = _build_bubble_footprint_db(tmp_path / "market.duckdb")

    result = run_market_bubble_footprint_research(
        db_path,
        start_date="2024-01-31",
        end_date="2024-12-31",
        return_horizons=(20, 60),
        market_scopes=("prime",),
        frequency="monthly",
    )

    assert isinstance(result, BubbleFootprintResult)
    assert result.latest_snapshot_date == "2024-12-31"
    assert not result.footprint_df.empty
    assert not result.latest_snapshot_df.empty
    assert not result.top_contributors_df.empty
    assert not result.sector_contributors_df.empty
    assert {
        "snapshot_date",
        "horizon",
        "bubble_regime",
        "bubble_score",
        "breadth_up_pct",
        "pct_above_sma50",
        "top10_mcap_share_pct",
        "top10_positive_contribution_share_pct",
        "expensive_mcap_share_pct",
        "return_p90_p10_spread_pct",
    }.issubset(result.footprint_df.columns)
    latest_60d = result.latest_snapshot_df.loc[
        result.latest_snapshot_df["horizon"] == 60
    ].iloc[0]
    assert latest_60d["bubble_regime"] in {
        "normal",
        "narrowing",
        "crowded",
        "blowoff_watch",
    }
    assert latest_60d["bubble_score"] >= 2
    assert latest_60d["top10_mcap_share_pct"] > 0
    assert latest_60d["expensive_mcap_share_pct"] > 0

    summary = build_bubble_footprint_summary_markdown(result)
    assert "Market Bubble Footprint" in summary
    assert "Latest Snapshot" in summary
    assert "Top Contributors" in summary

    bundle = write_bubble_footprint_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-bubble",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def test_rerating_bubble_regime_forward_response_joins_footprint_regime(
    tmp_path: Path,
) -> None:
    db_path = _build_bubble_footprint_db(tmp_path / "market.duckdb")

    result = run_rerating_bubble_regime_forward_response_research(
        db_path,
        start_date="2024-01-31",
        end_date="2024-10-31",
        signal_horizons=(20,),
        footprint_horizons=(60,),
        market_scopes=("prime",),
        frequency="monthly",
        min_observations=1,
        severe_loss_threshold_pct=-10.0,
        observation_sample_limit=100,
    )

    assert isinstance(result, ReratingBubbleRegimeResult)
    assert not result.rerating_bubble_regime_df.empty
    assert not result.regime_transition_df.empty
    assert not result.observation_sample_df.empty
    assert {
        "bubble_regime",
        "liquidity_regime",
        "value_condition",
        "horizon",
        "median_forward_excess_return_pct",
        "severe_loss_rate_pct",
    }.issubset(result.rerating_bubble_regime_df.columns)
    assert {"neutral_rerating", "crowded_rerating"}.intersection(
        set(result.rerating_bubble_regime_df["liquidity_regime"].astype(str))
    )
    assert {"all_value", "strong_value_confirmation"}.issubset(
        set(result.rerating_bubble_regime_df["value_condition"].astype(str))
    )
    assert set(result.observation_sample_df["bubble_regime"].astype(str)).issubset(
        {"normal", "narrowing", "crowded", "blowoff_watch"}
    )

    summary = build_rerating_bubble_regime_summary_markdown(result)
    assert "Rerating x Bubble Regime Forward Response" in summary
    assert "Regime Transition" in summary

    bundle = write_rerating_bubble_regime_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-rerating-bubble",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _build_bubble_footprint_db(db_path: Path) -> Path:
    dates = duckdb.execute(
        "SELECT strftime(d, '%Y-%m-%d') FROM range(DATE '2023-01-02', DATE '2025-02-01', INTERVAL 1 DAY) t(d) WHERE dayofweek(d) BETWEEN 1 AND 5"
    ).fetchall()
    date_values = [row[0] for row in dates]
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
            close DOUBLE,
            eps DOUBLE,
            bps DOUBLE,
            forward_eps DOUBLE,
            per DOUBLE,
            forward_per DOUBLE,
            pbr DOUBLE,
            market_cap DOUBLE,
            free_float_market_cap DOUBLE,
            basis_version TEXT,
            p_op DOUBLE,
            forward_p_op DOUBLE
        )
        """
    )

    stock_rows: list[tuple[str, str, float, float, float, float, int]] = []
    topix_rows: list[tuple[str, float, float, float, float]] = []
    master_rows: list[tuple[str, str, str, str, str, str | None]] = []
    valuation_rows: list[
        tuple[
            str,
            str,
            str,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            str,
            float,
            float,
        ]
    ] = []
    codes = [f"{1000 + index}" for index in range(70)]
    for day_index, date in enumerate(date_values):
        topix_close = 1000.0 + day_index * 1.1
        topix_rows.append(
            (
                date,
                topix_close * 0.998,
                topix_close * 1.003,
                topix_close * 0.997,
                topix_close,
            )
        )
        for code_index, code in enumerate(codes):
            is_large_winner = code_index < 8
            is_laggard = code_index >= 35
            base = 80.0 + code_index
            slope = 0.45 if is_large_winner else (-0.05 if is_laggard else 0.10)
            close = max(5.0, base + day_index * slope)
            volume = 5_000 + code_index * 200 + day_index * (90 if is_large_winner else 10)
            market_cap = (
                900_000_000_000.0 + code_index * 80_000_000_000.0
                if is_large_winner
                else 40_000_000_000.0 + code_index * 1_000_000_000.0
            )
            pbr = 6.0 + code_index * 0.05 if is_large_winner else 1.0 + code_index * 0.01
            forward_per = 45.0 if is_large_winner else 12.0 + code_index * 0.1
            per = 50.0 if is_large_winner else 14.0 + code_index * 0.1
            eps = close / per
            forward_eps = close / forward_per
            stock_rows.append(
                (code, date, close * 0.995, close * 1.01, close * 0.99, close, volume)
            )
            master_rows.append((date, code, f"Name {code}", "0101", "Prime", None))
            valuation_rows.append(
                (
                    code,
                    date,
                    date,
                    close,
                    eps,
                    close * 0.8,
                    forward_eps,
                    per,
                    forward_per,
                    pbr,
                    market_cap,
                    market_cap * 0.8,
                    "unit",
                    per * 0.8,
                    forward_per * 0.7,
                )
            )
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)", topix_rows)
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?)", master_rows
    )
    conn.executemany(
        "INSERT INTO daily_valuation VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        valuation_rows,
    )
    conn.close()
    return db_path
