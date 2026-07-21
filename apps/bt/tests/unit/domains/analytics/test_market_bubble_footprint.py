from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

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
from src.domains.analytics.market_bubble_footprint_monitor import (
    _build_market_bubble_footprint_as_of_frame,
)

from daily_ranking_market_v5_fixture import (
    refresh_daily_ranking_provider_window,
    upgrade_daily_ranking_fixture_to_market_v5,
)

_EXPECTED_BUBBLE_REQUIRED_TABLES = (
    "stock_data_raw",
    "stock_data",
    "stock_provider_windows",
    "stock_adjustment_events",
    "current_basis_fundamentals_state",
    "current_basis_recompute_pending",
    "stock_master_daily",
    "daily_valuation",
    "statements",
    "statement_metrics_adjusted",
    "topix_data",
    "indices_data",
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
    assert result.required_tables == _EXPECTED_BUBBLE_REQUIRED_TABLES
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


def test_market_bubble_footprint_rejects_poisoned_stock_data(tmp_path: Path) -> None:
    db_path = _build_bubble_footprint_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    conn.execute("UPDATE stock_data SET close = close * 50.0")
    conn.close()

    with pytest.raises(RuntimeError, match="provider vintage lineage"):
        run_market_bubble_footprint_research(
            db_path,
            start_date="2024-01-31",
            end_date="2024-12-31",
            return_horizons=(20, 60),
            market_scopes=("prime",),
            frequency="monthly",
        )


def test_live_market_bubble_footprint_rejects_poisoned_stock_data(
    tmp_path: Path,
) -> None:
    db_path = _build_bubble_footprint_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    conn.execute("UPDATE stock_data SET close = close * 50.0")
    conn.close()

    with pytest.raises(RuntimeError, match="provider vintage lineage"):
        _build_market_bubble_footprint_as_of_frame(
            str(db_path),
            baseline=pd.DataFrame(),
            markets=("prime",),
            date="2024-12-31",
        )


def test_market_bubble_footprint_ignores_stale_future_price_basis_valuation(
    tmp_path: Path,
) -> None:
    db_path = _build_bubble_footprint_db(tmp_path / "market.duckdb")
    kwargs = {
        "start_date": "2024-01-31",
        "end_date": "2024-12-31",
        "return_horizons": (60,),
        "market_scopes": ("prime",),
        "frequency": "monthly",
    }
    baseline = run_market_bubble_footprint_research(db_path, **kwargs)
    baseline_share = float(
        baseline.latest_snapshot_df.iloc[0]["expensive_mcap_share_pct"]
    )
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        INSERT INTO daily_valuation
        SELECT * REPLACE (
            '2099-12-31' AS price_basis_date,
            1e18 AS market_cap,
            8e17 AS free_float_market_cap,
            999.0 AS forward_per,
            99.0 AS pbr
        )
        FROM daily_valuation
        WHERE code = '1000' AND date = '2024-12-31'
        """
    )
    conn.close()

    stale = run_market_bubble_footprint_research(db_path, **kwargs)

    assert float(
        stale.latest_snapshot_df.iloc[0]["expensive_mcap_share_pct"]
    ) == pytest.approx(baseline_share)


def test_live_market_bubble_footprint_rejects_duplicate_current_valuation(
    tmp_path: Path,
) -> None:
    db_path = _build_bubble_footprint_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        INSERT INTO daily_valuation
        SELECT * FROM daily_valuation
        WHERE code = '1000' AND date = '2024-12-31'
        """
    )
    conn.close()

    with pytest.raises(RuntimeError, match="lineage"):
        _build_market_bubble_footprint_as_of_frame(
            str(db_path),
            baseline=pd.DataFrame(),
            markets=("prime",),
            date="2024-12-31",
        )


def test_rerating_bubble_regime_rejects_split_valuation_witness(
    tmp_path: Path,
) -> None:
    db_path = _build_bubble_footprint_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        INSERT INTO daily_valuation
        SELECT * REPLACE ('2099-12-31' AS price_basis_date)
        FROM daily_valuation
        WHERE code = '1000' AND date = '2024-10-31'
        """
    )
    conn.execute(
        """
        UPDATE daily_valuation
        SET source_fingerprint = repeat('0', 64)
        WHERE code = '1000' AND date = '2024-10-31'
          AND price_basis_date = date
        """
    )
    conn.close()

    with pytest.raises(RuntimeError, match="lineage"):
        run_rerating_bubble_regime_forward_response_research(
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
    assert result.required_tables == _EXPECTED_BUBBLE_REQUIRED_TABLES
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


def test_rerating_bubble_regime_rejects_poisoned_stock_data(tmp_path: Path) -> None:
    db_path = _build_bubble_footprint_db(tmp_path / "market.duckdb")
    kwargs = {
        "start_date": "2024-01-31",
        "end_date": "2024-10-31",
        "signal_horizons": (20,),
        "footprint_horizons": (60,),
        "market_scopes": ("prime",),
        "frequency": "monthly",
        "min_observations": 1,
        "severe_loss_threshold_pct": -10.0,
        "observation_sample_limit": 100,
    }
    run_rerating_bubble_regime_forward_response_research(db_path, **kwargs)
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "UPDATE stock_data SET "
        "close = close * (1.0 + (dayofyear(CAST(date AS DATE)) % 17) / 10.0), "
        "volume = volume * (1 + CAST(code AS INTEGER) % 11)"
    )
    conn.close()

    with pytest.raises(RuntimeError, match="provider vintage lineage"):
        run_rerating_bubble_regime_forward_response_research(db_path, **kwargs)


def test_rerating_bubble_regime_keeps_equivalent_provider_split_adjustment(
    tmp_path: Path,
) -> None:
    db_path = _build_bubble_footprint_db(tmp_path / "market.duckdb")
    kwargs = {
        "start_date": "2024-01-31",
        "end_date": "2024-10-31",
        "signal_horizons": (20,),
        "footprint_horizons": (60,),
        "market_scopes": ("prime",),
        "frequency": "monthly",
        "min_observations": 1,
        "severe_loss_threshold_pct": -10.0,
        "observation_sample_limit": 100,
    }
    baseline = run_rerating_bubble_regime_forward_response_research(db_path, **kwargs)
    _add_equivalent_provider_split(db_path, code="1000", event_date="2024-07-01")

    split = run_rerating_bubble_regime_forward_response_research(db_path, **kwargs)

    pd.testing.assert_frame_equal(baseline.footprint_df, split.footprint_df)
    pd.testing.assert_frame_equal(
        baseline.rerating_bubble_regime_df,
        split.rerating_bubble_regime_df,
    )
    assert split.observation_count == baseline.observation_count
    assert (
        split.footprint_df["observation_count"]
        == split.footprint_df["code_count"]
    ).all()


def _add_equivalent_provider_split(
    db_path: Path,
    *,
    code: str,
    event_date: str,
) -> None:
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        UPDATE stock_data_raw
        SET open = adjusted_open / 0.5,
            high = adjusted_high / 0.5,
            low = adjusted_low / 0.5,
            close = adjusted_close / 0.5,
            volume = CAST(round(adjusted_volume * 0.5) AS BIGINT),
            turnover_value = (adjusted_close / 0.5)
                * CAST(round(adjusted_volume * 0.5) AS BIGINT)
        WHERE code = ? AND date < CAST(? AS DATE)
        """,
        [code, event_date],
    )
    conn.execute(
        "UPDATE stock_data_raw SET adjustment_factor = 0.5 "
        "WHERE code = ? AND date = CAST(? AS DATE)",
        [code, event_date],
    )
    fingerprint = refresh_daily_ranking_provider_window(conn, code=code)
    conn.execute(
        "INSERT INTO stock_adjustment_events VALUES (?, ?, 0.5, ?)",
        [code, event_date, fingerprint],
    )
    conn.close()


def _build_bubble_footprint_db(db_path: Path) -> Path:
    dates = duckdb.execute(
        "SELECT strftime(d, '%Y-%m-%d') FROM range(DATE '2021-01-04', DATE '2025-02-01', INTERVAL 1 DAY) t(d) WHERE dayofweek(d) BETWEEN 1 AND 5"
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
    codes = [f"{1000 + index}" for index in range(120)]
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
    upgrade_daily_ranking_fixture_to_market_v5(conn)
    conn.close()
    return db_path
