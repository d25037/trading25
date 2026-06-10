from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.domains.analytics.daily_ranking_research_base import (
    DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE,
    DAILY_RANKING_RESEARCH_PANEL_TABLE,
    DAILY_RANKING_RESEARCH_RANKED_TABLE,
    assert_daily_ranking_research_tables,
    create_daily_ranking_research_panel,
    daily_ranking_query_end_date,
    daily_ranking_query_start_date,
    normalize_daily_ranking_market_scopes,
)
from src.domains.analytics.ranking_color_evidence import (
    RankingColorEvidenceResult,
    _LIQUIDITY_REGIMES,
    _TOPIX_REGIMES,
    build_summary_markdown,
    run_ranking_color_evidence_research,
    write_ranking_color_evidence_bundle,
)


def test_ranking_color_evidence_uses_daily_valuation_fast_path(tmp_path: Path) -> None:
    db_path = _build_ranking_color_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert not result.ranking_color_evidence_df.empty
    assert not result.per_relation_evidence_df.empty
    assert not result.low_per_relation_evidence_df.empty
    assert not result.low_per_relation_level_evidence_df.empty
    assert not result.forward_per_pop_interaction_df.empty
    assert not result.topix_regime_liquidity_value_evidence_df.empty
    assert not result.rerating_good_valuation_chain_df.empty
    assert not result.liquidity_color_long_trend_evidence_df.empty
    assert not result.overvalued_size_liquidity_interaction_df.empty
    assert {
        "valuation_feature",
        "ranking_color_bucket",
        "evidence_tier",
        "median_forward_excess_return_pct",
    }.issubset(result.ranking_color_evidence_df.columns)
    assert {
        "low_forward_per_low_forward_p_op",
        "low_forward_per_high_forward_p_op",
    }.issubset(set(result.forward_per_pop_interaction_df["interaction_bucket"].astype(str)))
    assert {
        "per_percentile",
        "forward_per_percentile",
        "forward_p_op_percentile",
        "forward_per_to_per_ratio",
        "forward_per_to_per_ratio_percentile",
        "forward_p_op_to_per_ratio",
        "forward_p_op_to_per_ratio_percentile",
        "forecast_operating_profit_growth_ratio",
        "forecast_operating_profit_growth_ratio_percentile",
        "forecast_operating_profit_growth_pct",
        "per_to_fop_growth_ratio",
        "per_to_fop_growth_ratio_percentile",
        "forward_per_to_fop_growth_ratio",
        "forward_per_to_fop_growth_ratio_percentile",
        "pbr_percentile",
        "topix_recent_return_20d_pct",
        "topix_recent_return_60d_pct",
        "recent_return_120d_pct",
        "recent_return_150d_pct",
        "liquidity_residual_z",
        "liquidity_regime",
    }.issubset(result.observation_sample_df.columns)
    assert set(
        result.liquidity_color_long_trend_evidence_df["liquidity_regime"].astype(str)
    ).issubset({"crowded_rerating", "neutral_rerating"})
    assert {
        "green",
        "blue",
    }.issubset(set(result.liquidity_color_long_trend_evidence_df["ui_color"].astype(str)))
    assert {120, 150}.issubset(
        set(result.liquidity_color_long_trend_evidence_df["trend_window"].astype(int))
    )
    assert set(
        result.liquidity_color_long_trend_evidence_df["trend_condition"].astype(str)
    ).issubset({"trend_positive", "trend_non_positive"})
    assert {
        "topix_20d_lt_0_60d_gt_0",
        "topix_60d_lt_0",
    }.issubset(set(result.topix_regime_liquidity_value_evidence_df["topix_regime"].astype(str)))
    assert {
        "high_per20_high_pbr20",
        "high_forward_per20_high_pbr20",
    }.issubset(
        set(
            result.overvalued_size_liquidity_interaction_df[
                "valuation_condition"
            ].astype(str)
        )
    )
    assert {
        "market_cap_abs_bucket",
        "adv60_abs_bucket",
        "median_market_cap_bil_jpy",
        "median_med_adv60_mil_jpy",
    }.issubset(result.overvalued_size_liquidity_interaction_df.columns)
    assert {
        "all_value",
        "strong_value_confirmation",
    }.issubset(
        set(result.topix_regime_liquidity_value_evidence_df["value_condition"].astype(str))
    )
    assert {
        "all_good",
        "per_gt_fwdper_gt_fwdpop",
        "good_without_chain",
    }.issubset(set(result.rerating_good_valuation_chain_df["chain_condition"].astype(str)))
    assert {
        "all_rerating_good",
        "neutral_rerating_good",
    }.issubset(set(result.rerating_good_valuation_chain_df["good_scope"].astype(str)))
    assert {
        "forward_per_to_per_ratio",
        "forward_p_op_to_per_ratio",
    }.issubset(set(result.per_relation_evidence_df["relation_feature"].astype(str)))
    assert {
        "low_per_10pct",
        "low_per_20pct",
    }.issubset(set(result.low_per_relation_evidence_df["per_scope"].astype(str)))
    assert "ratio_lte_0_8" in set(
        result.low_per_relation_level_evidence_df["relation_level_bucket"].astype(str)
    )
    assert "crowded_rerating" in _LIQUIDITY_REGIMES
    assert "neutral_rerating" in _LIQUIDITY_REGIMES
    assert "topix_20d_lt_0_60d_gt_0" in {regime for regime, _ in _TOPIX_REGIMES}
    assert "rerating_participation" not in _LIQUIDITY_REGIMES
    assert "statements" not in set(result.required_tables)
    sample = result.observation_sample_df
    high_forward_p_op = sample.loc[sample["code"] == "3333"].iloc[0]
    invalid_forward_p_op = sample.loc[sample["code"] == "6666"].iloc[0]
    assert high_forward_p_op["forward_p_op_percentile"] == 1.0
    assert high_forward_p_op["forward_p_op_to_per_ratio_percentile"] == 1.0
    assert pd.isna(invalid_forward_p_op["forward_p_op_percentile"])
    assert pd.isna(invalid_forward_p_op["forward_p_op_to_per_ratio_percentile"])


def test_daily_ranking_research_base_creates_public_panel_aliases(
    tmp_path: Path,
) -> None:
    db_path = _build_ranking_color_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))

    assert_daily_ranking_research_tables(conn)
    spec = create_daily_ranking_research_panel(
        conn,
        query_start=daily_ranking_query_start_date(
            "2024-03-01",
            warmup_calendar_days=150,
        ),
        query_end=daily_ranking_query_end_date("2024-04-30", max_horizon=20),
        analysis_start_date="2024-03-01",
        analysis_end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
    )

    assert spec.panel_table == DAILY_RANKING_RESEARCH_PANEL_TABLE
    assert spec.ranked_table == DAILY_RANKING_RESEARCH_RANKED_TABLE
    assert spec.liquidity_ranked_table == DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE
    assert spec.market_scopes == ("prime",)
    assert spec.horizons == (20,)
    assert normalize_daily_ranking_market_scopes(("0101",)) == ("prime",)
    public_count_row = conn.execute(
        f"SELECT count(*) FROM {DAILY_RANKING_RESEARCH_PANEL_TABLE}"
    ).fetchone()
    legacy_count_row = conn.execute("SELECT count(*) FROM ranking_color_panel").fetchone()
    assert public_count_row is not None
    assert legacy_count_row is not None
    public_count = public_count_row[0]
    legacy_count = legacy_count_row[0]
    assert public_count == legacy_count
    ranked_columns = {
        str(row[1])
        for row in conn.execute(
            f"PRAGMA table_info('{DAILY_RANKING_RESEARCH_RANKED_TABLE}')"
        ).fetchall()
    }
    assert {
        "per_percentile",
        "forward_per_percentile",
        "forward_p_op_percentile",
        "pbr_percentile",
        "forward_close_excess_return_20d_pct",
        "forecast_operating_profit_growth_ratio",
        "forecast_operating_profit_growth_ratio_percentile",
        "per_to_fop_growth_ratio",
        "per_to_fop_growth_ratio_percentile",
        "forward_per_to_fop_growth_ratio",
        "forward_per_to_fop_growth_ratio_percentile",
        "valuation_signal",
        "strong_value_confirmation",
        "medium_value_confirmation",
        "overvalued_warning",
        "very_overvalued_warning",
        "no_positive_earnings_valuation",
        "no_value_confirmation",
    }.issubset(ranked_columns)
    conn.close()


def test_ranking_color_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_ranking_color_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Color Evidence" in summary
    assert "Forward Valuation vs PER Relation Evidence" in summary
    assert "Low PER x Forward Valuation Relation Evidence" in summary
    assert "Low PER x Forward Valuation Relation Level Evidence" in summary
    assert "Forward PER x Forward P/OP Interaction" in summary
    assert "TOPIX Regime x Liquidity x Value Evidence" in summary
    assert "Rerating Good x PER > Fwd PER > Fwd P/OP" in summary
    assert "Liquidity Color x Long Trend Evidence" in summary
    assert "Overvalued x Size x Liquidity Interaction" in summary

    bundle = write_ranking_color_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def test_ranking_color_evidence_treats_tse_first_section_as_prime(
    tmp_path: Path,
) -> None:
    db_path = _build_ranking_color_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "UPDATE stock_master_daily SET market_code = '0101', market_name = '東証一部'"
    )
    conn.close()

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert set(result.observation_sample_df["market"].astype(str)) == {"prime"}


def _run_test_research(db_path: Path) -> RankingColorEvidenceResult:
    return run_ranking_color_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_ranking_color_db(db_path: Path) -> Path:
    dates = pd.bdate_range("2023-07-03", "2024-06-28").strftime("%Y-%m-%d").tolist()
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
            p_op DOUBLE,
            forward_p_op DOUBLE,
            market_cap DOUBLE,
            free_float_market_cap DOUBLE,
            basis_version TEXT
        )
        """
    )

    stock_rows: list[tuple[str, str, float, float, float, float, int]] = []
    master_rows: list[tuple[str, str, str, str, str, str | None]] = []
    codes = [
        ("1111", "Alpha", "0111", 100.0, 0.35),
        ("2222", "Beta", "0111", 180.0, -0.15),
        ("3333", "Gamma", "0111", 90.0, 0.05),
        ("4444", "Delta", "0111", 120.0, 0.12),
        ("5555", "Epsilon", "0111", 150.0, -0.03),
        ("6666", "Zeta", "0111", 75.0, 0.18),
    ]
    codes.extend(
        (
            str(7000 + extra_index),
            f"Filler {extra_index}",
            "0111",
            80.0 + extra_index,
            -0.08 + extra_index * 0.003,
        )
        for extra_index in range(60)
    )
    for index, date in enumerate(dates):
        for code, name, market_code, base, slope in codes:
            close = base + index * slope
            open_price = close * 0.995
            volume = 5_000 + index * 10 + int(code)
            stock_rows.append(
                (code, date, open_price, close * 1.01, close * 0.99, close, volume)
            )
            master_rows.append((date, code, name, market_code, "Market", None))
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?)", master_rows
    )
    topix_rows: list[tuple[str, float, float, float, float]] = []
    for index, date in enumerate(dates):
        if index < 150:
            topix_close = 1000.0 + index
        elif index < 190:
            topix_close = 1150.0 - (index - 150) * 1.2
        else:
            topix_close = 1102.0 + (index - 190) * 0.5
        topix_rows.append(
            (
                date,
                topix_close * 0.998,
                topix_close * 1.002,
                topix_close * 0.996,
                topix_close,
            )
        )
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)", topix_rows)
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
            str,
        ]
    ] = []
    for date in dates:
        valuation_rows.extend(
            [
                ("1111", date, date, 12.0, 8.0, 0.5, 7.0, 6.0, 110_000_000.0, 90_000_000.0, "unit"),
                ("2222", date, date, 18.0, 30.0, 0.7, 8.0, 9.0, 220_000_000.0, 180_000_000.0, "unit"),
                ("3333", date, date, 14.0, 10.0, 2.0, 11.0, 80.0, 90_000_000.0, 70_000_000.0, "unit"),
                ("4444", date, date, 16.0, 14.0, 1.1, 9.0, 10.0, 120_000_000.0, 110_000_000.0, "unit"),
                ("5555", date, date, 20.0, 18.0, 1.6, 14.0, 15.0, 150_000_000.0, 140_000_000.0, "unit"),
                ("6666", date, date, 22.0, 22.0, 2.5, 20.0, 0.0, 75_000_000.0, 60_000_000.0, "unit"),
            ]
        )
        valuation_rows.extend(
            (
                str(7000 + extra_index),
                date,
                date,
                13.0 + extra_index * 0.1,
                11.0 + extra_index * 0.1,
                0.9 + extra_index * 0.01,
                8.0 + extra_index * 0.1,
                12.0 + extra_index * 0.1,
                100_000_000.0 + extra_index * 1_000_000.0,
                80_000_000.0 + extra_index * 1_000_000.0,
                "unit",
            )
            for extra_index in range(60)
        )
    conn.executemany(
        "INSERT INTO daily_valuation VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        valuation_rows,
    )
    conn.close()
    return db_path
