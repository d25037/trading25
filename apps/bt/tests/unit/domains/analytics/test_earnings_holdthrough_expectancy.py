from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.earnings_holdthrough_expectancy import (
    EarningsHoldthroughExpectancyResult,
    _attach_prime_liquidity_residual_panel,
    _build_prime_liquidity_residual_panel,
    _classify_overheat_state,
    build_summary_markdown,
    run_earnings_holdthrough_expectancy_research,
    write_earnings_holdthrough_expectancy_bundle,
)


def test_earnings_holdthrough_features_are_pit_safe_and_classified(
    tmp_path: Path,
) -> None:
    db_path = _build_earnings_holdthrough_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    event = result.event_feature_df[
        (result.event_feature_df["code"] == "1111")
        & (result.event_feature_df["disclosed_date"] == "2024-01-10")
    ].iloc[0]

    assert event["pre_event_date"] == "2024-01-09"
    assert event["entry_date"] == "2024-01-11"
    assert event["is_fy"] is True
    assert event["has_next_guidance"] is True
    assert event["guidance_strength"] == "positive"
    assert event["actual_strength"] == "positive"
    assert event["event_strength"] == "positive"
    assert event["pre_return_3d_pct"] == pytest.approx((106.0 / 102.0 - 1.0) * 100.0)
    assert event["pre_return_5d_pct"] == pytest.approx((106.0 / 100.0 - 1.0) * 100.0)
    assert event["med_adv60_mil_jpy"] == pytest.approx((104.0 * 200.0) / 1_000_000.0)
    assert event["adv60_to_free_float_pct"] == pytest.approx(
        (104.0 * 200.0) / (106.0 * 900_000.0) * 100.0
    )

    # The disclosure-day spike must not leak into pre-disclosure liquidity.
    assert event["med_adv60_mil_jpy"] < (130.0 * 5000.0) / 1_000_000.0


def test_earnings_holdthrough_research_emits_interaction_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_earnings_holdthrough_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert {"all", "prime", "standard"}.issubset(
        set(result.coverage_diagnostics_df["market_scope"])
    )
    assert not result.bucket_expectancy_df.empty
    assert {"is_fy", "has_next_guidance", "event_strength"}.issubset(
        result.bucket_expectancy_df.columns
    )
    assert not result.precondition_outcome_df.empty
    assert {
        "positive_event_rate_pct",
        "negative_event_rate_pct",
        "median_forward_excess_return_pct",
    }.issubset(result.precondition_outcome_df.columns)
    assert "event_strength" not in result.precondition_outcome_df.columns
    assert "has_next_guidance" not in result.precondition_outcome_df.columns
    assert not result.liquidity_interaction_df.empty
    assert "liquidity_regime" in result.liquidity_interaction_df.columns
    assert not result.signed_premove_df.empty
    assert "aligned" in set(result.signed_premove_df["signed_pre_move"])
    assert not result.holdthrough_return_df.empty


def test_prime_liquidity_residual_panel_classifies_daily_ranking_states() -> None:
    source_df = pd.DataFrame(
        [
            {
                "code": "1001",
                "date": "2024-01-09",
                "adv_jpy": 100.0,
                "adv_sessions": 3,
                "free_float_market_cap_jpy": 1000.0,
            },
            {
                "code": "1002",
                "date": "2024-01-09",
                "adv_jpy": 200.0,
                "adv_sessions": 3,
                "free_float_market_cap_jpy": 2000.0,
            },
            {
                "code": "1003",
                "date": "2024-01-09",
                "adv_jpy": 400.0,
                "adv_sessions": 3,
                "free_float_market_cap_jpy": 4000.0,
            },
            {
                "code": "1004",
                "date": "2024-01-09",
                "adv_jpy": 800.0,
                "adv_sessions": 3,
                "free_float_market_cap_jpy": 8000.0,
            },
            {
                "code": "9999",
                "date": "2024-01-09",
                "adv_jpy": 12000.0,
                "adv_sessions": 3,
                "free_float_market_cap_jpy": 16000.0,
            },
        ]
    )
    panel_df = _build_prime_liquidity_residual_panel(
        source_df,
        liquidity_window=3,
        min_regression_observations=3,
    )
    event_df = pd.DataFrame(
        [
            {
                "code": "9999",
                "pre_event_date": "2024-01-09",
                "pre_return_20d_pct": 8.0,
                "pre_return_60d_pct": 12.0,
                "adv60_to_free_float_pct": 0.0,
            }
        ]
    )

    enriched = _attach_prime_liquidity_residual_panel(event_df, panel_df)

    event = enriched.iloc[0]
    assert event["liquidity_residual_z"] > 1.0
    assert event["liquidity_residual_z_bucket"] == "high"
    assert event["liquidity_regime"] == "rerating_participation"


def test_earnings_holdthrough_classifies_overheat_from_20d_return() -> None:
    assert _classify_overheat_state(29.99) == "not_overheat"
    assert _classify_overheat_state(30.0) == "overheat"
    assert _classify_overheat_state(float("nan")) == "missing"


def test_earnings_holdthrough_research_writes_bundle_and_summary(tmp_path: Path) -> None:
    db_path = _build_earnings_holdthrough_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Bucket Expectancy" in summary
    assert "Liquidity Interaction" in summary

    bundle = write_earnings_holdthrough_expectancy_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
        notes="coverage",
    )

    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()
    assert bundle.summary_path.read_text(encoding="utf-8").startswith(
        "# Earnings Hold-Through Expectancy"
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"pre_windows": (0,)}, "pre_windows must be positive"),
        ({"horizons": (0,)}, "horizons must be positive"),
        ({"liquidity_window": 0}, "liquidity_window must be positive"),
        ({"severe_loss_threshold_pct": 0.0}, "severe_loss_threshold_pct must be negative"),
    ],
)
def test_earnings_holdthrough_research_rejects_invalid_params(
    tmp_path: Path,
    kwargs: dict[str, object],
    message: str,
) -> None:
    db_path = _build_earnings_holdthrough_db(tmp_path / "market.duckdb")

    with pytest.raises(ValueError, match=message):
        run_earnings_holdthrough_expectancy_research(db_path, **kwargs)


def test_earnings_holdthrough_research_requires_existing_db(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        run_earnings_holdthrough_expectancy_research(tmp_path / "missing.duckdb")


def _run_test_research(db_path: Path) -> EarningsHoldthroughExpectancyResult:
    return run_earnings_holdthrough_expectancy_research(
        db_path,
        start_date="2024-01-10",
        end_date="2024-01-12",
        pre_windows=(3, 5),
        horizons=(1, 3),
        liquidity_window=3,
        severe_loss_threshold_pct=-10.0,
    )


def _build_earnings_holdthrough_db(db_path: Path) -> Path:
    dates = [
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
        "2024-01-05",
        "2024-01-08",
        "2024-01-09",
        "2024-01-10",
        "2024-01-11",
        "2024-01-12",
        "2024-01-15",
    ]
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE stocks (
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
        CREATE TABLE stock_data (
            code TEXT,
            date TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE
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
        CREATE TABLE statements (
            code TEXT,
            disclosed_date TEXT,
            type_of_document TEXT,
            type_of_current_period TEXT,
            forecast_eps DOUBLE,
            next_year_forecast_earnings_per_share DOUBLE,
            profit DOUBLE,
            earnings_per_share DOUBLE,
            shares_outstanding DOUBLE,
            treasury_shares DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?)",
        [
            ("1111", "Alpha", "0111", "Prime", "TOPIX Core30"),
            ("2222", "Beta", "0112", "Standard", None),
            ("3333", "Gamma", "0112", "Standard", None),
            ("4444", "Delta", "0111", "Prime", None),
        ],
    )

    alpha_close = [100.0, 101.0, 102.0, 103.0, 104.0, 106.0, 130.0, 108.0, 109.0, 110.0]
    beta_close = [200.0, 199.0, 198.0, 197.0, 196.0, 194.0, 170.0, 192.0, 191.0, 190.0]
    gamma_close = [50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0]
    delta_close = [80.0, 79.0, 78.0, 77.0, 76.0, 75.0, 74.0, 73.0, 72.0, 71.0]
    stock_rows: list[tuple[str, str, float, float, float, float, float]] = []
    for date, close in zip(dates, alpha_close, strict=True):
        volume = 5000.0 if date == "2024-01-10" else 200.0
        stock_rows.append(("1111", date, close - 1.0, close + 1.0, close - 2.0, close, volume))
    for date, close in zip(dates, beta_close, strict=True):
        volume = 6000.0 if date == "2024-01-10" else 300.0
        stock_rows.append(("2222", date, close + 1.0, close + 2.0, close - 1.0, close, volume))
    for date, close in zip(dates, gamma_close, strict=True):
        stock_rows.append(("3333", date, close, close + 1.0, close - 1.0, close, 150.0))
    for date, close in zip(dates, delta_close, strict=True):
        stock_rows.append(("4444", date, close, close + 1.0, close - 1.0, close, 180.0))
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)", stock_rows)

    topix_rows = []
    for idx, date in enumerate(dates):
        close = 1000.0 + (10.0 * idx)
        topix_rows.append((date, close - 1.0, close + 1.0, close - 2.0, close))
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)", topix_rows)

    conn.executemany(
        "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("1111", "2024-01-05", "FinancialStatement", "3Q", 80.0, None, 100.0, 70.0, 1_000_000.0, 100_000.0),
            ("1111", "2024-01-10", "FinancialStatement", "FY", 100.0, 120.0, 130.0, 95.0, 1_000_000.0, 100_000.0),
            ("2222", "2024-01-05", "FinancialStatement", "3Q", 90.0, None, 120.0, 80.0, 2_000_000.0, 0.0),
            ("2222", "2024-01-10", "FinancialStatement", "FY", 70.0, None, 90.0, 60.0, 2_000_000.0, 0.0),
            ("3333", "2024-01-12", "FinancialStatement", "FY", 60.0, 65.0, 110.0, 55.0, 900_000.0, 0.0),
            ("4444", "2024-01-12", "FinancialStatement", "FY", 55.0, 50.0, 100.0, 50.0, 800_000.0, 0.0),
        ],
    )
    conn.close()
    return db_path
