from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest

from src.domains.analytics.pre_disclosure_flow_volatility import (
    PreDisclosureFlowVolatilityResult,
    build_summary_markdown,
    run_pre_disclosure_flow_volatility_research,
    write_pre_disclosure_flow_volatility_bundle,
)


def test_pre_disclosure_features_use_only_sessions_before_disclosure(
    tmp_path: Path,
) -> None:
    db_path = _build_pre_disclosure_research_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    event = result.event_feature_df[
        (result.event_feature_df["code"] == "1111")
        & (result.event_feature_df["disclosed_date"] == "2024-01-10")
    ].iloc[0]

    assert event["pre_event_date"] == "2024-01-09"
    assert event["entry_date"] == "2024-01-11"
    assert event["pre_return_3d_pct"] == pytest.approx((106.0 / 102.0 - 1.0) * 100.0)
    assert event["pre_abret_3d_pct"] == pytest.approx(
        ((106.0 / 102.0) - (1050.0 / 1020.0)) * 100.0
    )

    # The disclosure-day spike must not leak into pre-disclosure features.
    assert event["pre_volume_mean_3d"] == pytest.approx(200.0)
    assert event["pre_volume_z_3d"] < 5.0
    assert event["pre_atr_pct"] < 10.0


def test_pre_disclosure_research_emits_market_split_summaries(
    tmp_path: Path,
) -> None:
    db_path = _build_pre_disclosure_research_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    bucket_summary = result.market_score_bucket_forward_return_df
    interaction_summary = result.market_atr_volume_interaction_df
    signed_summary = result.market_signed_move_df
    diagnostics = result.market_sample_diagnostics_df

    assert {"all", "prime", "standard"}.issubset(set(diagnostics["market_scope"]))
    assert not bucket_summary.empty
    assert {"all", "prime", "standard"}.issubset(set(bucket_summary["market_scope"]))
    assert {"low", "high"}.issubset(set(bucket_summary["score_bucket"]))
    assert not interaction_summary.empty
    assert {"atr_segment", "volume_segment"}.issubset(interaction_summary.columns)
    assert not signed_summary.empty
    assert "aligned" in set(signed_summary["signed_pre_move"])


def test_pre_disclosure_research_writes_bundle_and_summary(tmp_path: Path) -> None:
    db_path = _build_pre_disclosure_research_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Market Score Bucket Forward Returns" in summary
    assert "ATR x Volume Interaction" in summary

    bundle = write_pre_disclosure_flow_volatility_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
        notes="coverage",
    )

    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()
    assert bundle.summary_path.read_text(encoding="utf-8").startswith(
        "# Pre-Disclosure Flow/Volatility"
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"pre_windows": (0,)}, "pre_windows must be positive"),
        ({"horizons": (0,)}, "horizons must be positive"),
        ({"atr_period": 1}, "atr_period must be at least 2"),
        ({"baseline_window": 1}, "baseline_window must be at least 2"),
        ({"bucket_count": 1}, "bucket_count must be at least 2"),
    ],
)
def test_pre_disclosure_research_rejects_invalid_params(
    tmp_path: Path,
    kwargs: dict[str, Any],
    message: str,
) -> None:
    db_path = _build_pre_disclosure_research_db(tmp_path / "market.duckdb")

    with pytest.raises(ValueError, match=message):
        run_pre_disclosure_flow_volatility_research(db_path, **kwargs)


def test_pre_disclosure_research_requires_existing_db(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        run_pre_disclosure_flow_volatility_research(tmp_path / "missing.duckdb")


def _run_test_research(db_path: Path) -> PreDisclosureFlowVolatilityResult:
    return run_pre_disclosure_flow_volatility_research(
        db_path,
        start_date="2024-01-10",
        end_date="2024-01-12",
        pre_windows=(3,),
        horizons=(1, 3),
        atr_period=3,
        baseline_window=3,
        bucket_count=2,
    )


def _build_pre_disclosure_research_db(db_path: Path) -> Path:
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
            profit DOUBLE
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
        "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("1111", "2024-01-05", "ForecastRevision", "3Q", 80.0, 80.0, 100.0),
            ("1111", "2024-01-10", "ForecastRevision", "3Q", 100.0, 100.0, 130.0),
            ("2222", "2024-01-05", "ForecastRevision", "3Q", 90.0, 90.0, 120.0),
            ("2222", "2024-01-10", "ForecastRevision", "3Q", 70.0, 70.0, 90.0),
            ("3333", "2024-01-12", "FinancialStatement", "FY", 60.0, 60.0, 110.0),
            ("4444", "2024-01-12", "FinancialStatement", "FY", 55.0, 55.0, 100.0),
        ],
    )
    conn.close()
    return db_path
