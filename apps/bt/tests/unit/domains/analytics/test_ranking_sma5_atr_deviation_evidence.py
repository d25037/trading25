from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb

from tests.unit.domains.analytics.test_daily_ranking_research_base import (
    _build_market_v5_research_fixture,
    _refresh_provider_window,
)
from src.domains.analytics.ranking_sma5_atr_deviation_evidence import (
    RankingSma5AtrDeviationEvidenceResult,
    build_summary_markdown,
    run_ranking_sma5_atr_deviation_evidence_research,
    write_ranking_sma5_atr_deviation_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_sma5_count_long_evidence import (
    _build_sma5_count_long_db,
)
from tests.unit.domains.analytics.test_ranking_sma5_deviation_evidence import (
    _add_statements_fixture,
)


def test_sma5_atr_deviation_evidence_builds_direction_threshold_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_sma5_count_long_db(tmp_path / "market.duckdb")
    _add_statements_fixture(db_path)

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.atr_windows == (5, 20)
    assert {
        "stock_provider_windows",
        "stock_adjustment_events",
        "current_basis_recompute_pending",
        "current_basis_fundamentals_state",
        "statement_metrics_adjusted",
    }.issubset(result.required_tables)
    assert not result.coverage_diagnostics_df.empty
    assert not result.sma5_atr_deviation_bucket_evidence_df.empty
    assert not result.long_scaffold_sma5_atr_threshold_evidence_df.empty
    assert not result.short_overlay_sma5_atr_threshold_evidence_df.empty
    assert {
        "sma5_atr5_deviation",
        "sma5_atr20_deviation",
        "sma5_atr5_deviation_bucket",
        "sma5_atr20_deviation_bucket",
        "atr5",
        "atr20",
        "forward_close_excess_return_5d_pct",
        "forward_close_excess_return_20d_pct",
    }.issubset(result.observation_sample_df.columns)
    assert {5, 20}.issubset(
        set(result.sma5_atr_deviation_bucket_evidence_df["atr_window"].astype(int))
    )
    assert {"above", "below"}.issubset(
        set(
            result.long_scaffold_sma5_atr_threshold_evidence_df["direction"].astype(str)
        )
    )
    assert {0.05, 0.1}.issubset(
        set(
            result.long_scaffold_sma5_atr_threshold_evidence_df[
                "threshold_abs_atr"
            ].astype(float)
        )
    )


def test_sma5_atr_deviation_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_sma5_count_long_db(tmp_path / "market.duckdb")
    _add_statements_fixture(db_path)
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking SMA5 ATR Deviation Evidence" in summary
    assert "SMA5 ATR Deviation Bucket Evidence" in summary
    assert "Long Scaffold x SMA5 ATR Threshold Evidence" in summary

    bundle = write_ranking_sma5_atr_deviation_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def test_sma5_atr_uses_requested_valid_session_history_for_large_window(
    tmp_path: Path,
) -> None:
    db_path, valid_dates = _build_sparse_large_atr_window_db(
        tmp_path / "large-atr-window.duckdb"
    )
    analysis_date = valid_dates[599]

    result = run_ranking_sma5_atr_deviation_evidence_research(
        db_path,
        start_date=analysis_date.isoformat(),
        end_date=analysis_date.isoformat(),
        horizons=(5,),
        atr_windows=(600,),
        threshold_abs_atr=(0.5,),
        market_scopes=("prime",),
        liquidity_bands=("high", "mid", "low"),
        min_observations=1,
        observation_sample_limit=100,
    )

    assert result.observation_count > 0
    assert {value.date() for value in result.observation_sample_df["date"]} == {
        analysis_date
    }
    assert result.observation_sample_df["atr600"].notna().all()
    assert result.observation_sample_df["sma5_atr600_deviation"].notna().all()


def _build_sparse_large_atr_window_db(
    db_path: Path,
) -> tuple[Path, tuple[date, ...]]:
    valid_dates = tuple(
        date(2020, 1, 1) + timedelta(days=index * 3) for index in range(700)
    )
    conn = _build_market_v5_research_fixture(db_path, session_dates=valid_dates)
    _add_sma5_atr_statement_columns(conn)
    conn.execute("ALTER TABLE stock_master_daily ADD COLUMN sector_33_code TEXT")
    conn.execute("ALTER TABLE stock_master_daily ADD COLUMN sector_33_name TEXT")
    conn.execute(
        "UPDATE stock_master_daily SET sector_33_code = '3600', "
        "sector_33_name = 'Machinery'"
    )
    extra_securities = tuple(
        (
            f"{4000 + index:04d}",
            f"Extra {index:03d}",
            50.0 + index * 1.3,
            0.02 + (index % 9) * 0.01,
            (100.0 + index * 2.0) * 1_000_000_000.0,
            100_000 + index * 500 + (index % 7) * 7_000,
        )
        for index in range(98)
    )
    for security_index, (
        code,
        company_name,
        base_close,
        slope,
        market_cap,
        base_volume,
    ) in enumerate(extra_securities):
        conn.executemany(
            "INSERT INTO stock_data_raw VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    code,
                    session_date,
                    (close := base_close + index * slope) * 0.995,
                    close * 1.01,
                    close * 0.99,
                    close,
                    (volume := base_volume + index * (security_index + 2)),
                    close * volume,
                    1.0,
                    close * 0.995,
                    close * 1.01,
                    close * 0.99,
                    close,
                    volume,
                )
                for index, session_date in enumerate(valid_dates)
            ],
        )
        conn.execute(
            "INSERT INTO stock_data SELECT code, date, adjusted_open, "
            "adjusted_high, adjusted_low, adjusted_close, adjusted_volume "
            "FROM stock_data_raw WHERE code = ?",
            [code],
        )
        conn.executemany(
            "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    session_date,
                    code,
                    company_name,
                    "0111",
                    "Prime",
                    None,
                    "3600",
                    "Machinery",
                )
                for session_date in valid_dates
            ],
        )
        conn.executemany(
            "INSERT INTO daily_valuation VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    code,
                    session_date,
                    session_date,
                    12.0 + security_index,
                    10.0 + security_index,
                    1.0 + security_index * 0.2,
                    8.0 + security_index,
                    7.0 + security_index,
                    market_cap,
                    market_cap * (0.55 + (security_index % 4) * 0.1),
                    valid_dates[-1],
                    f"fundamentals-{code}",
                )
                for session_date in valid_dates
            ],
        )
        conn.execute(
            "INSERT INTO stock_provider_windows (code, coverage_start, coverage_end, "
            "provider_plan, provider_as_of, source_fingerprint, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [code, valid_dates[0], valid_dates[-1], "premium", valid_dates[-1], "", "now"],
        )
        conn.execute(
            "INSERT INTO current_basis_fundamentals_state VALUES (?, ?, ?, 0, ?)",
            [code, valid_dates[-1], f"fundamentals-{code}", "now"],
        )
        _refresh_provider_window(conn, code=code, provider_as_of=valid_dates[-1])
    conn.execute(
        """
        CREATE TABLE index_master (
            code TEXT, name TEXT, name_english TEXT, category TEXT,
            data_start_date TEXT, created_at TEXT, updated_at TEXT
        )
        """
    )
    invalid_dates = tuple(
        session_date + timedelta(days=1) for session_date in valid_dates
    )
    conn.executemany(
        "INSERT INTO stock_data_raw VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "1111",
                session_date,
                0.0,
                0.0,
                0.0,
                0.0,
                10_000,
                0.0,
                1.0,
                0.0,
                0.0,
                0.0,
                0.0,
                10_000,
            )
            for session_date in invalid_dates
        ],
    )
    conn.executemany(
        "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("1111", session_date, 0.0, 0.0, 0.0, 0.0, 10_000)
            for session_date in invalid_dates
        ],
    )
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                session_date,
                "1111",
                "Alpha",
                "0111",
                "Prime",
                None,
                "3600",
                "Machinery",
            )
            for session_date in invalid_dates
        ],
    )
    _refresh_provider_window(
        conn,
        code="1111",
        provider_as_of=invalid_dates[-1],
    )
    conn.close()
    return db_path, valid_dates


def _add_sma5_atr_statement_columns(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("ALTER TABLE statements ADD COLUMN sales DOUBLE")
    conn.execute("ALTER TABLE statements ADD COLUMN type_of_document TEXT")


def _run_test_research(db_path: Path) -> RankingSma5AtrDeviationEvidenceResult:
    return run_ranking_sma5_atr_deviation_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(5, 20),
        threshold_abs_atr=(0.05, 0.1),
        market_scopes=("prime",),
        liquidity_bands=("high", "mid", "low"),
        min_observations=1,
        observation_sample_limit=100,
    )
