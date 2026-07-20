from __future__ import annotations

from pathlib import Path

import duckdb

from src.domains.analytics.ranking_sma5_position_state_evidence import (
    RankingSma5PositionStateEvidenceResult,
    build_summary_markdown,
    run_ranking_sma5_position_state_evidence_research,
    write_ranking_sma5_position_state_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_sma5_count_long_evidence import (
    _build_sma5_count_long_db,
)


def test_sma5_position_state_evidence_builds_position_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_sma5_position_state_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.position_day_count > 0
    assert result.trade_count > 0
    assert result.analysis_start_date == "2024-03-01"
    assert result.analysis_end_date == "2024-04-30"
    assert not result.coverage_diagnostics_df.empty
    assert not result.entry_rule_evidence_df.empty
    assert not result.position_state_daily_evidence_df.empty
    assert not result.position_state_trade_evidence_df.empty
    assert not result.exit_reason_evidence_df.empty
    assert not result.rotation_evidence_df.empty
    assert {
        "date",
        "code",
        "long_scaffold",
        "entry_rule",
        "exit_rule",
        "held_state",
        "entry_signal",
        "exit_signal",
        "next_session_excess_return_pct",
        "sma5_above_count_5d",
        "below_sma5_streak",
        "sma5_atr20_deviation",
    }.issubset(result.observation_sample_df.columns)
    assert {
        "combined_count_streak_atr",
        "count_0_1",
        "below_sma5_streak_ge3",
        "atr20_below_le_neg1",
    }.issubset(set(result.position_state_daily_evidence_df["exit_rule"].astype(str)))
    assert {
        "position_day_count",
        "median_daily_excess_return_pct",
        "cumulative_excess_return_pct",
        "date_level_ir",
        "severe_loss_day_rate_pct",
    }.issubset(result.position_state_daily_evidence_df.columns)
    assert {
        "trade_count",
        "median_trade_excess_return_pct",
        "median_holding_days",
        "win_trade_rate_pct",
    }.issubset(result.position_state_trade_evidence_df.columns)
    assert {
        "rotation_rule",
        "exit_event_count",
        "target_available_rate_pct",
        "median_source_next_session_excess_return_pct",
        "median_rotation_basket_excess_return_pct",
        "median_rotation_minus_source_pct",
        "rotation_outperform_rate_pct",
    }.issubset(result.rotation_evidence_df.columns)
    assert {
        "valid_same_scaffold_basket",
        "healthy_same_scaffold_basket",
    }.issubset(set(result.rotation_evidence_df["rotation_rule"].astype(str)))


def test_sma5_position_state_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_sma5_position_state_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking SMA5 Position State Evidence" in summary
    assert "Position State Daily Evidence" in summary
    assert "Position State Trade Evidence" in summary
    assert "Exit Reason Evidence" in summary
    assert "Rotation Evidence" in summary

    bundle = write_ranking_sma5_position_state_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(db_path: Path) -> RankingSma5PositionStateEvidenceResult:
    return run_ranking_sma5_position_state_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        market_scopes=("prime",),
        long_scaffolds=("all_market",),
        min_position_days=1,
        min_trades=1,
        observation_sample_limit=100,
    )


def _build_sma5_position_state_db(db_path: Path) -> Path:
    _build_sma5_count_long_db(db_path)
    conn = duckdb.connect(str(db_path))
    dates = [
        str(row[0])
        for row in conn.execute(
            "SELECT DISTINCT date FROM stock_data_raw ORDER BY date"
        ).fetchall()
    ]
    target_codes = ("1111", "2222", "3333", "4444", "5555", "6666")
    for date_index, date in enumerate(dates):
        for code_index, code in enumerate(target_codes):
            wave = (date_index + code_index) % 18
            pullback = wave in {8, 9, 10, 11}
            base = 110.0 + code_index * 15.0 + date_index * (0.18 + code_index * 0.01)
            close = base - (wave - 7) * 3.0 if pullback else base + wave * 0.35
            high = close + 2.0
            low = close - 2.0
            open_price = close - 0.4
            conn.execute(
                """
                UPDATE stock_data_raw
                SET open = ?, high = ?, low = ?, close = ?
                WHERE code = ? AND date = ?
                """,
                [open_price, high, low, close, code, date],
            )
    conn.execute(
        "UPDATE stock_data SET open = 1, high = 1, low = 1, close = 1, volume = 0"
    )
    conn.close()
    return db_path
