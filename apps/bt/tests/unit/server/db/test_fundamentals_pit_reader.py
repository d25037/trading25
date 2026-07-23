from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from src.application.contracts.fundamentals_pit import FundamentalsPitSnapshotError
from src.infrastructure.data_access import clients
from src.infrastructure.data_access.clients import DirectMarketClient, DirectMarketDataClient
from src.infrastructure.db.market.market_reader import MarketDbReader
from tests.unit.server.db.market_writer_test_support import open_market_db


@pytest.fixture
def v5_market(tmp_path: Path) -> Path:
    path = tmp_path / "market.duckdb"
    db = open_market_db(str(path))
    db.close()
    conn = duckdb.connect(str(path))
    try:
        conn.executemany(
            "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)",
            [
                ("2024-06-27", 100.0, 101.0, 99.0, 100.5, None),
                ("2024-06-28", 100.5, 102.0, 100.0, 101.0, None),
            ],
        )
        conn.executemany(
            """
            INSERT INTO stock_master_daily (
                date, code, company_name, company_name_english, market_code,
                market_name, sector_17_code, sector_17_name, sector_33_code,
                sector_33_name, scale_category, listed_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    day,
                    code,
                    name,
                    None,
                    "0111",
                    "Prime",
                    "6",
                    "Auto",
                    "3700",
                    "Transport",
                    None,
                    "1949-05-16",
                )
                for day in ("2024-06-27", "2024-06-28")
                for code, name in (("72030", "Toyota"), ("6758", "Sony"))
            ],
        )

        price_rows: list[tuple[object, ...]] = []
        raw_rows: list[tuple[object, ...]] = []
        for code, close in (("7203", 200.0), ("6758", 100.0)):
            for day_index in range(60):
                day = (date(2024, 4, 1) + timedelta(days=day_index)).isoformat()
                price_rows.append(
                    (code, day, close, close, close, close, 1_000 + day_index, 1.0, None)
                )
                raw_rows.append(
                    (
                        code,
                        day,
                        close * 10,
                        close * 10,
                        close * 10,
                        close * 10,
                        100 + day_index,
                        None,
                        0.1,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    )
                )
            price_rows.extend(
                [
                    (code, "2024-06-27", close, close, close, close, 2_000, 1.0, None),
                    (code, "2024-06-28", close, close, close, close, 2_100, 1.0, None),
                    (code, "2024-07-01", 9_999.0, 9_999.0, 9_999.0, 9_999.0, 1, 1.0, None),
                ]
            )
        # A provider alias exists on the same date; the normalized 4-digit row wins.
        price_rows.append(
            ("72030", "2024-06-28", 777.0, 777.0, 777.0, 777.0, 7, 1.0, None)
        )
        conn.executemany(
            """
            INSERT INTO stock_data (
                code, date, open, high, low, close, volume, adjustment_factor, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            price_rows,
        )
        conn.executemany(
            """
            INSERT INTO stock_data_raw (
                code, date, open, high, low, close, volume, turnover_value,
                adjustment_factor, adjusted_open, adjusted_high, adjusted_low,
                adjusted_close, adjusted_volume, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            raw_rows,
        )
        conn.executemany(
            """
            INSERT INTO stock_provider_windows (
                code, coverage_start, coverage_end, provider_plan, provider_as_of,
                source_fingerprint, updated_at
            ) VALUES (?, '2024-04-01', '2024-07-01', 'premium', ?, ?, '2024-07-01T17:00:00+09:00')
            """,
            [
                ("7203", "2024-07-01", "provider-7203"),
                ("6758", "2024-07-01", "provider-6758"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO statements (
                code, statement_id, disclosed_date, disclosed_at,
                period_start, period_end, type_of_document,
                type_of_current_period, earnings_per_share, bps, forecast_eps,
                shares_outstanding, treasury_shares
            ) VALUES (?, ?, ?, ?, ?, ?, 'FinancialStatements', 'FY', ?, ?, ?, ?, ?)
            """,
            [
                (
                    "72030",
                    "toyota-fy-2024",
                    "2024-05-10",
                    "2024-05-10T15:00:00+09:00",
                    "2023-04-01",
                    "2024-03-31",
                    50.0,
                    100.0,
                    60.0,
                    100_000_000.0,
                    10_000_000.0,
                ),
                (
                    "7203",
                    "toyota-revision-2024",
                    "2024-06-20",
                    "2024-06-20T15:00:00+09:00",
                    "2024-04-01",
                    "2025-03-31",
                    None,
                    None,
                    70.0,
                    100_000_000.0,
                    10_000_000.0,
                ),
                (
                    "7203",
                    "toyota-future",
                    "2024-07-01",
                    "2024-07-01T15:00:00+09:00",
                    "2024-04-01",
                    "2025-03-31",
                    999.0,
                    999.0,
                    999.0,
                    100_000_000.0,
                    10_000_000.0,
                ),
                (
                    "6758",
                    "sony-fy-2024",
                    "2024-05-09",
                    "2024-05-09T15:00:00+09:00",
                    "2023-04-01",
                    "2024-03-31",
                    25.0,
                    50.0,
                    30.0,
                    80_000_000.0,
                    5_000_000.0,
                ),
            ],
        )
        conn.execute(
            """
            UPDATE statements
            SET type_of_document = 'EarnForecastRevision'
            WHERE statement_id = 'toyota-revision-2024'
            """
        )
        conn.executemany(
            """
            INSERT INTO statement_metrics_adjusted (
                code, statement_id, disclosed_date, disclosed_at, period_end,
                period_type, fundamentals_adjustment_basis_date, raw_eps,
                adjusted_eps, raw_bps, adjusted_bps, raw_forecast_eps,
                adjusted_forecast_eps, raw_shares_outstanding,
                adjusted_shares_outstanding, raw_treasury_shares,
                adjusted_treasury_shares, adjustment_factor_cumulative,
                source_fingerprint, created_at
            ) VALUES (?, ?, ?, ?, ?, 'FY', '2024-07-01', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1.0, ?, NULL)
            """,
            [
                (
                    "7203",
                    "toyota-fy-2024",
                    "2024-05-10",
                    "2024-05-10T15:00:00+09:00",
                    "2024-03-31",
                    50.0,
                    50.0,
                    100.0,
                    100.0,
                    60.0,
                    60.0,
                    100_000_000.0,
                    100_000_000.0,
                    10_000_000.0,
                    10_000_000.0,
                    "current-toyota",
                ),
                (
                    "7203",
                    "toyota-revision-2024",
                    "2024-06-20",
                    "2024-06-20T15:00:00+09:00",
                    "2025-03-31",
                    None,
                    None,
                    None,
                    None,
                    70.0,
                    70.0,
                    100_000_000.0,
                    100_000_000.0,
                    10_000_000.0,
                    10_000_000.0,
                    "current-toyota",
                ),
                (
                    "7203",
                    "toyota-future",
                    "2024-07-01",
                    "2024-07-01T15:00:00+09:00",
                    "2025-03-31",
                    999.0,
                    999.0,
                    999.0,
                    999.0,
                    999.0,
                    999.0,
                    100_000_000.0,
                    100_000_000.0,
                    10_000_000.0,
                    10_000_000.0,
                    "current-toyota",
                ),
                (
                    "6758",
                    "sony-fy-2024",
                    "2024-05-09",
                    "2024-05-09T15:00:00+09:00",
                    "2024-03-31",
                    25.0,
                    25.0,
                    50.0,
                    50.0,
                    30.0,
                    30.0,
                    80_000_000.0,
                    80_000_000.0,
                    5_000_000.0,
                    5_000_000.0,
                    "current-sony",
                ),
            ],
        )
        conn.executemany(
            """
            INSERT INTO current_basis_fundamentals_state (
                code, fundamentals_adjustment_basis_date, source_fingerprint,
                statement_count, materialized_at
            ) VALUES (?, '2024-07-01', ?, ?, '2024-07-01T17:00:00+09:00')
            """,
            [
                ("7203", "current-toyota", 3),
                ("6758", "current-sony", 1),
            ],
        )
    finally:
        conn.close()
    db = open_market_db(str(path))
    try:
        db.materialize_daily_valuation(full_rebuild=True)
    finally:
        db.close()
    return path


def _reader_client(monkeypatch: pytest.MonkeyPatch, path: Path) -> DirectMarketClient:
    reader = MarketDbReader(str(path))
    monkeypatch.setattr(clients, "_resolve_market_reader", lambda snapshot_id=None: reader)
    return DirectMarketClient()


def _update(path: Path, sql: str) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(sql)
    finally:
        conn.close()


def _materialize(path: Path) -> None:
    db = open_market_db(str(path))
    try:
        db.materialize_daily_valuation(full_rebuild=True)
    finally:
        db.close()


def test_snapshot_resolves_weekend_with_provider_metadata_and_exact_master(
    monkeypatch: pytest.MonkeyPatch, v5_market: Path
) -> None:
    snapshot = _reader_client(monkeypatch, v5_market).get_fundamentals_pit_snapshot(
        "72030", date(2024, 6, 30)
    )

    assert snapshot.requested_cutoff_date == date(2024, 6, 30)
    assert snapshot.knowledge_cutoff_date == date(2024, 6, 30)
    assert snapshot.effective_market_date == date(2024, 6, 28)
    assert snapshot.stock_master_snapshot_date == date(2024, 6, 28)
    assert snapshot.fundamentals_adjustment_basis_date == date(2024, 7, 1)
    assert snapshot.provider_as_of == "2024-07-01"
    assert snapshot.provider_coverage_start == date(2024, 4, 1)
    assert snapshot.provider_coverage_end == date(2024, 7, 1)
    assert snapshot.stock_info.code == "72030"
    assert {row["statement_id"] for row in snapshot.adjusted_statement_metrics} == {
        "toyota-fy-2024",
        "toyota-revision-2024",
    }
    assert set(snapshot.prime_liquidity_panel["code"]) == {"6758", "7203"}
    assert "basis_id" not in snapshot.prime_liquidity_panel.columns


def test_snapshot_daily_valuation_equals_canonical_asof_relation(
    monkeypatch: pytest.MonkeyPatch, v5_market: Path
) -> None:
    snapshot = _reader_client(monkeypatch, v5_market).get_fundamentals_pit_snapshot(
        "7203", date(2024, 6, 30)
    )
    conn = duckdb.connect(str(v5_market), read_only=True)
    try:
        expected = conn.execute(
            """
            SELECT date, close, eps, forward_eps, per, forward_per, statement_id
            FROM daily_valuation
            WHERE code = '7203' AND date <= '2024-06-28'
            ORDER BY date
            """
        ).fetchall()
    finally:
        conn.close()

    actual = [
        (
            row["date"],
            row["close"],
            row["eps"],
            row["forward_eps"],
            row["per"],
            row["forward_per"],
            row["statement_id"],
        )
        for row in snapshot.daily_valuation
    ]
    assert actual == expected


def test_snapshot_uses_provider_adjusted_price_and_four_digit_dedupe(
    monkeypatch: pytest.MonkeyPatch, v5_market: Path
) -> None:
    snapshot = _reader_client(monkeypatch, v5_market).get_fundamentals_pit_snapshot(
        "72030", date(2024, 6, 30)
    )

    assert snapshot.ohlcv.loc["2024-06-28", "Close"] == 200.0
    assert snapshot.ohlcv.loc["2024-06-28", "Close"] not in {777.0, 2_000.0}
    assert snapshot.daily_valuation[-1]["close"] == 200.0


def test_snapshot_never_leaks_future_statement_or_adjusted_metric(
    monkeypatch: pytest.MonkeyPatch, v5_market: Path
) -> None:
    snapshot = _reader_client(monkeypatch, v5_market).get_fundamentals_pit_snapshot(
        "7203", date(2024, 6, 30)
    )

    assert snapshot.statements.index.max().date() == date(2024, 6, 20)
    assert {row["statement_id"] for row in snapshot.adjusted_statement_metrics} == {
        "toyota-fy-2024",
        "toyota-revision-2024",
    }
    assert all(row["statement_id"] != "toyota-future" for row in snapshot.daily_valuation)
    assert all(row["forward_eps"] != 999.0 for row in snapshot.daily_valuation)


def test_snapshot_accepts_suspended_symbol_and_uses_latest_prior_observation(
    monkeypatch: pytest.MonkeyPatch, v5_market: Path
) -> None:
    _update(
        v5_market,
        "DELETE FROM stock_data WHERE code IN ('7203', '72030') AND date = '2024-06-28'",
    )
    _materialize(v5_market)
    snapshot = _reader_client(monkeypatch, v5_market).get_fundamentals_pit_snapshot(
        "7203", date(2024, 6, 30)
    )

    assert snapshot.effective_market_date == date(2024, 6, 28)
    assert snapshot.ohlcv.index.max().date() == date(2024, 6, 27)
    assert snapshot.daily_valuation[-1]["date"] == "2024-06-27"


def test_snapshot_accepts_suspended_prime_peer_without_exact_price(
    monkeypatch: pytest.MonkeyPatch, v5_market: Path
) -> None:
    _update(v5_market, "DELETE FROM stock_data WHERE code = '6758' AND date = '2024-06-28'")
    snapshot = _reader_client(monkeypatch, v5_market).get_fundamentals_pit_snapshot(
        "7203", date(2024, 6, 30)
    )

    assert "6758" not in set(snapshot.prime_liquidity_panel["code"])


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ("DELETE FROM stock_provider_windows WHERE code = '7203'", "provider_window_required"),
        (
            "UPDATE stock_provider_windows SET provider_plan = '' WHERE code = '7203'",
            "provider_window_required",
        ),
        (
            "INSERT INTO current_basis_recompute_pending VALUES ('7203', 'provider_refresh', 'pending-fp', '2024-07-01')",
            "current_adjusted_metrics_required",
        ),
        (
            "INSERT INTO current_basis_recompute_pending VALUES ('72030', 'provider_refresh', 'pending-fp', '2024-07-01')",
            "current_adjusted_metrics_required",
        ),
        (
            "DELETE FROM statement_metrics_adjusted WHERE statement_id = 'toyota-revision-2024'",
            "current_adjusted_metrics_required",
        ),
        (
            "UPDATE statement_metrics_adjusted SET fundamentals_adjustment_basis_date = '2024-06-30' WHERE code = '7203'",
            "current_adjusted_metrics_required",
        ),
        (
            "DELETE FROM current_basis_fundamentals_state WHERE code = '7203'",
            "current_adjusted_metrics_required",
        ),
        (
            "UPDATE current_basis_fundamentals_state SET fundamentals_adjustment_basis_date = '2024-06-30' WHERE code = '7203'",
            "current_adjusted_metrics_required",
        ),
        (
            "UPDATE current_basis_fundamentals_state SET source_fingerprint = 'stale' WHERE code = '7203'",
            "current_adjusted_metrics_required",
        ),
        (
            "UPDATE current_basis_fundamentals_state SET statement_count = 2 WHERE code = '7203'",
            "current_adjusted_metrics_required",
        ),
        (
            "UPDATE current_basis_fundamentals_state SET materialized_at = '' WHERE code = '7203'",
            "current_adjusted_metrics_required",
        ),
        (
            "DELETE FROM statement_metrics_adjusted WHERE statement_id = 'toyota-future'",
            "current_adjusted_metrics_required",
        ),
        (
            "DELETE FROM stock_master_daily WHERE date = '2024-06-28'",
            "stock_master_snapshot_required",
        ),
        (
            "DELETE FROM stock_master_daily WHERE date = '2024-06-28' AND code = '72030'",
            "stock_not_listed_as_of",
        ),
    ],
)
def test_snapshot_fails_closed_with_v5_recovery_reason(
    monkeypatch: pytest.MonkeyPatch,
    v5_market: Path,
    mutation: str,
    reason: str,
) -> None:
    _update(v5_market, mutation)

    with pytest.raises(FundamentalsPitSnapshotError) as exc_info:
        _reader_client(monkeypatch, v5_market).get_fundamentals_pit_snapshot(
            "7203", date(2024, 6, 30)
        )
    assert exc_info.value.reason == reason
    assert "market_db_sync" in str(exc_info.value) or reason in {
        "stock_master_snapshot_required",
        "stock_not_listed_as_of",
    }


def test_snapshot_requires_state_when_raw_and_metric_relations_are_both_empty(
    monkeypatch: pytest.MonkeyPatch,
    v5_market: Path,
) -> None:
    _update(v5_market, "DELETE FROM statements WHERE code IN ('7203', '72030')")
    _update(v5_market, "DELETE FROM statement_metrics_adjusted WHERE code = '7203'")
    _update(v5_market, "DELETE FROM current_basis_fundamentals_state WHERE code = '7203'")

    with pytest.raises(FundamentalsPitSnapshotError) as exc_info:
        _reader_client(monkeypatch, v5_market).get_fundamentals_pit_snapshot(
            "7203", date(2024, 6, 30)
        )

    assert exc_info.value.reason == "current_adjusted_metrics_required"
    assert "market_db_sync" in str(exc_info.value)


def test_snapshot_fails_closed_when_prime_peer_provider_window_is_missing(
    monkeypatch: pytest.MonkeyPatch, v5_market: Path
) -> None:
    _update(v5_market, "DELETE FROM stock_provider_windows WHERE code = '6758'")

    with pytest.raises(FundamentalsPitSnapshotError) as exc_info:
        _reader_client(monkeypatch, v5_market).get_fundamentals_pit_snapshot(
            "7203", date(2024, 6, 30)
        )
    assert exc_info.value.reason == "provider_window_required"


def test_snapshot_fails_closed_when_prime_peers_have_mixed_provider_plans(
    monkeypatch: pytest.MonkeyPatch, v5_market: Path
) -> None:
    _update(v5_market, "UPDATE stock_provider_windows SET provider_plan = 'free' WHERE code = '6758'")

    with pytest.raises(FundamentalsPitSnapshotError) as exc_info:
        _reader_client(monkeypatch, v5_market).get_fundamentals_pit_snapshot(
            "7203", date(2024, 6, 30)
        )
    assert exc_info.value.reason == "provider_window_required"


def test_snapshot_without_cutoff_uses_current_global_market_frontier(
    monkeypatch: pytest.MonkeyPatch, v5_market: Path
) -> None:
    snapshot = _reader_client(monkeypatch, v5_market).get_fundamentals_pit_snapshot(
        "7203", None
    )
    assert snapshot.requested_cutoff_date is None
    assert snapshot.knowledge_cutoff_date == date(2024, 6, 28)
    assert snapshot.effective_market_date == date(2024, 6, 28)


def test_market_data_client_exposes_only_snapshot_delegate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = object()
    adapter = DirectMarketDataClient("quickTesting")
    monkeypatch.setattr(
        adapter._market,
        "get_fundamentals_pit_snapshot",
        lambda symbol, cutoff: sentinel,
    )
    assert adapter.get_fundamentals_pit_snapshot("7203", date(2024, 6, 30)) is sentinel
