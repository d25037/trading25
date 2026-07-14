from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from src.application.contracts.fundamentals_pit import FundamentalsPitSnapshotError
from src.infrastructure.data_access import clients
from src.infrastructure.data_access.clients import (
    DirectMarketClient,
    DirectMarketDataClient,
)
from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.market_reader import MarketDbReader


_BASIS_COLUMNS = """
    code, basis_id, valid_from, valid_to_exclusive,
    adjustment_through_date, source_fingerprint,
    materialized_through_date, status, created_at, updated_at
"""


@pytest.fixture
def v4_market(tmp_path: Path) -> Path:
    path = tmp_path / "market.duckdb"
    db = MarketDb(str(path))
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
        raw_rows: list[tuple[object, ...]] = []
        for code, close in (("72030", 200.0), ("6758", 100.0)):
            for day_index in range(1, 61):
                day = (date(2024, 4, 1) + timedelta(days=day_index - 1)).isoformat()
                raw_rows.append(
                    (code, day, close, close, close, close, 1000 + day_index, 1.0, None)
                )
            raw_rows.extend(
                [
                    (code, "2024-06-27", close, close, close, close, 2000, 1.0, None),
                    (code, "2024-06-28", close, close, close, close, 2100, 1.0, None),
                    (code, "2024-07-01", 9999.0, 9999.0, 9999.0, 9999.0, 1, 1.0, None),
                ]
            )
        conn.executemany(
            "INSERT INTO stock_data_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", raw_rows
        )
        conn.executemany(
            f"INSERT INTO stock_adjustment_bases ({_BASIS_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    code,
                    f"event-pit-v1:{code}:2024-06-28",
                    "2024-06-28",
                    None,
                    "2024-06-28",
                    f"fp-{code}",
                    "2024-06-28",
                    "ready",
                    None,
                    None,
                )
                for code in ("7203", "6758")
            ],
        )
        conn.executemany(
            "INSERT INTO stock_adjustment_basis_segments VALUES (?, ?, ?, ?, ?)",
            [
                (code, f"event-pit-v1:{code}:2024-06-28", "2024-04-01", None, 1.0)
                for code in ("7203", "6758")
            ],
        )
        conn.executemany(
            """
            INSERT INTO statements (
                code, disclosed_date, type_of_document, type_of_current_period,
                earnings_per_share, forecast_eps
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                ("72030", "2024-05-10", "FinancialStatements", "FY", 50.0, 60.0),
                ("7203", "2024-06-20", "FinancialStatements", "FY", None, 70.0),
                ("7203", "2024-07-01", "FinancialStatements", "FY", 999.0, 999.0),
            ],
        )
        conn.execute(
            """
            INSERT INTO statement_metrics_adjusted (
                code, disclosed_date, period_end, period_type, price_basis_date,
                adjusted_eps, adjusted_forecast_eps, basis_version
            ) VALUES
                ('7203', '2024-05-10', '2024-03-31', 'FY', '2024-06-28', 50, 60, 'event-pit-v1:7203:2024-06-28'),
                ('7203', '2024-06-20', '2025-03-31', 'FY', '2024-06-28', NULL, 70, 'event-pit-v1:7203:2024-06-28')
            """
        )
        conn.execute(
            """
            INSERT INTO daily_valuation (
                code, date, price_basis_date, close, eps, forward_eps,
                free_float_market_cap, statement_disclosed_date,
                forward_eps_disclosed_date, basis_version
            )
            SELECT normalized_code, date, '2024-06-28', close,
                   CASE WHEN normalized_code = '7203' AND date >= '2024-05-10' THEN 50 END,
                   CASE WHEN normalized_code = '7203' AND date >= '2024-05-10' THEN 60 END,
                   CASE WHEN normalized_code = '7203' THEN 10000000000.0 ELSE 8000000000.0 END,
                   CASE WHEN normalized_code = '7203' AND date >= '2024-05-10' THEN '2024-05-10' END,
                   CASE WHEN normalized_code = '7203' AND date >= '2024-05-10' THEN '2024-05-10' END,
                   'event-pit-v1:' || normalized_code || ':2024-06-28'
            FROM (
                SELECT CASE WHEN length(code) = 5 AND right(code, 1) = '0'
                            THEN left(code, 4) ELSE code END AS normalized_code,
                       date, close,
                       ROW_NUMBER() OVER (
                           PARTITION BY CASE WHEN length(code) = 5 AND right(code, 1) = '0'
                                             THEN left(code, 4) ELSE code END, date
                           ORDER BY length(code)
                       ) AS rn
                FROM stock_data_raw
                WHERE date <= '2024-06-28'
            ) AS raw
            WHERE rn = 1
            """,
        )
    finally:
        conn.close()
    return path


def _reader_client(monkeypatch: pytest.MonkeyPatch, path: Path) -> DirectMarketClient:
    reader = MarketDbReader(str(path))
    monkeypatch.setattr(
        clients, "_resolve_market_reader", lambda snapshot_id=None: reader
    )
    return DirectMarketClient()


def _update(path: Path, sql: str) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(sql)
    finally:
        conn.close()


def _add_large_prime_universe(path: Path, size: int = 30) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            f"""
            INSERT INTO stock_master_daily (
                date, code, company_name, market_code, market_name,
                sector_17_code, sector_17_name, sector_33_code,
                sector_33_name, listed_date
            )
            SELECT '2024-06-28', CAST(code AS VARCHAR), 'Prime ' || code,
                   '0111', 'Prime', '6', 'Auto', '3700', 'Transport', '2000-01-01'
            FROM range(8000, {8000 + size}) AS codes(code)
            """
        )
        conn.execute(
            f"""
            INSERT INTO stock_data_raw
            SELECT CAST(code AS VARCHAR),
                   strftime(DATE '2024-04-01' + day_index * INTERVAL 1 DAY, '%Y-%m-%d'),
                   code, code, code, code, 1000 + day_index, 1.0, NULL
            FROM range(8000, {8000 + size}) AS codes(code)
            CROSS JOIN range(0, 60) AS days(day_index)
            """
        )
        conn.execute(
            f"""
            INSERT INTO stock_data_raw
            SELECT CAST(code AS VARCHAR), '2024-06-28',
                   code, code, code, code, 2000, 1.0, NULL
            FROM range(8000, {8000 + size}) AS codes(code)
            """
        )
        conn.execute(
            f"""
            INSERT INTO stock_adjustment_bases
            SELECT CAST(code AS VARCHAR),
                   'event-pit-v1:' || code || ':2024-06-28',
                   '2024-06-28', NULL, '2024-06-28', 'fp-' || code,
                   '2024-06-28', 'ready', NULL, NULL
            FROM range(8000, {8000 + size}) AS codes(code)
            """
        )
        conn.execute(
            f"""
            INSERT INTO stock_adjustment_basis_segments
            SELECT CAST(code AS VARCHAR),
                   'event-pit-v1:' || code || ':2024-06-28',
                   '2024-04-01', NULL, 1.0
            FROM range(8000, {8000 + size}) AS codes(code)
            """
        )
        conn.execute(
            f"""
            INSERT INTO daily_valuation (
                code, date, price_basis_date, close, free_float_market_cap,
                basis_version
            )
            SELECT CAST(code AS VARCHAR), '2024-06-28', '2024-06-28', code,
                   code * 1000000,
                   'event-pit-v1:' || code || ':2024-06-28'
            FROM range(8000, {8000 + size}) AS codes(code)
            """
        )
        conn.execute(
            """
            UPDATE stock_adjustment_bases
            SET valid_to_exclusive = '2024-07-01'
            WHERE code = '8000'
            """
        )
        conn.execute(
            """
            INSERT INTO stock_adjustment_bases VALUES (
                '8000', 'event-pit-v1:8000:2024-07-01', '2024-07-01', NULL,
                '2024-07-01', 'fp-future', '2024-07-01', 'ready', NULL, NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO stock_adjustment_basis_segments VALUES (
                '8000', 'event-pit-v1:8000:2024-07-01',
                '2024-04-01', NULL, 0.01
            )
            """
        )
        conn.execute(
            """
            INSERT INTO stock_data_raw VALUES (
                '8000', '2024-07-01', 999999, 999999, 999999, 999999,
                1, 0.01, NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO stock_data VALUES (
                '8000', '2024-06-28', 999999, 999999, 999999, 999999,
                1, 1.0, NULL
            )
            """
        )
    finally:
        conn.close()


def _add_sparse_prime_history(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            INSERT INTO stock_master_daily (
                date, code, company_name, market_code, market_name,
                sector_17_code, sector_17_name, sector_33_code,
                sector_33_name, listed_date
            ) VALUES (
                '2024-06-28', '9000', 'Sparse Prime', '0111', 'Prime',
                '6', 'Auto', '3700', 'Transport', '2000-01-01'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO stock_data_raw
            SELECT '9000',
                   strftime(DATE '2024-06-28' - (59 - observation) * INTERVAL 10 DAY, '%Y-%m-%d'),
                   10, 10, 10, 10, observation + 1, 1.0, NULL
            FROM range(0, 60) AS observations(observation)
            """
        )
        conn.execute(
            """
            INSERT INTO stock_data_raw VALUES (
                '9000', '2024-07-01', 999999, 999999, 999999, 999999,
                1, 0.5, NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO stock_adjustment_bases VALUES (
                '9000', 'event-pit-v1:9000:2022-01-01', '2022-01-01', NULL,
                '2022-01-01', 'fp-sparse', '2024-06-28', 'ready', NULL, NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO stock_adjustment_basis_segments VALUES (
                '9000', 'event-pit-v1:9000:2022-01-01',
                '2022-01-01', NULL, 1.0
            )
            """
        )
        conn.execute(
            """
            INSERT INTO daily_valuation (
                code, date, price_basis_date, close, free_float_market_cap,
                basis_version
            ) VALUES (
                '9000', '2024-06-28', '2022-01-01', 10, 9000000000,
                'event-pit-v1:9000:2022-01-01'
            )
            """
        )
    finally:
        conn.close()


def test_snapshot_resolves_weekend_to_one_basis_and_exact_master(
    monkeypatch: pytest.MonkeyPatch, v4_market: Path
) -> None:
    snapshot = _reader_client(monkeypatch, v4_market).get_fundamentals_pit_snapshot(
        "7203", date.fromisoformat("2024-06-30")
    )

    assert snapshot.requested_cutoff_date == date(2024, 6, 30)
    assert snapshot.knowledge_cutoff_date == date(2024, 6, 30)
    assert snapshot.effective_market_date == date(2024, 6, 28)
    assert snapshot.basis_id == "event-pit-v1:7203:2024-06-28"
    assert snapshot.stock_info.code[:4] == "7203"
    assert snapshot.stock_master_snapshot_date == date(2024, 6, 28)
    assert snapshot.statements.index.max().date() == date(2024, 6, 20)
    assert set(snapshot.statements["forecastEps"].dropna()) == {60.0, 70.0}
    assert {row["period_end"] for row in snapshot.adjusted_statement_metrics} == {
        "2024-03-31",
        "2025-03-31",
    }
    assert snapshot.ohlcv.index.max().date() == date(2024, 6, 28)
    assert set(snapshot.prime_liquidity_panel["code"]) == {"6758", "7203"}
    assert set(snapshot.prime_liquidity_panel["basis_id"]) == {
        "event-pit-v1:6758:2024-06-28",
        "event-pit-v1:7203:2024-06-28",
    }
    assert set(snapshot.prime_liquidity_panel["stock_master_snapshot_date"]) == {
        "2024-06-28"
    }


def test_snapshot_rejects_future_nested_provenance(
    monkeypatch: pytest.MonkeyPatch, v4_market: Path
) -> None:
    _update(
        v4_market,
        "UPDATE daily_valuation SET forward_eps_disclosed_date = '2024-07-01' WHERE code = '7203'",
    )

    with pytest.raises(FundamentalsPitSnapshotError) as exc_info:
        _reader_client(monkeypatch, v4_market).get_fundamentals_pit_snapshot(
            "7203", date(2024, 6, 30)
        )
    assert exc_info.value.reason == "pit_snapshot_inconsistent"


def test_snapshot_rejects_future_prime_peer_nested_provenance(
    monkeypatch: pytest.MonkeyPatch, v4_market: Path
) -> None:
    _update(
        v4_market,
        "UPDATE daily_valuation SET forward_sales_disclosed_date = '2024-07-01' "
        "WHERE code = '6758' AND date = '2024-06-28'",
    )

    with pytest.raises(FundamentalsPitSnapshotError) as exc_info:
        _reader_client(monkeypatch, v4_market).get_fundamentals_pit_snapshot(
            "7203", date(2024, 6, 30)
        )
    assert exc_info.value.reason == "pit_snapshot_inconsistent"


def test_snapshot_rejects_prime_peer_provenance_without_source_disclosure(
    monkeypatch: pytest.MonkeyPatch, v4_market: Path
) -> None:
    _update(
        v4_market,
        "UPDATE daily_valuation SET statement_disclosed_date = '2024-05-10' "
        "WHERE code = '6758' AND date = '2024-06-28'",
    )

    with pytest.raises(FundamentalsPitSnapshotError) as exc_info:
        _reader_client(monkeypatch, v4_market).get_fundamentals_pit_snapshot(
            "7203", date(2024, 6, 30)
        )
    assert exc_info.value.reason == "pit_snapshot_inconsistent"


def test_snapshot_accepts_suspended_requested_symbol_with_global_basis_coverage(
    monkeypatch: pytest.MonkeyPatch, v4_market: Path
) -> None:
    _update(v4_market, "DELETE FROM stock_data_raw WHERE code = '72030' AND date = '2024-06-28'")
    _update(v4_market, "DELETE FROM daily_valuation WHERE code = '7203' AND date = '2024-06-28'")

    snapshot = _reader_client(monkeypatch, v4_market).get_fundamentals_pit_snapshot(
        "7203", date(2024, 6, 30)
    )

    assert snapshot.effective_market_date == date(2024, 6, 28)
    assert snapshot.materialized_through_date == date(2024, 6, 28)
    assert snapshot.ohlcv.index.max().date() == date(2024, 6, 27)


def test_snapshot_accepts_suspended_prime_peer_without_exact_price_observation(
    monkeypatch: pytest.MonkeyPatch, v4_market: Path
) -> None:
    _update(v4_market, "DELETE FROM stock_data_raw WHERE code = '6758' AND date = '2024-06-28'")
    _update(v4_market, "DELETE FROM daily_valuation WHERE code = '6758' AND date = '2024-06-28'")

    snapshot = _reader_client(monkeypatch, v4_market).get_fundamentals_pit_snapshot(
        "7203", date(2024, 6, 30)
    )

    assert "6758" not in set(snapshot.prime_liquidity_panel["code"])


@pytest.mark.parametrize(
    "mutation",
    [
        "DELETE FROM statement_metrics_adjusted WHERE code = '7203' AND disclosed_date = '2024-06-20'",
        "DELETE FROM daily_valuation WHERE code = '7203' AND date = '2024-06-27'",
        "DELETE FROM daily_valuation WHERE code = '7203'",
    ],
)
def test_snapshot_rejects_incomplete_adjusted_metric_or_valuation_coverage(
    monkeypatch: pytest.MonkeyPatch, v4_market: Path, mutation: str
) -> None:
    _update(v4_market, mutation)

    with pytest.raises(FundamentalsPitSnapshotError) as exc_info:
        _reader_client(monkeypatch, v4_market).get_fundamentals_pit_snapshot(
            "7203", date(2024, 6, 30)
        )
    assert exc_info.value.reason == "pit_snapshot_inconsistent"


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (
            "DELETE FROM stock_adjustment_bases WHERE code = '7203'",
            "historical_adjustment_basis_required",
        ),
        (
            "UPDATE stock_adjustment_bases SET materialized_through_date = '2024-06-27' WHERE code = '7203'",
            "historical_adjustment_basis_required",
        ),
        (
            "DELETE FROM stock_master_daily WHERE date = '2024-06-28'",
            "stock_master_snapshot_required",
        ),
        (
            "DELETE FROM stock_master_daily WHERE date = '2024-06-28' AND code = '72030'",
            "stock_not_listed_as_of",
        ),
        (
            "UPDATE statement_metrics_adjusted SET basis_version = 'other' WHERE code = '7203'",
            "pit_snapshot_inconsistent",
        ),
        (
            "UPDATE stock_adjustment_bases SET source_fingerprint = '' WHERE code = '7203'",
            "pit_snapshot_inconsistent",
        ),
        (
            "UPDATE daily_valuation SET statement_disclosed_date = '2024-04-01' WHERE code = '7203'",
            "pit_snapshot_inconsistent",
        ),
    ],
)
def test_snapshot_fails_closed_with_typed_reason(
    monkeypatch: pytest.MonkeyPatch,
    v4_market: Path,
    mutation: str,
    reason: str,
) -> None:
    _update(v4_market, mutation)

    with pytest.raises(FundamentalsPitSnapshotError) as exc_info:
        _reader_client(monkeypatch, v4_market).get_fundamentals_pit_snapshot(
            "7203", date(2024, 6, 30)
        )
    assert exc_info.value.reason == reason


def test_snapshot_excludes_future_valuation_and_ohlcv_sentinels(
    monkeypatch: pytest.MonkeyPatch, v4_market: Path
) -> None:
    _update(
        v4_market,
        """
        INSERT INTO daily_valuation (
            code, date, price_basis_date, close, basis_version
        ) VALUES (
            '7203', '2024-07-01', '2024-06-28', 9999,
            'event-pit-v1:7203:2024-06-28'
        )
        """,
    )

    snapshot = _reader_client(monkeypatch, v4_market).get_fundamentals_pit_snapshot(
        "7203", date(2024, 6, 30)
    )
    assert max(row["date"] for row in snapshot.daily_valuation) == "2024-06-28"
    assert snapshot.ohlcv.index.max().date() == date(2024, 6, 28)
    assert 9999.0 not in snapshot.ohlcv["Close"].to_list()


def test_snapshot_without_cutoff_uses_current_global_market_frontier(
    monkeypatch: pytest.MonkeyPatch, v4_market: Path
) -> None:
    snapshot = _reader_client(monkeypatch, v4_market).get_fundamentals_pit_snapshot(
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


def test_prime_panel_query_count_is_constant_for_large_universe(
    monkeypatch: pytest.MonkeyPatch, v4_market: Path
) -> None:
    _add_large_prime_universe(v4_market)
    reader = MarketDbReader(str(v4_market))
    query_count = 0
    original_query = reader.query
    original_query_one = reader.query_one

    def counted_query(sql: str, params: tuple[object, ...] = ()):
        nonlocal query_count
        query_count += 1
        return original_query(sql, params)

    def counted_query_one(sql: str, params: tuple[object, ...] = ()):
        nonlocal query_count
        query_count += 1
        return original_query_one(sql, params)

    monkeypatch.setattr(reader, "query", counted_query)
    monkeypatch.setattr(reader, "query_one", counted_query_one)
    monkeypatch.setattr(
        clients, "_resolve_market_reader", lambda snapshot_id=None: reader
    )

    snapshot = DirectMarketClient().get_fundamentals_pit_snapshot(
        "7203", date(2024, 6, 30)
    )

    assert query_count <= 14
    assert len(snapshot.prime_liquidity_panel) == 32
    row = snapshot.prime_liquidity_panel.set_index("code").loc["8000"]
    assert row["basis_id"] == "event-pit-v1:8000:2024-06-28"
    assert row["close"] == 8000.0
    assert row["close"] != 999999.0


def test_prime_panel_uses_true_trailing_observations_beyond_270_days(
    monkeypatch: pytest.MonkeyPatch, v4_market: Path
) -> None:
    _add_sparse_prime_history(v4_market)

    snapshot = _reader_client(monkeypatch, v4_market).get_fundamentals_pit_snapshot(
        "7203", date(2024, 6, 30)
    )

    row = snapshot.prime_liquidity_panel.set_index("code").loc["9000"]
    assert row["basis_id"] == "event-pit-v1:9000:2022-01-01"
    assert row["close"] == 10.0
    assert row["adv20_jpy"] == pytest.approx(505.0)
    assert row["adv60_jpy"] == pytest.approx(305.0)
