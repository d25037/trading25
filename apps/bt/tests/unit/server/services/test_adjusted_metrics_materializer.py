from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest

from src.application.services.adjusted_metrics_materializer import (
    AdjustedMetricsBuildResult,
    AdjustedMetricsMaterializer,
)
from src.infrastructure.db.market.market_db import MarketDb
from tests.unit.server.db.market_writer_test_support import (
    open_market_db,
    publish_stock_data,
    publish_statements,
)


@pytest.fixture()
def market_db(tmp_path: Path) -> MarketDb:
    db = open_market_db(str(tmp_path / "market.duckdb"))
    yield db
    db.close()


def _seed_provider_basis(
    db: MarketDb,
    code: str,
    *,
    basis_date: str = "2024-12-30",
    factor: float = 0.5,
    event_date: str = "2024-08-01",
) -> None:
    db._execute(
        """
        INSERT INTO stock_provider_windows (
            code, coverage_start, coverage_end, provider_as_of,
            source_fingerprint, updated_at
        ) VALUES (?, '2020-01-01', ?, ?, 'window-fingerprint', '2025-01-01T00:00:00Z')
        """,
        [code, basis_date, basis_date],
    )
    db._execute(
        """
        INSERT INTO stock_adjustment_events (
            code, date, adjustment_factor, source_fingerprint
        ) VALUES (?, ?, ?, 'event-fingerprint')
        """,
        [code, event_date, factor],
    )


def _statement(
    statement_id: str,
    *,
    code: str = "7203",
    document_type: str = "FYFinancialStatements_Consolidated_JP",
    disclosed_date: str = "2024-05-10",
    disclosed_at: str = "2024-05-10T15:30:00+09:00",
) -> dict[str, Any]:
    return {
        "code": code,
        "statement_id": statement_id,
        "disclosure_number": statement_id,
        "disclosed_date": disclosed_date,
        "disclosed_at": disclosed_at,
        "period_start": "2023-04-01",
        "period_end": "2024-03-31",
        "type_of_current_period": "FY",
        "type_of_document": document_type,
        "earnings_per_share": 100.0,
        "diluted_earnings_per_share": 98.0,
        "bps": 1000.0,
        "forecast_eps": 120.0,
        "dividend_fy": 30.0,
        "forecast_dividend_fy": 40.0,
        "shares_outstanding": 10_000_000.0,
        "treasury_shares": 1_000_000.0,
        "sales": 2_500_000_000.0,
        "payout_ratio": 28.3,
    }


def test_rebuild_current_basis_adjusts_only_strictly_later_events(
    market_db: MarketDb,
) -> None:
    _seed_provider_basis(market_db, "7203")
    publish_statements(market_db, [_statement("disclosure-1")])

    result = AdjustedMetricsMaterializer(market_db).rebuild_current_basis(["72030"])

    assert result.completed_codes == 1
    assert result.total_codes == 1
    assert result.current_basis_statement_count == 1
    assert result.mutation_stats["statements"].inserted == 1
    row = market_db._fetchone(
        """
        SELECT statement_id, fundamentals_adjustment_basis_date,
               raw_eps, adjusted_eps, raw_diluted_eps, adjusted_diluted_eps,
               raw_forecast_dividend_fy, adjusted_forecast_dividend_fy,
               raw_shares_outstanding, adjusted_shares_outstanding,
               raw_treasury_shares, adjusted_treasury_shares,
               adjustment_factor_cumulative, source_fingerprint
        FROM statement_metrics_adjusted WHERE code = '7203'
        """
    )
    assert row is not None
    assert row[:2] == ("disclosure-1", "2024-12-30")
    assert row[2:8] == pytest.approx((100.0, 50.0, 98.0, 49.0, 40.0, 20.0))
    assert row[8:12] == pytest.approx(
        (10_000_000.0, 20_000_000.0, 1_000_000.0, 2_000_000.0)
    )
    assert row[12] == pytest.approx(0.5)
    assert isinstance(row[13], str) and row[13]

    raw = market_db._fetchone(
        "SELECT sales, payout_ratio FROM statements WHERE statement_id = 'disclosure-1'"
    )
    assert raw == (2_500_000_000.0, 28.3)


def test_rebuild_atomically_publishes_current_basis_provenance_state(
    market_db: MarketDb,
) -> None:
    _seed_provider_basis(market_db, "7203")
    publish_statements(
        market_db,
        [_statement("disclosure-1"), _statement("disclosure-2")],
    )

    AdjustedMetricsMaterializer(market_db).rebuild_current_basis(["7203"])

    state = market_db._fetchone(
        """
        SELECT fundamentals_adjustment_basis_date, source_fingerprint,
               statement_count, materialized_at
        FROM current_basis_fundamentals_state
        WHERE code = '7203'
        """
    )
    metric_fingerprints = market_db._fetchall(
        "SELECT DISTINCT source_fingerprint FROM statement_metrics_adjusted "
        "WHERE code = '7203'"
    )
    assert state is not None
    assert state[0] == "2024-12-30"
    assert state[2] == 2
    assert isinstance(state[3], str) and state[3]
    assert metric_fingerprints == [(state[1],)]
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM current_basis_recompute_pending WHERE code = '7203'"
    ) == (0,)


def test_event_on_disclosure_date_is_not_reapplied(market_db: MarketDb) -> None:
    _seed_provider_basis(market_db, "7203", event_date="2024-05-10")
    publish_statements(market_db, [_statement("disclosure-1")])

    AdjustedMetricsMaterializer(market_db).rebuild_current_basis(["7203"])

    assert market_db._fetchone(
        "SELECT adjusted_eps, adjusted_shares_outstanding FROM statement_metrics_adjusted"
    ) == (100.0, 10_000_000.0)


def test_fy_metrics_prefer_next_year_forecasts_without_mutating_raw_statement(
    market_db: MarketDb,
) -> None:
    _seed_provider_basis(market_db, "7203")
    statement = _statement("disclosure-1")
    statement["forecast_eps"] = 120.0
    statement["next_year_forecast_earnings_per_share"] = 140.0
    statement["forecast_dividend_fy"] = 40.0
    statement["next_year_forecast_dividend_fy"] = 50.0
    publish_statements(market_db, [statement])

    AdjustedMetricsMaterializer(market_db).rebuild_current_basis([])

    assert market_db._fetchone(
        "SELECT raw_forecast_eps, raw_forecast_dividend_fy "
        "FROM statement_metrics_adjusted WHERE code = '7203'"
    ) == (140.0, 50.0)
    assert market_db._fetchone(
        "SELECT forecast_eps, next_year_forecast_earnings_per_share, "
        "forecast_dividend_fy, next_year_forecast_dividend_fy "
        "FROM statements WHERE code = '7203'"
    ) == (120.0, 140.0, 40.0, 50.0)


def test_rebuild_preserves_same_day_distinct_documents_and_is_idempotent(
    market_db: MarketDb,
) -> None:
    _seed_provider_basis(market_db, "7203")
    publish_statements(
        market_db,
        [
            _statement("earnings-revision", document_type="EarnForecastRevision"),
            _statement("dividend-revision", document_type="DividendForecastRevision"),
        ],
    )
    materializer = AdjustedMetricsMaterializer(market_db)

    first = materializer.rebuild_current_basis(["7203"])
    second = materializer.rebuild_current_basis(["7203"])

    assert first.mutation_stats["statements"].inserted == 2
    assert second.mutation_stats["statements"].mutated_rows == 0
    assert market_db._fetchall(
        "SELECT statement_id FROM statement_metrics_adjusted ORDER BY statement_id"
    ) == [("dividend-revision",), ("earnings-revision",)]
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM current_basis_recompute_pending WHERE code = '7203'"
    ) == (0,)


def test_rebuild_reconciles_only_requested_codes(market_db: MarketDb) -> None:
    for code in ("7203", "6758"):
        _seed_provider_basis(market_db, code)
        publish_statements(market_db, [_statement(f"disclosure-{code}", code=code)])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_current_basis(["6758"])
    before = market_db._fetchone(
        "SELECT source_fingerprint FROM statement_metrics_adjusted WHERE code = '6758'"
    )

    result = materializer.rebuild_current_basis(["72030"])

    assert result.total_codes == 1
    assert market_db._fetchone(
        "SELECT source_fingerprint FROM statement_metrics_adjusted WHERE code = '6758'"
    ) == before
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM statement_metrics_adjusted WHERE code = '7203'"
    ) == (1,)


def test_rebuild_removes_stale_metric_for_deleted_statement(market_db: MarketDb) -> None:
    _seed_provider_basis(market_db, "7203")
    publish_statements(market_db, [_statement("disclosure-1")])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_current_basis(["7203"])
    market_db._execute("DELETE FROM statements WHERE code = '7203'")

    result = materializer.rebuild_current_basis(["7203"])

    assert result.mutation_stats["statements"].deleted == 1
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM statement_metrics_adjusted WHERE code = '7203'"
    ) == (0,)


def test_current_basis_publish_rolls_back_code_on_constraint_failure(
    market_db: MarketDb,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_provider_basis(market_db, "7203")
    publish_statements(market_db, [_statement("disclosure-1")])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_current_basis(["7203"])
    before = market_db._fetchall_dicts(
        "SELECT * FROM statement_metrics_adjusted WHERE code = '7203'"
    )
    corrected = _statement("disclosure-1")
    corrected["earnings_per_share"] = 101.0
    publish_statements(market_db, [corrected])

    original = market_db.publish_current_basis_statement_metrics

    def invalid_publish(
        code: str,
        rows: list[dict[str, Any]],
        *,
        expected_source_fingerprint: str,
    ) -> Any:
        invalid = [*rows, {**rows[0], "statement_id": None}]
        return original(
            code,
            invalid,
            expected_source_fingerprint=expected_source_fingerprint,
        )

    monkeypatch.setattr(market_db, "publish_current_basis_statement_metrics", invalid_publish)

    with pytest.raises(duckdb.ConstraintException):
        materializer.rebuild_current_basis(["7203"])

    assert market_db._fetchall_dicts(
        "SELECT * FROM statement_metrics_adjusted WHERE code = '7203'"
    ) == before
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM current_basis_recompute_pending WHERE code = '7203'"
    ) == (1,)


def test_failed_updater_retries_persisted_pending_after_identical_statement(
    market_db: MarketDb,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_provider_basis(market_db, "7203")
    statement = _statement("disclosure-1")
    publish_statements(market_db, [statement])
    materializer = AdjustedMetricsMaterializer(market_db)
    original = market_db.publish_current_basis_statement_metrics

    def fail_publish(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("injected updater failure")

    monkeypatch.setattr(
        market_db, "publish_current_basis_statement_metrics", fail_publish
    )
    with pytest.raises(RuntimeError, match="injected updater failure"):
        materializer.rebuild_current_basis(["7203"])

    publish_statements(market_db, [statement])
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM current_basis_recompute_pending WHERE code = '7203'"
    ) == (1,)
    monkeypatch.setattr(
        market_db, "publish_current_basis_statement_metrics", original
    )

    retry = materializer.rebuild_current_basis([])

    assert retry.completed_codes == 1
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM current_basis_recompute_pending WHERE code = '7203'"
    ) == (0,)


def test_initial_unit_factor_append_builds_current_metrics(market_db: MarketDb) -> None:
    publish_stock_data(
        market_db,
        [
            {
                "code": "7203",
                "date": "2024-12-30",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1000,
                "adjustment_factor": 1.0,
            }
        ],
    )
    publish_statements(market_db, [_statement("disclosure-1")])

    result = AdjustedMetricsMaterializer(market_db).rebuild_current_basis([])

    assert result.completed_codes == 1
    assert market_db._fetchone(
        "SELECT fundamentals_adjustment_basis_date, adjusted_eps "
        "FROM statement_metrics_adjusted WHERE code = '7203'"
    ) == ("2024-12-30", 100.0)
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM current_basis_recompute_pending WHERE code = '7203'"
    ) == (0,)


def test_failed_updater_retries_persisted_pending_after_price_event_change(
    market_db: MarketDb,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_provider_basis(market_db, "7203")
    publish_statements(market_db, [_statement("disclosure-1")])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_current_basis([])
    original = market_db.publish_current_basis_statement_metrics
    market_db._execute("BEGIN TRANSACTION")
    market_db._execute(
        "UPDATE stock_adjustment_events SET adjustment_factor = 0.25 "
        "WHERE code = '7203'"
    )
    market_db._execute(
        """
        INSERT INTO current_basis_recompute_pending
            (code, reason, source_fingerprint, updated_at)
        VALUES ('7203', 'provider_basis_change', 'price-v2', '2025-01-02T00:00:00Z')
        """
    )
    market_db._execute("COMMIT")

    def fail_publish(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("injected price updater failure")

    monkeypatch.setattr(
        market_db, "publish_current_basis_statement_metrics", fail_publish
    )
    with pytest.raises(RuntimeError, match="injected price updater failure"):
        materializer.rebuild_current_basis([])
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM current_basis_recompute_pending WHERE code = '7203'"
    ) == (1,)

    monkeypatch.setattr(
        market_db, "publish_current_basis_statement_metrics", original
    )
    retry = materializer.rebuild_current_basis([])

    assert retry.completed_codes == 1
    assert market_db._fetchone(
        "SELECT adjusted_eps FROM statement_metrics_adjusted WHERE code = '7203'"
    ) == (25.0,)
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM current_basis_recompute_pending WHERE code = '7203'"
    ) == (0,)


def test_materializer_exposes_only_current_basis_entrypoint(market_db: MarketDb) -> None:
    materializer = AdjustedMetricsMaterializer(market_db)

    assert callable(materializer.rebuild_current_basis)
    assert not hasattr(materializer, "rebuild_all")
    assert not hasattr(materializer, "rebuild_codes")
    assert not hasattr(materializer, "reconcile")
    assert not hasattr(materializer, "reconcile_code")


def test_materializer_result_has_no_legacy_basis_aliases() -> None:
    result_fields = AdjustedMetricsBuildResult.__dataclass_fields__

    assert "current_basis_statement_count" in result_fields
    assert "pending_current_basis_code_count" in result_fields
    assert "fundamentals_adjustment_basis_date" in result_fields
    assert "basis_count" not in result_fields
    assert "published_basis_count" not in result_fields
    assert "ready_basis_count" not in result_fields
    assert "active_basis_version" not in result_fields
