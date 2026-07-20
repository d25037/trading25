from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from src.application.services.adjusted_metrics_materializer import (
    AdjustedMetricsMaterializer,
)
from src.infrastructure.db.market.market_db import MarketDb
from tests.unit.server.db.market_writer_test_support import (
    open_market_db,
    publish_statements,
    publish_stock_data,
)


@pytest.fixture()
def market_db(tmp_path: Path) -> Iterator[MarketDb]:
    db = open_market_db(str(tmp_path / "market.duckdb"))
    yield db
    db.close()


def _statement(*, eps: float = 100.0) -> dict[str, Any]:
    return {
        "code": "7203",
        "statement_id": "disclosure-1",
        "disclosure_number": "disclosure-1",
        "disclosed_date": "2024-05-10",
        "disclosed_at": "2024-05-10T15:30:00+09:00",
        "period_start": "2023-04-01",
        "period_end": "2024-03-31",
        "type_of_current_period": "FY",
        "type_of_document": "FYFinancialStatements",
        "earnings_per_share": eps,
        "bps": eps * 10,
        "forecast_eps": eps * 1.2,
        "shares_outstanding": 10_000_000.0,
    }


def _seed_current_sources(db: MarketDb) -> None:
    publish_stock_data(
        db,
        [
            {
                "code": "7203",
                "date": "2024-12-30",
                "open": 500.0,
                "high": 500.0,
                "low": 500.0,
                "close": 500.0,
                "volume": 100,
                "adjustment_factor": 1.0,
            }
        ],
    )
    publish_statements(db, [_statement()])


def test_current_basis_publish_clears_pending_only_after_success(
    market_db: MarketDb,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_current_sources(market_db)
    materializer = AdjustedMetricsMaterializer(market_db)
    original = market_db.publish_current_basis_statement_metrics

    def fail(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("injected")

    monkeypatch.setattr(market_db, "publish_current_basis_statement_metrics", fail)
    with pytest.raises(RuntimeError, match="injected"):
        materializer.rebuild_current_basis([])
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM current_basis_recompute_pending"
    ) == (1,)

    monkeypatch.setattr(
        market_db, "publish_current_basis_statement_metrics", original
    )
    materializer.rebuild_current_basis([])
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM current_basis_recompute_pending"
    ) == (0,)


def test_source_drift_rolls_back_metric_reconcile_and_keeps_pending(
    market_db: MarketDb,
) -> None:
    _seed_current_sources(market_db)
    source = market_db.load_current_basis_fundamentals_source("7203")
    assert source is not None

    with pytest.raises(RuntimeError, match="sources drifted"):
        market_db.publish_current_basis_statement_metrics(
            "7203", [], expected_source_fingerprint="stale-fingerprint"
        )

    assert market_db._fetchone(
        "SELECT COUNT(*) FROM current_basis_recompute_pending"
    ) == (1,)
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM statement_metrics_adjusted"
    ) == (0,)


def test_current_basis_reconcile_is_idempotent_and_set_exact(
    market_db: MarketDb,
) -> None:
    _seed_current_sources(market_db)
    materializer = AdjustedMetricsMaterializer(market_db)

    first = materializer.rebuild_current_basis([])
    second = materializer.rebuild_current_basis(["7203"])

    assert first.mutation_stats["statements"].inserted == 1
    assert second.mutation_stats["statements"].mutated_rows == 0
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM statement_metrics_adjusted WHERE code = '7203'"
    ) == (1,)


def test_removed_retained_basis_entrypoints_fail_closed(market_db: MarketDb) -> None:
    with pytest.raises(ValueError, match="explicit affected codes"):
        AdjustedMetricsMaterializer(market_db).rebuild_all()
    with pytest.raises(ValueError, match="unsupported in Market v5"):
        market_db.get_adjusted_statement_metrics_for_basis(
            "7203", basis_id="event-pit-v1:7203:2024-01-01"
        )
    with pytest.raises(ValueError, match="unsupported in Market v5"):
        market_db.get_daily_valuation_for_basis(
            "7203", basis_id="event-pit-v1:7203:2024-01-01"
        )
