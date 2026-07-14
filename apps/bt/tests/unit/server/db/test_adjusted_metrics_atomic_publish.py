from __future__ import annotations

from pathlib import Path

import pytest

from src.application.services.adjusted_metrics_materializer import (
    AdjustedMetricsMaterializer,
)
from src.domains.fundamentals.adjustment_basis import (
    StockAdjustmentBasis,
    StockAdjustmentBasisSegment,
    StockAdjustmentLineage,
)
from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.valuation_writers import (
    AdjustedBasisMaterializationPlan,
)


@pytest.fixture()
def market_db(tmp_path: Path) -> MarketDb:
    db = MarketDb(str(tmp_path / "market.duckdb"))
    yield db
    db.close()


def _price(
    date: str,
    *,
    close: float,
    adjustment_factor: float,
    code: str = "7203",
) -> dict[str, object]:
    return {
        "code": code,
        "date": date,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 100,
        "adjustment_factor": adjustment_factor,
        "created_at": "2026-07-14T00:00:00",
    }


def _statement(*, eps: float = 100.0) -> dict[str, object]:
    return {
        "code": "7203",
        "disclosed_date": "2024-05-10",
        "type_of_current_period": "FY",
        "earnings_per_share": eps,
        "bps": eps * 10,
        "forecast_eps": eps * 1.2,
        "shares_outstanding": 10_000_000.0,
    }


def _ready_snapshot(db: MarketDb) -> tuple[list[dict[str, object]], ...]:
    return (
        db._fetchall_dicts(
            "SELECT * FROM stock_adjustment_bases WHERE status = 'ready' ORDER BY basis_id"
        ),
        db._fetchall_dicts(
            "SELECT * FROM stock_adjustment_basis_segments ORDER BY basis_id, source_date_from"
        ),
        db._fetchall_dicts(
            "SELECT * FROM statement_metrics_adjusted ORDER BY basis_version, disclosed_date"
        ),
        db._fetchall_dicts(
            "SELECT * FROM daily_valuation ORDER BY basis_version, date"
        ),
    )


def test_publish_failure_keeps_previous_ready_snapshot(
    market_db: MarketDb,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db.upsert_statements([_statement()])
    market_db.upsert_stock_data([_price("2024-12-30", close=500.0, adjustment_factor=1.0)])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_all()
    before = _ready_snapshot(market_db)
    market_db.upsert_statements([_statement(eps=200.0)])

    def _raise_injected(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected")

    monkeypatch.setattr(market_db, "_commit_basis_publish", _raise_injected)

    with pytest.raises(RuntimeError, match="injected"):
        materializer.rebuild_codes(["7203"])

    assert _ready_snapshot(market_db) == before


def test_invalid_factor_publishes_no_ready_basis_or_metrics(market_db: MarketDb) -> None:
    market_db.upsert_statements([_statement()])
    market_db.upsert_stock_data([
        _price("2024-12-30", close=500.0, adjustment_factor=1.0),
        _price("2025-01-06", close=600.0, adjustment_factor=0.0),
    ])

    result = AdjustedMetricsMaterializer(market_db).rebuild_all()

    assert result.ready_basis_count == 1
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM stock_adjustment_bases WHERE status = 'invalid'"
    ) == (1,)
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM statement_metrics_adjusted "
        "WHERE basis_version = 'event-pit-v1:7203:2025-01-06'"
    ) == (0,)


def test_invalid_factor_correction_removes_previous_ready_outputs(
    market_db: MarketDb,
) -> None:
    market_db.upsert_statements([_statement()])
    market_db.upsert_stock_data([
        _price("2024-12-30", close=500.0, adjustment_factor=1.0),
        _price("2025-01-06", close=600.0, adjustment_factor=0.5),
    ])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_all()
    basis_id = "event-pit-v1:7203:2025-01-06"
    assert market_db.get_daily_valuation_for_basis("7203", basis_id=basis_id)

    market_db.upsert_stock_data([
        _price("2025-01-06", close=600.0, adjustment_factor=0.0)
    ])
    materializer.rebuild_all()

    assert market_db._fetchone(
        "SELECT status FROM stock_adjustment_bases WHERE basis_id = ?", [basis_id]
    ) == ("invalid",)
    assert market_db.get_adjusted_statement_metrics_for_basis(
        "7203", basis_id=basis_id
    ) == []
    assert market_db.get_daily_valuation_for_basis("7203", basis_id=basis_id) == []
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM daily_valuation "
        "WHERE basis_version = 'event-pit-v1:7203:2025-01-06'"
    ) == (0,)


def test_ordinary_price_append_only_changes_active_basis(market_db: MarketDb) -> None:
    market_db.upsert_statements([_statement()])
    market_db.upsert_stock_data([
        _price("2024-12-30", close=500.0, adjustment_factor=1.0),
        _price("2025-01-06", close=600.0, adjustment_factor=0.5),
    ])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_all()
    closed_id = "event-pit-v1:7203:2024-12-30"
    closed_before = (
        market_db._fetchall_dicts(
            "SELECT * FROM stock_adjustment_bases WHERE basis_id = ?", [closed_id]
        ),
        market_db._fetchall_dicts(
            "SELECT * FROM statement_metrics_adjusted WHERE basis_version = ?", [closed_id]
        ),
        market_db._fetchall_dicts(
            "SELECT * FROM daily_valuation WHERE basis_version = ?", [closed_id]
        ),
    )
    market_db.upsert_stock_data([
        _price("2025-01-07", close=620.0, adjustment_factor=1.0)
    ])

    materializer.rebuild_all()

    closed_after = (
        market_db._fetchall_dicts(
            "SELECT * FROM stock_adjustment_bases WHERE basis_id = ?", [closed_id]
        ),
        market_db._fetchall_dicts(
            "SELECT * FROM statement_metrics_adjusted WHERE basis_version = ?", [closed_id]
        ),
        market_db._fetchall_dicts(
            "SELECT * FROM daily_valuation WHERE basis_version = ?", [closed_id]
        ),
    )
    assert closed_after == closed_before
    assert [
        row["date"]
        for row in market_db.get_daily_valuation_for_basis(
            "7203", basis_id="event-pit-v1:7203:2025-01-06"
        )
    ] == ["2024-12-30", "2025-01-06", "2025-01-07"]


def test_event_correction_rebuilds_changed_basis_forward(market_db: MarketDb) -> None:
    market_db.upsert_statements([_statement()])
    market_db.upsert_stock_data([
        _price("2024-12-30", close=500.0, adjustment_factor=1.0),
        _price("2025-01-06", close=600.0, adjustment_factor=0.5),
    ])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_all()

    market_db.upsert_stock_data([
        _price("2025-01-06", close=600.0, adjustment_factor=0.25)
    ])
    materializer.rebuild_all()

    active = market_db.get_adjusted_statement_metrics_for_basis(
        "7203", basis_id="event-pit-v1:7203:2025-01-06"
    )
    assert active[0]["adjusted_eps"] == pytest.approx(25.0)
    assert market_db._fetchone(
        "SELECT COUNT(*) FROM stock_adjustment_bases WHERE code = '7203'"
    ) == (2,)


def test_event_deletion_retains_previously_materialized_basis_graph(
    market_db: MarketDb,
) -> None:
    market_db.upsert_statements([_statement()])
    market_db.upsert_stock_data([
        _price("2024-12-30", close=500.0, adjustment_factor=1.0),
        _price("2025-01-06", close=600.0, adjustment_factor=0.5),
        _price("2025-01-07", close=620.0, adjustment_factor=1.0),
    ])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_all()
    orphan_id = "event-pit-v1:7203:2025-01-06"
    retained_before = (
        market_db._fetchall_dicts(
            "SELECT * FROM stock_adjustment_bases WHERE basis_id = ?", [orphan_id]
        ),
        market_db._fetchall_dicts(
            "SELECT * FROM statement_metrics_adjusted WHERE basis_version = ?",
            [orphan_id],
        ),
        market_db._fetchall_dicts(
            "SELECT * FROM daily_valuation WHERE basis_version = ?", [orphan_id]
        ),
    )

    market_db._execute(
        "DELETE FROM stock_data_raw WHERE code = '7203' AND date = '2025-01-06'"
    )
    materializer.rebuild_all()

    assert (
        market_db._fetchall_dicts(
            "SELECT * FROM stock_adjustment_bases WHERE basis_id = ?", [orphan_id]
        ),
        market_db._fetchall_dicts(
            "SELECT * FROM statement_metrics_adjusted WHERE basis_version = ?",
            [orphan_id],
        ),
        market_db._fetchall_dicts(
            "SELECT * FROM daily_valuation WHERE basis_version = ?", [orphan_id]
        ),
    ) == retained_before


def test_statement_correction_fans_into_every_observable_basis(market_db: MarketDb) -> None:
    market_db.upsert_statements([_statement()])
    market_db.upsert_stock_data([
        _price("2024-12-30", close=500.0, adjustment_factor=1.0),
        _price("2025-01-06", close=600.0, adjustment_factor=0.5),
    ])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_all()

    market_db.upsert_statements([_statement(eps=200.0)])
    materializer.rebuild_all()

    rows = market_db._fetchall_dicts(
        "SELECT basis_version, adjusted_eps FROM statement_metrics_adjusted ORDER BY basis_version"
    )
    assert [(row["basis_version"], row["adjusted_eps"]) for row in rows] == [
        ("event-pit-v1:7203:2024-12-30", pytest.approx(200.0)),
        ("event-pit-v1:7203:2025-01-06", pytest.approx(100.0)),
    ]


def test_raw_price_correction_fans_into_every_covering_basis(market_db: MarketDb) -> None:
    market_db.upsert_statements([_statement()])
    market_db.upsert_stock_data([
        _price("2024-12-30", close=500.0, adjustment_factor=1.0),
        _price("2025-01-06", close=600.0, adjustment_factor=0.5),
    ])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_all()

    market_db.upsert_stock_data([
        _price("2024-12-30", close=700.0, adjustment_factor=1.0)
    ])
    materializer.rebuild_all()

    rows = market_db._fetchall_dicts(
        "SELECT basis_version, close FROM daily_valuation "
        "WHERE date = '2024-12-30' ORDER BY basis_version"
    )
    assert [(row["basis_version"], row["close"]) for row in rows] == [
        ("event-pit-v1:7203:2024-12-30", pytest.approx(700.0)),
        ("event-pit-v1:7203:2025-01-06", pytest.approx(350.0)),
    ]


def test_atomic_publish_rejects_ready_basis_without_segment_coverage(
    market_db: MarketDb,
) -> None:
    basis_id = "event-pit-v1:7203:2024-12-30"
    plan = AdjustedBasisMaterializationPlan(
        lineages=(
            StockAdjustmentLineage(
                code="7203",
                bases=(
                    StockAdjustmentBasis(
                        code="7203",
                        basis_id=basis_id,
                        valid_from="2024-12-30",
                        valid_to_exclusive=None,
                        adjustment_through_date="2024-12-30",
                        source_fingerprint="fingerprint",
                        materialized_through_date="2024-12-30",
                        status="ready",
                    ),
                ),
                segments=(),
            ),
        ),
        adjusted_statement_rows=(),
        daily_valuation_rows=(),
        replace_basis_ids={"7203": (basis_id,)},
        orphan_basis_ids={},
    )

    with pytest.raises(ValueError, match="segment coverage"):
        market_db.publish_adjusted_basis_materialization(plan)

    assert market_db._fetchone(
        "SELECT COUNT(*) FROM stock_adjustment_bases WHERE basis_id = ?", [basis_id]
    ) == (0,)


def test_atomic_publish_rejects_staged_basis_omitted_from_replacements(
    market_db: MarketDb,
) -> None:
    market_db.upsert_statements([_statement()])
    market_db.upsert_stock_data([
        _price("2024-12-30", close=500.0, adjustment_factor=1.0)
    ])
    AdjustedMetricsMaterializer(market_db).rebuild_all()
    before = _ready_snapshot(market_db)
    basis_id = "event-pit-v1:7203:2024-12-30"
    plan = AdjustedBasisMaterializationPlan(
        lineages=(
            StockAdjustmentLineage(
                code="7203",
                bases=(
                    StockAdjustmentBasis(
                        code="7203",
                        basis_id=basis_id,
                        valid_from="2024-12-30",
                        valid_to_exclusive=None,
                        adjustment_through_date="2024-12-30",
                        source_fingerprint="changed-fingerprint",
                        materialized_through_date="2024-12-30",
                        status="ready",
                    ),
                ),
                segments=(
                    StockAdjustmentBasisSegment(
                        code="7203",
                        basis_id=basis_id,
                        source_date_from="2024-12-30",
                        source_date_to_exclusive=None,
                        cumulative_factor=2.0,
                    ),
                ),
            ),
        ),
        adjusted_statement_rows=(),
        daily_valuation_rows=(),
        replace_basis_ids={},
        orphan_basis_ids={},
    )

    with pytest.raises(ValueError, match="declared replacement"):
        market_db.publish_adjusted_basis_materialization(plan)

    assert _ready_snapshot(market_db) == before
