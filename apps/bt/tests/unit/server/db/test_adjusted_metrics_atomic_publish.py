from __future__ import annotations

from pathlib import Path

import pytest

from src.application.services.adjusted_metrics_materializer import (
    AdjustmentLineageReconstructionError,
    AdjustedMetricsMaterializer,
)
from src.domains.fundamentals.adjustment_basis import (
    StockAdjustmentBasis,
    StockAdjustmentLineage,
)
from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.valuation_writers import (
    AdjustedBasisMaterializationPlan,
    BasisSnapshot,
    StructuralBasisPlan,
    _basis_snapshots_equal,
    _semantic_stats,
    publish_adjusted_basis_materialization,
)
from src.infrastructure.db.market.market_mutations import MarketMutationStats
from tests.unit.server.db.market_writer_test_support import (
    publish_statements,
    publish_stock_data,
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
    publish_statements(market_db,[_statement()])
    publish_stock_data(market_db,[_price("2024-12-30", close=500.0, adjustment_factor=1.0)])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_all()
    before = _ready_snapshot(market_db)
    publish_statements(market_db,[_statement(eps=200.0)])

    def _raise_injected(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected")

    monkeypatch.setattr(market_db, "_commit_basis_publish", _raise_injected)

    with pytest.raises(RuntimeError, match="injected"):
        materializer.rebuild_codes(["7203"])

    assert _ready_snapshot(market_db) == before


def test_invalid_factor_publishes_no_ready_basis_or_metrics(market_db: MarketDb) -> None:
    publish_statements(market_db,[_statement()])
    publish_stock_data(market_db,[
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
    publish_statements(market_db,[_statement()])
    publish_stock_data(market_db,[
        _price("2024-12-30", close=500.0, adjustment_factor=1.0),
        _price("2025-01-06", close=600.0, adjustment_factor=0.5),
    ])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_all()
    basis_id = "event-pit-v1:7203:2025-01-06"
    assert market_db.get_daily_valuation_for_basis("7203", basis_id=basis_id)

    publish_stock_data(market_db,[
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
    publish_statements(market_db,[_statement()])
    publish_stock_data(market_db,[
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
    publish_stock_data(market_db,[
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
    publish_statements(market_db,[_statement()])
    publish_stock_data(market_db,[
        _price("2024-12-30", close=500.0, adjustment_factor=1.0),
        _price("2025-01-06", close=600.0, adjustment_factor=0.5),
    ])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_all()

    publish_stock_data(market_db,[
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


def test_event_deletion_fails_closed_and_retains_materialized_basis_graph(
    market_db: MarketDb,
) -> None:
    publish_statements(market_db,[_statement()])
    publish_stock_data(market_db,[
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

    with pytest.raises(
        AdjustmentLineageReconstructionError,
        match=(
            "cannot reconstruct retained adjustment bases for code 7203: "
            "event-pit-v1:7203:2025-01-06"
        ),
    ):
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


def test_event_deletion_with_statement_correction_does_not_publish_or_succeed(
    market_db: MarketDb,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publish_statements(market_db,[_statement()])
    publish_stock_data(market_db,[
        _price("2024-12-30", close=500.0, adjustment_factor=1.0),
        _price("2025-01-06", close=600.0, adjustment_factor=0.5),
    ])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_all()
    before = _ready_snapshot(market_db)
    market_db._execute(
        "DELETE FROM stock_data_raw WHERE code = '7203' AND date = '2025-01-06'"
    )
    publish_statements(market_db,[_statement(eps=200.0)])
    publish_calls: list[object] = []
    monkeypatch.setattr(
        market_db,
        "publish_adjusted_basis_materialization",
        lambda plan: publish_calls.append(plan),
    )

    with pytest.raises(
        AdjustmentLineageReconstructionError,
        match="cannot reconstruct retained adjustment bases",
    ):
        materializer.rebuild_all()

    assert publish_calls == []
    assert _ready_snapshot(market_db) == before


def test_consistent_lineage_statement_correction_still_publishes(
    market_db: MarketDb,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publish_statements(market_db,[_statement()])
    publish_stock_data(market_db,[
        _price("2024-12-30", close=500.0, adjustment_factor=1.0),
        _price("2025-01-06", close=600.0, adjustment_factor=0.5),
    ])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_all()
    publish_statements(market_db,[_statement(eps=200.0)])
    publish_calls: list[object] = []
    original_publish = market_db.publish_adjusted_basis_materialization

    def _capture_publish(plan: object) -> object:
        publish_calls.append(plan)
        return original_publish(plan)  # type: ignore[arg-type]

    monkeypatch.setattr(
        market_db,
        "publish_adjusted_basis_materialization",
        _capture_publish,
    )

    result = materializer.rebuild_all()

    assert result.completed_codes == 1
    assert publish_calls
    adjusted_eps = sorted(
        row["adjusted_eps"]
        for row in market_db._fetchall_dicts(
            "SELECT adjusted_eps FROM statement_metrics_adjusted WHERE code = '7203'"
        )
    )
    assert adjusted_eps == pytest.approx([100.0, 200.0])


def test_statement_correction_fans_into_every_observable_basis(market_db: MarketDb) -> None:
    publish_statements(market_db,[_statement()])
    publish_stock_data(market_db,[
        _price("2024-12-30", close=500.0, adjustment_factor=1.0),
        _price("2025-01-06", close=600.0, adjustment_factor=0.5),
    ])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_all()

    publish_statements(market_db,[_statement(eps=200.0)])
    materializer.rebuild_all()

    rows = market_db._fetchall_dicts(
        "SELECT basis_version, adjusted_eps FROM statement_metrics_adjusted ORDER BY basis_version"
    )
    assert [(row["basis_version"], row["adjusted_eps"]) for row in rows] == [
        ("event-pit-v1:7203:2024-12-30", pytest.approx(200.0)),
        ("event-pit-v1:7203:2025-01-06", pytest.approx(100.0)),
    ]


def test_raw_price_correction_fans_into_every_covering_basis(market_db: MarketDb) -> None:
    publish_statements(market_db,[_statement()])
    publish_stock_data(market_db,[
        _price("2024-12-30", close=500.0, adjustment_factor=1.0),
        _price("2025-01-06", close=600.0, adjustment_factor=0.5),
    ])
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_all()

    publish_stock_data(market_db,[
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
        plans=(
            StructuralBasisPlan(
                kind="structural",
                lineage=StockAdjustmentLineage(
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
                adjusted_statement_rows=(),
                daily_valuation_rows=(),
                expected_snapshot=None,
                expected_source_fingerprint=market_db.load_adjusted_source_fingerprint(
                    "7203"
                ),
            ),
        ),
    )

    with pytest.raises(ValueError, match="segment coverage"):
        market_db.publish_adjusted_basis_materialization(plan)

    assert market_db._fetchone(
        "SELECT COUNT(*) FROM stock_adjustment_bases WHERE basis_id = ?", [basis_id]
    ) == (0,)


def test_empty_atomic_publish_has_zero_mutation(
    market_db: MarketDb,
) -> None:
    result = market_db.publish_adjusted_basis_materialization(
        AdjustedBasisMaterializationPlan(plans=())
    )

    assert result.basis.stats.mutated_rows == 0
    assert result.segments.stats.mutated_rows == 0
    assert result.statements.stats.mutated_rows == 0
    assert result.valuations.stats.mutated_rows == 0


def test_empty_atomic_publish_does_not_acquire_lock_or_begin() -> None:
    class Forbidden:
        def __getattribute__(self, name: str) -> object:
            raise AssertionError(f"empty plan touched {name}")

    result = publish_adjusted_basis_materialization(
        Forbidden(), Forbidden(), AdjustedBasisMaterializationPlan(plans=())
    )

    assert result.plan_counts == {
        "structural": 0, "frontier_extension": 0, "no_op": 0
    }


def test_nan_comparison_matches_is_not_distinct_from() -> None:
    existing = ({"code": "7203", "value": float("nan")},)
    desired = ({"code": "7203", "value": float("nan")},)

    assert _semantic_stats(
        desired,
        existing,
        key_columns=("code",),
        compare_columns=("value",),
    ) == MarketMutationStats(1, 0, 0, 1, 0)
    snapshot = BasisSnapshot(
        basis={"code": "7203", "value": float("nan")},
        segments=(),
        statement_rows=(),
        valuation_rows=(),
    )
    same = BasisSnapshot(
        basis={"code": "7203", "value": float("nan")},
        segments=(),
        statement_rows=(),
        valuation_rows=(),
    )
    assert _basis_snapshots_equal(snapshot, same)
