from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest

from src.infrastructure.db.market import adjustment_basis_writers
from src.infrastructure.db.market.adjustment_basis_queries import (
    load_raw_adjustment_points,
)
from src.domains.fundamentals.adjustment_basis import (
    RawAdjustmentPoint,
    StockAdjustmentBasis,
    StockAdjustmentBasisSegment,
    StockAdjustmentLineage,
    build_stock_adjustment_lineage,
)
from src.infrastructure.db.market.market_db import MarketDb


@pytest.fixture()
def market_db(tmp_path: Path) -> Iterator[MarketDb]:
    db = MarketDb(str(tmp_path / "market.duckdb"))
    yield db
    db.close()


def _lineage() -> StockAdjustmentLineage:
    return build_stock_adjustment_lineage(
        "72030",
        [
            RawAdjustmentPoint("72030", "2024-01-04", 1.0),
            RawAdjustmentPoint("7203", "2024-06-27", 1.0),
            RawAdjustmentPoint("72030", "2024-06-28", 0.5),
            RawAdjustmentPoint("7203", "2024-12-30", 1.0),
        ],
    )


def _publish_two_regimes(market_db: MarketDb) -> StockAdjustmentLineage:
    lineage = _lineage()
    market_db.publish_stock_adjustment_lineages([lineage], remove_basis_ids={})
    return lineage


def _seed_raw_and_current_prices(market_db: MarketDb) -> None:
    market_db._execute(
        """
        INSERT INTO stock_data_raw
            (code, date, open, high, low, close, volume, adjustment_factor)
        VALUES
            ('72030', '2024-01-04', 900, 1100, 800, 1000, 100, 1.0),
            ('7203', '2024-06-27', 950, 1150, 850, 1000, 120, 1.0),
            ('7203', '2024-06-28', 550, 650, 500, 600, 300, 0.5)
        """
    )
    market_db._execute(
        """
        INSERT INTO stock_data
            (code, date, open, high, low, close, volume, adjustment_factor)
        VALUES ('7203', '2024-01-04', 9, 11, 8, 10, 9999, 1.0)
        """
    )


def test_load_raw_adjustment_points_normalizes_aliases_and_filters_codes(
    market_db: MarketDb,
) -> None:
    _seed_raw_and_current_prices(market_db)

    rows = market_db.load_raw_adjustment_points(["72030"])

    assert [(row["code"], row["date"], row["adjustment_factor"]) for row in rows] == [
        ("7203", "2024-01-04", 1.0),
        ("7203", "2024-06-27", 1.0),
        ("7203", "2024-06-28", 0.5),
    ]


def test_load_raw_adjustment_points_filters_physical_aliases_before_normalization() -> None:
    captured: dict[str, object] = {}

    def _fetchall_dicts(query: str, params: list[object]) -> list[dict[str, object]]:
        captured["query"] = query
        captured["params"] = params
        return []

    load_raw_adjustment_points(_fetchall_dicts, ["7203"])

    query = " ".join(str(captured["query"]).split())
    assert "FROM stock_data_raw WHERE code IN (?, ?)" in query
    assert captured["params"] == ["7203", "72030"]


def test_list_adjustment_materialization_codes_uses_raw_and_retained_catalog(
    market_db: MarketDb,
) -> None:
    market_db._execute(
        """
        INSERT INTO stock_data_raw
            (code, date, open, high, low, close, volume, adjustment_factor)
        VALUES
            ('72030', '2024-01-04', 900, 1100, 800, 1000, 100, 1.0),
            ('1301', '2024-01-04', 90, 110, 80, 100, 100, 1.0)
        """
    )
    retained = build_stock_adjustment_lineage(
        "67580",
        [RawAdjustmentPoint("67580", "2024-01-04", 1.0)],
    )
    market_db.publish_stock_adjustment_lineages([retained], remove_basis_ids={})

    assert market_db.list_adjustment_materialization_codes() == [
        "1301",
        "6758",
        "7203",
    ]


def test_ready_basis_resolution_requires_interval_and_coverage(market_db: MarketDb) -> None:
    _publish_two_regimes(market_db)

    first = market_db.get_ready_adjustment_basis("72030", "2024-06-27")
    second = market_db.get_ready_adjustment_basis("7203", "2024-06-28")

    assert first is not None and first["valid_from"] == "2024-01-04"
    assert second is not None and second["valid_from"] == "2024-06-28"
    assert market_db.get_ready_adjustment_basis("7203", "2026-01-01") is None


def test_ready_basis_resolution_returns_none_for_ambiguous_catalog(
    market_db: MarketDb,
) -> None:
    _publish_two_regimes(market_db)
    market_db._execute(
        """
        INSERT INTO stock_adjustment_bases (
            code, basis_id, valid_from, valid_to_exclusive,
            adjustment_through_date, source_fingerprint,
            materialized_through_date, status
        ) VALUES (
            '7203', 'event-pit-v1:7203:2024-06-01', '2024-06-01', NULL,
            '2024-06-01', 'corrupt-overlap', '2024-12-30', 'ready'
        )
        """
    )

    assert market_db.get_ready_adjustment_basis("7203", "2024-06-27") is None


def test_basis_projection_never_reads_current_stock_data(market_db: MarketDb) -> None:
    _seed_raw_and_current_prices(market_db)
    _publish_two_regimes(market_db)

    rows = market_db.get_basis_adjusted_stock_data(
        "7203",
        "event-pit-v1:7203:2024-06-28",
        end="2024-06-28",
    )

    assert rows[0]["close"] == 500.0
    assert rows[0]["volume"] == 200
    assert rows[-1]["close"] == 600.0


def test_segments_are_returned_in_source_interval_order(market_db: MarketDb) -> None:
    _publish_two_regimes(market_db)

    rows = market_db.get_adjustment_basis_segments(
        "72030", "event-pit-v1:7203:2024-06-28"
    )

    assert [row["source_date_from"] for row in rows] == ["2024-01-04", "2024-06-28"]
    assert [row["cumulative_factor"] for row in rows] == [0.5, 1.0]


def test_publish_removes_only_explicit_orphan_basis_ids(market_db: MarketDb) -> None:
    lineage = _publish_two_regimes(market_db)
    first_id = lineage.bases[0].basis_id
    second_id = lineage.bases[1].basis_id

    market_db.publish_stock_adjustment_lineages(
        [],
        remove_basis_ids={"72030": [first_id]},
    )

    assert market_db.get_adjustment_basis_segments("7203", first_id) == []
    assert market_db.get_adjustment_basis_segments("7203", second_id) != []
    remaining = market_db._fetchall(
        "SELECT basis_id FROM stock_adjustment_bases WHERE code = '7203' ORDER BY valid_from"
    )
    assert remaining == [(second_id,)]


def test_publish_rejects_overlapping_basis_intervals(market_db: MarketDb) -> None:
    lineage = _lineage()
    overlapping = StockAdjustmentLineage(
        code=lineage.code,
        bases=(
            lineage.bases[0],
            StockAdjustmentBasis(
                **{
                    **lineage.bases[1].__dict__,
                    "valid_from": "2024-06-26",
                    "basis_id": "event-pit-v1:7203:2024-06-26",
                }
            ),
        ),
        segments=lineage.segments,
    )

    with pytest.raises(ValueError, match="overlapping basis intervals"):
        market_db.publish_stock_adjustment_lineages([overlapping], remove_basis_ids={})


def test_publish_rejects_basis_id_that_does_not_match_identity(market_db: MarketDb) -> None:
    lineage = _lineage()
    mismatched_basis = StockAdjustmentBasis(
        **{
            **lineage.bases[0].__dict__,
            "basis_id": "event-pit-v1:7203:wrong",
        }
    )
    mismatched = StockAdjustmentLineage(
        code=lineage.code,
        bases=(mismatched_basis,),
        segments=(),
    )

    with pytest.raises(ValueError, match="basis identity"):
        market_db.publish_stock_adjustment_lineages([mismatched], remove_basis_ids={})

    assert market_db._fetchone("SELECT COUNT(*) FROM stock_adjustment_bases") == (0,)


def test_publish_rejects_changed_valid_from_with_stale_basis_id(
    market_db: MarketDb,
) -> None:
    lineage = _lineage()
    stale_identity = StockAdjustmentBasis(
        **{
            **lineage.bases[0].__dict__,
            "valid_from": "2024-01-05",
        }
    )
    mismatched = StockAdjustmentLineage(
        code=lineage.code,
        bases=(stale_identity,),
        segments=(),
    )

    with pytest.raises(ValueError, match="basis identity"):
        market_db.publish_stock_adjustment_lineages([mismatched], remove_basis_ids={})

    assert market_db._fetchone("SELECT COUNT(*) FROM stock_adjustment_bases") == (0,)


def test_publish_rejects_overlapping_intervals_across_lineages(market_db: MarketDb) -> None:
    lineage = _lineage()

    with pytest.raises(ValueError, match="overlapping basis intervals"):
        market_db.publish_stock_adjustment_lineages(
            [lineage, lineage],
            remove_basis_ids={},
        )


def test_publish_rejects_inverted_basis_interval(market_db: MarketDb) -> None:
    lineage = _lineage()
    inverted = StockAdjustmentLineage(
        code=lineage.code,
        bases=(
            StockAdjustmentBasis(
                **{
                    **lineage.bases[0].__dict__,
                    "valid_to_exclusive": "2024-01-03",
                }
            ),
        ),
        segments=tuple(
            segment
            for segment in lineage.segments
            if segment.basis_id == lineage.bases[0].basis_id
        ),
    )

    with pytest.raises(ValueError, match="invalid basis interval"):
        market_db.publish_stock_adjustment_lineages([inverted], remove_basis_ids={})


def test_publish_rejects_inverted_segment_interval(market_db: MarketDb) -> None:
    lineage = _lineage()
    basis = lineage.bases[0]
    inverted_segment = StockAdjustmentBasisSegment(
        code=lineage.code,
        basis_id=basis.basis_id,
        source_date_from="2024-01-04",
        source_date_to_exclusive="2024-01-03",
        cumulative_factor=1.0,
    )
    malformed = StockAdjustmentLineage(
        code=lineage.code,
        bases=(basis,),
        segments=(inverted_segment,),
    )

    with pytest.raises(ValueError, match="invalid basis segment interval"):
        market_db.publish_stock_adjustment_lineages([malformed], remove_basis_ids={})


def test_partial_publish_rejects_overlap_with_retained_catalog_atomically(
    market_db: MarketDb,
) -> None:
    _publish_two_regimes(market_db)
    before = market_db._fetchall(
        """
        SELECT code, basis_id, valid_from, valid_to_exclusive, status
        FROM stock_adjustment_bases
        ORDER BY code, valid_from
        """
    )
    overlapping_basis = StockAdjustmentBasis(
        code="7203",
        basis_id="event-pit-v1:7203:2024-06-01",
        valid_from="2024-06-01",
        valid_to_exclusive=None,
        adjustment_through_date="2024-06-01",
        source_fingerprint="partial-overlap",
        materialized_through_date="2024-12-30",
        status="ready",
    )
    overlapping = StockAdjustmentLineage(
        code="7203",
        bases=(overlapping_basis,),
        segments=(
            StockAdjustmentBasisSegment(
                code="7203",
                basis_id=overlapping_basis.basis_id,
                source_date_from="2024-01-04",
                source_date_to_exclusive=None,
                cumulative_factor=1.0,
            ),
        ),
    )

    with pytest.raises(ValueError, match="overlapping basis intervals"):
        market_db.publish_stock_adjustment_lineages([overlapping], remove_basis_ids={})

    assert market_db._fetchall(
        """
        SELECT code, basis_id, valid_from, valid_to_exclusive, status
        FROM stock_adjustment_bases
        ORDER BY code, valid_from
        """
    ) == before


class _FailSecondRegistrationConnection:
    def __init__(self, conn: object) -> None:
        self._conn = conn
        self._register_calls = 0

    def register(self, name: str, frame: object) -> None:
        self._register_calls += 1
        if self._register_calls == 2:
            raise RuntimeError("injected second registration failure")
        self._conn.register(name, frame)  # type: ignore[attr-defined]

    def unregister(self, name: str) -> None:
        self._conn.unregister(name)  # type: ignore[attr-defined]

    def execute(self, sql: str, params: object = None) -> object:
        if params is None:
            return self._conn.execute(sql)  # type: ignore[attr-defined]
        return self._conn.execute(sql, params)  # type: ignore[attr-defined]


def test_second_registration_failure_cleans_first_relation_and_allows_retry(
    market_db: MarketDb,
) -> None:
    lineage = _lineage()
    failing_conn = _FailSecondRegistrationConnection(market_db._conn)

    with pytest.raises(RuntimeError, match="injected second registration failure"):
        adjustment_basis_writers.publish_stock_adjustment_lineages(
            failing_conn,
            market_db._lock,
            [lineage],
            remove_basis_ids={},
        )

    assert market_db._fetchone(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name IN (
            '__staged_stock_adjustment_bases',
            '__staged_stock_adjustment_basis_segments'
        )
        """
    ) == (0,)
    market_db.publish_stock_adjustment_lineages([lineage], remove_basis_ids={})


def test_publish_rolls_back_orphan_removal_when_catalog_upsert_fails(
    market_db: MarketDb,
) -> None:
    lineage = _publish_two_regimes(market_db)
    first_id = lineage.bases[0].basis_id
    invalid_basis = StockAdjustmentBasis(
        **{
            **lineage.bases[0].__dict__,
            "status": cast(object, "not-a-status"),
        }
    )
    invalid_lineage = StockAdjustmentLineage(
        code=lineage.code,
        bases=(invalid_basis,),
        segments=tuple(
            segment for segment in lineage.segments if segment.basis_id == first_id
        ),
    )

    with pytest.raises(Exception, match="CHECK constraint"):
        market_db.publish_stock_adjustment_lineages(
            [invalid_lineage],
            remove_basis_ids={"7203": [first_id]},
        )

    assert market_db._fetchone(
        "SELECT status FROM stock_adjustment_bases WHERE code = '7203' AND basis_id = ?",
        [first_id],
    ) == ("ready",)
