from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest

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


def test_ready_basis_resolution_requires_interval_and_coverage(market_db: MarketDb) -> None:
    _publish_two_regimes(market_db)

    first = market_db.get_ready_adjustment_basis("72030", "2024-06-27")
    second = market_db.get_ready_adjustment_basis("7203", "2024-06-28")

    assert first is not None and first["valid_from"] == "2024-01-04"
    assert second is not None and second["valid_from"] == "2024-06-28"
    assert market_db.get_ready_adjustment_basis("7203", "2026-01-01") is None


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
