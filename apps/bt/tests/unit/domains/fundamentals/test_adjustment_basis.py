from __future__ import annotations

import pytest

from src.domains.fundamentals.adjustment_basis import (
    RawAdjustmentPoint,
    build_stock_adjustment_lineage,
)


def test_build_lineage_creates_origin_and_split_regimes() -> None:
    rows = [
        RawAdjustmentPoint("72030", "2024-01-04", 1.0),
        RawAdjustmentPoint("7203", "2024-06-28", 0.5),
        RawAdjustmentPoint("7203", "2025-03-31", 2.0),
    ]

    lineage = build_stock_adjustment_lineage("72030", rows)

    assert lineage.code == "7203"
    assert [basis.valid_from for basis in lineage.bases] == [
        "2024-01-04",
        "2024-06-28",
        "2025-03-31",
    ]
    assert lineage.bases[0].valid_to_exclusive == "2024-06-28"
    assert lineage.bases[1].basis_id == "event-pit-v1:7203:2024-06-28"
    assert lineage.bases[-1].valid_to_exclusive is None


def test_segments_use_only_events_after_source_through_basis_date() -> None:
    lineage = build_stock_adjustment_lineage(
        "7203",
        [
            RawAdjustmentPoint("7203", "2024-01-04", 1.0),
            RawAdjustmentPoint("72030", "2024-06-28", 0.5),
            RawAdjustmentPoint("7203", "2025-03-31", 2.0),
        ],
    )

    latest_segments = [
        segment
        for segment in lineage.segments
        if segment.basis_id == "event-pit-v1:7203:2025-03-31"
    ]
    assert [
        (
            segment.source_date_from,
            segment.source_date_to_exclusive,
            segment.cumulative_factor,
        )
        for segment in latest_segments
    ] == [
        ("2024-01-04", "2024-06-28", 1.0),
        ("2024-06-28", "2025-03-31", 2.0),
        ("2025-03-31", None, 1.0),
    ]


def test_invalid_factor_invalidates_forward_lineage() -> None:
    lineage = build_stock_adjustment_lineage(
        "7203",
        [
            RawAdjustmentPoint("7203", "2024-01-04", 1.0),
            RawAdjustmentPoint("7203", "2024-06-28", 0.0),
            RawAdjustmentPoint("7203", "2025-03-31", 2.0),
        ],
    )

    assert [basis.status for basis in lineage.bases] == ["ready", "invalid", "invalid"]


def test_same_normalized_date_with_conflicting_factors_is_rejected() -> None:
    with pytest.raises(ValueError, match="conflicting adjustment factors"):
        build_stock_adjustment_lineage(
            "72030",
            [
                RawAdjustmentPoint("7203", "2024-06-28", 0.5),
                RawAdjustmentPoint("72030", "2024-06-28", 2.0),
            ],
        )


def test_fingerprint_is_deterministic_after_alias_normalization_and_sorting() -> None:
    ordered = build_stock_adjustment_lineage(
        "7203",
        [
            RawAdjustmentPoint("7203", "2024-01-04", 1.0),
            RawAdjustmentPoint("72030", "2024-06-28", 0.5),
        ],
    )
    reversed_with_duplicate = build_stock_adjustment_lineage(
        "72030",
        [
            RawAdjustmentPoint("7203", "2024-06-28", 0.5),
            RawAdjustmentPoint("72030", "2024-01-04", 1.0),
            RawAdjustmentPoint("7203", "2024-01-04", 1.0),
        ],
    )

    assert ordered.bases[-1].source_fingerprint == reversed_with_duplicate.bases[-1].source_fingerprint
