from __future__ import annotations

from src.application.services.index_master_catalog import (
    _in_hex_range,
    _resolve_category,
    build_index_master_seed_rows,
    get_index_catalog_codes,
)


def test_in_hex_range_handles_true_false_and_invalid() -> None:
    assert _in_hex_range("004A", "0040", "0060")
    assert not _in_hex_range("003F", "0040", "0060")
    assert not _in_hex_range("not-hex", "0040", "0060")


def test_resolve_category_covers_all_known_buckets() -> None:
    assert _resolve_category("0000") == "topix"
    assert _resolve_category("0500") == "market"
    assert _resolve_category("8100") == "style"
    assert _resolve_category("004A") == "sector33"
    assert _resolve_category("008B") == "sector17"
    assert _resolve_category("0999") == "unknown"


def test_get_index_catalog_codes_contains_known_codes() -> None:
    codes = get_index_catalog_codes()
    assert "0000" in codes
    assert "0040" in codes
    assert "8503" in codes
    assert len(codes) > 60


def test_build_index_master_seed_rows_respects_existing_codes() -> None:
    rows = build_index_master_seed_rows(existing_codes={"0000", "0040"})

    codes = {str(row["code"]) for row in rows}
    assert "0000" not in codes
    assert "0040" not in codes
    assert "0500" in codes

    sample = next(row for row in rows if row["code"] == "0500")
    assert sample["name"] == "東証プライム市場指数"
    assert sample["category"] == "market"
    assert sample["data_start_date"] is None
    assert sample["created_at"]


def test_build_index_master_seed_rows_returns_all_when_existing_empty() -> None:
    rows = build_index_master_seed_rows()
    codes = {str(row["code"]) for row in rows}
    assert "0000" in codes
    assert "0080" in codes
    assert "812C" in codes
