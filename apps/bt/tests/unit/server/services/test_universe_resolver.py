from __future__ import annotations

from typing import Any

import pytest

from src.application.services.universe_resolver import (
    UniverseResolutionError,
    resolve_universe,
)
from src.infrastructure.db.market.market_db import MarketDb


@pytest.fixture
def market_db(tmp_path) -> MarketDb:  # type: ignore[no-untyped-def]
    db = MarketDb(str(tmp_path / "market.duckdb"))
    try:
        yield db
    finally:
        db.close()


def _insert_master(db: MarketDb, snapshot_date: str) -> None:
    db.upsert_stock_master_daily(
        snapshot_date,
        [
            {
                "code": "1301",
                "company_name": "Prime Small",
                "market_code": "0111",
                "market_name": "プライム",
                "sector_17_code": "1",
                "sector_17_name": "食品",
                "sector_33_code": "0050",
                "sector_33_name": "水産・農林業",
                "scale_category": None,
                "listed_date": "1949-01-01",
                "created_at": "now",
            },
            {
                "code": "7203",
                "company_name": "Toyota",
                "market_code": "0111",
                "market_name": "プライム",
                "sector_17_code": "6",
                "sector_17_name": "自動車",
                "sector_33_code": "3700",
                "sector_33_name": "輸送用機器",
                "scale_category": "TOPIX Core30",
                "listed_date": "1949-05-16",
                "created_at": "now",
            },
            {
                "code": "6758",
                "company_name": "Sony",
                "market_code": "0111",
                "market_name": "プライム",
                "sector_17_code": "9",
                "sector_17_name": "電機・精密",
                "sector_33_code": "3650",
                "sector_33_name": "電気機器",
                "scale_category": "TOPIX Large70",
                "listed_date": "1958-12-01",
                "created_at": "now",
            },
            {
                "code": "1400",
                "company_name": "Standard Co",
                "market_code": "0112",
                "market_name": "スタンダード",
                "sector_17_code": "10",
                "sector_17_name": "情報通信",
                "sector_33_code": "5250",
                "sector_33_name": "情報・通信業",
                "scale_category": None,
                "listed_date": "2000-01-01",
                "created_at": "now",
            },
            {
                "code": "6666",
                "company_name": "Prime Mid",
                "market_code": "0111",
                "market_name": "プライム",
                "sector_17_code": "6",
                "sector_17_name": "自動車",
                "sector_33_code": "3700",
                "sector_33_name": "輸送用機器",
                "scale_category": "TOPIX Mid400",
                "listed_date": "2001-01-01",
                "created_at": "now",
            },
        ],
    )


def test_resolve_market_and_topix100_universes_from_stock_master_daily(market_db: MarketDb) -> None:
    _insert_master(market_db, "2024-01-05")

    prime = resolve_universe(market_db, as_of_date="2024-01-05", preset="prime")
    topix100 = resolve_universe(market_db, as_of_date="2024-01-05", preset="topix100")

    assert prime.codes == ["1301", "6666", "6758", "7203"]
    assert prime.provenance.sourceTable == "stock_master_daily"
    assert prime.provenance.filters["marketCodes"] == ["0111"]
    assert topix100.codes == ["6758", "7203"]
    assert topix100.provenance.filters["scaleCategories"] == ["TOPIX Core30", "TOPIX Large70"]


def test_resolve_universe_does_not_fallback_to_latest_snapshot(market_db: MarketDb) -> None:
    _insert_master(market_db, "2024-01-05")
    market_db.rebuild_stocks_latest()

    result = resolve_universe(market_db, as_of_date="2024-01-04", preset="prime")

    assert result.codes == []
    assert result.provenance.warnings == [
        "stock_master_daily has no exact rows for preset=prime as_of_date=2024-01-04; latest fallback was not used"
    ]


def test_resolve_prime_ex_topix500_uses_stock_master_scale_categories(market_db: MarketDb) -> None:
    _insert_master(market_db, "2024-01-05")

    result = resolve_universe(market_db, as_of_date="2024-01-05", preset="primeExTopix500")

    assert result.codes == ["1301"]
    assert result.provenance.sourceTable == "stock_master_daily"
    assert result.provenance.filters["marketCodes"] == ["0111"]
    assert result.provenance.filters["excludeScaleCategories"] == [
        "TOPIX Core30",
        "TOPIX Large70",
        "TOPIX Mid400",
    ]


class _SpyUniverseDb:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def get_stock_master_codes_for_date(
        self,
        as_of_date: str,
        *,
        market_codes: list[str] | None = None,
        scale_categories: list[str] | None = None,
        exclude_scale_categories: list[str] | None = None,
    ) -> list[str]:
        self.calls.append(
            {
                "as_of_date": as_of_date,
                "market_codes": market_codes,
                "scale_categories": scale_categories,
                "exclude_scale_categories": exclude_scale_categories,
            }
        )
        return ["1301"]


def test_resolve_prime_ex_topix500_uses_single_exclusion_query() -> None:
    db = _SpyUniverseDb()

    result = resolve_universe(db, as_of_date="2024-01-05", preset="primeExTopix500")

    assert result.codes == ["1301"]
    assert db.calls == [
        {
            "as_of_date": "2024-01-05",
            "market_codes": ["0111"],
            "scale_categories": None,
            "exclude_scale_categories": [
                "TOPIX Core30",
                "TOPIX Large70",
                "TOPIX Mid400",
            ],
        }
    ]


def test_resolve_custom_universe_requires_codes() -> None:
    with pytest.raises(UniverseResolutionError) as exc_info:
        resolve_universe(object(), as_of_date="2024-01-05", preset="custom")  # type: ignore[arg-type]

    assert exc_info.value.code == "universe.custom_codes_required"
