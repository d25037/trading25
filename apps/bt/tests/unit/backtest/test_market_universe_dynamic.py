from __future__ import annotations

import pandas as pd

from src.domains.backtest.core import market_universe
from src.infrastructure.db.market.market_db import MarketDb


def _upsert_master(db: MarketDb, snapshot_date: str, rows: list[dict[str, object]]) -> None:
    db.upsert_stock_master_daily(
        snapshot_date,
        [
            {
                "company_name": str(row["code"]),
                "market_name": str(row["market_name"]),
                "sector_17_code": "1",
                "sector_17_name": "sector17",
                "sector_33_code": "0050",
                "sector_33_name": "sector33",
                "listed_date": "2000-01-01",
                "created_at": "now",
                **row,
            }
            for row in rows
        ],
    )


def test_resolve_backtest_universe_codes_returns_date_range_superset(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "market.duckdb"
    db = MarketDb(str(db_path))
    try:
        _upsert_master(
            db,
            "2024-01-05",
            [
                {
                    "code": "1301",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "scale_category": None,
                },
                {
                    "code": "1400",
                    "market_code": "0112",
                    "market_name": "スタンダード",
                    "scale_category": None,
                },
            ],
        )
        _upsert_master(
            db,
            "2024-01-06",
            [
                {
                    "code": "1301",
                    "market_code": "0112",
                    "market_name": "スタンダード",
                    "scale_category": None,
                },
                {
                    "code": "1400",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "scale_category": None,
                },
            ],
        )
    finally:
        db.close()

    monkeypatch.setattr(market_universe, "_market_db_path", lambda: db_path)
    shared_config = {
        "data_source": "market",
        "universe_preset": "prime",
        "stock_codes": ["all"],
        "start_date": "2024-01-05",
        "end_date": "2024-01-06",
    }

    codes = market_universe.resolve_backtest_universe_codes(shared_config)

    assert codes == ["1301", "1400"]
    assert shared_config["universe_provenance"]["asOfDate"] == "2024-01-05..2024-01-06"


def test_build_dynamic_universe_eligibility_frame_uses_each_entry_date(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "market.duckdb"
    db = MarketDb(str(db_path))
    try:
        _upsert_master(
            db,
            "2024-01-05",
            [
                {
                    "code": "1301",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "scale_category": None,
                },
                {
                    "code": "1400",
                    "market_code": "0112",
                    "market_name": "スタンダード",
                    "scale_category": None,
                },
            ],
        )
        _upsert_master(
            db,
            "2024-01-06",
            [
                {
                    "code": "1301",
                    "market_code": "0112",
                    "market_name": "スタンダード",
                    "scale_category": None,
                },
                {
                    "code": "1400",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "scale_category": None,
                },
            ],
        )
    finally:
        db.close()

    monkeypatch.setattr(market_universe, "_market_db_path", lambda: db_path)

    eligibility = market_universe.build_dynamic_universe_eligibility_frame(
        {
            "data_source": "market",
            "universe_preset": "prime",
            "static_universe": False,
        },
        index=pd.DatetimeIndex(pd.to_datetime(["2024-01-05", "2024-01-06"])),
        columns=["1301", "1400"],
    )

    assert eligibility.to_dict(orient="list") == {
        "1301": [True, False],
        "1400": [False, True],
    }


def test_prime_ex_topix500_dynamic_gate_rejects_standard_on_entry_date(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "market.duckdb"
    db = MarketDb(str(db_path))
    try:
        _upsert_master(
            db,
            "2024-01-05",
            [
                {
                    "code": "1301",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "scale_category": None,
                },
                {
                    "code": "1400",
                    "market_code": "0112",
                    "market_name": "スタンダード",
                    "scale_category": None,
                },
                {
                    "code": "1500",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "scale_category": "TOPIX Mid400",
                },
            ],
        )
        _upsert_master(
            db,
            "2024-01-06",
            [
                {
                    "code": "1301",
                    "market_code": "0112",
                    "market_name": "スタンダード",
                    "scale_category": None,
                },
                {
                    "code": "1400",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "scale_category": None,
                },
                {
                    "code": "1500",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "scale_category": "TOPIX Mid400",
                },
            ],
        )
    finally:
        db.close()

    monkeypatch.setattr(market_universe, "_market_db_path", lambda: db_path)

    eligibility = market_universe.build_dynamic_universe_eligibility_frame(
        {
            "data_source": "market",
            "universe_preset": "primeExTopix500",
            "static_universe": False,
        },
        index=pd.DatetimeIndex(pd.to_datetime(["2024-01-05", "2024-01-06"])),
        columns=["1301", "1400", "1500"],
    )

    assert eligibility.to_dict(orient="list") == {
        "1301": [True, False],
        "1400": [False, True],
        "1500": [False, False],
    }


def test_dynamic_universe_eligibility_frame_applies_code_filters(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "market.duckdb"
    db = MarketDb(str(db_path))
    try:
        _upsert_master(
            db,
            "2024-01-05",
            [
                {
                    "code": "1301",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "scale_category": None,
                },
                {
                    "code": "1400",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "scale_category": None,
                },
            ],
        )
    finally:
        db.close()

    monkeypatch.setattr(market_universe, "_market_db_path", lambda: db_path)

    eligibility = market_universe.build_dynamic_universe_eligibility_frame(
        {
            "data_source": "market",
            "universe_preset": "prime",
            "universe_filters": {"codes": ["1301"]},
            "static_universe": False,
        },
        index=pd.DatetimeIndex(pd.to_datetime(["2024-01-05"])),
        columns=["1301", "1400"],
    )

    assert eligibility.to_dict(orient="list") == {
        "1301": [True],
        "1400": [False],
    }


def test_dynamic_universe_eligibility_frame_static_and_custom_paths(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "market.duckdb"
    MarketDb(str(db_path)).close()
    monkeypatch.setattr(market_universe, "_market_db_path", lambda: db_path)
    index = pd.DatetimeIndex(pd.to_datetime(["2024-01-05", "2024-01-06"]))

    static_eligibility = market_universe.build_dynamic_universe_eligibility_frame(
        {
            "data_source": "market",
            "universe_preset": "prime",
            "static_universe": True,
        },
        index=index,
        columns=["1301", "1400"],
    )
    custom_eligibility = market_universe.build_dynamic_universe_eligibility_frame(
        {
            "data_source": "market",
            "universe_preset": "custom",
            "universe_filters": {"codes": ["1301"]},
            "static_universe": False,
        },
        index=index,
        columns=["1301", "1400"],
    )

    assert static_eligibility.to_dict(orient="list") == {
        "1301": [True, True],
        "1400": [True, True],
    }
    assert custom_eligibility.to_dict(orient="list") == {
        "1301": [True, True],
        "1400": [False, False],
    }
