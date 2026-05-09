from typing import Any

from src.domains.fundamentals.statement_adapter import (
    market_statement_row_to_jquants_statement,
)


class DuckRowLike:
    def __init__(self, values: dict[str, Any]) -> None:
        self._values = values

    def __getitem__(self, key: str) -> Any:
        return self._values[key]


def test_market_statement_adapter_reads_mapping_rows() -> None:
    stmt = market_statement_row_to_jquants_statement(
        {
            "code": "3861",
            "disclosed_date": "2026-02-06",
            "type_of_current_period": "3Q",
            "earnings_per_share": 33.81,
            "forecast_eps": 54.25,
            "bps": 1164.1,
            "shares_outstanding": 1014381817.0,
        }
    )

    assert stmt.Code == "3861"
    assert stmt.DiscDate == "2026-02-06"
    assert stmt.CurPerType == "3Q"
    assert stmt.EPS == 33.81
    assert stmt.FEPS == 54.25
    assert stmt.BPS == 1164.1
    assert stmt.ShOutFY == 1014381817.0


def test_market_statement_adapter_reads_duckdb_row_like_objects() -> None:
    stmt = market_statement_row_to_jquants_statement(
        DuckRowLike(
            {
                "code": "3861",
                "disclosed_date": "2026-02-06",
                "type_of_current_period": "3Q",
                "earnings_per_share": 33.81,
                "forecast_eps": 54.25,
                "bps": 1164.1,
                "shares_outstanding": 1014381817.0,
            }
        )
    )

    assert stmt.Code == "3861"
    assert stmt.DiscDate == "2026-02-06"
    assert stmt.CurPerType == "3Q"
    assert stmt.EPS == 33.81
    assert stmt.FEPS == 54.25
    assert stmt.BPS == 1164.1
    assert stmt.ShOutFY == 1014381817.0
