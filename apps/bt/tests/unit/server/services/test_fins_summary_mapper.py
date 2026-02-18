from __future__ import annotations

from src.server.services.fins_summary_mapper import convert_fins_summary_rows


def test_convert_fins_summary_rows_normalizes_empty_numeric_values() -> None:
    rows = convert_fins_summary_rows(
        [
            {
                "Code": "13010",
                "DiscDate": "2026-02-10",
                "EPS": "",
                "NP": "123.4",
                "Eq": None,
                "NxFEPS": " ",
                "BPS": "1000",
                "Sales": "2000",
                "OP": "",
                "OdP": "",
                "CFO": "",
                "DivAnn": "",
                "FEPS": "",
                "CFI": "",
                "CFF": "",
                "CashEq": "",
                "TA": "",
                "ShOutFY": "",
                "TrShFY": "",
            }
        ]
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["code"] == "1301"
    assert row["disclosed_date"] == "2026-02-10"
    assert row["earnings_per_share"] is None
    assert row["profit"] == 123.4
    assert row["equity"] is None
    assert row["next_year_forecast_earnings_per_share"] is None
    assert row["bps"] == 1000.0
    assert row["sales"] == 2000.0
    assert row["forecast_eps"] is None


def test_convert_fins_summary_rows_skips_rows_missing_required_fields() -> None:
    rows = convert_fins_summary_rows(
        [
            {"Code": "72030", "EPS": 100},
            {"DiscDate": "2026-02-10", "EPS": 100},
        ]
    )

    assert rows == []

