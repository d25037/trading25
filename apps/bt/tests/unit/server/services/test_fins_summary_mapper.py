from __future__ import annotations

from src.application.services.fins_summary_mapper import convert_fins_summary_rows


def test_convert_fins_summary_rows_normalizes_empty_numeric_values() -> None:
    rows = convert_fins_summary_rows(
        [
            {
                "Code": "13010",
                "DiscDate": "2026-02-10",
                "CurPerSt": "2025-04-01",
                "CurPerEn": "2026-03-31",
                "EPS": "",
                "NP": "123.4",
                "Eq": None,
                "NxFEPS": " ",
                "BPS": "1000",
                "Sales": "2000",
                "OP": "",
                "FOP": "3500",
                "NxFOP": "4200",
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
    assert row["forecast_operating_profit"] == 3500.0
    assert row["next_year_forecast_operating_profit"] == 4200.0
    assert row["forecast_eps"] is None


def test_convert_fins_summary_rows_skips_rows_missing_required_fields() -> None:
    rows = convert_fins_summary_rows(
        [
            {
                "Code": "72030",
                "CurPerSt": "2025-04-01",
                "CurPerEn": "2026-03-31",
                "EPS": 100,
            },
            {
                "DiscDate": "2026-02-10",
                "CurPerSt": "2025-04-01",
                "CurPerEn": "2026-03-31",
                "EPS": 100,
            },
        ]
    )

    assert rows == []


def test_convert_fins_summary_rows_preserves_provider_disclosure_identity() -> None:
    rows = convert_fins_summary_rows(
        [
            {
                "Code": "72030",
                "DiscDate": "2026-02-10",
                "DiscTime": "15:30:00",
                "DiscNo": "20260210123456",
                "CurPerSt": "2025-04-01",
                "CurPerEn": "2026-03-31",
                "CurPerType": "FY",
                "DocType": "FYFinancialStatements_Consolidated_JP",
                "EPS": "100",
                "DEPS": "98.5",
                "Sales": "2500000000",
                "PayoutRatioAnn": "28.3",
            }
        ]
    )

    assert rows == [
        {
            **rows[0],
            "code": "7203",
            "statement_id": "20260210123456",
            "disclosure_number": "20260210123456",
            "disclosed_date": "2026-02-10",
            "disclosed_at": "2026-02-10T15:30:00+09:00",
            "period_start": "2025-04-01",
            "period_end": "2026-03-31",
            "type_of_current_period": "FY",
            "type_of_document": "FYFinancialStatements_Consolidated_JP",
            "earnings_per_share": 100.0,
            "diluted_earnings_per_share": 98.5,
            "sales": 2_500_000_000.0,
            "payout_ratio": 28.3,
        }
    ]


def test_fallback_statement_id_uses_identity_not_correctable_values() -> None:
    identity = {
        "Code": "72030",
        "DiscDate": "2026-02-10",
        "DiscTime": "15:30:00",
        "CurPerSt": "2025-04-01",
        "CurPerEn": "2026-03-31",
        "CurPerType": "FY",
        "DocType": "FYFinancialStatements_Consolidated_JP",
    }

    original = convert_fins_summary_rows([{**identity, "EPS": 100.0, "Sales": 1000.0}])[0]
    corrected = convert_fins_summary_rows([{**identity, "EPS": 101.0, "Sales": 1100.0}])[0]

    assert original["statement_id"] == corrected["statement_id"]
    assert original["statement_id"].startswith("fallback:")
    assert original["disclosure_number"] is None


def test_same_day_distinct_documents_receive_distinct_fallback_ids() -> None:
    common = {
        "Code": "72030",
        "DiscDate": "2026-02-10",
        "DiscTime": "15:30:00",
        "CurPerSt": "2025-04-01",
        "CurPerEn": "2026-03-31",
        "CurPerType": "FY",
    }

    rows = convert_fins_summary_rows(
        [
            {**common, "DocType": "EarnForecastRevision", "FEPS": 120.0},
            {**common, "DocType": "DividendForecastRevision", "FDivAnn": 40.0},
        ]
    )

    assert len(rows) == 2
    assert rows[0]["statement_id"] != rows[1]["statement_id"]


def test_fallback_statement_id_canonicalizes_equivalent_disclosure_times() -> None:
    identity = {
        "Code": "72030",
        "DiscDate": "2026-02-10",
        "CurPerSt": "2025-04-01",
        "CurPerEn": "2026-03-31",
        "CurPerType": "FY",
        "DocType": "EarnForecastRevision",
    }

    short_time = convert_fins_summary_rows([{**identity, "DiscTime": "15:30"}])[0]
    full_time = convert_fins_summary_rows([{**identity, "DiscTime": "15:30:00"}])[0]

    assert short_time["disclosed_at"] == full_time["disclosed_at"]
    assert short_time["statement_id"] == full_time["statement_id"]
