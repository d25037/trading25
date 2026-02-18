"""
Fins Summary Mapper

J-Quants `/fins/summary` のレスポンスを DB 行へ変換する共通ヘルパー。
"""

from __future__ import annotations

from typing import Any

from src.lib.market_db.query_helpers import normalize_stock_code


def convert_fins_summary_rows(
    data: list[dict[str, Any]],
    *,
    default_code: str | None = None,
) -> list[dict[str, Any]]:
    """`/fins/summary` payload を statements テーブル行に変換"""
    rows: list[dict[str, Any]] = []
    default_code_normalized = normalize_stock_code(default_code or "")

    for item in data:
        code = normalize_stock_code(
            str(item.get("Code") or default_code_normalized or "")
        )
        disclosed_date = str(item.get("DiscDate") or "")

        if not code or not disclosed_date:
            continue

        rows.append({
            "code": code,
            "disclosed_date": disclosed_date,
            "earnings_per_share": item.get("EPS"),
            "profit": item.get("NP"),
            "equity": item.get("Eq"),
            "type_of_current_period": item.get("CurPerType"),
            "type_of_document": item.get("DocType"),
            "next_year_forecast_earnings_per_share": item.get("NxFEPS"),
            "bps": item.get("BPS"),
            "sales": item.get("Sales"),
            "operating_profit": item.get("OP"),
            "ordinary_profit": item.get("OdP"),
            "operating_cash_flow": item.get("CFO"),
            "dividend_fy": item.get("DivAnn"),
            "forecast_eps": item.get("FEPS"),
            "investing_cash_flow": item.get("CFI"),
            "financing_cash_flow": item.get("CFF"),
            "cash_and_equivalents": item.get("CashEq"),
            "total_assets": item.get("TA"),
            "shares_outstanding": item.get("ShOutFY"),
            "treasury_shares": item.get("TrShFY"),
        })

    return rows

