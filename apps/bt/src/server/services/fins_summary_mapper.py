"""
Fins Summary Mapper

J-Quants `/fins/summary` のレスポンスを DB 行へ変換する共通ヘルパー。
"""

from __future__ import annotations

from typing import Any

from src.lib.market_db.query_helpers import normalize_stock_code


def _to_nullable_float(value: Any) -> float | None:
    """Convert J-Quants numeric payload values to float or None.

    J-Quants can return empty string for optional numeric fields.
    SQLAlchemy REAL columns reject empty string, so normalize here.
    """
    if value is None:
        return None

    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return None
        try:
            return float(stripped)
        except ValueError:
            return None

    if isinstance(value, bool):
        return float(value)

    if isinstance(value, (int, float)):
        return float(value)

    return None


def _first_numeric(item: dict[str, Any], *keys: str) -> float | None:
    """指定キー群から最初に解釈できる数値を返す。"""
    for key in keys:
        value = _to_nullable_float(item.get(key))
        if value is not None:
            return value
    return None


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
            "earnings_per_share": _to_nullable_float(item.get("EPS")),
            "profit": _to_nullable_float(item.get("NP")),
            "equity": _to_nullable_float(item.get("Eq")),
            "type_of_current_period": item.get("CurPerType"),
            "type_of_document": item.get("DocType"),
            "next_year_forecast_earnings_per_share": _to_nullable_float(item.get("NxFEPS")),
            "bps": _to_nullable_float(item.get("BPS")),
            "sales": _to_nullable_float(item.get("Sales")),
            "operating_profit": _to_nullable_float(item.get("OP")),
            "ordinary_profit": _to_nullable_float(item.get("OdP")),
            "operating_cash_flow": _to_nullable_float(item.get("CFO")),
            "dividend_fy": _first_numeric(item, "DivAnn", "DivFY"),
            "forecast_dividend_fy": _first_numeric(item, "FDivAnn", "FDivFY"),
            "next_year_forecast_dividend_fy": _first_numeric(
                item, "NxFDivAnn", "NxFDivFY"
            ),
            "payout_ratio": _to_nullable_float(item.get("PayoutRatioAnn")),
            "forecast_payout_ratio": _to_nullable_float(item.get("FPayoutRatioAnn")),
            "next_year_forecast_payout_ratio": _to_nullable_float(
                item.get("NxFPayoutRatioAnn")
            ),
            "forecast_eps": _to_nullable_float(item.get("FEPS")),
            "investing_cash_flow": _to_nullable_float(item.get("CFI")),
            "financing_cash_flow": _to_nullable_float(item.get("CFF")),
            "cash_and_equivalents": _to_nullable_float(item.get("CashEq")),
            "total_assets": _to_nullable_float(item.get("TA")),
            "shares_outstanding": _to_nullable_float(item.get("ShOutFY")),
            "treasury_shares": _to_nullable_float(item.get("TrShFY")),
        })

    return rows
