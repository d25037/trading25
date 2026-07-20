"""
Fins Summary Mapper

J-Quants `/fins/summary` のレスポンスを DB 行へ変換する共通ヘルパー。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from src.infrastructure.db.market.query_helpers import normalize_stock_code


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


def _disclosed_at(disclosed_date: str, disclosed_time: Any) -> str:
    time_text = str(disclosed_time or "").strip() or "00:00:00"
    if len(time_text) == 5 and time_text[2] == ":":
        time_text = f"{time_text}:00"
    if time_text.endswith("Z") or "+" in time_text:
        return f"{disclosed_date}T{time_text}"
    return f"{disclosed_date}T{time_text}+09:00"


def _statement_id(
    item: dict[str, Any],
    *,
    code: str,
    disclosed_date: str,
    disclosed_at: str,
    period_start: str,
    period_end: str,
) -> tuple[str, str | None]:
    disclosure_number_value = item.get("DiscNo")
    if disclosure_number_value is not None:
        disclosure_number = str(disclosure_number_value)
        if disclosure_number.strip():
            return disclosure_number, disclosure_number

    identity = (
        code,
        disclosed_date,
        disclosed_at,
        period_start,
        period_end,
        str(item.get("CurPerType") or ""),
        str(item.get("DocType") or ""),
    )
    digest = hashlib.sha256(
        json.dumps(identity, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    return f"fallback:{digest}", None


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
        disclosed_at = _disclosed_at(disclosed_date, item.get("DiscTime"))
        period_start = str(item.get("CurPerSt") or "")
        period_end = str(item.get("CurPerEn") or "")

        if not code or not disclosed_date:
            continue

        statement_id, disclosure_number = _statement_id(
            item,
            code=code,
            disclosed_date=disclosed_date,
            disclosed_at=disclosed_at,
            period_start=period_start,
            period_end=period_end,
        )

        rows.append({
            "code": code,
            "statement_id": statement_id,
            "disclosure_number": disclosure_number,
            "disclosed_date": disclosed_date,
            "disclosed_at": disclosed_at,
            "period_start": period_start,
            "period_end": period_end,
            "earnings_per_share": _to_nullable_float(item.get("EPS")),
            "diluted_earnings_per_share": _to_nullable_float(item.get("DEPS")),
            "profit": _to_nullable_float(item.get("NP")),
            "equity": _to_nullable_float(item.get("Eq")),
            "type_of_current_period": item.get("CurPerType"),
            "type_of_document": item.get("DocType"),
            "next_year_forecast_earnings_per_share": _to_nullable_float(item.get("NxFEPS")),
            "bps": _to_nullable_float(item.get("BPS")),
            "sales": _to_nullable_float(item.get("Sales")),
            "forecast_sales": _to_nullable_float(item.get("FSales")),
            "next_year_forecast_sales": _to_nullable_float(item.get("NxFSales")),
            "operating_profit": _to_nullable_float(item.get("OP")),
            "forecast_operating_profit": _to_nullable_float(item.get("FOP")),
            "next_year_forecast_operating_profit": _to_nullable_float(
                item.get("NxFOP")
            ),
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
