"""Query helper utilities for market ranking services."""

from __future__ import annotations

from src.shared.utils.market_code_alias import normalize_market_scope


def build_market_filter(market_codes: list[str]) -> tuple[str, list[str]]:
    """マーケットコードのWHERE句を構築"""
    if not market_codes:
        return "", []
    placeholders = ",".join("?" for _ in market_codes)
    return f" AND s.market_code IN ({placeholders})", market_codes


def normalize_middle_dot(text: str) -> str:
    return text.replace("\u30fb", "\uff65")


def normalize_sector_filter_name(text: str) -> str:
    normalized = normalize_middle_dot(text.strip())
    for prefix in ("東証業種別 ", "TOPIX-17 "):
        if normalized.startswith(prefix):
            return normalized.removeprefix(prefix).strip()
    return normalized


def build_stock_scope_filter(
    market_codes: list[str],
    *,
    sector33_name: str | None = None,
    sector17_name: str | None = None,
) -> tuple[str, list[str]]:
    clause, params = build_market_filter(market_codes)
    if sector33_name:
        clause += " AND replace(s.sector_33_name, '・', '･') = ?"
        params.append(normalize_sector_filter_name(sector33_name))
    if sector17_name:
        clause += " AND replace(s.sector_17_name, '・', '･') = ?"
        params.append(normalize_sector_filter_name(sector17_name))
    return clause, params


def normalized_code_sql(column_ref: str) -> str:
    """4桁/5桁コード混在を吸収する正規化SQL式。"""
    return (
        "CASE "
        f"WHEN length({column_ref}) = 5 AND right({column_ref}, 1) = '0' "
        f"THEN left({column_ref}, 4) "
        f"ELSE {column_ref} "
        "END"
    )


def prefer_4digit_order_sql(column_ref: str) -> str:
    return f"CASE WHEN length({column_ref}) = 4 THEN 0 ELSE 1 END"


def canonical_market_label(market_code: str) -> str:
    return str(normalize_market_scope(market_code, default=market_code))


def positive_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None:
        return None
    if denominator <= 0:
        return None
    ratio = numerator / denominator
    return ratio if ratio > 0 else None


def normalize_equity_code(code: object) -> str:
    text = str(code).strip()
    if len(text) == 5 and text.endswith("0"):
        return text[:4]
    return text


def equity_code_variants(code: object) -> tuple[str, ...]:
    normalized = normalize_equity_code(code)
    variants = {normalized}
    if len(normalized) == 4 and normalized.isdigit():
        variants.add(f"{normalized}0")
    return tuple(sorted(variants))


def stocks_canonical_cte() -> str:
    normalized = normalized_code_sql("code")
    order = prefer_4digit_order_sql("code")
    return f"""
        stocks_canonical AS (
            SELECT
                code,
                company_name,
                market_code,
                sector_17_name,
                sector_33_name,
                normalized_code
            FROM (
                SELECT
                    code,
                    company_name,
                    market_code,
                    sector_17_name,
                    sector_33_name,
                    {normalized} AS normalized_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized}
                        ORDER BY {order}
                    ) AS rn
                FROM stock_master_daily
                WHERE date = ?
            )
            WHERE rn = 1
        )
    """


def stock_data_dedup_cte(
    cte_name: str,
    *,
    where_clause: str,
    code_ref: str = "code",
    include_ohlc: bool = True,
) -> str:
    normalized = normalized_code_sql(code_ref)
    order = prefer_4digit_order_sql(code_ref)
    select_ohlc = (
        ", open, high, low, close, volume" if include_ohlc else ", close, volume"
    )
    return f"""
        {cte_name} AS (
            SELECT
                normalized_code,
                date
                {select_ohlc}
            FROM (
                SELECT
                    {normalized} AS normalized_code,
                    date
                    {select_ohlc},
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized}, date
                        ORDER BY {order}
                    ) AS rn
                FROM stock_data
                WHERE {where_clause}
            )
            WHERE rn = 1
        )
    """


def limit_clause(limit: int) -> tuple[str, tuple[int, ...]]:
    if limit <= 0:
        return "", ()
    return " LIMIT ?", (limit,)
