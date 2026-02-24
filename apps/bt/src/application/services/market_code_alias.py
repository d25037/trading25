"""
Market Code Alias Utilities

市場コードの表現ゆれ（legacy: prime/standard/growth, current: 0111/0112/0113）を吸収する。
"""

from __future__ import annotations

MARKET_CODE_ALIASES: dict[str, tuple[str, ...]] = {
    "prime": ("prime", "0111"),
    "standard": ("standard", "0112"),
    "growth": ("growth", "0113"),
    "0111": ("prime", "0111"),
    "0112": ("standard", "0112"),
    "0113": ("growth", "0113"),
}


def parse_requested_market_codes(
    markets: str,
    fallback: list[str] | None = None,
) -> list[str]:
    """クエリ文字列を市場コード配列に変換。空なら fallback を返す。"""
    market_codes = [m.strip() for m in markets.split(",") if m.strip()]
    if market_codes:
        return market_codes
    if fallback is not None:
        return fallback[:]
    return ["prime"]


def expand_market_codes(market_codes: list[str]) -> list[str]:
    """市場コードを alias 展開し、重複を除去して返す。"""
    expanded: list[str] = []
    seen: set[str] = set()

    for market_code in market_codes:
        alias_key = market_code.lower()
        candidates = MARKET_CODE_ALIASES.get(alias_key, (market_code,))
        for candidate in candidates:
            if candidate in seen:
                continue
            expanded.append(candidate)
            seen.add(candidate)

    return expanded


def resolve_market_codes(
    markets: str,
    fallback: list[str] | None = None,
) -> tuple[list[str], list[str]]:
    """入力市場コードとクエリ用市場コードをまとめて解決する。"""
    requested_market_codes = parse_requested_market_codes(markets, fallback=fallback)
    query_market_codes = expand_market_codes(requested_market_codes)
    return requested_market_codes, query_market_codes
