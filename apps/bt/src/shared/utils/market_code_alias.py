"""
Market Code Alias Utilities

市場コードの表現ゆれを吸収する。

current:
- 0111 Prime / 0112 Standard / 0113 Growth

historical JPX segments before the 2022 market restructuring:
- 0101 東証一部 -> Prime proxy
- 0102 東証二部, 0106 JASDAQ Standard -> Standard proxy
- 0104 Mothers, 0107 JASDAQ Growth -> Growth proxy
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

CANONICAL_MARKET_SCOPES: tuple[str, ...] = ("prime", "standard", "growth")
CURRENT_MARKET_CODES_BY_SCOPE: dict[str, tuple[str, ...]] = {
    "prime": ("prime", "0111"),
    "standard": ("standard", "0112"),
    "growth": ("growth", "0113"),
}
HISTORICAL_MARKET_CODES_BY_SCOPE: dict[str, tuple[str, ...]] = {
    "prime": ("0101",),
    "standard": ("0102", "0106"),
    "growth": ("0104", "0107"),
}
MARKET_CODES_BY_SCOPE: dict[str, tuple[str, ...]] = {
    scope: (
        *CURRENT_MARKET_CODES_BY_SCOPE[scope],
        *HISTORICAL_MARKET_CODES_BY_SCOPE[scope],
    )
    for scope in CANONICAL_MARKET_SCOPES
}
MARKET_CODE_ALIASES: dict[str, tuple[str, ...]] = {
    alias: aliases
    for aliases in MARKET_CODES_BY_SCOPE.values()
    for alias in aliases
}
MARKET_ALIAS_TO_CANONICAL: dict[str, str] = {
    alias: scope
    for scope, aliases in MARKET_CODES_BY_SCOPE.items()
    for alias in aliases
}
MARKET_ALIAS_TO_CANONICAL.update(
    {
        "プライム": "prime",
        "東証一部": "prime",
        "市場第一部": "prime",
        "standard market": "standard",
        "スタンダード": "standard",
        "東証二部": "standard",
        "jasdaq スタンダード": "standard",
        "jasdaq standard": "standard",
        "growth market": "growth",
        "グロース": "growth",
        "マザーズ": "growth",
        "jasdaq グロース": "growth",
        "jasdaq growth": "growth",
    }
)
MARKET_SCOPE_LABELS: dict[str, str] = {
    "prime": "Prime",
    "standard": "Standard",
    "growth": "Growth",
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


def expand_market_codes(
    market_codes: Sequence[str],
    *,
    include_legacy: bool = True,
) -> list[str]:
    """市場コードを alias 展開し、重複を除去して返す。"""
    expanded: list[str] = []
    seen: set[str] = set()

    for market_code in market_codes:
        alias_key = market_code.strip().lower()
        canonical = MARKET_ALIAS_TO_CANONICAL.get(alias_key)
        if canonical is None:
            candidates = (market_code,)
        elif include_legacy:
            candidates = MARKET_CODE_ALIASES[canonical]
        else:
            candidates = CURRENT_MARKET_CODES_BY_SCOPE[canonical]
        for candidate in candidates:
            if candidate in seen:
                continue
            expanded.append(candidate)
            seen.add(candidate)

    return expanded


def resolve_market_codes(
    markets: str,
    fallback: list[str] | None = None,
    *,
    include_legacy: bool = True,
) -> tuple[list[str], list[str]]:
    """入力市場コードとクエリ用市場コードをまとめて解決する。"""
    requested_market_codes = parse_requested_market_codes(markets, fallback=fallback)
    query_market_codes = expand_market_codes(
        requested_market_codes,
        include_legacy=include_legacy,
    )
    return requested_market_codes, query_market_codes


def normalize_market_scope(
    market_code: object | None,
    *,
    market_name: object | None = None,
    default: str | None = None,
) -> str | None:
    """市場コード/市場名を research/backtest 用 canonical scope に正規化する。"""
    code = str(market_code or "").strip().lower()
    if code:
        canonical = MARKET_ALIAS_TO_CANONICAL.get(code)
        if canonical is not None:
            return canonical

    name = str(market_name or "").strip().lower()
    if name:
        canonical = MARKET_ALIAS_TO_CANONICAL.get(name)
        if canonical is not None:
            return canonical
        for needle, scope in (
            ("プライム", "prime"),
            ("prime", "prime"),
            ("東証一部", "prime"),
            ("市場第一部", "prime"),
            ("スタンダード", "standard"),
            ("standard", "standard"),
            ("東証二部", "standard"),
            ("グロース", "growth"),
            ("growth", "growth"),
            ("マザーズ", "growth"),
        ):
            if needle in name:
                return scope
    return default


def canonicalize_market_list(markets: Iterable[str]) -> list[str]:
    """市場リストを canonical order に寄せ、未知値は後ろに保持する。"""
    raw = list(markets)
    normalized_values = [
        normalize_market_scope(market, default=str(market).strip().lower())
        for market in raw
    ]
    canonical: list[str] = []
    seen: set[str] = set()

    for preferred in CANONICAL_MARKET_SCOPES:
        if preferred in normalized_values:
            canonical.append(preferred)
            seen.add(preferred)

    for normalized in normalized_values:
        if normalized is None or normalized in seen:
            continue
        canonical.append(normalized)
        seen.add(normalized)

    return canonical


def format_market_scope_label(markets: Sequence[str]) -> str:
    normalized = canonicalize_market_list(markets)
    if not normalized:
        return "Auto"
    if normalized == list(CANONICAL_MARKET_SCOPES):
        return "All Markets"
    return " + ".join(MARKET_SCOPE_LABELS.get(market, market) for market in normalized)
