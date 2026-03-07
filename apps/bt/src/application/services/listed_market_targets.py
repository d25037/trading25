from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from src.infrastructure.db.market.query_helpers import normalize_stock_code

LISTED_MARKET_CODES = frozenset({"0111", "0112", "0113", "prime", "standard", "growth"})
_PREFERRED_SHARE_SUFFIXES = frozenset({"5", "6"})
_PREFERRED_SHARE_MARKER = "優先株式"
_ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_BASIC_DATE_PATTERN = re.compile(r"^(\d{4})(\d{2})(\d{2})$")


@dataclass(frozen=True)
class FrontierCodeCache:
    frontier: str | None
    codes: tuple[str, ...] = ()


def normalize_market_code(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def is_listed_market_code(value: Any) -> bool:
    return normalize_market_code(value) in LISTED_MARKET_CODES


def normalize_frontier_date(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if _ISO_DATE_PATTERN.match(text):
        return text
    match = _BASIC_DATE_PATTERN.match(text)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return text


def resolve_fundamentals_canonical_code(
    code: Any,
    *,
    company_name: Any = None,
    market_code: Any = None,
) -> str:
    normalized = normalize_stock_code(str(code or "").strip())
    if not normalized:
        return ""
    if not is_listed_market_code(market_code):
        return normalized

    company_name_text = str(company_name or "").strip()
    if (
        len(normalized) == 5
        and normalized[-1] in _PREFERRED_SHARE_SUFFIXES
        and _PREFERRED_SHARE_MARKER in company_name_text
    ):
        return normalized[:4]
    return normalized


def extract_listed_market_codes(rows: Iterable[Mapping[str, Any]]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for row in rows:
        if not is_listed_market_code(row.get("market_code")):
            continue
        code = normalize_stock_code(str(row.get("code", "")).strip())
        if not code or code in seen:
            continue
        seen.add(code)
        deduped.append(code)
    return deduped


def build_fundamentals_target_map(rows: Iterable[Mapping[str, Any]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for row in rows:
        if not is_listed_market_code(row.get("market_code")):
            continue
        exact_code = normalize_stock_code(str(row.get("code", "")).strip())
        if not exact_code:
            continue
        mapping[exact_code] = resolve_fundamentals_canonical_code(
            exact_code,
            company_name=row.get("company_name"),
            market_code=row.get("market_code"),
        )
    return mapping


def group_target_codes_by_canonical(target_map: Mapping[str, str]) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = {}
    for exact_code, canonical_code in target_map.items():
        grouped.setdefault(canonical_code, []).append(exact_code)
    return {
        canonical_code: tuple(sorted(set(exact_codes)))
        for canonical_code, exact_codes in grouped.items()
    }


def build_fundamentals_coverage(
    target_map: Mapping[str, str],
    statement_codes: set[str],
    *,
    empty_skipped_codes: set[str] | None = None,
    limit_missing: int = 20,
    limit_empty: int = 20,
) -> dict[str, Any]:
    sorted_target_codes = sorted(target_map)
    skipped_codes = set(empty_skipped_codes or set())
    alias_covered_codes = sorted(
        code
        for code in sorted_target_codes
        if target_map[code] != code and target_map[code] in statement_codes
    )
    unresolved_codes = [
        code
        for code in sorted_target_codes
        if target_map[code] not in statement_codes
    ]
    empty_codes = [code for code in unresolved_codes if code in skipped_codes]
    missing_codes = [code for code in unresolved_codes if code not in skipped_codes]
    target_count = len(sorted_target_codes)
    covered_count = target_count - len(unresolved_codes)
    coverage_ratio = round((covered_count / target_count), 4) if target_count > 0 else 0.0

    return {
        "targetCount": target_count,
        "coveredCount": covered_count,
        "missingCount": len(missing_codes),
        "coverageRatio": coverage_ratio,
        "missingCodes": missing_codes[: max(limit_missing, 0)],
        "emptySkippedCount": len(empty_codes),
        "emptySkippedCodes": empty_codes[: max(limit_empty, 0)],
        "issuerAliasCoveredCount": len(alias_covered_codes),
        "issuerAliasCoveredCodes": alias_covered_codes[: max(limit_empty, 0)],
    }


def build_fundamentals_fetch_codes(
    target_map: Mapping[str, str],
    statement_codes: set[str],
    *,
    previous_failed_codes: list[str] | None = None,
    empty_skipped_codes: set[str] | None = None,
) -> list[str]:
    grouped = group_target_codes_by_canonical(target_map)
    skipped_codes = set(empty_skipped_codes or set())
    failed_codes = [
        target_map.get(normalize_stock_code(str(code).strip()), normalize_stock_code(str(code).strip()))
        for code in previous_failed_codes or []
    ]

    fetch_codes: list[str] = []
    seen: set[str] = set()
    for code in failed_codes:
        if not code or code in seen or code in statement_codes:
            continue
        exact_codes = grouped.get(code)
        if exact_codes is not None and all(exact_code in skipped_codes for exact_code in exact_codes):
            continue
        if code and code not in seen:
            seen.add(code)
            fetch_codes.append(code)

    for canonical_code in sorted(grouped):
        exact_codes = grouped[canonical_code]
        if canonical_code in statement_codes:
            continue
        if all(code in skipped_codes for code in exact_codes):
            continue
        if canonical_code in seen:
            continue
        seen.add(canonical_code)
        fetch_codes.append(canonical_code)
    return fetch_codes


def parse_frontier_code_cache(raw: str | None) -> FrontierCodeCache:
    if not raw:
        return FrontierCodeCache(frontier=None, codes=())
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return FrontierCodeCache(frontier=None, codes=())
    if not isinstance(payload, dict):
        return FrontierCodeCache(frontier=None, codes=())

    frontier = normalize_frontier_date(
        payload.get("frontier") if isinstance(payload.get("frontier"), str) else None
    )
    loaded_codes = payload.get("codes")
    if not isinstance(loaded_codes, list):
        return FrontierCodeCache(frontier=frontier, codes=())

    deduped: list[str] = []
    seen: set[str] = set()
    for value in loaded_codes:
        code = normalize_stock_code(str(value).strip())
        if not code or code in seen:
            continue
        seen.add(code)
        deduped.append(code)
    return FrontierCodeCache(frontier=frontier, codes=tuple(deduped))


def resolve_frontier_cache_codes(raw: str | None, frontier: str | None) -> set[str]:
    parsed = parse_frontier_code_cache(raw)
    normalized_frontier = normalize_frontier_date(frontier)
    if parsed.frontier is None or normalized_frontier is None:
        return set()
    if parsed.frontier != normalized_frontier:
        return set()
    return set(parsed.codes)


def serialize_frontier_code_cache(frontier: str | None, codes: Iterable[str]) -> str:
    normalized_frontier = normalize_frontier_date(frontier)
    deduped: list[str] = []
    seen: set[str] = set()
    for value in codes:
        code = normalize_stock_code(str(value).strip())
        if not code or code in seen:
            continue
        seen.add(code)
        deduped.append(code)
    return json.dumps(
        {
            "frontier": normalized_frontier,
            "codes": deduped,
        },
        ensure_ascii=False,
    )
