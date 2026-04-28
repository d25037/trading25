"""
Universe Resolver

market.duckdb schema v3 の stock_master_daily / index_membership_daily を SoT に、
履歴日付ごとの universe を解決する。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from src.infrastructure.db.market.query_helpers import normalize_stock_code

UniversePreset = Literal["prime", "standard", "growth", "topix100", "primeExTopix500", "custom"]

_MARKET_CODE_BY_PRESET: dict[str, str] = {
    "prime": "0111",
    "standard": "0112",
    "growth": "0113",
}
_TOPIX100_SCALE_CATEGORIES = ("TOPIX Core30", "TOPIX Large70")
_DEFAULT_TOPIX500_INDEX_CODE = "TOPIX500"


class UniverseResolverDbLike(Protocol):
    def get_stock_master_rows_for_date(
        self,
        as_of_date: str,
        *,
        market_codes: list[str] | None = None,
        scale_categories: list[str] | None = None,
    ) -> list[dict[str, Any]]: ...

    def get_index_membership_codes(self, as_of_date: str, index_code: str) -> set[str]: ...


@dataclass(frozen=True)
class UniverseProvenance:
    sourceTable: str
    asOfDate: str
    preset: str
    rowCount: int
    resolvedCount: int
    filters: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class UniverseResolution:
    codes: list[str]
    provenance: UniverseProvenance


class UniverseResolutionError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "universe.unsupported",
        provenance: UniverseProvenance | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.provenance = provenance


def resolve_universe(
    db: UniverseResolverDbLike,
    *,
    as_of_date: str,
    preset: str,
    filters: dict[str, Any] | None = None,
) -> UniverseResolution:
    """Resolve a PIT universe without falling back to latest stock snapshots."""
    normalized_date = as_of_date.strip()
    if not normalized_date:
        raise UniverseResolutionError(
            "as_of_date is required for universe resolution",
            code="universe.as_of_date_required",
        )
    normalized_preset = preset.strip()
    request_filters = dict(filters or {})

    if normalized_preset in _MARKET_CODE_BY_PRESET:
        return _resolve_market_code_universe(
            db,
            as_of_date=normalized_date,
            preset=normalized_preset,
            market_code=_MARKET_CODE_BY_PRESET[normalized_preset],
            filters=request_filters,
        )
    if normalized_preset == "topix100":
        return _resolve_topix100_universe(
            db,
            as_of_date=normalized_date,
            filters=request_filters,
        )
    if normalized_preset == "primeExTopix500":
        return _resolve_prime_ex_topix500_universe(
            db,
            as_of_date=normalized_date,
            filters=request_filters,
        )
    if normalized_preset == "custom":
        return _resolve_custom_universe(as_of_date=normalized_date, filters=request_filters)

    raise UniverseResolutionError(
        f"Unsupported universe preset: {preset}",
        code="universe.preset_unsupported",
    )


def _resolve_market_code_universe(
    db: UniverseResolverDbLike,
    *,
    as_of_date: str,
    preset: str,
    market_code: str,
    filters: dict[str, Any],
) -> UniverseResolution:
    rows = db.get_stock_master_rows_for_date(as_of_date, market_codes=[market_code])
    warnings = _coverage_warnings(rows, as_of_date=as_of_date, preset=preset)
    codes = _apply_code_filters(_codes_from_rows(rows), filters)
    return UniverseResolution(
        codes=codes,
        provenance=UniverseProvenance(
            sourceTable="stock_master_daily",
            asOfDate=as_of_date,
            preset=preset,
            rowCount=len(rows),
            resolvedCount=len(codes),
            filters={"marketCodes": [market_code], **filters},
            warnings=warnings,
        ),
    )


def _resolve_topix100_universe(
    db: UniverseResolverDbLike,
    *,
    as_of_date: str,
    filters: dict[str, Any],
) -> UniverseResolution:
    rows = db.get_stock_master_rows_for_date(
        as_of_date,
        scale_categories=list(_TOPIX100_SCALE_CATEGORIES),
    )
    warnings = _coverage_warnings(rows, as_of_date=as_of_date, preset="topix100")
    codes = _apply_code_filters(_codes_from_rows(rows), filters)
    return UniverseResolution(
        codes=codes,
        provenance=UniverseProvenance(
            sourceTable="stock_master_daily",
            asOfDate=as_of_date,
            preset="topix100",
            rowCount=len(rows),
            resolvedCount=len(codes),
            filters={"scaleCategories": list(_TOPIX100_SCALE_CATEGORIES), **filters},
            warnings=warnings,
        ),
    )


def _resolve_prime_ex_topix500_universe(
    db: UniverseResolverDbLike,
    *,
    as_of_date: str,
    filters: dict[str, Any],
) -> UniverseResolution:
    topix500_index_code = str(filters.get("topix500IndexCode") or _DEFAULT_TOPIX500_INDEX_CODE)
    prime = _resolve_market_code_universe(
        db,
        as_of_date=as_of_date,
        preset="primeExTopix500",
        market_code=_MARKET_CODE_BY_PRESET["prime"],
        filters={},
    )
    topix500_codes = db.get_index_membership_codes(as_of_date, topix500_index_code)
    if not topix500_codes:
        provenance = UniverseProvenance(
            sourceTable="stock_master_daily,index_membership_daily",
            asOfDate=as_of_date,
            preset="primeExTopix500",
            rowCount=prime.provenance.rowCount,
            resolvedCount=0,
            filters={"marketCodes": [_MARKET_CODE_BY_PRESET["prime"]], "topix500IndexCode": topix500_index_code, **filters},
            warnings=[
                *prime.provenance.warnings,
                f"index_membership_daily has no exact TOPIX500 membership for {as_of_date}",
            ],
        )
        raise UniverseResolutionError(
            "Exact TOPIX500 membership is unavailable for the requested as_of_date",
            code="universe.topix500_membership_unavailable",
            provenance=provenance,
        )
    codes = _apply_code_filters(
        [code for code in prime.codes if code not in topix500_codes],
        filters,
    )
    return UniverseResolution(
        codes=codes,
        provenance=UniverseProvenance(
            sourceTable="stock_master_daily,index_membership_daily",
            asOfDate=as_of_date,
            preset="primeExTopix500",
            rowCount=prime.provenance.rowCount,
            resolvedCount=len(codes),
            filters={
                "marketCodes": [_MARKET_CODE_BY_PRESET["prime"]],
                "topix500IndexCode": topix500_index_code,
                "excludedMembershipCount": len(topix500_codes),
                **filters,
            },
            warnings=prime.provenance.warnings,
        ),
    )


def _resolve_custom_universe(*, as_of_date: str, filters: dict[str, Any]) -> UniverseResolution:
    raw_codes = filters.get("codes")
    if not isinstance(raw_codes, list) or not raw_codes:
        raise UniverseResolutionError(
            "custom universe requires filters.codes",
            code="universe.custom_codes_required",
        )
    codes = _normalize_codes(raw_codes)
    return UniverseResolution(
        codes=codes,
        provenance=UniverseProvenance(
            sourceTable="filters.codes",
            asOfDate=as_of_date,
            preset="custom",
            rowCount=len(raw_codes),
            resolvedCount=len(codes),
            filters={"codes": codes},
            warnings=[],
        ),
    )


def _codes_from_rows(rows: list[dict[str, Any]]) -> list[str]:
    return _normalize_codes([row.get("code") for row in rows])


def _normalize_codes(raw_codes: list[Any]) -> list[str]:
    codes: list[str] = []
    seen: set[str] = set()
    for raw_code in raw_codes:
        code = normalize_stock_code(raw_code)
        if code and code not in seen:
            seen.add(code)
            codes.append(code)
    return sorted(codes)


def _apply_code_filters(codes: list[str], filters: dict[str, Any]) -> list[str]:
    include_raw = filters.get("codes") or filters.get("includeCodes")
    exclude_raw = filters.get("excludeCodes")
    filtered = list(codes)
    if isinstance(include_raw, list) and include_raw:
        include = set(_normalize_codes(include_raw))
        filtered = [code for code in filtered if code in include]
    if isinstance(exclude_raw, list) and exclude_raw:
        exclude = set(_normalize_codes(exclude_raw))
        filtered = [code for code in filtered if code not in exclude]
    return sorted(dict.fromkeys(filtered))


def _coverage_warnings(rows: list[dict[str, Any]], *, as_of_date: str, preset: str) -> list[str]:
    if rows:
        return []
    return [
        f"stock_master_daily has no exact rows for preset={preset} as_of_date={as_of_date}; latest fallback was not used"
    ]
