"""Universe and scope helpers for market screening."""

from __future__ import annotations

from collections.abc import Callable

from src.application.services.screening_execution import StockUniverseItem, StrategyRuntime
from src.application.services.strategy_dataset_metadata import format_market_scope_label
from src.infrastructure.db.market.market_reader import MarketDbReadable
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.shared.utils.market_code_alias import expand_market_codes

TOPIX500_SCALE_CATEGORIES = ("TOPIX Core30", "TOPIX Large70", "TOPIX Mid400")


def load_stock_universe(
    reader: MarketDbReadable,
    market_codes: list[str],
    *,
    as_of_date: str | None = None,
    get_latest_stock_master_date: Callable[[], str | None],
    get_latest_market_date: Callable[[], str | None],
    stock_master_daily_has_date: Callable[[str], bool],
) -> list[StockUniverseItem]:
    """市場フィルタ済み銘柄母集団を読み込む。"""
    if not market_codes:
        return []

    effective_as_of_date = as_of_date or get_latest_stock_master_date() or get_latest_market_date()
    placeholders = ",".join("?" for _ in market_codes)
    if effective_as_of_date is not None and stock_master_daily_has_date(effective_as_of_date):
        source_table = "stock_master_daily"
        date_clause = "date = ? AND "
        params = (effective_as_of_date, *market_codes)
    else:
        # Legacy/unit-test DBs may not have the v3 daily master yet. Real v3
        # databases resolve PIT universes from stock_master_daily above.
        source_table = "stocks"
        date_clause = ""
        params = tuple(market_codes)
    rows = reader.query(
        f"""
        SELECT code, company_name, scale_category, sector_33_name
        FROM {source_table}
        WHERE {date_clause}market_code IN ({placeholders})
        ORDER BY code
        """,
        params,
    )

    deduped: dict[str, StockUniverseItem] = {}
    for row in rows:
        code = normalize_stock_code(str(row["code"]))
        if code in deduped:
            continue
        deduped[code] = StockUniverseItem(
            code=code,
            company_name=row["company_name"],
            scale_category=row["scale_category"],
            sector_33_name=row["sector_33_name"],
        )

    return list(deduped.values())


def filter_stock_universe_by_codes(
    stock_universe: list[StockUniverseItem],
    allowed_codes: frozenset[str] | None,
) -> list[StockUniverseItem]:
    if not allowed_codes:
        return []
    return [stock for stock in stock_universe if stock.code in allowed_codes]


def collect_dataset_universe_codes(
    strategy_runtimes: list[StrategyRuntime],
) -> frozenset[str] | None:
    union_codes: set[str] = set()
    has_dataset_universe = False
    for strategy in strategy_runtimes:
        if strategy.dataset_universe_codes is None:
            continue
        has_dataset_universe = True
        union_codes.update(strategy.dataset_universe_codes)
    if not has_dataset_universe:
        return None
    return frozenset(union_codes)


def collect_dataset_universe_codes_as_of(
    strategy_runtimes: list[StrategyRuntime],
    *,
    as_of_date: str,
    has_stock_master_daily: Callable[[], bool],
    resolve_universe_codes_from_stock_master: Callable[..., set[str]],
) -> frozenset[str] | None:
    union_codes: set[str] = set()
    has_universe = False
    stock_master_daily_available = has_stock_master_daily()
    for strategy in strategy_runtimes:
        preset = strategy.shared_config.universe_preset
        if preset is not None and stock_master_daily_available:
            codes = resolve_universe_codes_from_stock_master(
                preset=preset,
                as_of_date=as_of_date,
            )
            has_universe = True
            union_codes.update(codes)
            continue
        if strategy.dataset_universe_codes is None:
            continue
        has_universe = True
        union_codes.update(strategy.dataset_universe_codes)
    if not has_universe:
        return None
    return frozenset(union_codes)


def resolve_universe_codes_from_stock_master(
    reader: MarketDbReadable,
    *,
    preset: str,
    as_of_date: str,
) -> set[str]:
    filters: list[str] = ["date = ?"]
    params: list[str] = [as_of_date]
    if preset == "prime":
        market_codes = expand_market_codes(["prime"])
        filters.append(f"market_code IN ({','.join('?' for _ in market_codes)})")
        params.extend(market_codes)
    elif preset == "standard":
        market_codes = expand_market_codes(["standard"])
        filters.append(f"market_code IN ({','.join('?' for _ in market_codes)})")
        params.extend(market_codes)
    elif preset == "growth":
        market_codes = expand_market_codes(["growth"])
        filters.append(f"market_code IN ({','.join('?' for _ in market_codes)})")
        params.extend(market_codes)
    elif preset == "topix100":
        filters.append("coalesce(scale_category, '') IN ('TOPIX Core30', 'TOPIX Large70')")
    elif preset == "primeExTopix500":
        market_codes = expand_market_codes(["prime"])
        filters.append(f"market_code IN ({','.join('?' for _ in market_codes)})")
        params.extend(market_codes)
        filters.append("coalesce(scale_category, '') NOT IN (?, ?, ?)")
        params.extend(TOPIX500_SCALE_CATEGORIES)
    else:
        return set()
    where_clause = " AND ".join(filters)
    rows = reader.query(
        f"""
        SELECT code
        FROM stock_master_daily
        WHERE {where_clause}
        ORDER BY code
        """,
        tuple(params),
    )
    return {normalize_stock_code(str(row["code"])) for row in rows}


def resolve_scope_label(
    *,
    requested_market_codes: list[str],
    strategy_runtimes: list[StrategyRuntime],
    use_strategy_dataset_universe: bool,
) -> str:
    if not use_strategy_dataset_universe:
        return format_market_scope_label(requested_market_codes)

    scope_labels: list[str] = []
    seen_labels: set[str] = set()
    for strategy in strategy_runtimes:
        scope_label = strategy.dataset_scope_label
        if scope_label is None:
            return format_market_scope_label(requested_market_codes)
        if scope_label in seen_labels:
            continue
        scope_labels.append(scope_label)
        seen_labels.add(scope_label)

    if scope_labels:
        return " + ".join(scope_labels)
    return format_market_scope_label(requested_market_codes)
