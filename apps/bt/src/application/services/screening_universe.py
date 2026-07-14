"""Universe and scope helpers for market screening."""

from __future__ import annotations

from collections.abc import Callable

from src.application.services.screening_execution import StockUniverseItem, StrategyRuntime
from src.application.services.strategy_dataset_metadata import format_market_scope_label
from src.infrastructure.db.market.market_reader import MarketDbReadable
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.shared.utils.market_code_alias import expand_market_codes

TOPIX500_INDEX_CODE = "TOPIX500"


def load_stock_universe(
    reader: MarketDbReadable,
    market_codes: list[str],
    *,
    as_of_date: str,
    stock_master_daily_has_date: Callable[[str], bool],
) -> list[StockUniverseItem]:
    """市場フィルタ済み銘柄母集団を読み込む。"""
    if not market_codes:
        return []
    if not stock_master_daily_has_date(as_of_date):
        raise ValueError(
            "stock_master_daily snapshot is unavailable for screening "
            f"reference date {as_of_date}; run market DB sync before screening"
        )

    placeholders = ",".join("?" for _ in market_codes)
    rows = reader.query(
        f"""
        SELECT code, company_name, scale_category, sector_33_name
        FROM stock_master_daily
        WHERE date = ? AND market_code IN ({placeholders})
        ORDER BY code
        """,
        (as_of_date, *market_codes),
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
    codes = {normalize_stock_code(str(row["code"])) for row in rows}
    if preset != "primeExTopix500":
        return codes

    try:
        membership_rows = reader.query(
            """
            SELECT code
            FROM index_membership_daily
            WHERE date = ? AND index_code = ?
            ORDER BY code
            """,
            (as_of_date, TOPIX500_INDEX_CODE),
        )
    except Exception as exc:  # noqa: BLE001 - convert storage shape problem to screening input error
        raise ValueError(
            "primeExTopix500 requires exact TOPIX500 membership in index_membership_daily"
        ) from exc
    topix500_codes = {
        normalize_stock_code(str(row["code"]))
        for row in membership_rows
    }
    if not topix500_codes:
        raise ValueError(
            "primeExTopix500 requires exact TOPIX500 membership in index_membership_daily"
        )
    return codes - topix500_codes


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
