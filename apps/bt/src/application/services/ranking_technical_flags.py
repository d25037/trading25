"""Technical confirmation flags for market ranking items."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date as calendar_date, timedelta

from src.application.contracts import ranking as ranking_contracts
from src.application.services.ranking_collection_filters import (
    group_ranking_items_by_normalized_code,
)
from src.application.services.ranking_query_helpers import (
    build_market_filter,
    normalized_code_sql,
    prefer_4digit_order_sql,
    stocks_canonical_cte,
)
from src.application.services.ranking_response_items import finite_or_none
from src.application.services.ranking_state_flags import (
    ATR20_ACCELERATION_TECHNICAL_FLAG,
    MOMENTUM_20_60_TOP20_TECHNICAL_FLAG,
)
from src.domains.analytics.daily_ranking_core import (
    DailyRankingTechnicalInputs,
    classify_technical_state,
)
from src.infrastructure.db.market.market_reader import MarketDbReader


@dataclass(frozen=True)
class RankingTechnicalMetrics:
    technical_flags: tuple[ranking_contracts.RankingTechnicalFlag, ...]
    recent_return_20d_pct: float | None
    recent_return_60d_pct: float | None
    momentum_20d_percentile: float | None
    momentum_60d_percentile: float | None
    atr20_to_atr60: float | None
    atr20_change_20d_pct: float | None


def enrich_ranking_collections_with_technical_flags(
    reader: MarketDbReader,
    collections: tuple[list[ranking_contracts.RankingItem], ...],
    *,
    target_date: str,
    market_codes: list[str] | None = None,
) -> None:
    items_by_code = group_ranking_items_by_normalized_code(collections)
    if not items_by_code:
        return

    metrics_by_code = load_ranking_technical_metrics(
        reader,
        target_date=target_date,
        codes=tuple(items_by_code.keys()),
        market_codes=market_codes,
    )
    for code, items in items_by_code.items():
        metrics = metrics_by_code.get(code)
        if metrics is None:
            continue
        for item in items:
            item.technicalFlags = list(
                dict.fromkeys([*item.technicalFlags, *metrics.technical_flags])
            )


def load_ranking_technical_metrics(
    reader: MarketDbReader,
    *,
    target_date: str,
    codes: tuple[str, ...],
    market_codes: list[str] | None = None,
) -> dict[str, RankingTechnicalMetrics]:
    if not target_date or not codes:
        return {}

    placeholders = ",".join("?" for _ in codes)
    price_code = normalized_code_sql("sd.code")
    price_order = prefer_4digit_order_sql("sd.code")
    stocks_cte = stocks_canonical_cte()
    market_clause, market_params = build_market_filter(market_codes or [])
    lower_bound_date = _technical_feature_lower_bound_date(target_date)
    rows = reader.query(
        f"""
        WITH
        {stocks_cte},
        market_universe AS (
            SELECT normalized_code AS code
            FROM stocks_canonical s
            WHERE 1 = 1{market_clause}
        ),
        raw_prices AS (
            SELECT
                {price_code} AS code,
                sd.date,
                sd.open,
                sd.high,
                sd.low,
                sd.close,
                ROW_NUMBER() OVER (
                    PARTITION BY {price_code}, sd.date
                    ORDER BY {price_order}
                ) AS rn
            FROM stock_data sd
            JOIN market_universe mu
              ON mu.code = {price_code}
            WHERE sd.date >= ?
              AND sd.date <= ?
              AND sd.open > 0
              AND sd.high > 0
              AND sd.low > 0
              AND sd.close > 0
        ),
        prices AS (
            SELECT code, date, open, high, low, close
            FROM raw_prices
            WHERE rn = 1
        ),
        true_range_base AS (
            SELECT
                *,
                LAG(close) OVER (PARTITION BY code ORDER BY date) AS prev_close
            FROM prices
        ),
        true_range AS (
            SELECT
                *,
                GREATEST(
                    high - low,
                    COALESCE(ABS(high - prev_close), 0.0),
                    COALESCE(ABS(low - prev_close), 0.0)
                ) AS true_range
            FROM true_range_base
        ),
        featured AS (
            SELECT
                *,
                AVG(true_range) OVER (
                    PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS atr20,
                COUNT(true_range) OVER (
                    PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS atr20_sessions,
                AVG(true_range) OVER (
                    PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS atr60,
                COUNT(true_range) OVER (
                    PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS atr60_sessions,
                LAG(close, 20) OVER (PARTITION BY code ORDER BY date) AS close_lag_20d,
                LAG(close, 60) OVER (PARTITION BY code ORDER BY date) AS close_lag_60d
            FROM true_range
        ),
        featured_with_lag AS (
            SELECT
                *,
                LAG(atr20, 20) OVER (PARTITION BY code ORDER BY date) AS atr20_lag_20d
            FROM featured
        ),
        target_features AS (
            SELECT
                code,
                CASE
                    WHEN close_lag_20d > 0 THEN (close / close_lag_20d - 1.0) * 100.0
                END AS recent_return_20d_pct,
                CASE
                    WHEN close_lag_60d > 0 THEN (close / close_lag_60d - 1.0) * 100.0
                END AS recent_return_60d_pct,
                CASE
                    WHEN atr20_sessions >= 20 AND atr60_sessions >= 60 AND atr60 > 0
                        THEN atr20 / atr60
                END AS atr20_to_atr60,
                CASE
                    WHEN atr20_sessions >= 20
                     AND atr60_sessions >= 60
                     AND atr20_lag_20d > 0
                        THEN (atr20 / atr20_lag_20d - 1.0) * 100.0
                END AS atr20_change_20d_pct
            FROM featured_with_lag
            WHERE date = ?
        ),
        ranked AS (
            SELECT
                *,
                CASE
                    WHEN recent_return_20d_pct IS NOT NULL THEN percent_rank() OVER (
                        ORDER BY recent_return_20d_pct NULLS LAST
                    )
                END AS momentum_20d_percentile,
                CASE
                    WHEN recent_return_60d_pct IS NOT NULL THEN percent_rank() OVER (
                        ORDER BY recent_return_60d_pct NULLS LAST
                    )
                END AS momentum_60d_percentile
            FROM target_features
        )
        SELECT
            code,
            recent_return_20d_pct,
            recent_return_60d_pct,
            momentum_20d_percentile,
            momentum_60d_percentile,
            atr20_to_atr60,
            atr20_change_20d_pct
        FROM ranked
        WHERE code IN ({placeholders})
        """,
        (target_date, *market_params, lower_bound_date, target_date, target_date, *codes),
    )

    metrics_by_code: dict[str, RankingTechnicalMetrics] = {}
    for row in rows:
        recent_return_20d_pct = finite_or_none(row["recent_return_20d_pct"])
        recent_return_60d_pct = finite_or_none(row["recent_return_60d_pct"])
        momentum_20d_percentile = finite_or_none(row["momentum_20d_percentile"])
        momentum_60d_percentile = finite_or_none(row["momentum_60d_percentile"])
        atr20_to_atr60 = finite_or_none(row["atr20_to_atr60"])
        atr20_change_20d_pct = finite_or_none(row["atr20_change_20d_pct"])
        metrics_by_code[str(row["code"])] = RankingTechnicalMetrics(
            technical_flags=classify_technical_flags(
                recent_return_20d_pct=recent_return_20d_pct,
                momentum_20d_percentile=momentum_20d_percentile,
                momentum_60d_percentile=momentum_60d_percentile,
                atr20_to_atr60=atr20_to_atr60,
                atr20_change_20d_pct=atr20_change_20d_pct,
            ),
            recent_return_20d_pct=recent_return_20d_pct,
            recent_return_60d_pct=recent_return_60d_pct,
            momentum_20d_percentile=momentum_20d_percentile,
            momentum_60d_percentile=momentum_60d_percentile,
            atr20_to_atr60=atr20_to_atr60,
            atr20_change_20d_pct=atr20_change_20d_pct,
        )
    return metrics_by_code


def _technical_feature_lower_bound_date(target_date: str) -> str:
    try:
        parsed = calendar_date.fromisoformat(target_date)
    except ValueError:
        return "1900-01-01"
    return (parsed - timedelta(days=220)).isoformat()


def classify_technical_flags(
    *,
    recent_return_20d_pct: float | None,
    momentum_20d_percentile: float | None,
    momentum_60d_percentile: float | None,
    atr20_to_atr60: float | None,
    atr20_change_20d_pct: float | None,
) -> tuple[ranking_contracts.RankingTechnicalFlag, ...]:
    state = classify_technical_state(
        DailyRankingTechnicalInputs(
            atr20_change_20d_pct=atr20_change_20d_pct,
            atr20_to_atr60=atr20_to_atr60,
            recent_return_20d_pct=recent_return_20d_pct,
            recent_return_20d_percentile=momentum_20d_percentile,
            recent_return_60d_percentile=momentum_60d_percentile,
        )
    )
    adapters: dict[str, ranking_contracts.RankingTechnicalFlag] = {
        "atr20_acceleration": ATR20_ACCELERATION_TECHNICAL_FLAG,
        "momentum_20_60_top20": MOMENTUM_20_60_TOP20_TECHNICAL_FLAG,
    }
    return tuple(adapters[flag] for flag in state.api_flags)
