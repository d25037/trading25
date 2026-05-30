"""Technical confirmation flags for market ranking items."""

from __future__ import annotations

from dataclasses import dataclass

from src.application.services.ranking_collection_filters import (
    group_ranking_items_by_normalized_code,
)
from src.application.services.ranking_query_helpers import (
    normalized_code_sql,
    prefer_4digit_order_sql,
)
from src.application.services.ranking_response_items import finite_or_none
from src.application.services.ranking_state_flags import (
    ATR20_ACCELERATION_CHANGE_20D_THRESHOLD_PCT,
    ATR20_ACCELERATION_MAX_ATR20_TO_ATR60,
    ATR20_ACCELERATION_TECHNICAL_FLAG,
    SHORT_TERM_OVERHEAT_RETURN_20D_THRESHOLD_PCT,
)
from src.entrypoints.http.schemas.ranking import RankingItem, RankingTechnicalFlag
from src.infrastructure.db.market.market_reader import MarketDbReader


@dataclass(frozen=True)
class RankingTechnicalMetrics:
    technical_flags: tuple[RankingTechnicalFlag, ...]
    recent_return_20d_pct: float | None
    atr20_to_atr60: float | None
    atr20_change_20d_pct: float | None


def enrich_ranking_collections_with_technical_flags(
    reader: MarketDbReader,
    collections: tuple[list[RankingItem], ...],
    *,
    target_date: str,
) -> None:
    items_by_code = group_ranking_items_by_normalized_code(collections)
    if not items_by_code:
        return

    metrics_by_code = load_ranking_technical_metrics(
        reader,
        target_date=target_date,
        codes=tuple(items_by_code.keys()),
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
) -> dict[str, RankingTechnicalMetrics]:
    if not target_date or not codes:
        return {}

    placeholders = ",".join("?" for _ in codes)
    price_code = normalized_code_sql("sd.code")
    price_order = prefer_4digit_order_sql("sd.code")
    rows = reader.query(
        f"""
        WITH raw_prices AS (
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
            WHERE {price_code} IN ({placeholders})
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
                LAG(close, 20) OVER (PARTITION BY code ORDER BY date) AS close_lag_20d
            FROM true_range
        ),
        featured_with_lag AS (
            SELECT
                *,
                LAG(atr20, 20) OVER (PARTITION BY code ORDER BY date) AS atr20_lag_20d
            FROM featured
        )
        SELECT
            code,
            CASE
                WHEN close_lag_20d > 0 THEN (close / close_lag_20d - 1.0) * 100.0
            END AS recent_return_20d_pct,
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
        """,
        (*codes, target_date, target_date),
    )

    metrics_by_code: dict[str, RankingTechnicalMetrics] = {}
    for row in rows:
        recent_return_20d_pct = finite_or_none(row["recent_return_20d_pct"])
        atr20_to_atr60 = finite_or_none(row["atr20_to_atr60"])
        atr20_change_20d_pct = finite_or_none(row["atr20_change_20d_pct"])
        metrics_by_code[str(row["code"])] = RankingTechnicalMetrics(
            technical_flags=classify_technical_flags(
                recent_return_20d_pct=recent_return_20d_pct,
                atr20_to_atr60=atr20_to_atr60,
                atr20_change_20d_pct=atr20_change_20d_pct,
            ),
            recent_return_20d_pct=recent_return_20d_pct,
            atr20_to_atr60=atr20_to_atr60,
            atr20_change_20d_pct=atr20_change_20d_pct,
        )
    return metrics_by_code


def classify_technical_flags(
    *,
    recent_return_20d_pct: float | None,
    atr20_to_atr60: float | None,
    atr20_change_20d_pct: float | None,
) -> tuple[RankingTechnicalFlag, ...]:
    if (
        recent_return_20d_pct is not None
        and recent_return_20d_pct < SHORT_TERM_OVERHEAT_RETURN_20D_THRESHOLD_PCT
        and atr20_change_20d_pct is not None
        and atr20_change_20d_pct >= ATR20_ACCELERATION_CHANGE_20D_THRESHOLD_PCT
        and atr20_to_atr60 is not None
        and atr20_to_atr60 < ATR20_ACCELERATION_MAX_ATR20_TO_ATR60
    ):
        return (ATR20_ACCELERATION_TECHNICAL_FLAG,)
    return ()
