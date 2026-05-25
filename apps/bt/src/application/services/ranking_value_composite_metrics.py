"""Value-composite ranking metric query helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

import pandas as pd

from src.domains.analytics.fundamental_ranking import (
    normalize_period_label,
    to_nullable_float,
)
from src.domains.analytics.value_composite_scoring import (
    VALUE_COMPOSITE_REQUIRED_POSITIVE_COLUMNS,
    VALUE_COMPOSITE_SCORE_COLUMN,
    build_value_composite_score_frame,
)
from src.domains.fundamentals import (
    FundamentalsCalculator,
    market_statement_row_to_jquants_statement,
)
from src.application.services.ranking_daily_queries import get_trading_date_before
from src.application.services.ranking_query_helpers import (
    canonical_market_label,
    equity_code_variants,
    normalize_equity_code,
    normalized_code_sql,
    prefer_4digit_order_sql,
)
from src.application.services.ranking_response_items import (
    build_value_composite_item,
    finite_or_none,
    int_or_none,
    str_or_none,
)
from src.application.services.ranking_statement_selection import (
    latest_actual_fy_disclosed_date,
)
from src.application.services.ranking_value_composite_config import ValueCompositeProfileSpec
from src.entrypoints.http.schemas.ranking import (
    ValueCompositeForwardEpsMode,
    ValueCompositeRankingItem,
)
from src.shared.utils.share_adjustment import ShareAdjustmentEvent
from src.infrastructure.db.market.market_reader import MarketDbReader


def resolve_value_composite_target_date(
    reader: MarketDbReader,
    date: str | None,
) -> str:
    if date:
        return date
    date_row = reader.query_one("SELECT MAX(date) as max_date FROM stock_data")
    if date_row is None or date_row["max_date"] is None:
        raise ValueError("No trading data available in database")
    return str(date_row["max_date"])


def resolve_value_composite_symbol_target_date(
    reader: MarketDbReader,
    code: str,
    target_date: str,
) -> str:
    code_variants = equity_code_variants(code)
    placeholders = ",".join("?" for _ in code_variants)
    row = reader.query_one(
        f"""
        SELECT MAX(date) AS max_date
        FROM stock_data
        WHERE date <= ?
          AND code IN ({placeholders})
        """,
        (target_date, *code_variants),
    )
    if row is None or row["max_date"] is None:
        return target_date
    return str(row["max_date"])


def append_value_composite_profile_metrics(
    frame: pd.DataFrame,
    reader: MarketDbReader,
    *,
    target_date: str,
    profile: ValueCompositeProfileSpec,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    profile_metrics = load_value_composite_profile_metrics(
        reader,
        target_date=target_date,
        codes=[str(code) for code in result["code"].tolist()],
        profile=profile,
    )
    technical_columns = [
        "avg_trading_value_60d_mil_jpy",
        "avg_trading_value_60d_source_sessions",
    ]
    if profile.breakout_window is not None:
        technical_columns.extend(
            [
                f"new_high_{profile.breakout_window}d",
                f"days_since_new_high_{profile.breakout_window}d",
                f"close_to_prior_high_{profile.breakout_window}d_pct",
            ]
        )
    normalized_codes = result["code"].map(normalize_equity_code)
    for column in technical_columns:
        result[column] = normalized_codes.map(
            lambda code, column=column: profile_metrics.get(str(code), {}).get(column)
        )
    return result


def apply_value_composite_profile(
    frame: pd.DataFrame,
    profile: ValueCompositeProfileSpec,
    *,
    apply_liquidity_filter: bool,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    base_score = pd.to_numeric(result[VALUE_COMPOSITE_SCORE_COLUMN], errors="coerce")
    result["score_before_boost"] = base_score
    result["breakout_boost"] = 0.0
    if profile.breakout_window is not None and profile.breakout_lookback_sessions is not None:
        days_column = f"days_since_new_high_{profile.breakout_window}d"
        if days_column in result.columns and profile.breakout_score_boost is not None:
            denominator = max(int(profile.breakout_lookback_sessions), 1)
            days_since = pd.to_numeric(result[days_column], errors="coerce")
            recency = (
                (denominator - days_since.clip(lower=0, upper=denominator))
                / denominator
            ).fillna(0.0)
            result["breakout_boost"] = recency * float(profile.breakout_score_boost)
    result[VALUE_COMPOSITE_SCORE_COLUMN] = base_score + pd.to_numeric(
        result["breakout_boost"],
        errors="coerce",
    ).fillna(0.0)
    if profile.min_adv60_mil_jpy is not None:
        adv60 = pd.to_numeric(result["avg_trading_value_60d_mil_jpy"], errors="coerce")
        result["liquidity_eligible"] = adv60 >= float(profile.min_adv60_mil_jpy)
        if apply_liquidity_filter:
            result = result[result["liquidity_eligible"]].copy()
    return result


def build_value_composite_score_frame_from_adjusted(
    adjusted: pd.DataFrame,
    *,
    weights: Mapping[str, float],
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for row in adjusted.to_dict(orient="records"):
        price = finite_or_none(row.get("current_price"))
        volume = finite_or_none(row.get("volume"))
        pbr = finite_or_none(row.get("pbr"))
        forward_per = finite_or_none(row.get("forward_per"))
        market_cap = finite_or_none(row.get("market_cap"))
        if price is None or price <= 0:
            continue
        records.append(
            {
                "code": str(row["code"]),
                "company_name": str(row["company_name"]),
                "market_code": str(row["market_code"]),
                "market": canonical_market_label(str(row["market_code"])),
                "sector_33_name": str(row["sector_33_name"]),
                "current_price": price,
                "volume": volume if volume is not None else 0.0,
                "pbr": pbr,
                "forward_per": forward_per,
                "market_cap_bil_jpy": (
                    market_cap / 1_000_000_000.0
                    if market_cap is not None
                    else None
                ),
                "bps": finite_or_none(row.get("bps")),
                "forward_eps": finite_or_none(row.get("forward_eps")),
                "latest_fy_disclosed_date": str_or_none(
                    row.get("statement_disclosed_date")
                ),
                "forward_eps_disclosed_date": str_or_none(
                    row.get("forward_eps_disclosed_date")
                ),
                "forward_eps_source": str_or_none(row.get("forward_eps_source")),
            }
        )
    return score_value_composite_records(records, weights=weights)


def score_value_composite_records(
    records: list[dict[str, Any]],
    *,
    weights: Mapping[str, float],
) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    scored = build_value_composite_score_frame(
        pd.DataFrame.from_records(records),
        group_columns=("market",),
        required_positive_columns=VALUE_COMPOSITE_REQUIRED_POSITIVE_COLUMNS,
        score_column=VALUE_COMPOSITE_SCORE_COLUMN,
        weights=weights,
    )
    scored = scored[
        pd.to_numeric(scored[VALUE_COMPOSITE_SCORE_COLUMN], errors="coerce").notna()
    ].copy()
    return scored.sort_values(
        [VALUE_COMPOSITE_SCORE_COLUMN, "code"],
        ascending=[False, True],
        kind="stable",
    ).reset_index(drop=True)


def build_value_composite_score_frame_from_statement_rows(
    stock_rows: list[Mapping[str, Any]],
    statement_rows: list[Mapping[str, Any]],
    *,
    target_date: str,
    price_basis_date: str,
    adjustment_events_by_code: Mapping[str, list[ShareAdjustmentEvent]],
    valuation_calculator: FundamentalsCalculator,
    weights: Mapping[str, float],
    forward_eps_mode: ValueCompositeForwardEpsMode,
) -> pd.DataFrame:
    raw_statements_by_code: dict[str, list[Mapping[str, Any]]] = {}
    for row in statement_rows:
        code = str(row["code"])
        raw_statements_by_code.setdefault(code, []).append(row)

    records: list[dict[str, Any]] = []
    for stock in stock_rows:
        code = str(stock["code"])
        raw_statements = raw_statements_by_code.get(code, [])
        if not raw_statements:
            continue
        price = to_nullable_float(stock["current_price"])
        volume = to_nullable_float(stock["volume"])
        if price is None or price <= 0:
            continue

        valuation_rows = (
            raw_statements
            if forward_eps_mode == "latest"
            else [
                row
                for row in raw_statements
                if normalize_period_label(row["type_of_current_period"]) == "FY"
            ]
        )
        valuation_statements = [
            market_statement_row_to_jquants_statement(row, code_fallback=code)
            for row in valuation_rows
        ]
        valuation = valuation_calculator.calculate_latest_valuation(
            valuation_statements,
            close=price,
            price_date=target_date,
            prefer_consolidated=True,
            share_adjustment_events=adjustment_events_by_code.get(code, []),
            price_basis_date=price_basis_date,
        )
        if valuation is None:
            continue

        pbr = valuation.pbr
        forward_per = valuation.forwardPer
        market_cap_bil_jpy = (
            valuation.marketCap / 1_000_000_000.0
            if valuation.marketCap is not None
            else None
        )
        bps = price / pbr if pbr is not None and pbr > 0 else None

        records.append(
            {
                "code": code,
                "company_name": str(stock["company_name"]),
                "market_code": str(stock["market_code"]),
                "market": canonical_market_label(str(stock["market_code"])),
                "sector_33_name": str(stock["sector_33_name"]),
                "current_price": price,
                "volume": volume if volume is not None else 0.0,
                "pbr": pbr,
                "forward_per": forward_per,
                "market_cap_bil_jpy": market_cap_bil_jpy,
                "bps": bps,
                "forward_eps": valuation.forwardEps,
                "latest_fy_disclosed_date": latest_actual_fy_disclosed_date(
                    raw_statements,
                    as_of_date=target_date,
                ),
                "forward_eps_disclosed_date": valuation.forwardEpsDisclosedDate,
                "forward_eps_source": valuation.forwardEpsSource,
            }
        )

    return score_value_composite_records(records, weights=weights)


def append_value_composite_technical_metrics(
    frame: pd.DataFrame,
    reader: MarketDbReader,
    *,
    target_date: str,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    technical_metrics = load_value_composite_technical_metrics(
        reader,
        target_date=target_date,
        codes=[str(code) for code in result["code"].tolist()],
    )
    technical_columns = (
        "technical_feature_date",
        "breakout_feature_date",
        "rebound_from_252d_low_pct",
        "return_252d_pct",
        "volatility_20d_pct",
        "volatility_60d_pct",
        "downside_volatility_60d_pct",
        "avg_trading_value_60d_mil_jpy",
        "avg_trading_value_60d_source_sessions",
        "new_high_20d",
        "days_since_new_high_20d",
        "close_to_prior_high_20d_pct",
        "new_high_120d",
        "days_since_new_high_120d",
        "close_to_prior_high_120d_pct",
    )
    normalized_codes = result["code"].map(normalize_equity_code)
    for column in technical_columns:
        result[column] = normalized_codes.map(
            lambda code, column=column: technical_metrics.get(str(code), {}).get(column)
        )
    return result


def build_value_composite_ranking_items(
    scored: pd.DataFrame,
    reader: MarketDbReader,
    *,
    target_date: str,
) -> list[ValueCompositeRankingItem]:
    scored = append_value_composite_technical_metrics(
        scored,
        reader,
        target_date=target_date,
    )
    return [
        build_value_composite_item(cast(Mapping[str, Any], row), rank)
        for rank, row in enumerate(scored.to_dict(orient="records"), start=1)
    ]


def find_value_composite_score_item(
    scored: pd.DataFrame,
    reader: MarketDbReader,
    *,
    normalized_target_code: str,
    target_date: str,
) -> tuple[ValueCompositeRankingItem | None, int]:
    rows = scored.to_dict(orient="records")
    for rank, row in enumerate(rows, start=1):
        if normalize_equity_code(row["code"]) != normalized_target_code:
            continue
        row_payload: dict[str, Any] = {str(key): value for key, value in row.items()}
        row_df = append_value_composite_technical_metrics(
            pd.DataFrame.from_records([row_payload]),
            reader,
            target_date=target_date,
        )
        return (
            build_value_composite_item(
                cast(Mapping[str, Any], row_df.iloc[0].to_dict()),
                rank,
            ),
            len(rows),
        )
    return None, len(rows)


def find_value_composite_target_stock(
    stock_rows: list[Mapping[str, Any]],
    code: str,
) -> Mapping[str, Any] | None:
    normalized_target_code = normalize_equity_code(code)
    for row in stock_rows:
        if normalize_equity_code(row["code"]) == normalized_target_code:
            return row
    return None


def load_value_composite_technical_metrics(
    reader: MarketDbReader,
    *,
    target_date: str,
    codes: list[str],
) -> dict[str, dict[str, Any]]:
    normalized_codes = sorted({normalize_equity_code(code) for code in codes})
    if not normalized_codes:
        return {}
    placeholders = ",".join("?" for _ in normalized_codes)
    normalized = normalized_code_sql("code")
    order = prefer_4digit_order_sql("code")
    sql = f"""
        WITH stock_history AS (
            SELECT
                normalized_code,
                date,
                close
            FROM (
                SELECT
                    {normalized} AS normalized_code,
                    date,
                    close,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized}, date
                        ORDER BY {order}
                    ) AS rn
                FROM stock_data
                WHERE date <= ?
                  AND {normalized} IN ({placeholders})
            )
            WHERE rn = 1
        ),
        returns AS (
            SELECT
                normalized_code,
                date,
                close,
                close / NULLIF(LAG(close, 252) OVER (
                    PARTITION BY normalized_code ORDER BY date
                ), 0) - 1 AS return_252d,
                close / NULLIF(MIN(close) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                ), 0) - 1 AS rebound_from_252d_low,
                close / NULLIF(LAG(close) OVER (
                    PARTITION BY normalized_code ORDER BY date
                ), 0) - 1 AS daily_return
            FROM stock_history
        ),
        metrics AS (
            SELECT
                normalized_code,
                date,
                rebound_from_252d_low * 100.0 AS rebound_from_252d_low_pct,
                return_252d * 100.0 AS return_252d_pct,
                STDDEV_SAMP(daily_return) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) * SQRT(252.0) * 100.0 AS volatility_20d_pct,
                COUNT(daily_return) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS volatility_20d_count,
                STDDEV_SAMP(daily_return) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) * SQRT(252.0) * 100.0 AS volatility_60d_pct,
                COUNT(daily_return) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS volatility_60d_count,
                STDDEV_SAMP(CASE WHEN daily_return < 0 THEN daily_return ELSE NULL END) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) * SQRT(252.0) * 100.0 AS downside_volatility_60d_pct,
                COUNT(CASE WHEN daily_return < 0 THEN daily_return ELSE NULL END) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS downside_volatility_60d_count,
                ROW_NUMBER() OVER (
                    PARTITION BY normalized_code ORDER BY date DESC
                ) AS latest_rank
            FROM returns
        ),
        latest_metrics AS (
            SELECT
                normalized_code,
                date AS technical_feature_date,
                rebound_from_252d_low_pct,
                return_252d_pct,
                CASE WHEN volatility_20d_count >= 20 THEN volatility_20d_pct ELSE NULL END AS volatility_20d_pct,
                CASE WHEN volatility_60d_count >= 60 THEN volatility_60d_pct ELSE NULL END AS volatility_60d_pct,
                CASE
                    WHEN downside_volatility_60d_count >= 2 THEN downside_volatility_60d_pct
                    ELSE NULL
                END AS downside_volatility_60d_pct
            FROM metrics
            WHERE latest_rank = 1
        ),
        signal_history AS (
            SELECT
                normalized_code,
                date,
                high,
                close,
                volume,
                close * volume AS trading_value,
                ROW_NUMBER() OVER (
                    PARTITION BY normalized_code ORDER BY date
                ) AS signal_row_number
            FROM (
                SELECT
                    {normalized} AS normalized_code,
                    date,
                    high,
                    close,
                    volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized}, date
                        ORDER BY {order}
                    ) AS rn
                FROM stock_data
                WHERE date < ?
                  AND {normalized} IN ({placeholders})
            )
            WHERE rn = 1
        ),
        signal_metrics AS (
            SELECT
                normalized_code,
                date,
                high,
                close,
                signal_row_number,
                MAX(high) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) AS prior_high_20d,
                MAX(high) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 120 PRECEDING AND 1 PRECEDING
                ) AS prior_high_120d,
                AVG(trading_value) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS avg_trading_value_60d,
                COUNT(trading_value) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS avg_trading_value_60d_source_sessions,
                ROW_NUMBER() OVER (
                    PARTITION BY normalized_code ORDER BY date DESC
                ) AS latest_signal_rank
            FROM signal_history
        ),
        signal_flags AS (
            SELECT
                *,
                prior_high_20d IS NOT NULL AND high > prior_high_20d AS new_high_20d,
                prior_high_120d IS NOT NULL AND high > prior_high_120d AS new_high_120d
            FROM signal_metrics
        ),
        latest_signal AS (
            SELECT *
            FROM signal_flags
            WHERE latest_signal_rank = 1
        ),
        latest_breakout AS (
            SELECT
                normalized_code,
                MAX(CASE WHEN new_high_20d THEN signal_row_number ELSE NULL END)
                    AS latest_new_high_20d_row_number,
                MAX(CASE WHEN new_high_120d THEN signal_row_number ELSE NULL END)
                    AS latest_new_high_120d_row_number
            FROM signal_flags
            GROUP BY normalized_code
        )
        SELECT
            latest_metrics.*,
            latest_signal.date AS breakout_feature_date,
            CASE
                WHEN latest_signal.avg_trading_value_60d_source_sessions >= 60
                THEN latest_signal.avg_trading_value_60d / 1000000.0
                ELSE NULL
            END AS avg_trading_value_60d_mil_jpy,
            latest_signal.avg_trading_value_60d_source_sessions,
            latest_signal.new_high_20d,
            latest_signal.signal_row_number - latest_breakout.latest_new_high_20d_row_number
                AS days_since_new_high_20d,
            CASE
                WHEN latest_signal.prior_high_20d IS NULL OR latest_signal.prior_high_20d = 0
                THEN NULL
                ELSE (latest_signal.close / latest_signal.prior_high_20d - 1.0) * 100.0
            END AS close_to_prior_high_20d_pct,
            latest_signal.new_high_120d,
            latest_signal.signal_row_number - latest_breakout.latest_new_high_120d_row_number
                AS days_since_new_high_120d,
            CASE
                WHEN latest_signal.prior_high_120d IS NULL OR latest_signal.prior_high_120d = 0
                THEN NULL
                ELSE (latest_signal.close / latest_signal.prior_high_120d - 1.0) * 100.0
            END AS close_to_prior_high_120d_pct
        FROM latest_metrics
        LEFT JOIN latest_signal USING (normalized_code)
        LEFT JOIN latest_breakout USING (normalized_code)
    """
    rows = reader.query(
        sql,
        (target_date, *normalized_codes, target_date, *normalized_codes),
    )
    return {
        str(row["normalized_code"]): {
            "technical_feature_date": str_or_none(row["technical_feature_date"]),
            "breakout_feature_date": str_or_none(row["breakout_feature_date"]),
            "rebound_from_252d_low_pct": finite_or_none(
                row["rebound_from_252d_low_pct"]
            ),
            "return_252d_pct": finite_or_none(row["return_252d_pct"]),
            "volatility_20d_pct": finite_or_none(row["volatility_20d_pct"]),
            "volatility_60d_pct": finite_or_none(row["volatility_60d_pct"]),
            "downside_volatility_60d_pct": finite_or_none(
                row["downside_volatility_60d_pct"]
            ),
            "avg_trading_value_60d_mil_jpy": finite_or_none(
                row["avg_trading_value_60d_mil_jpy"]
            ),
            "avg_trading_value_60d_source_sessions": int_or_none(
                row["avg_trading_value_60d_source_sessions"]
            ),
            "new_high_20d": (
                bool(row["new_high_20d"]) if row["new_high_20d"] is not None else None
            ),
            "days_since_new_high_20d": int_or_none(row["days_since_new_high_20d"]),
            "close_to_prior_high_20d_pct": finite_or_none(
                row["close_to_prior_high_20d_pct"]
            ),
            "new_high_120d": (
                bool(row["new_high_120d"])
                if row["new_high_120d"] is not None
                else None
            ),
            "days_since_new_high_120d": int_or_none(row["days_since_new_high_120d"]),
            "close_to_prior_high_120d_pct": finite_or_none(
                row["close_to_prior_high_120d_pct"]
            ),
        }
        for row in rows
    }


def load_value_composite_profile_metrics(
    reader: MarketDbReader,
    *,
    target_date: str,
    codes: list[str],
    profile: ValueCompositeProfileSpec,
) -> dict[str, dict[str, Any]]:
    normalized_codes = sorted({normalize_equity_code(code) for code in codes})
    if not normalized_codes:
        return {}

    placeholders = ",".join("?" for _ in normalized_codes)
    normalized = normalized_code_sql("code")
    order = prefer_4digit_order_sql("code")
    required_session_offset = 0
    if profile.min_adv60_mil_jpy is not None:
        required_session_offset = max(required_session_offset, 59)
    if profile.breakout_window is not None:
        required_session_offset = max(
            required_session_offset,
            int(profile.breakout_window) + int(profile.breakout_lookback_sessions or 0),
        )
    start_date = get_trading_date_before(reader, target_date, required_session_offset)
    lower_bound_clause = " AND date >= ?" if start_date is not None else ""
    params: tuple[Any, ...] = (
        (target_date, start_date, *normalized_codes)
        if start_date is not None
        else (target_date, *normalized_codes)
    )

    if profile.breakout_window is None:
        sql = f"""
            WITH signal_history AS (
                SELECT
                    normalized_code,
                    date,
                    close,
                    volume,
                    close * volume AS trading_value
                FROM (
                    SELECT
                        {normalized} AS normalized_code,
                        date,
                        close,
                        volume,
                        ROW_NUMBER() OVER (
                            PARTITION BY {normalized}, date
                            ORDER BY {order}
                        ) AS rn
                    FROM stock_data
                    WHERE date < ?{lower_bound_clause}
                      AND {normalized} IN ({placeholders})
                )
                WHERE rn = 1
            ),
            signal_metrics AS (
                SELECT
                    normalized_code,
                    date,
                    AVG(trading_value) OVER (
                        PARTITION BY normalized_code
                        ORDER BY date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) AS avg_trading_value_60d,
                    COUNT(trading_value) OVER (
                        PARTITION BY normalized_code
                        ORDER BY date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) AS avg_trading_value_60d_source_sessions,
                    ROW_NUMBER() OVER (
                        PARTITION BY normalized_code ORDER BY date DESC
                    ) AS latest_signal_rank
                FROM signal_history
            )
            SELECT
                normalized_code,
                CASE
                    WHEN avg_trading_value_60d_source_sessions >= 60
                    THEN avg_trading_value_60d / 1000000.0
                    ELSE NULL
                END AS avg_trading_value_60d_mil_jpy,
                avg_trading_value_60d_source_sessions
            FROM signal_metrics
            WHERE latest_signal_rank = 1
        """
        rows = reader.query(sql, params)
        return {
            str(row["normalized_code"]): {
                "avg_trading_value_60d_mil_jpy": finite_or_none(
                    row["avg_trading_value_60d_mil_jpy"]
                ),
                "avg_trading_value_60d_source_sessions": int_or_none(
                    row["avg_trading_value_60d_source_sessions"]
                ),
            }
            for row in rows
        }

    breakout_window = int(profile.breakout_window)
    sql = f"""
        WITH signal_history AS (
            SELECT
                normalized_code,
                date,
                high,
                close,
                volume,
                close * volume AS trading_value,
                ROW_NUMBER() OVER (
                    PARTITION BY normalized_code ORDER BY date
                ) AS signal_row_number
            FROM (
                SELECT
                    {normalized} AS normalized_code,
                    date,
                    high,
                    close,
                    volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized}, date
                        ORDER BY {order}
                    ) AS rn
                FROM stock_data
                WHERE date < ?{lower_bound_clause}
                  AND {normalized} IN ({placeholders})
            )
            WHERE rn = 1
        ),
        signal_metrics AS (
            SELECT
                normalized_code,
                date,
                high,
                close,
                signal_row_number,
                MAX(high) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN {breakout_window} PRECEDING AND 1 PRECEDING
                ) AS prior_high,
                AVG(trading_value) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS avg_trading_value_60d,
                COUNT(trading_value) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS avg_trading_value_60d_source_sessions,
                ROW_NUMBER() OVER (
                    PARTITION BY normalized_code ORDER BY date DESC
                ) AS latest_signal_rank
            FROM signal_history
        ),
        signal_flags AS (
            SELECT
                *,
                prior_high IS NOT NULL AND high > prior_high AS new_high
            FROM signal_metrics
        ),
        latest_signal AS (
            SELECT *
            FROM signal_flags
            WHERE latest_signal_rank = 1
        ),
        latest_breakout AS (
            SELECT
                normalized_code,
                MAX(CASE WHEN new_high THEN signal_row_number ELSE NULL END)
                    AS latest_new_high_row_number
            FROM signal_flags
            GROUP BY normalized_code
        )
        SELECT
            latest_signal.normalized_code,
            CASE
                WHEN latest_signal.avg_trading_value_60d_source_sessions >= 60
                THEN latest_signal.avg_trading_value_60d / 1000000.0
                ELSE NULL
            END AS avg_trading_value_60d_mil_jpy,
            latest_signal.avg_trading_value_60d_source_sessions,
            latest_signal.new_high,
            latest_signal.signal_row_number - latest_breakout.latest_new_high_row_number
                AS days_since_new_high,
            CASE
                WHEN latest_signal.prior_high IS NULL OR latest_signal.prior_high = 0
                THEN NULL
                ELSE (latest_signal.close / latest_signal.prior_high - 1.0) * 100.0
            END AS close_to_prior_high_pct
        FROM latest_signal
        LEFT JOIN latest_breakout USING (normalized_code)
    """
    rows = reader.query(sql, params)
    return {
        str(row["normalized_code"]): {
            "avg_trading_value_60d_mil_jpy": finite_or_none(
                row["avg_trading_value_60d_mil_jpy"]
            ),
            "avg_trading_value_60d_source_sessions": int_or_none(
                row["avg_trading_value_60d_source_sessions"]
            ),
            f"new_high_{breakout_window}d": (
                bool(row["new_high"]) if row["new_high"] is not None else None
            ),
            f"days_since_new_high_{breakout_window}d": int_or_none(
                row["days_since_new_high"]
            ),
            f"close_to_prior_high_{breakout_window}d_pct": finite_or_none(
                row["close_to_prior_high_pct"]
            ),
        }
        for row in rows
    }
