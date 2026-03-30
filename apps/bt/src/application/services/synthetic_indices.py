"""Helpers for synthetic indices derived from the local market snapshot."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.application.services.options_225 import OPTIONS_225_SYNTHETIC_INDEX_CODE
from src.infrastructure.db.market.market_reader import MarketDbReadable

NT_RATIO_SYNTHETIC_INDEX_CODE = "NT_RATIO"
NT_RATIO_SYNTHETIC_INDEX_NAME = "NT倍率"
NT_RATIO_SYNTHETIC_INDEX_NAME_EN = "NT Ratio (Nikkei 225 / TOPIX)"
NT_RATIO_SYNTHETIC_INDEX_CATEGORY = "synthetic"
VI_SYNTHETIC_INDEX_CODE = "N225_VI"
VI_SYNTHETIC_INDEX_NAME = "日経VI"
VI_SYNTHETIC_INDEX_NAME_EN = "Nikkei VI (BaseVol derived)"
VI_SYNTHETIC_INDEX_CATEGORY = "synthetic"


@dataclass(frozen=True)
class ScalarIndexRow:
    date: str
    value: float


NtRatioRow = ScalarIndexRow
ViRow = ScalarIndexRow

_VI_PER_DAY_CTE = """
    WITH per_day AS (
        SELECT
            date,
            COUNT(DISTINCT CASE WHEN base_volatility > 0 THEN CAST(base_volatility AS DOUBLE) END)
                AS positive_value_count,
            MIN(CASE WHEN base_volatility > 0 THEN CAST(base_volatility AS DOUBLE) END)
                AS positive_value
        FROM options_225_data
        GROUP BY date
    )
"""


def _query_scalar_data_start_date(
    reader: MarketDbReadable | None,
    sql: str,
    params: tuple[Any, ...] = (),
) -> str | None:
    if reader is None:
        return None

    try:
        row = reader.query_one(sql, params)
    except Exception:
        return None

    if row is None or row["data_start_date"] is None:
        return None
    return str(row["data_start_date"])


def _query_scalar_rows(
    reader: MarketDbReadable | None,
    sql: str,
    params: tuple[Any, ...] = (),
) -> list[ScalarIndexRow]:
    if reader is None:
        return []

    try:
        rows = reader.query(sql, params)
    except Exception:
        return []

    return [
        ScalarIndexRow(date=str(row["date"]), value=round(float(row["value"]), 6))
        for row in rows
        if row["value"] is not None
    ]


def get_nt_ratio_data_start_date(reader: MarketDbReadable | None) -> str | None:
    """Return the first date where both TOPIX and synthetic Nikkei data exist."""
    return _query_scalar_data_start_date(
        reader,
        """
        SELECT MIN(t.date) AS data_start_date
        FROM topix_data t
        JOIN indices_data nikkei
            ON nikkei.date = t.date
           AND nikkei.code = ?
        WHERE t.close IS NOT NULL
            AND t.close > 0
            AND nikkei.close IS NOT NULL
            AND nikkei.close > 0
        """,
        (OPTIONS_225_SYNTHETIC_INDEX_CODE,),
    )


def get_nt_ratio_rows(reader: MarketDbReadable | None) -> list[NtRatioRow]:
    """Return daily NT ratio rows built from Nikkei 225 UnderPx and TOPIX closes."""
    return _query_scalar_rows(
        reader,
        """
        SELECT
            t.date,
            CAST(nikkei.close AS DOUBLE) / CAST(t.close AS DOUBLE) AS value
        FROM topix_data t
        JOIN indices_data nikkei
            ON nikkei.date = t.date
           AND nikkei.code = ?
        WHERE t.close IS NOT NULL
            AND t.close > 0
            AND nikkei.close IS NOT NULL
            AND nikkei.close > 0
        ORDER BY t.date
        """,
        (OPTIONS_225_SYNTHETIC_INDEX_CODE,),
    )


def get_vi_data_start_date(reader: MarketDbReadable | None) -> str | None:
    """Return the first date where a single positive BaseVol value exists."""
    return _query_scalar_data_start_date(
        reader,
        f"""
        {_VI_PER_DAY_CTE}
        SELECT MIN(date) AS data_start_date
        FROM per_day
        WHERE positive_value_count = 1
          AND positive_value IS NOT NULL
        """,
    )


def get_vi_rows(reader: MarketDbReadable | None) -> list[ViRow]:
    """Return daily VI rows built from a single positive BaseVol value per day."""
    return _query_scalar_rows(
        reader,
        f"""
        {_VI_PER_DAY_CTE}
        SELECT date, positive_value AS value
        FROM per_day
        WHERE positive_value_count = 1
          AND positive_value IS NOT NULL
        ORDER BY date
        """,
    )
