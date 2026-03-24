"""Helpers for synthetic indices derived from the local market snapshot."""

from __future__ import annotations

from dataclasses import dataclass

from src.application.services.options_225 import OPTIONS_225_SYNTHETIC_INDEX_CODE
from src.infrastructure.db.market.market_reader import MarketDbReadable

NT_RATIO_SYNTHETIC_INDEX_CODE = "NT_RATIO"
NT_RATIO_SYNTHETIC_INDEX_NAME = "NT倍率"
NT_RATIO_SYNTHETIC_INDEX_NAME_EN = "NT Ratio (Nikkei 225 / TOPIX)"
NT_RATIO_SYNTHETIC_INDEX_CATEGORY = "synthetic"


@dataclass(frozen=True)
class NtRatioRow:
    date: str
    value: float


def get_nt_ratio_data_start_date(reader: MarketDbReadable | None) -> str | None:
    """Return the first date where both TOPIX and synthetic Nikkei data exist."""
    if reader is None:
        return None

    try:
        row = reader.query_one(
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
    except Exception:
        return None

    if row is None or row["data_start_date"] is None:
        return None
    return str(row["data_start_date"])


def get_nt_ratio_rows(reader: MarketDbReadable | None) -> list[NtRatioRow]:
    """Return daily NT ratio rows built from Nikkei 225 UnderPx and TOPIX closes."""
    if reader is None:
        return []

    try:
        rows = reader.query(
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
    except Exception:
        return []

    return [
        NtRatioRow(date=str(row["date"]), value=round(float(row["value"]), 6))
        for row in rows
        if row["value"] is not None
    ]
