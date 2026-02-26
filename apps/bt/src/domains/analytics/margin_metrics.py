"""Margin analytics domain calculations."""

from __future__ import annotations

from typing import Any

import pandas as pd


def _format_date(idx: Any) -> str:
    return idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)


def compute_margin_long_pressure(
    margin_df: pd.DataFrame,
    volume: pd.Series[float],
    average_period: int = 15,
) -> list[dict[str, Any]]:
    """信用買い圧力: (LongVol - ShortVol) / N-day avg volume."""
    net_margin = margin_df["longMarginVolume"] - margin_df["shortMarginVolume"]
    avg_vol = volume.rolling(average_period).mean()

    records: list[dict[str, Any]] = []
    for idx in net_margin.index:
        if idx not in avg_vol.index:
            continue
        av = avg_vol.get(idx)
        if pd.isna(av) or av == 0:
            continue
        nm = float(net_margin[idx])  # type: ignore[arg-type]
        if pd.isna(nm):
            continue
        lv = float(margin_df.at[idx, "longMarginVolume"])  # type: ignore[arg-type]
        sv = float(margin_df.at[idx, "shortMarginVolume"])  # type: ignore[arg-type]
        records.append(
            {
                "date": _format_date(idx),
                "pressure": round(nm / float(av), 4),
                "longVol": int(lv),
                "shortVol": int(sv),
                "avgVolume": round(float(av), 2),
            }
        )
    return records


def compute_margin_flow_pressure(
    margin_df: pd.DataFrame,
    volume: pd.Series[float],
    average_period: int = 15,
) -> list[dict[str, Any]]:
    """信用フロー圧力: Delta(LongVol - ShortVol) / N-day avg volume."""
    net_margin = margin_df["longMarginVolume"] - margin_df["shortMarginVolume"]
    prev_net_margin = net_margin.shift(1)
    delta = net_margin - prev_net_margin
    avg_vol = volume.rolling(average_period).mean()

    records: list[dict[str, Any]] = []
    for idx in delta.index:
        if idx not in avg_vol.index:
            continue
        av = avg_vol.get(idx)
        d = delta.get(idx)
        if pd.isna(av) or av == 0 or pd.isna(d):
            continue
        prev_val = float(prev_net_margin[idx])  # type: ignore[arg-type]
        records.append(
            {
                "date": _format_date(idx),
                "flowPressure": round(float(d) / float(av), 4),
                "currentNetMargin": int(float(net_margin[idx])),  # type: ignore[arg-type]
                "previousNetMargin": int(prev_val) if not pd.isna(prev_val) else None,
                "avgVolume": round(float(av), 2),
            }
        )
    return records


def compute_margin_turnover_days(
    margin_df: pd.DataFrame,
    volume: pd.Series[float],
    average_period: int = 15,
) -> list[dict[str, Any]]:
    """信用回転日数: LongVol / N-day avg volume."""
    long_vol = margin_df["longMarginVolume"]
    avg_vol = volume.rolling(average_period).mean()

    records: list[dict[str, Any]] = []
    for idx in long_vol.index:
        if idx not in avg_vol.index:
            continue
        av = avg_vol.get(idx)
        lv = long_vol.get(idx)
        if pd.isna(av) or av == 0 or pd.isna(lv):
            continue
        records.append(
            {
                "date": _format_date(idx),
                "turnoverDays": round(float(lv / av), 4),
                "longVol": int(lv),
                "avgVolume": round(float(av), 2),
            }
        )
    return records


def _get_iso_week_key(dt: Any) -> str:
    iso = dt.isocalendar()
    return f"{iso[0]}-{iso[1]:02d}"


def compute_margin_volume_ratio(
    margin_df: pd.DataFrame,
    volume: pd.Series[float],
    average_period: int = 15,
) -> list[dict[str, Any]]:
    """信用残高 / 週間平均出来高 比率."""
    positive_vol = volume[volume > 0]
    if positive_vol.empty:
        return []

    week_keys = positive_vol.index.to_series().apply(_get_iso_week_key)
    weekly_avg_map: dict[str, float] = positive_vol.groupby(week_keys).mean().to_dict()

    records: list[dict[str, Any]] = []
    for idx in margin_df.index:
        week_key = _get_iso_week_key(idx)
        avg_vol = weekly_avg_map.get(week_key)
        if avg_vol is None or avg_vol == 0:
            continue

        lv = float(margin_df.at[idx, "longMarginVolume"])  # type: ignore[arg-type]
        sv = float(margin_df.at[idx, "shortMarginVolume"])  # type: ignore[arg-type]
        if pd.isna(lv) or pd.isna(sv):
            continue
        avg_vol_f = float(avg_vol)
        records.append(
            {
                "date": _format_date(idx),
                "longRatio": round(lv / avg_vol_f, 4),
                "shortRatio": round(sv / avg_vol_f, 4),
                "longVol": int(lv),
                "shortVol": int(sv),
                "weeklyAvgVolume": round(avg_vol_f, 2),
            }
        )
    return records
