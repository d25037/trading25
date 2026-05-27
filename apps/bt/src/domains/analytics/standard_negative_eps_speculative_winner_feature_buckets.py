"""Bucket definitions for standard negative-EPS speculative winner research."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class FeatureSpec:
    identifier: str
    label: str
    bucket_column: str
    bucket_order: tuple[str, ...] | None = None


_FEATURE_SPECS: tuple[FeatureSpec, ...] = (
    FeatureSpec(
        identifier="entry_market_cap_bil_jpy",
        label="Entry market cap (JPY bn)",
        bucket_column="entry_market_cap_bucket",
        bucket_order=("<5b", "5b-20b", "20b-50b", "50b-200b", ">=200b", "missing"),
    ),
    FeatureSpec(
        identifier="entry_adv",
        label="Entry ADV (JPY)",
        bucket_column="entry_adv_bucket",
        bucket_order=("<5m", "5m-20m", "20m-100m", "100m-500m", ">=500m", "missing"),
    ),
    FeatureSpec(
        identifier="entry_open",
        label="Entry open (JPY)",
        bucket_column="entry_open_bucket",
        bucket_order=("<100", "100-300", "300-1000", ">=1000", "missing"),
    ),
    FeatureSpec(
        identifier="prior_252d_return_pct",
        label="Prior 252d return (%)",
        bucket_column="prior_252d_return_bucket",
        bucket_order=("-80% to -50%", "-50% to -20%", ">-20%", "missing"),
    ),
    FeatureSpec(
        identifier="prior_20d_return_pct",
        label="Prior 20d return (%)",
        bucket_column="prior_20d_return_bucket",
        bucket_order=("<=-30%", "-30% to 0%", ">0%", "missing"),
    ),
    FeatureSpec(
        identifier="prior_63d_return_pct",
        label="Prior 63d return (%)",
        bucket_column="prior_63d_return_bucket",
        bucket_order=("<=-50%", "-50% to -10%", ">-10%", "missing"),
    ),
    FeatureSpec(
        identifier="volume_ratio_20d",
        label="Volume ratio 20d",
        bucket_column="volume_ratio_20d_bucket",
        bucket_order=("<0.7", "0.7-1.5", ">1.5", "missing"),
    ),
    FeatureSpec(
        identifier="pre_entry_volatility_20d",
        label="Pre-entry volatility 20d (%)",
        bucket_column="pre_entry_volatility_20d_bucket",
        bucket_order=("low", "mid", "high", "missing"),
    ),
    FeatureSpec(
        identifier="equity_ratio_pct",
        label="Equity ratio (%)",
        bucket_column="equity_ratio_bucket",
        bucket_order=("<30%", "30-50%", ">=50%", "missing"),
    ),
    FeatureSpec(
        identifier="profit_margin_pct",
        label="Profit margin (%)",
        bucket_column="profit_margin_bucket",
        bucket_order=("<=0%", "0-5%", ">5%", "missing"),
    ),
    FeatureSpec(
        identifier="cfo_margin_pct",
        label="CFO margin (%)",
        bucket_column="cfo_margin_bucket",
        bucket_order=("<=0%", "0-10%", ">10%", "missing"),
    ),
    FeatureSpec(
        identifier="sector_33_name",
        label="Sector (33)",
        bucket_column="sector_bucket",
        bucket_order=None,
    ),
)
_FEATURE_SPEC_BY_KEY: dict[str, FeatureSpec] = {
    spec.identifier: spec for spec in _FEATURE_SPECS
}
_FEATURE_ORDER: dict[str, int] = {
    spec.identifier: idx for idx, spec in enumerate(_FEATURE_SPECS)
}

_FEATURE_BUCKET_DEF_COLUMNS = ["feature_key", "bucket_label", "definition"]
_FEATURE_BUCKET_DEFINITIONS: tuple[tuple[str, str, str], ...] = (
    ("entry_market_cap_bil_jpy", "<5b", "entry market cap < 5bn JPY"),
    ("entry_market_cap_bil_jpy", "5b-20b", "5bn <= entry market cap < 20bn JPY"),
    ("entry_market_cap_bil_jpy", "20b-50b", "20bn <= entry market cap < 50bn JPY"),
    ("entry_market_cap_bil_jpy", "50b-200b", "50bn <= entry market cap < 200bn JPY"),
    ("entry_market_cap_bil_jpy", ">=200b", "entry market cap >= 200bn JPY"),
    ("entry_adv", "<5m", "entry ADV < 5m JPY"),
    ("entry_adv", "5m-20m", "5m <= entry ADV < 20m JPY"),
    ("entry_adv", "20m-100m", "20m <= entry ADV < 100m JPY"),
    ("entry_adv", "100m-500m", "100m <= entry ADV < 500m JPY"),
    ("entry_adv", ">=500m", "entry ADV >= 500m JPY"),
    ("entry_open", "<100", "entry open < 100 JPY"),
    ("entry_open", "100-300", "100 <= entry open < 300 JPY"),
    ("entry_open", "300-1000", "300 <= entry open < 1000 JPY"),
    ("entry_open", ">=1000", "entry open >= 1000 JPY"),
    ("prior_252d_return_pct", "-80% to -50%", "prior 252-session return <= -50%"),
    ("prior_252d_return_pct", "-50% to -20%", "-50% < prior 252-session return <= -20%"),
    ("prior_252d_return_pct", ">-20%", "prior 252-session return > -20%"),
    ("prior_20d_return_pct", "<=-30%", "prior 20-session return <= -30%"),
    ("prior_20d_return_pct", "-30% to 0%", "-30% < prior 20-session return <= 0%"),
    ("prior_20d_return_pct", ">0%", "prior 20-session return > 0%"),
    ("prior_63d_return_pct", "<=-50%", "prior 63-session return <= -50%"),
    ("prior_63d_return_pct", "-50% to -10%", "-50% < prior 63-session return <= -10%"),
    ("prior_63d_return_pct", ">-10%", "prior 63-session return > -10%"),
    (
        "volume_ratio_20d",  # gitleaks:allow - feature identifier
        "<0.7",
        "trailing 20-session mean volume / trailing 252-session mean volume < 0.7",
    ),
    (
        "volume_ratio_20d",  # gitleaks:allow - feature identifier
        "0.7-1.5",
        "0.7 <= trailing 20-session mean volume / trailing 252-session mean volume <= 1.5",
    ),
    (
        "volume_ratio_20d",  # gitleaks:allow - feature identifier
        ">1.5",
        "trailing 20-session mean volume / trailing 252-session mean volume > 1.5",
    ),
    ("pre_entry_volatility_20d", "low", "annualized trailing 20-session close volatility < 40%"),
    (
        "pre_entry_volatility_20d",
        "mid",
        "40% <= annualized trailing 20-session close volatility < 80%",
    ),
    (
        "pre_entry_volatility_20d",
        "high",
        "annualized trailing 20-session close volatility >= 80%",
    ),
    ("equity_ratio_pct", "<30%", "equity / total assets < 30%"),
    ("equity_ratio_pct", "30-50%", "30% <= equity / total assets < 50%"),
    ("equity_ratio_pct", ">=50%", "equity / total assets >= 50%"),
    ("profit_margin_pct", "<=0%", "profit / sales <= 0%"),
    ("profit_margin_pct", "0-5%", "0% < profit / sales <= 5%"),
    ("profit_margin_pct", ">5%", "profit / sales > 5%"),
    ("cfo_margin_pct", "<=0%", "operating cash flow / sales <= 0%"),
    ("cfo_margin_pct", "0-10%", "0% < operating cash flow / sales <= 10%"),
    ("cfo_margin_pct", ">10%", "operating cash flow / sales > 10%"),
)
_MISSING_FEATURE_KEYS: tuple[str, ...] = tuple(spec.identifier for spec in _FEATURE_SPECS)


def _to_nullable_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        return None
    return coerced if math.isfinite(coerced) else None


def bucket_entry_market_cap(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric < 5.0:
        return "<5b"
    if numeric < 20.0:
        return "5b-20b"
    if numeric < 50.0:
        return "20b-50b"
    if numeric < 200.0:
        return "50b-200b"
    return ">=200b"


def bucket_entry_adv(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric < 5_000_000.0:
        return "<5m"
    if numeric < 20_000_000.0:
        return "5m-20m"
    if numeric < 100_000_000.0:
        return "20m-100m"
    if numeric < 500_000_000.0:
        return "100m-500m"
    return ">=500m"


def bucket_entry_open(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric < 100.0:
        return "<100"
    if numeric < 300.0:
        return "100-300"
    if numeric < 1000.0:
        return "300-1000"
    return ">=1000"


def bucket_prior_252d_return(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric <= -50.0:
        return "-80% to -50%"
    if numeric <= -20.0:
        return "-50% to -20%"
    return ">-20%"


def bucket_prior_20d_return(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric <= -30.0:
        return "<=-30%"
    if numeric <= 0.0:
        return "-30% to 0%"
    return ">0%"


def bucket_prior_63d_return(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric <= -50.0:
        return "<=-50%"
    if numeric <= -10.0:
        return "-50% to -10%"
    return ">-10%"


def bucket_volume_ratio_20d(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric < 0.7:
        return "<0.7"
    if numeric <= 1.5:
        return "0.7-1.5"
    return ">1.5"


def bucket_pre_entry_volatility_20d(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric < 40.0:
        return "low"
    if numeric < 80.0:
        return "mid"
    return "high"


def bucket_equity_ratio(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric < 30.0:
        return "<30%"
    if numeric < 50.0:
        return "30-50%"
    return ">=50%"


def bucket_profit_margin(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric <= 0.0:
        return "<=0%"
    if numeric <= 5.0:
        return "0-5%"
    return ">5%"


def bucket_cfo_margin(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric <= 0.0:
        return "<=0%"
    if numeric <= 10.0:
        return "0-10%"
    return ">10%"


def bucket_sector_name(value: str | None, *, keep_named: bool) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return "missing"
    if keep_named:
        return normalized
    return "other"


def build_feature_bucket_def_df(
    *,
    sparse_sector_min_event_count: int,
) -> pd.DataFrame:
    rows = [
        {
            "feature_key": feature_key,
            "bucket_label": bucket_label,
            "definition": definition,
        }
        for feature_key, bucket_label, definition in _FEATURE_BUCKET_DEFINITIONS
    ]
    rows.extend(_sector_definition_rows(sparse_sector_min_event_count))
    rows.extend(
        {
            "feature_key": feature_key,
            "bucket_label": "missing",
            "definition": "value unavailable at FY disclosure or before entry",
        }
        for feature_key in _MISSING_FEATURE_KEYS
    )
    return pd.DataFrame(rows, columns=_FEATURE_BUCKET_DEF_COLUMNS)


def _sector_definition_rows(sparse_sector_min_event_count: int) -> list[dict[str, str]]:
    return [
        {
            "feature_key": "sector_33_name",
            "bucket_label": "named sector",
            "definition": (
                "keep sector_33_name when the realized-event count across the study is "
                f">= {sparse_sector_min_event_count}"
            ),
        },
        {
            "feature_key": "sector_33_name",
            "bucket_label": "other",
            "definition": (
                "collapse sector_33_name into other when the realized-event count across the study is "
                f"< {sparse_sector_min_event_count}"
            ),
        },
    ]
