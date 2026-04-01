"""Runner-first helpers for the synthetic risk-adjusted-return playground."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

import numpy as np
import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.strategy.indicators.calculations import compute_risk_adjusted_return

RatioType = Literal["sharpe", "sortino"]
RISK_ADJUSTED_RETURN_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/risk-adjusted-return-playground"
)


@dataclass(frozen=True)
class RiskAdjustedReturnResearchResult:
    seed: int
    n_days: int
    lookback_period: int
    ratio_type: RatioType
    analysis_start_date: str
    analysis_end_date: str
    latest_ratio: float | None
    series_df: pd.DataFrame


def run_risk_adjusted_return_research(
    *,
    lookback_period: int = 60,
    ratio_type: RatioType = "sortino",
    seed: int = 42,
    n_days: int = 504,
) -> RiskAdjustedReturnResearchResult:
    if lookback_period <= 1:
        raise ValueError("lookback_period must be greater than 1")
    if n_days <= lookback_period:
        raise ValueError("n_days must be greater than lookback_period")
    if ratio_type not in ("sharpe", "sortino"):
        raise ValueError("ratio_type must be 'sharpe' or 'sortino'")

    rng = np.random.default_rng(seed)
    returns = rng.normal(loc=0.0005, scale=0.018, size=n_days)
    dates = pd.bdate_range("2022-01-03", periods=n_days)
    close = pd.Series(100.0 * np.cumprod(1.0 + returns), index=dates, name="close")
    ratio = compute_risk_adjusted_return(
        close=close,
        lookback_period=lookback_period,
        ratio_type=ratio_type,
    )
    latest = ratio.dropna().iloc[-1] if not ratio.dropna().empty else None
    series_df = pd.DataFrame(
        {
            "date": dates.strftime("%Y-%m-%d"),
            "close": close.to_numpy(),
            "risk_adjusted_return": ratio.to_numpy(),
        }
    )
    return RiskAdjustedReturnResearchResult(
        seed=seed,
        n_days=n_days,
        lookback_period=lookback_period,
        ratio_type=ratio_type,
        analysis_start_date=str(series_df["date"].iloc[0]),
        analysis_end_date=str(series_df["date"].iloc[-1]),
        latest_ratio=float(latest) if latest is not None else None,
        series_df=series_df,
    )


def write_risk_adjusted_return_research_bundle(
    result: RiskAdjustedReturnResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RISK_ADJUSTED_RETURN_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_risk_adjusted_return_research",
        params={
            "seed": result.seed,
            "n_days": result.n_days,
            "lookback_period": result.lookback_period,
            "ratio_type": result.ratio_type,
        },
        db_path="synthetic://risk-adjusted-return",
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "seed": result.seed,
            "n_days": result.n_days,
            "lookback_period": result.lookback_period,
            "ratio_type": result.ratio_type,
            "analysis_start_date": result.analysis_start_date,
            "analysis_end_date": result.analysis_end_date,
            "latest_ratio": result.latest_ratio,
        },
        result_tables={"series_df": result.series_df},
        summary_markdown=_build_research_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_risk_adjusted_return_research_bundle(
    bundle_path: str | Path,
) -> RiskAdjustedReturnResearchResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return RiskAdjustedReturnResearchResult(
        seed=int(metadata["seed"]),
        n_days=int(metadata["n_days"]),
        lookback_period=int(metadata["lookback_period"]),
        ratio_type=cast(RatioType, metadata["ratio_type"]),
        analysis_start_date=str(metadata["analysis_start_date"]),
        analysis_end_date=str(metadata["analysis_end_date"]),
        latest_ratio=cast(float | None, metadata.get("latest_ratio")),
        series_df=tables["series_df"],
    )


def get_risk_adjusted_return_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        RISK_ADJUSTED_RETURN_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_risk_adjusted_return_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        RISK_ADJUSTED_RETURN_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_research_bundle_summary_markdown(
    result: RiskAdjustedReturnResearchResult,
) -> str:
    latest_ratio = "N/A" if result.latest_ratio is None else f"{result.latest_ratio:.4f}"
    return "\n".join(
        [
            "# Risk Adjusted Return Playground",
            "",
            "## Snapshot",
            "",
            f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
            f"- Seed: `{result.seed}`",
            f"- Days: `{result.n_days}`",
            f"- Lookback period: `{result.lookback_period}`",
            f"- Ratio type: `{result.ratio_type}`",
            "",
            "## Current Read",
            "",
            f"- Latest valid ratio: `{latest_ratio}`",
            "",
            "## Artifact Tables",
            "",
            "- `series_df`",
        ]
    )
